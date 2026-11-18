#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <string>
#include <thread>
#include <mutex>
#include <unordered_set>
#include <filesystem>
#include <atomic>
#include <cstdlib>
#include <grpcpp/grpcpp.h>
#include "stream.grpc.pb.h"
#include "BoundedQueue.hpp"
#define CPPHTTPLIB_OPENSSL_SUPPORT
#include "httplib.h"

namespace fs = std::filesystem;

struct UploadJob {
    std::string filename;
    std::string sha256_hash;
    fs::path staging_path;
    int producer_id;
};

class MediaConsumerService final : public media::VideoUploadService::Service {
public:
    MediaConsumerService(size_t max_queue_len, int num_consumers)
        : upload_queue_(max_queue_len), job_counter_(0) {
        fs::create_directories("staging");
        fs::create_directories("uploads");
        fs::create_directories("previews");

        for (int i = 0; i < num_consumers; ++i) {
            workers_.emplace_back(&MediaConsumerService::ConsumerWorker, this, i);
        }
    }

    ~MediaConsumerService() {
        upload_queue_.close();
        for (auto& w : workers_) {
            if (w.joinable()) w.join();
        }
    }

    std::vector<std::string> GetCompletedVideos() {
        std::lock_guard<std::mutex> lock(completed_mtx_);
        return completed_videos_;
    }

    grpc::Status UploadVideo(grpc::ServerContext* context,
                             grpc::ServerReader<media::VideoPacket>* reader,
                             media::UploadResponse* response) override {
        media::VideoPacket pkt;
        if (!reader->Read(&pkt) || !pkt.has_metadata()) {
            return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Missing initial metadata packet");
        }

        const auto& meta = pkt.metadata();
        std::string filename = meta.filename();
        std::string hash = meta.sha256_hash();
        int prod_id = meta.producer_id();

        {
            std::lock_guard<std::mutex> lock(hash_mtx_);
            if (!hash.empty() && seen_hashes_.count(hash)) {
                response->set_accepted(false);
                response->set_status_message("Duplicate video rejected");
                return grpc::Status(grpc::StatusCode::ALREADY_EXISTS, "Duplicate video already uploaded");
            }
        }

        uint64_t id = job_counter_.fetch_add(1);
        fs::path staging_file = fs::path("staging") / (std::to_string(id) + "_" + filename);
        std::ofstream out(staging_file, std::ios::binary);
        if (!out) {
            return grpc::Status(grpc::StatusCode::INTERNAL, "Server failed to open staging file");
        }

        while (reader->Read(&pkt)) {
            if (pkt.has_chunk_data()) {
                const std::string& data = pkt.chunk_data();
                out.write(data.data(), static_cast<std::streamsize>(data.size()));
            }
        }
        out.close();

        {
            std::lock_guard<std::mutex> lock(hash_mtx_);
            if (!hash.empty() && seen_hashes_.count(hash)) {
                fs::remove(staging_file);
                response->set_accepted(false);
                response->set_status_message("Duplicate video rejected");
                return grpc::Status(grpc::StatusCode::ALREADY_EXISTS, "Duplicate video already uploaded");
            }

            UploadJob job{filename, hash, staging_file, prod_id};
            if (!upload_queue_.try_push(std::move(job))) {
                fs::remove(staging_file);
                std::cout << "[Queue FULL] Dropped '" << filename << "' from Producer " << prod_id << "\n";
                response->set_accepted(false);
                response->set_status_message("Queue is full; video dropped");
                return grpc::Status(grpc::StatusCode::RESOURCE_EXHAUSTED, "Queue is full (capacity reached)");
            }

            if (!hash.empty()) {
                seen_hashes_.insert(hash);
            }
        }

        std::cout << "[Enqueued] '" << filename << "' from Producer " << prod_id
                  << " (Queue size: " << upload_queue_.size() << ")\n";

        response->set_accepted(true);
        response->set_status_message("Queued for processing");
        return grpc::Status::OK;
    }

private:
    void ConsumerWorker(int worker_id) {
        while (auto job_opt = upload_queue_.pop()) {
            UploadJob job = std::move(*job_opt);

            fs::path final_video = fs::path("uploads") / job.filename;
            fs::path preview_video = fs::path("previews") / ("preview_" + job.filename + ".mp4");

            std::error_code ec;
            fs::rename(job.staging_path, final_video, ec);
            if (ec) {
                fs::copy_file(job.staging_path, final_video, fs::copy_options::overwrite_existing, ec);
                fs::remove(job.staging_path, ec);
            }

            std::string preview_cmd = "ffmpeg -y -hide_banner -loglevel error -i \"" +
                                      final_video.string() +
                                      "\" -t 10 -c:v libx264 -preset ultrafast -an \"" +
                                      preview_video.string() + "\"";
            std::system(preview_cmd.c_str());

            {
                std::lock_guard<std::mutex> lock(completed_mtx_);
                completed_videos_.push_back(job.filename);
            }

            std::cout << "[Consumer " << worker_id << "] Ready in GUI: '" << job.filename << "'\n";
        }
    }

    BoundedQueue<UploadJob> upload_queue_;
    std::vector<std::thread> workers_;
    std::mutex hash_mtx_;
    std::unordered_set<std::string> seen_hashes_;
    std::mutex completed_mtx_;
    std::vector<std::string> completed_videos_;
    std::atomic<uint64_t> job_counter_;
};

void StartHttpsGuiServer(MediaConsumerService* service, int port) {
    httplib::SSLServer svr("cert.pem", "key.pem");
    if (!svr.is_valid()) {
        std::cerr << "[HTTPS GUI] Failed to load cert.pem / key.pem. Ensure they exist in the working directory.\n";
        return;
    }

    svr.Get("/", [](const httplib::Request&, httplib::Response& res) {
        std::ifstream f("index.html");
        if (!f) {
            res.status = 404;
            res.set_content("index.html not found", "text/plain");
            return;
        }
        std::ostringstream ss;
        ss << f.rdbuf();
        res.set_content(ss.str(), "text/html");
    });

    svr.Get("/api/videos", [service](const httplib::Request&, httplib::Response& res) {
        auto list = service->GetCompletedVideos();
        std::ostringstream json;
        json << "[";
        for (size_t i = 0; i < list.size(); ++i) {
            if (i > 0) json << ",";
            json << "\"" << list[i] << "\"";
        }
        json << "]";
        res.set_content(json.str(), "application/json");
    });

    svr.set_mount_point("/uploads", "./uploads");
    svr.set_mount_point("/previews", "./previews");

    std::cout << "HTTPS Consumer Web GUI running at https://localhost:" << port << "\n";
    svr.listen("0.0.0.0", port);
}

int main(int argc, char** argv) {
    int c = 2;
    size_t q = 3;

    if (argc >= 3) {
        c = std::stoi(argv[1]);
        q = static_cast<size_t>(std::stoul(argv[2]));
    } else {
        std::cout << "Enter number of consumer threads (c): ";
        std::cin >> c;
        std::cout << "Enter max queue length (q): ";
        std::cin >> q;
    }

    MediaConsumerService service(q, c);

    std::thread gui_thread(StartHttpsGuiServer, &service, 8443);
    gui_thread.detach();

    std::string server_address("0.0.0.0:50051");
    grpc::ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
    std::cout << "gRPC Consumer listening on " << server_address
              << " (c=" << c << " workers, q=" << q << " max queue)...\n";
    server->Wait();

    return 0;
}