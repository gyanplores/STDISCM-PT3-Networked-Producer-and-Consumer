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
#include <algorithm>
#include <utility>
#include <cstdlib>
#include <cstdio>
#include <cctype>
#include <grpcpp/grpcpp.h>
#include <openssl/evp.h>
#include "stream.grpc.pb.h"
#include "BoundedQueue.hpp"
#define CPPHTTPLIB_OPENSSL_SUPPORT
#include "httplib.h"

namespace fs = std::filesystem;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// The filename comes from the network, so it can't be trusted. This turns it
// into a name that is safe on disk AND safe inside the ffmpeg command line:
//   - any folder part is dropped ("../../x.mp4" -> "x.mp4")
//   - every character except letters, digits, '.', '_' and '-' becomes '_'
//     (so quotes, '$', '&', spaces, slashes can never reach the shell)
// The original name is NOT lost: it is kept separately for the gallery.
std::string MakeSafeName(const std::string& original) {
    std::string base = fs::path(original).filename().string();
    std::string safe;
    safe.reserve(base.size());
    for (unsigned char ch : base) {
        if (std::isalnum(ch) || ch == '.' || ch == '_' || ch == '-') {
            safe.push_back(static_cast<char>(ch));
        } else {
            safe.push_back('_');
        }
    }
    if (safe.size() > 100) safe = safe.substr(safe.size() - 100);  // keep the end so the extension survives
    if (safe.empty()) safe = "video";
    return safe;
}

// Incremental SHA-256 (OpenSSL). The server feeds it every chunk as the chunk
// is written to disk, so the whole file never has to be read a second time.
class Sha256 {
public:
    Sha256() : ctx_(EVP_MD_CTX_new()) { EVP_DigestInit_ex(ctx_, EVP_sha256(), nullptr); }
    ~Sha256() { EVP_MD_CTX_free(ctx_); }
    Sha256(const Sha256&) = delete;
    Sha256& operator=(const Sha256&) = delete;

    void Update(const char* data, size_t len) { EVP_DigestUpdate(ctx_, data, len); }

    // Returns the hash as lowercase hex (the same format the producer sends).
    std::string Finish() {
        unsigned char md[EVP_MAX_MD_SIZE];
        unsigned int len = 0;
        EVP_DigestFinal_ex(ctx_, md, &len);
        std::string hex;
        char buf[3];
        for (unsigned int i = 0; i < len; ++i) {
            std::snprintf(buf, sizeof(buf), "%02x", md[i]);
            hex += buf;
        }
        return hex;
    }

private:
    EVP_MD_CTX* ctx_;
};

// Makes a string safe to place inside a JSON "..." value. Without this, a
// filename containing a quote or backslash would break the gallery's video list.
std::string JsonEscape(const std::string& s) {
    std::string out;
    for (unsigned char c : s) {
        switch (c) {
            case '"':  out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\n': out += "\\n"; break;
            case '\r': out += "\\r"; break;
            case '\t': out += "\\t"; break;
            default:
                if (c < 0x20) {
                    char buf[8];
                    std::snprintf(buf, sizeof(buf), "\\u%04x", c);
                    out += buf;
                } else {
                    out += static_cast<char>(c);
                }
        }
    }
    return out;
}

// True only if `text` is a whole number between lo and hi (inclusive).
// std::stoi alone would crash on "abc" and silently accept "5abc".
bool ParseIntInRange(const std::string& text, int lo, int hi, int& out) {
    try {
        size_t used = 0;
        long value = std::stol(text, &used);
        if (used != text.size() || value < lo || value > hi) return false;
        out = static_cast<int>(value);
        return true;
    } catch (...) {
        return false;
    }
}

// The consumer needs ffmpeg for previews. Check once at startup so a missing
// ffmpeg is a loud warning instead of a mystery later.
bool FfmpegAvailable() {
#ifdef _WIN32
    return std::system("ffmpeg -version >NUL 2>&1") == 0;
#else
    return std::system("ffmpeg -version >/dev/null 2>&1") == 0;
#endif
}

// ---------------------------------------------------------------------------
// Consumer service
// ---------------------------------------------------------------------------

struct UploadJob {
    std::string display_name;  // the filename as the producer sent it (shown in the gallery)
    std::string stored_name;   // "<id>_<safe name>": the real file name on disk, unique per upload
    std::string sha256_hash;
    fs::path staging_path;
    int producer_id;
};

struct CompletedVideo {
    std::string stored_name;
    std::string display_name;
    bool has_preview;
};

// Holds one claimed queue place for the length of an upload. If the function
// returns early for ANY reason (corrupted, cancelled, duplicate, error), the
// destructor gives the place back automatically. Only commit() keeps it.
class QueueReservation {
public:
    explicit QueueReservation(BoundedQueue<UploadJob>& q) : q_(q), active_(true) {}
    ~QueueReservation() { if (active_) q_.cancel_reservation(); }
    QueueReservation(const QueueReservation&) = delete;
    QueueReservation& operator=(const QueueReservation&) = delete;

    void commit(UploadJob job) {
        active_ = false;
        q_.commit(std::move(job));
    }

private:
    BoundedQueue<UploadJob>& q_;
    bool active_;
};

class MediaConsumerService final : public media::VideoUploadService::Service {
public:
    MediaConsumerService(size_t max_queue_len, int num_consumers)
        : upload_queue_(max_queue_len), job_counter_(0) {
        fs::create_directories("staging");
        fs::create_directories("uploads");
        fs::create_directories("previews");
        LoadExistingUploads();

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

    std::vector<CompletedVideo> GetCompletedVideos() {
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

        // Copy everything out of the metadata NOW: `pkt` is reused for every
        // chunk below, which would invalidate a reference into it.
        const auto& meta = pkt.metadata();
        std::string display_name = meta.filename();
        std::string hash = meta.sha256_hash();
        int prod_id = meta.producer_id();

        // Quick duplicate check before receiving any bytes.
        {
            std::lock_guard<std::mutex> lock(hash_mtx_);
            if (!hash.empty() && seen_hashes_.count(hash)) {
                response->set_accepted(false);
                response->set_status_message("Duplicate video rejected");
                return grpc::Status(grpc::StatusCode::ALREADY_EXISTS, "Duplicate video already uploaded");
            }
        }

        // Claim a queue place NOW, before any video bytes are received. If the
        // queue is full we answer immediately: the producer finds out at once
        // and nothing is uploaded and then thrown away. (Before, the server
        // received the whole video first and only then noticed the queue was full.)
        if (!upload_queue_.try_reserve()) {
            std::cout << "[Queue FULL] Rejected '" << display_name << "' from Producer " << prod_id
                      << " before receiving it\n";
            response->set_accepted(false);
            response->set_status_message("Queue is full; video dropped");
            return grpc::Status(grpc::StatusCode::RESOURCE_EXHAUSTED, "Queue is full (capacity reached)");
        }
        QueueReservation reservation(upload_queue_);  // gives the place back on any early return

        // Every upload gets a unique id, so two videos with the same filename
        // can never overwrite each other. The id is part of the stored name.
        uint64_t id = job_counter_.fetch_add(1);
        std::string stored_name = std::to_string(id) + "_" + MakeSafeName(display_name);
        fs::path staging_file = fs::path("staging") / stored_name;
        std::ofstream out(staging_file, std::ios::binary);
        if (!out) {
            return grpc::Status(grpc::StatusCode::INTERNAL, "Server failed to open staging file");
        }

        // Receive the chunks, hashing them while writing.
        Sha256 hasher;
        while (reader->Read(&pkt)) {
            if (pkt.has_chunk_data()) {
                const std::string& data = pkt.chunk_data();
                out.write(data.data(), static_cast<std::streamsize>(data.size()));
                hasher.Update(data.data(), data.size());
            }
        }
        out.flush();
        bool write_ok = static_cast<bool>(out);  // false if the disk write failed (e.g. disk full)
        out.close();

        std::error_code ec;

        // Check 1: did the producer disappear mid-upload? The loop above ends
        // the same way for "finished" and "connection died", so ask gRPC.
        if (context->IsCancelled()) {
            fs::remove(staging_file, ec);
            std::cout << "[Cancelled] Upload of '" << display_name << "' from Producer " << prod_id
                      << " was interrupted; partial file deleted\n";
            return grpc::Status(grpc::StatusCode::CANCELLED, "Upload cancelled before it finished");
        }

        if (!write_ok) {
            fs::remove(staging_file, ec);
            return grpc::Status(grpc::StatusCode::INTERNAL, "Server failed while writing the upload to disk");
        }

        // Check 2: integrity. The hash of the bytes we received must equal the
        // hash the producer computed from the file it sent. A mismatch means
        // the file was corrupted or cut short, so it is thrown away.
        // (An empty hash means the producer sent none: no check is possible.)
        if (!hash.empty() && hasher.Finish() != hash) {
            fs::remove(staging_file, ec);
            std::cout << "[Corrupted] Hash mismatch for '" << display_name << "' from Producer " << prod_id
                      << "; upload rejected\n";
            response->set_accepted(false);
            response->set_status_message("Hash mismatch; upload rejected");
            return grpc::Status(grpc::StatusCode::DATA_LOSS, "Hash mismatch: upload corrupted or incomplete");
        }

        {
            // The duplicate check is repeated here because another producer may
            // have uploaded the same video while this one was in transit. The
            // hash is trustworthy now (it was just verified above).
            std::lock_guard<std::mutex> lock(hash_mtx_);
            if (!hash.empty() && seen_hashes_.count(hash)) {
                fs::remove(staging_file, ec);
                response->set_accepted(false);
                response->set_status_message("Duplicate video rejected");
                return grpc::Status(grpc::StatusCode::ALREADY_EXISTS, "Duplicate video already uploaded");
            }

            // The place was claimed at the start, so this cannot fail.
            reservation.commit(UploadJob{display_name, stored_name, hash, staging_file, prod_id});

            if (!hash.empty()) {
                seen_hashes_.insert(hash);
            }
        }

        std::cout << "[Enqueued] '" << display_name << "' from Producer " << prod_id
                  << " (Queue size: " << upload_queue_.size() << ")\n";

        response->set_accepted(true);
        response->set_status_message("Queued for processing");
        return grpc::Status::OK;
    }

private:
    // Runs once at startup. Videos saved by an earlier run are still in uploads/,
    // so (1) the new id counter must start AFTER the highest id already used,
    // otherwise a new upload could get the name "0_x.mp4" again and overwrite an
    // old video, and (2) those videos are put back in the gallery.
    // The original filename is not stored anywhere, so the gallery shows the
    // saved name without its "<id>_" prefix.
    void LoadExistingUploads() {
        std::vector<std::pair<uint64_t, CompletedVideo>> found;
        uint64_t next_id = 0;
        std::error_code ec;
        for (const auto& entry : fs::directory_iterator("uploads", ec)) {
            if (!entry.is_regular_file()) continue;
            std::string name = entry.path().filename().string();
            size_t us = name.find('_');
            if (us == std::string::npos || us == 0) continue;
            uint64_t id = 0;
            try {
                size_t used = 0;
                id = std::stoull(name.substr(0, us), &used);
                if (used != us) continue;
            } catch (...) { continue; }
            bool has_preview = fs::exists(fs::path("previews") / ("preview_" + name + ".mp4"));
            found.push_back({id, CompletedVideo{name, name.substr(us + 1), has_preview}});
            if (id + 1 > next_id) next_id = id + 1;
        }
        std::sort(found.begin(), found.end(),
                  [](const auto& a, const auto& b) { return a.first < b.first; });
        for (auto& f : found) completed_videos_.push_back(f.second);
        job_counter_ = next_id;
        if (!found.empty()) {
            std::cout << "Found " << found.size() << " video(s) from an earlier run; next id is " << next_id << "\n";
        }
    }

    void ConsumerWorker(int worker_id) {
        while (auto job_opt = upload_queue_.pop()) {
            UploadJob job = std::move(*job_opt);

            fs::path final_video = fs::path("uploads") / job.stored_name;
            fs::path preview_video = fs::path("previews") / ("preview_" + job.stored_name + ".mp4");

            std::error_code ec;
            fs::rename(job.staging_path, final_video, ec);
            if (ec) {
                // e.g. staging/ and uploads/ are on different drives: copy instead of rename
                std::error_code copy_ec, rm_ec;
                fs::copy_file(job.staging_path, final_video, fs::copy_options::overwrite_existing, copy_ec);
                fs::remove(job.staging_path, rm_ec);
                if (copy_ec) {
                    std::cout << "[Consumer " << worker_id << "] ERROR: could not save '" << job.display_name
                              << "': " << copy_ec.message() << "\n";
                    continue;
                }
            }

            // stored_name only contains letters, digits, '.', '_' and '-' (see
            // MakeSafeName), so putting it inside this command line is safe.
            std::string preview_cmd = "ffmpeg -nostdin -y -hide_banner -loglevel error -i \"" +
                                      final_video.string() +
                                      "\" -t 10 -c:v libx264 -preset ultrafast -an \"" +
                                      preview_video.string() + "\"";
            bool preview_ok = (std::system(preview_cmd.c_str()) == 0);
            if (!preview_ok) {
                // The video is still saved and still listed, just without a preview.
                std::cout << "[Consumer " << worker_id << "] WARNING: ffmpeg could not make a preview for '"
                          << job.display_name << "'\n";
            }

            {
                std::lock_guard<std::mutex> lock(completed_mtx_);
                completed_videos_.push_back({job.stored_name, job.display_name, preview_ok});
            }

            std::cout << "[Consumer " << worker_id << "] Ready in GUI: '" << job.display_name << "'\n";
        }
    }

    BoundedQueue<UploadJob> upload_queue_;
    std::vector<std::thread> workers_;
    std::mutex hash_mtx_;
    std::unordered_set<std::string> seen_hashes_;
    std::mutex completed_mtx_;
    std::vector<CompletedVideo> completed_videos_;
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

    // The gallery asks for this list every 2 seconds. Each entry has:
    //   file    : the unique stored name (used to build the video and preview URLs)
    //   name    : the original filename (shown to the user)
    //   preview : whether a 10-second preview exists
    svr.Get("/api/videos", [service](const httplib::Request&, httplib::Response& res) {
        auto list = service->GetCompletedVideos();
        std::ostringstream json;
        json << "[";
        for (size_t i = 0; i < list.size(); ++i) {
            if (i > 0) json << ",";
            json << "{\"file\":\"" << JsonEscape(list[i].stored_name)
                 << "\",\"name\":\"" << JsonEscape(list[i].display_name)
                 << "\",\"preview\":" << (list[i].has_preview ? "true" : "false") << "}";
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
    // c = consumer (worker) threads, q = max queue length.
    // Given on the command line ("server 2 3") or typed in when asked.
    std::string c_text, q_text;
    if (argc >= 3) {
        c_text = argv[1];
        q_text = argv[2];
    } else {
        std::cout << "Enter number of consumer threads (c): ";
        std::cin >> c_text;
        std::cout << "Enter max queue length (q): ";
        std::cin >> q_text;
    }

    // Reject bad input here with a clear message. c = 0 would start no workers
    // (the queue would fill up and then drop everything) and q = 0 would drop
    // every video.
    int c = 0, q_int = 0;
    if (!ParseIntInRange(c_text, 1, 1000, c)) {
        std::cerr << "error: c (consumer threads) must be a whole number from 1 to 1000, got '" << c_text << "'\n"
                  << "usage: server <c> <q>\n";
        return 1;
    }
    if (!ParseIntInRange(q_text, 1, 100000, q_int)) {
        std::cerr << "error: q (max queue length) must be a whole number from 1 to 100000, got '" << q_text << "'\n"
                  << "usage: server <c> <q>\n";
        return 1;
    }
    size_t q = static_cast<size_t>(q_int);

    if (!FfmpegAvailable()) {
        std::cerr << "WARNING: ffmpeg was not found on PATH. Videos will still be saved, "
                     "but no 10-second previews can be made.\n";
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