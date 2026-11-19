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
#include <ctime>
#include <cstdint>
#include <cstdlib>
#include <cstdio>
#include <cctype>
#include <grpcpp/grpcpp.h>
#include <openssl/evp.h>
#include "stream.grpc.pb.h"
#include "BoundedQueue.hpp"
#include "config.hpp"
#define CPPHTTPLIB_OPENSSL_SUPPORT
#include "httplib.h"

namespace fs = std::filesystem;
using json = nlohmann::json;

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
    // Call it once, after the last Update.
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

// The consumer needs ffmpeg for previews. Check once at startup so a missing
// ffmpeg is a loud warning instead of a mystery later.
bool FfmpegAvailable() {
#ifdef _WIN32
    return std::system("ffmpeg -version >NUL 2>&1") == 0;
#else
    return std::system("ffmpeg -version >/dev/null 2>&1") == 0;
#endif
}

// Length of a video in seconds, read with ffprobe (it comes with ffmpeg).
// Returns 0 if ffprobe is missing or cannot read the file.
double ProbeDurationSeconds(const fs::path& file) {
    std::string cmd = "ffprobe -v error -show_entries format=duration -of csv=p=0 \"" + file.string() + "\"";
#ifdef _WIN32
    cmd += " 2>NUL";
    FILE* pipe = _popen(cmd.c_str(), "r");
#else
    cmd += " 2>/dev/null";
    FILE* pipe = popen(cmd.c_str(), "r");
#endif
    if (!pipe) return 0.0;
    char line[128] = {0};
    double seconds = 0.0;
    if (std::fgets(line, sizeof(line), pipe)) seconds = std::atof(line);
#ifdef _WIN32
    _pclose(pipe);
#else
    pclose(pipe);
#endif
    return seconds > 0.0 ? seconds : 0.0;
}

// ---------------------------------------------------------------------------
// Data kept about every upload
// ---------------------------------------------------------------------------

struct UploadJob {
    std::string display_name;     // the filename as the producer sent it (shown in the gallery)
    std::string stored_name;      // "<id>_<safe name>": the real file name on disk, unique per upload
    std::string declared_hash;    // SHA-256 the PRODUCER said the file has
    std::string computed_hash;    // SHA-256 the CONSUMER measured on the bytes it received
    int64_t declared_size;        // size the producer said
    int64_t received_size;        // bytes the consumer actually received
    int64_t uploaded_at;          // when the upload finished (seconds since 1970)
    fs::path staging_path;
    int producer_id;
};

// Everything the gallery shows about one finished video. This is also what is
// saved in meta/<stored_name>.json, so it survives a server restart.
struct VideoInfo {
    std::string stored_name;
    std::string display_name;
    std::string declared_sha256;
    std::string computed_sha256;
    int64_t declared_size = 0;
    int64_t received_size = 0;
    int64_t uploaded_at = 0;
    int producer_id = -1;
    double duration = 0.0;
    bool has_preview = false;
    bool verified = false;        // producer's and consumer's size + SHA-256 are identical
};

json VideoToJson(const VideoInfo& v) {
    return json{
        {"stored_name", v.stored_name},     {"display_name", v.display_name},
        {"declared_sha256", v.declared_sha256}, {"computed_sha256", v.computed_sha256},
        {"declared_size", v.declared_size}, {"received_size", v.received_size},
        {"uploaded_at", v.uploaded_at},     {"producer_id", v.producer_id},
        {"duration", v.duration},           {"has_preview", v.has_preview},
        {"verified", v.verified}};
}

VideoInfo VideoFromJson(const json& j) {
    VideoInfo v;
    v.stored_name = j.value("stored_name", "");
    v.display_name = j.value("display_name", "");
    v.declared_sha256 = j.value("declared_sha256", "");
    v.computed_sha256 = j.value("computed_sha256", "");
    v.declared_size = j.value("declared_size", static_cast<int64_t>(0));
    v.received_size = j.value("received_size", static_cast<int64_t>(0));
    v.uploaded_at = j.value("uploaded_at", static_cast<int64_t>(0));
    v.producer_id = j.value("producer_id", -1);
    v.duration = j.value("duration", 0.0);
    v.has_preview = j.value("has_preview", false);
    v.verified = j.value("verified", false);
    return v;
}

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

// ---------------------------------------------------------------------------
// Consumer service
// ---------------------------------------------------------------------------

class MediaConsumerService final : public media::VideoUploadService::Service {
public:
    MediaConsumerService(size_t max_queue_len, int num_consumers)
        : upload_queue_(max_queue_len), job_counter_(0) {
        fs::create_directories("staging");
        fs::create_directories("uploads");
        fs::create_directories("previews");
        fs::create_directories("meta");
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

    std::vector<VideoInfo> GetCompletedVideos() {
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
        int64_t declared_size = meta.file_size();   // 0 if the producer did not send a size
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
        // and nothing is uploaded and then thrown away.
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

        // Receive the chunks, hashing and counting them while writing.
        Sha256 hasher;
        int64_t received_size = 0;
        while (reader->Read(&pkt)) {
            if (pkt.has_chunk_data()) {
                const std::string& data = pkt.chunk_data();
                out.write(data.data(), static_cast<std::streamsize>(data.size()));
                hasher.Update(data.data(), data.size());
                received_size += static_cast<int64_t>(data.size());
            }
        }
        out.flush();
        bool write_ok = static_cast<bool>(out);  // false if the disk write failed (e.g. disk full)
        out.close();
        std::string computed_hash = hasher.Finish();

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

        // Check 2: integrity. The size and SHA-256 of the bytes we received
        // must equal what the producer said it sent. A mismatch means the file
        // was corrupted or cut short, so it is thrown away.
        // (A missing size or hash means the producer sent none: no check possible.)
        bool size_ok = (declared_size == 0 || declared_size == received_size);
        bool hash_ok = (hash.empty() || hash == computed_hash);
        if (!size_ok || !hash_ok) {
            fs::remove(staging_file, ec);
            std::cout << "[Corrupted] '" << display_name << "' from Producer " << prod_id << " rejected: "
                      << "producer size=" << declared_size << " sha256=" << hash << " | "
                      << "consumer size=" << received_size << " sha256=" << computed_hash << "\n";
            response->set_accepted(false);
            response->set_status_message(!size_ok ? "Size mismatch; upload rejected" : "Hash mismatch; upload rejected");
            return grpc::Status(grpc::StatusCode::DATA_LOSS,
                                !size_ok ? "Size mismatch: upload corrupted or incomplete"
                                         : "Hash mismatch: upload corrupted or incomplete");
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
            reservation.commit(UploadJob{display_name, stored_name, hash, computed_hash,
                                         declared_size, received_size,
                                         static_cast<int64_t>(std::time(nullptr)),
                                         staging_file, prod_id});

            if (!hash.empty()) {
                seen_hashes_.insert(hash);
            }
        }

        if (!hash.empty()) {
            std::cout << "[Verified] '" << display_name << "' from Producer " << prod_id
                      << ": size=" << received_size << " bytes, sha256=" << computed_hash
                      << " (identical to what the producer sent)\n";
        }
        std::cout << "[Enqueued] '" << display_name << "' from Producer " << prod_id
                  << " (Queue size: " << upload_queue_.size() << ")\n";

        // Tell the producer what we measured, so it can compare with its own values.
        response->set_accepted(true);
        response->set_status_message("Queued for processing");
        response->set_received_sha256(computed_hash);
        response->set_received_size(received_size);
        return grpc::Status::OK;
    }

private:
    // Runs once at startup. Videos saved by an earlier run are still in uploads/,
    // so (1) the new id counter must start AFTER the highest id already used,
    // otherwise a new upload could get the name "0_x.mp4" again and overwrite an
    // old video, (2) those videos are put back in the gallery, and (3) their
    // hashes go back into the duplicate list.
    // Details come from meta/<stored_name>.json. If that file is missing (a video
    // from before this feature) the gallery shows the saved name and no hashes.
    void LoadExistingUploads() {
        std::vector<std::pair<uint64_t, VideoInfo>> found;
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

            VideoInfo info;
            std::ifstream meta_file(fs::path("meta") / (name + ".json"));
            try {
                if (meta_file) info = VideoFromJson(json::parse(meta_file));
            } catch (...) { info = VideoInfo(); }   // unreadable sidecar: fall back below
            if (info.stored_name.empty()) {
                std::error_code size_ec;
                info.stored_name = name;
                info.display_name = name.substr(us + 1);
                info.received_size = static_cast<int64_t>(fs::file_size(entry.path(), size_ec));
            }
            info.has_preview = fs::exists(fs::path("previews") / ("preview_" + name + ".mp4"));

            if (!info.declared_sha256.empty()) seen_hashes_.insert(info.declared_sha256);
            found.push_back({id, info});
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

            VideoInfo info;
            info.stored_name = job.stored_name;
            info.display_name = job.display_name;
            info.declared_sha256 = job.declared_hash;
            info.computed_sha256 = job.computed_hash;
            info.declared_size = job.declared_size;
            info.received_size = job.received_size;
            info.uploaded_at = job.uploaded_at;
            info.producer_id = job.producer_id;
            info.duration = ProbeDurationSeconds(final_video);
            info.has_preview = preview_ok;
            // "verified" = the producer sent a hash and it equals the one we measured
            // (a mismatch would already have been rejected in UploadVideo).
            info.verified = !job.declared_hash.empty() && job.declared_hash == job.computed_hash &&
                            (job.declared_size == 0 || job.declared_size == job.received_size);

            // Save the details next to the video so they survive a restart.
            {
                std::ofstream meta_out(fs::path("meta") / (job.stored_name + ".json"));
                meta_out << VideoToJson(info).dump(2);
            }

            {
                std::lock_guard<std::mutex> lock(completed_mtx_);
                completed_videos_.push_back(info);
            }

            std::cout << "[Consumer " << worker_id << "] Ready in GUI: '" << job.display_name << "'\n";
        }
    }

    BoundedQueue<UploadJob> upload_queue_;
    std::vector<std::thread> workers_;
    std::mutex hash_mtx_;
    std::unordered_set<std::string> seen_hashes_;
    std::mutex completed_mtx_;
    std::vector<VideoInfo> completed_videos_;
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

    // The gallery asks for this list every 2 seconds. One entry per video:
    //   file / name       unique stored name (for URLs) / original filename (shown)
    //   preview           whether a 10-second preview exists
    //   producer          which producer sent it
    //   size              bytes the consumer received; declared_size = what the producer said
    //   sha256            hash the producer sent;  computed_sha256 = hash the consumer measured
    //   verified          true when producer and consumer values are identical
    //   uploaded_at       seconds since 1970;  duration = video length in seconds (0 = unknown)
    svr.Get("/api/videos", [service](const httplib::Request&, httplib::Response& res) {
        json list = json::array();
        for (const auto& v : service->GetCompletedVideos()) {
            list.push_back({{"file", v.stored_name},
                            {"name", v.display_name},
                            {"preview", v.has_preview},
                            {"producer", v.producer_id},
                            {"size", v.received_size},
                            {"declared_size", v.declared_size},
                            {"sha256", v.declared_sha256},
                            {"computed_sha256", v.computed_sha256},
                            {"verified", v.verified},
                            {"uploaded_at", v.uploaded_at},
                            {"duration", v.duration}});
        }
        res.set_content(list.dump(), "application/json");
    });

    svr.set_mount_point("/uploads", "./uploads");
    svr.set_mount_point("/previews", "./previews");

    std::cout << "HTTPS Consumer Web GUI running at https://localhost:" << port << "\n";
    svr.listen("0.0.0.0", port);
}

int main(int argc, char** argv) {
    // Usage: server [c] [q] [--config <file>]
    //   c = consumer (worker) threads, q = max queue length.
    // Anything left out comes from config.json ("consumer" section); if c or q
    // is still unknown, the program asks for it.
    std::vector<std::string> args;
    std::string config_path;
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "--config" && i + 1 < argc) {
            config_path = argv[++i];
        } else {
            args.push_back(a);
        }
    }

    json settings = cfg::Section(cfg::Load(config_path), "consumer");

    std::string c_text = args.size() >= 1 ? args[0] : cfg::GetText(settings, "threads");
    std::string q_text = args.size() >= 2 ? args[1] : cfg::GetText(settings, "queue_length");
    if (c_text.empty()) {
        std::cout << "Enter number of consumer threads (c): ";
        std::cin >> c_text;
    }
    if (q_text.empty()) {
        std::cout << "Enter max queue length (q): ";
        std::cin >> q_text;
    }

    // Reject bad input here with a clear message. c = 0 would start no workers
    // (the queue would fill up and then drop everything) and q = 0 would drop
    // every video.
    int c = 0, q_int = 0, grpc_port = 50051, gui_port = 8443;
    if (!cfg::ParseIntInRange(c_text, 1, 1000, c)) {
        std::cerr << "error: c (consumer threads) must be a whole number from 1 to 1000, got '" << c_text << "'\n"
                  << "usage: server [c] [q] [--config <file>]\n";
        return 1;
    }
    if (!cfg::ParseIntInRange(q_text, 1, 100000, q_int)) {
        std::cerr << "error: q (max queue length) must be a whole number from 1 to 100000, got '" << q_text << "'\n"
                  << "usage: server [c] [q] [--config <file>]\n";
        return 1;
    }
    std::string grpc_text = cfg::GetText(settings, "grpc_port");
    std::string gui_text = cfg::GetText(settings, "gui_port");
    if ((!grpc_text.empty() && !cfg::ParseIntInRange(grpc_text, 1, 65535, grpc_port)) ||
        (!gui_text.empty() && !cfg::ParseIntInRange(gui_text, 1, 65535, gui_port))) {
        std::cerr << "error: grpc_port and gui_port in config.json must be numbers from 1 to 65535\n";
        return 1;
    }
    size_t q = static_cast<size_t>(q_int);

    if (!FfmpegAvailable()) {
        std::cerr << "WARNING: ffmpeg was not found on PATH. Videos will still be saved, "
                     "but no 10-second previews can be made.\n";
    }

    MediaConsumerService service(q, c);

    std::thread gui_thread(StartHttpsGuiServer, &service, gui_port);
    gui_thread.detach();

    std::string server_address = "0.0.0.0:" + std::to_string(grpc_port);
    grpc::ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
    std::cout << "gRPC Consumer listening on " << server_address
              << " (c=" << c << " workers, q=" << q << " max queue)...\n";
    server->Wait();

    return 0;
}