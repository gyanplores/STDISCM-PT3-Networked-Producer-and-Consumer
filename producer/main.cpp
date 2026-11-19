#include <iostream>
#include <fstream>
#include <sstream>
#include <iomanip>
#include <vector>
#include <string>
#include <thread>
#include <mutex>
#include <algorithm>
#include <filesystem>
#include <cstdio>
#include <cstdlib>
#include <cctype>
#include <openssl/evp.h>
#include <grpcpp/grpcpp.h>
#include "stream.grpc.pb.h"
#include "config.hpp"

namespace fs = std::filesystem;
std::mutex g_log_mtx;

// Only one thread prints at a time, so log lines never get mixed together.
void Log(const std::string& msg) {
    std::lock_guard<std::mutex> lock(g_log_mtx);
    std::cout << msg << std::endl;
}

// Compute SHA-256 hash of a file using OpenSSL EVP.
// IMPORTANT: this must be called on the exact file that gets sent, because the
// server re-computes the hash of what it received and compares the two.
std::string ComputeSHA256(const fs::path& filepath) {
    std::ifstream file(filepath, std::ios::binary);
    if (!file) return "";

    EVP_MD_CTX* ctx = EVP_MD_CTX_new();
    EVP_DigestInit_ex(ctx, EVP_sha256(), nullptr);

    char buf[65536];
    while (file.read(buf, sizeof(buf)) || file.gcount() > 0) {
        EVP_DigestUpdate(ctx, buf, static_cast<size_t>(file.gcount()));
    }

    unsigned char hash[EVP_MAX_MD_SIZE];
    unsigned int length = 0;
    EVP_DigestFinal_ex(ctx, hash, &length);
    EVP_MD_CTX_free(ctx);

    std::ostringstream oss;
    for (unsigned int i = 0; i < length; ++i) {
        oss << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(hash[i]);
    }
    return oss.str();
}

// Compress video via FFmpeg (Bonus #3) into a valid seekable MP4 temp file.
// Only used when compression is turned on (--compress or "compress": true).
bool CompressVideo(const fs::path& src, const fs::path& dst) {
    std::string cmd = "ffmpeg -nostdin -y -hide_banner -loglevel error -i \"" + src.string() +
                      "\" -c:v libx264 -crf 28 -preset veryfast -c:a aac -b:a 96k \"" +
                      dst.string() + "\"";
    return std::system(cmd.c_str()) == 0;
}

bool IsVideoFile(const fs::path& p) {
    std::string ext = p.extension().string();
    for (auto& c : ext) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    return ext == ".mp4" || ext == ".mkv" || ext == ".avi" || ext == ".mov" || ext == ".ts";
}

// "Dynamic threading": instead of being told how many producers to start, look at
// the input folder and find every sub-folder named p0, p1, p2, ... The number in
// the name is that producer's id. One producer thread is started per folder, so
// the spec still holds: every producer thread reads its own separate folder.
std::vector<int> DiscoverProducerFolders(const fs::path& root) {
    std::vector<int> ids;
    std::error_code ec;
    for (const auto& entry : fs::directory_iterator(root, ec)) {
        if (!entry.is_directory()) continue;
        std::string name = entry.path().filename().string();
        if (name.size() < 2 || name[0] != 'p') continue;
        int id = 0;
        if (cfg::ParseIntInRange(name.substr(1), 0, 100000, id)) ids.push_back(id);
    }
    std::sort(ids.begin(), ids.end());
    return ids;
}

void ProducerWorker(int producer_id, const std::string& server_addr, const fs::path& input_dir, bool compress) {
    const std::string tag = "[Producer " + std::to_string(producer_id) + "] ";

    if (!fs::exists(input_dir)) {
        fs::create_directories(input_dir);
        Log(tag + "Created empty folder: " + input_dir.string());
        return;
    }

    auto channel = grpc::CreateChannel(server_addr, grpc::InsecureChannelCredentials());
    auto stub = media::VideoUploadService::NewStub(channel);

    // Compressed copies are written here. The temp file name is built from a
    // counter, never from the video's own filename, so odd characters in a
    // filename can't end up inside the ffmpeg command line.
    fs::path temp_dir = fs::temp_directory_path() / ("prod_tmp_" + std::to_string(producer_id));
    fs::create_directories(temp_dir);
    int file_counter = 0;

    for (const auto& entry : fs::directory_iterator(input_dir)) {
        if (!entry.is_regular_file() || !IsVideoFile(entry.path())) continue;

        fs::path src_path = entry.path();
        std::string filename = src_path.filename().string();

        // The file we actually send: the original, or a compressed copy.
        fs::path send_path = src_path;
        bool is_temp_copy = false;
        std::string send_name = filename;

        if (compress) {
            Log(tag + "Compressing: " + filename);
            fs::path compressed_path = temp_dir / ("upload_" + std::to_string(file_counter++) + ".mp4");
            if (!CompressVideo(src_path, compressed_path)) {
                Log(tag + "FFmpeg compression failed for: " + filename);
                std::error_code ec;
                fs::remove(compressed_path, ec);
                continue;
            }
            send_path = compressed_path;
            is_temp_copy = true;
            // The compressed file is always MP4, so the name must say so
            // (otherwise "clip.mkv" would be saved as an MP4 with an .mkv name).
            send_name = src_path.stem().string() + ".mp4";
        }

        // The metadata the consumer will be able to check: size and SHA-256 of
        // the exact file we are about to send (see ComputeSHA256).
        std::error_code size_ec;
        std::uintmax_t file_size = fs::file_size(send_path, size_ec);
        std::string hash = ComputeSHA256(send_path);

        std::ifstream in_file(send_path, std::ios::binary);
        if (size_ec || !in_file) {
            Log(tag + "Could not open '" + filename + "', skipping");
            if (is_temp_copy) { std::error_code ec; fs::remove(send_path, ec); }
            continue;
        }

        Log(tag + "Sending '" + send_name + "'  size=" + std::to_string(file_size) +
            " bytes  sha256=" + hash);

        grpc::ClientContext context;
        media::UploadResponse response;
        auto writer = stub->UploadVideo(&context, &response);

        // 1. Send Metadata packet first
        media::VideoPacket meta_pkt;
        auto* meta = meta_pkt.mutable_metadata();
        meta->set_filename(send_name);
        meta->set_sha256_hash(hash);
        meta->set_producer_id(producer_id);
        meta->set_file_size(static_cast<int64_t>(file_size));

        if (!writer->Write(meta_pkt)) {
            Log(tag + "Server closed stream early for: " + filename);
        } else {
            // 2. Stream the video in 64KB chunks
            char buffer[65536];
            while (in_file.read(buffer, sizeof(buffer)) || in_file.gcount() > 0) {
                media::VideoPacket chunk_pkt;
                chunk_pkt.set_chunk_data(buffer, static_cast<size_t>(in_file.gcount()));
                if (!writer->Write(chunk_pkt)) {
                    break; // Server closed the stream (error or rejection)
                }
            }
        }

        in_file.close();
        writer->WritesDone();
        grpc::Status status = writer->Finish();
        if (is_temp_copy) {
            std::error_code ec;
            fs::remove(send_path, ec);
        }

        if (status.ok()) {
            Log(tag + "ACCEPTED '" + send_name + "' -> " + response.status_message());
            // Compare what the consumer measured with what we sent.
            if (response.received_sha256().empty()) {
                Log(tag + "  (consumer did not report its own size/hash)");
            } else if (response.received_sha256() == hash &&
                       response.received_size() == static_cast<int64_t>(file_size)) {
                Log(tag + "  VERIFIED: consumer received size=" + std::to_string(response.received_size()) +
                    " sha256=" + response.received_sha256() + "  (identical to what was sent)");
            } else {
                Log(tag + "  WARNING: consumer reports size=" + std::to_string(response.received_size()) +
                    " sha256=" + response.received_sha256() + "  (DIFFERENT from what was sent)");
            }
        } else if (status.error_code() == grpc::StatusCode::RESOURCE_EXHAUSTED) {
            Log(tag + "DROPPED (QUEUE FULL) '" + filename + "' -> " + status.error_message());
        } else if (status.error_code() == grpc::StatusCode::ALREADY_EXISTS) {
            Log(tag + "REJECTED (DUPLICATE) '" + filename + "' -> " + status.error_message());
        } else if (status.error_code() == grpc::StatusCode::DATA_LOSS) {
            Log(tag + "REJECTED (CORRUPTED IN TRANSIT) '" + filename + "' -> " + status.error_message());
        } else {
            Log(tag + "ERROR '" + filename + "': " + status.error_message());
        }
    }
}

int main(int argc, char** argv) {
    // Usage: client [p|auto] [server_address] [--compress] [--config <file>]
    //   p | auto       how many producer threads. "auto" = one per folder found in
    //                  producer_inputs/ (p0, p1, ...). A number = p0 ... p{p-1}.
    //   server_address default localhost:50051
    //   --compress     compress each video with ffmpeg before uploading (off by default)
    //   --config       use this config file instead of config.json
    // Anything left out comes from config.json ("producer" section), and
    // if p is still unknown the program asks for it.
    std::vector<std::string> args;
    bool compress_flag = false;
    std::string config_path;
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "--compress") {
            compress_flag = true;
        } else if (a == "--config" && i + 1 < argc) {
            config_path = argv[++i];
        } else {
            args.push_back(a);
        }
    }

    nlohmann::json settings = cfg::Section(cfg::Load(config_path), "producer");

    std::string threads_text = !args.empty() ? args[0] : cfg::GetText(settings, "threads");
    if (threads_text.empty()) {
        std::cout << "Enter number of producer threads (p), or 'auto': ";
        std::cin >> threads_text;
    }

    std::string server_addr = args.size() >= 2 ? args[1] : cfg::GetText(settings, "server_address");
    if (server_addr.empty()) server_addr = "localhost:50051";

    std::string input_root_text = cfg::GetText(settings, "input_folder");
    fs::path input_root = input_root_text.empty() ? fs::path("producer_inputs") : fs::path(input_root_text);

    bool compress = compress_flag || cfg::GetBool(settings, "compress", false);

    // Decide which producer folders (and so which threads) to use.
    std::vector<int> ids;
    if (threads_text == "auto") {
        ids = DiscoverProducerFolders(input_root);
        if (ids.empty()) {
            std::cerr << "error: threads is 'auto' but no folders named p0, p1, ... were found in '"
                      << input_root.string() << "'.\n"
                      << "Create them and put videos inside, or give a number instead of 'auto'.\n";
            return 1;
        }
    } else {
        int p = 0;
        if (!cfg::ParseIntInRange(threads_text, 1, 1000, p)) {
            std::cerr << "error: p (producer threads) must be a whole number from 1 to 1000 or 'auto', got '"
                      << threads_text << "'\n"
                      << "usage: client [p|auto] [server_address] [--compress] [--config <file>]\n";
            return 1;
        }
        for (int i = 0; i < p; ++i) ids.push_back(i);
    }

    if (compress) {
#ifdef _WIN32
        bool have_ffmpeg = std::system("ffmpeg -version >NUL 2>&1") == 0;
#else
        bool have_ffmpeg = std::system("ffmpeg -version >/dev/null 2>&1") == 0;
#endif
        if (!have_ffmpeg) {
            std::cerr << "error: compression needs ffmpeg on PATH, but it was not found.\n";
            return 1;
        }
    }

    std::cout << "Starting " << ids.size() << " Producer thread(s) targeting " << server_addr
              << (threads_text == "auto" ? " (found by scanning " + input_root.string() + ")" : "")
              << (compress ? " (compression ON)" : "") << "...\n";

    std::vector<std::thread> producers;
    for (int id : ids) {
        fs::path folder = input_root / ("p" + std::to_string(id));
        producers.emplace_back(ProducerWorker, id, server_addr, folder, compress);
    }

    for (auto& t : producers) {
        t.join();
    }

    std::cout << "All producer threads finished.\n";
    return 0;
}