#include <iostream>
#include <fstream>
#include <sstream>
#include <iomanip>
#include <vector>
#include <string>
#include <thread>
#include <mutex>
#include <filesystem>
#include <cstdio>
#include <cstdlib>
#include <cctype>
#include <openssl/evp.h>
#include <grpcpp/grpcpp.h>
#include "stream.grpc.pb.h"

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
// Only used when the program is started with --compress.
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

// True only if `text` is a whole number between lo and hi (inclusive).
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

        // Hash the file we are about to send (see ComputeSHA256).
        std::string hash = ComputeSHA256(send_path);

        std::ifstream in_file(send_path, std::ios::binary);
        if (!in_file) {
            Log(tag + "Could not open '" + filename + "', skipping");
            if (is_temp_copy) { std::error_code ec; fs::remove(send_path, ec); }
            continue;
        }

        grpc::ClientContext context;
        media::UploadResponse response;
        auto writer = stub->UploadVideo(&context, &response);

        // 1. Send Metadata packet first
        media::VideoPacket meta_pkt;
        auto* meta = meta_pkt.mutable_metadata();
        meta->set_filename(send_name);
        meta->set_sha256_hash(hash);
        meta->set_producer_id(producer_id);

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
            Log(tag + "ACCEPTED '" + filename + "' -> " + response.status_message());
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
    // Usage: client <p> [server_address] [--compress]
    //   p              number of producer threads (folders producer_inputs/p0 ... p{p-1})
    //   server_address default localhost:50051
    //   --compress     compress each video with ffmpeg before uploading (off by default)
    std::vector<std::string> args;
    bool compress = false;
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "--compress") compress = true;
        else args.push_back(a);
    }

    std::string p_text;
    std::string server_addr = "localhost:50051";

    if (!args.empty()) {
        p_text = args[0];
    } else {
        std::cout << "Enter number of producer threads (p): ";
        std::cin >> p_text;
    }
    if (args.size() >= 2) {
        server_addr = args[1];
    }

    int p = 0;
    if (!ParseIntInRange(p_text, 1, 1000, p)) {
        std::cerr << "error: p (producer threads) must be a whole number from 1 to 1000, got '" << p_text << "'\n"
                  << "usage: client <p> [server_address] [--compress]\n";
        return 1;
    }

    std::cout << "Starting " << p << " Producer threads targeting " << server_addr
              << (compress ? " (compression ON)" : "") << "...\n";

    if (compress) {
#ifdef _WIN32
        bool have_ffmpeg = std::system("ffmpeg -version >NUL 2>&1") == 0;
#else
        bool have_ffmpeg = std::system("ffmpeg -version >/dev/null 2>&1") == 0;
#endif
        if (!have_ffmpeg) {
            std::cerr << "error: --compress needs ffmpeg on PATH, but it was not found.\n";
            return 1;
        }
    }

    std::vector<std::thread> producers;
    for (int i = 0; i < p; ++i) {
        fs::path folder = fs::path("producer_inputs") / ("p" + std::to_string(i));
        producers.emplace_back(ProducerWorker, i, server_addr, folder, compress);
    }

    for (auto& t : producers) {
        t.join();
    }

    std::cout << "All producer threads finished.\n";
    return 0;
}