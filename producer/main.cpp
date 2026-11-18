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
#include <openssl/evp.h>
#include <grpcpp/grpcpp.h>
#include "stream.grpc.pb.h"

namespace fs = std::filesystem;
std::mutex g_log_mtx;

void Log(const std::string& msg) {
    std::lock_guard<std::mutex> lock(g_log_mtx);
    std::cout << msg << std::endl;
}

// Compute SHA-256 hash of a file using OpenSSL EVP
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

// Compress video via FFmpeg (Bonus #3) into a valid seekable MP4 temp file
bool CompressVideo(const fs::path& src, const fs::path& dst) {
    std::string cmd = "ffmpeg -y -hide_banner -loglevel error -i \"" + src.string() +
                      "\" -c:v libx264 -crf 28 -preset veryfast -c:a aac -b:a 96k \"" +
                      dst.string() + "\"";
    return std::system(cmd.c_str()) == 0;
}

bool IsVideoFile(const fs::path& p) {
    std::string ext = p.extension().string();
    for (auto& c : ext) c = static_cast<char>(std::tolower(c));
    return ext == ".mp4" || ext == ".mkv" || ext == ".avi" || ext == ".mov" || ext == ".ts";
}

void ProducerWorker(int producer_id, const std::string& server_addr, const fs::path& input_dir) {
    if (!fs::exists(input_dir)) {
        fs::create_directories(input_dir);
        Log("[Producer " + std::to_string(producer_id) + "] Created empty folder: " + input_dir.string());
        return;
    }

    auto channel = grpc::CreateChannel(server_addr, grpc::InsecureChannelCredentials());
    auto stub = media::VideoUploadService::NewStub(channel);

    fs::path temp_dir = fs::temp_directory_path() / ("prod_tmp_" + std::to_string(producer_id));
    fs::create_directories(temp_dir);

    for (const auto& entry : fs::directory_iterator(input_dir)) {
        if (!entry.is_regular_file() || !IsVideoFile(entry.path())) continue;

        fs::path src_path = entry.path();
        std::string filename = src_path.filename().string();
        std::string hash = ComputeSHA256(src_path);

        Log("[Producer " + std::to_string(producer_id) + "] Compressing: " + filename);
        fs::path compressed_path = temp_dir / (filename + "_compressed.mp4");

        if (!CompressVideo(src_path, compressed_path)) {
            Log("[Producer " + std::to_string(producer_id) + "] FFmpeg compression failed for: " + filename);
            continue;
        }

        std::ifstream in_file(compressed_path, std::ios::binary);
        if (!in_file) continue;

        grpc::ClientContext context;
        media::UploadResponse response;
        auto writer = stub->UploadVideo(&context, &response);

        // 1. Send Metadata packet first
        media::VideoPacket meta_pkt;
        auto* meta = meta_pkt.mutable_metadata();
        meta->set_filename(filename);
        meta->set_sha256_hash(hash);
        meta->set_producer_id(producer_id);

        if (!writer->Write(meta_pkt)) {
            Log("[Producer " + std::to_string(producer_id) + "] Server closed stream early for: " + filename);
        } else {
            // 2. Stream compressed video in 64KB chunks
            char buffer[65536];
            while (in_file.read(buffer, sizeof(buffer)) || in_file.gcount() > 0) {
                media::VideoPacket chunk_pkt;
                chunk_pkt.set_chunk_data(buffer, static_cast<size_t>(in_file.gcount()));
                if (!writer->Write(chunk_pkt)) {
                    break; // Server likely rejected (Queue Full or Duplicate)
                }
            }
        }

        in_file.close();
        writer->WritesDone();
        grpc::Status status = writer->Finish();
        fs::remove(compressed_path);

        if (status.ok()) {
            Log("[Producer " + std::to_string(producer_id) + "] ACCEPTED '" + filename + "' -> " + response.status_message());
        } else if (status.error_code() == grpc::StatusCode::RESOURCE_EXHAUSTED) {
            Log("[Producer " + std::to_string(producer_id) + "] DROPPED (QUEUE FULL) '" + filename + "' -> " + status.error_message());
        } else if (status.error_code() == grpc::StatusCode::ALREADY_EXISTS) {
            Log("[Producer " + std::to_string(producer_id) + "] REJECTED (DUPLICATE) '" + filename + "' -> " + status.error_message());
        } else {
            Log("[Producer " + std::to_string(producer_id) + "] ERROR '" + filename + "': " + status.error_message());
        }
    }
}

int main(int argc, char** argv) {
    int p = 2;
    std::string server_addr = "localhost:50051";

    if (argc >= 2) {
        p = std::stoi(argv[1]);
    } else {
        std::cout << "Enter number of producer threads (p): ";
        std::cin >> p;
    }
    if (argc >= 3) {
        server_addr = argv[2];
    }

    std::cout << "Starting " << p << " Producer threads targeting " << server_addr << "...\n";

    std::vector<std::thread> producers;
    for (int i = 0; i < p; ++i) {
        fs::path folder = fs::path("producer_inputs") / ("p" + std::to_string(i));
        producers.emplace_back(ProducerWorker, i, server_addr, folder);
    }

    for (auto& t : producers) {
        t.join();
    }

    std::cout << "All producer threads finished.\n";
    return 0;
}