// common/config.h
//
// Module 1: command-line input for BOTH programs.
//
// The spec says "the program accepts inputs from the user": p, c and q.
// Those inputs belong to different programs, so there are two config structs:
//   ProducerConfig : p (producer threads) + where the consumer is + video folders
//   ConsumerConfig : c (consumer threads) + q (queue size) + ports + uploads folder
//
// Rule for the rest of the code: it only ever sees a config that has already
// been validated. No other module should re-check these values or touch argv.
#pragma once

#include <filesystem>
#include <string>
#include <vector>

namespace config {

struct ProducerConfig {
  std::string host;                 // consumer's address, e.g. "192.168.1.20"
  int port = 0;                     // consumer's gRPC port
  int producers = 0;                // p: number of producer threads
  std::filesystem::path videos_dir; // parent folder holding 1/, 2/, ... p/

  // thread_dirs[i] is the folder that producer thread i reads from.
  // The spec requires "each thread must read from a separate folder", and
  // this vector is how Module 2 gets that mapping: thread i uses thread_dirs[i].
  std::vector<std::filesystem::path> thread_dirs;
};

struct ConsumerConfig {
  int port = 0;                       // gRPC port the consumer listens on
  int consumers = 0;                  // c: number of worker threads
  int queue_size = 0;                 // q: leaky-bucket capacity
  std::filesystem::path uploads_dir;  // the single folder all videos are saved to
  int gui_port = 0;                   // port for the browser GUI
};

// Both parse functions follow the same contract:
//   returns true  -> `cfg` is filled and valid, keep running
//   returns false -> do NOT keep running; return `exit_code` from main()
// (--help returns false with exit_code 0; bad input returns false with 1.)
bool parse_producer_config(int argc, char** argv, ProducerConfig& cfg, int& exit_code);
bool parse_consumer_config(int argc, char** argv, ConsumerConfig& cfg, int& exit_code);

// Prints the parsed settings. Handy for logs and for the demo video.
void print_config(const ProducerConfig& cfg);
void print_config(const ConsumerConfig& cfg);

// True if the file extension looks like a video. Lives here (not in the
// producer) because Module 1 uses it to validate folders and Module 2 will
// reuse it to pick which files to upload, so both always agree.
bool has_video_extension(const std::filesystem::path& file);

}  // namespace config