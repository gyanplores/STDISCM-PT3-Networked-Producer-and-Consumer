// common/config.cpp -- see config.h for the big picture.
#include "config.h"

#include <CLI/CLI.hpp>  // small header-only library: parses --options, builds --help

#include <algorithm>
#include <cctype>
#include <fstream>
#include <iostream>

namespace fs = std::filesystem;

namespace config {

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

bool has_video_extension(const fs::path& file) {
  // Compare in lowercase so "CLIP.MP4" is accepted too.
  std::string ext = file.extension().string();
  std::transform(ext.begin(), ext.end(), ext.begin(),
                 [](unsigned char ch) { return std::tolower(ch); });
  return ext == ".mp4" || ext == ".mkv" || ext == ".avi" || ext == ".mov" ||
         ext == ".webm";
}

namespace {

// Sanity limits for user input. Not from the spec; they only exist to turn
// obvious typos into clear errors. Raise them if your tests need more.
constexpr int kMaxThreads = 1000;
constexpr int kMaxQueue = 100000;

// Counts video files directly inside `dir` (not in subfolders).
int count_videos(const fs::path& dir) {
  int n = 0;
  std::error_code ec;  // the ec overloads never throw; we just skip unreadable entries
  for (fs::directory_iterator it(dir, ec), end; !ec && it != end; it.increment(ec)) {
    if (it->is_regular_file(ec) && has_video_extension(it->path())) ++n;
  }
  return n;
}

// Shared by both programs: print the problems found, then say how to get help.
// Returning false lets the caller write `return false;` in one line.
bool fail(const std::vector<std::string>& problems, int& exit_code) {
  for (const auto& p : problems) std::cerr << "error: " << p << "\n";
  std::cerr << "Run with --help to see the options.\n";
  exit_code = 1;
  return false;
}

// Runs CLI11's parser and converts its result to our "true/false + exit_code"
// contract. CLI11 throws on --help and on bad input (e.g. --queue abc,
// a missing required option, or a number out of range); app.exit() prints the
// right message and gives us the exit code (0 for --help, nonzero for errors).
bool run_parser(CLI::App& app, int argc, char** argv, int& exit_code) {
  try {
    app.parse(argc, argv);
    return true;
  } catch (const CLI::ParseError& e) {
    exit_code = app.exit(e);
    return false;
  }
}

}  // namespace

// ---------------------------------------------------------------------------
// Producer
// ---------------------------------------------------------------------------

bool parse_producer_config(int argc, char** argv, ProducerConfig& cfg, int& exit_code) {
  CLI::App app{"Media upload producer: reads videos and uploads them to the consumer"};

  std::string videos_dir;
  // Defaults are assigned BEFORE add_option so that --help can display them.
  cfg.host = "localhost";
  cfg.port = 50051;
  app.add_option("--host", cfg.host, "Consumer address")
      ->capture_default_str();
  // CLI::Range rejects out-of-range values with a clear message.
  app.add_option("--port", cfg.port, "Consumer gRPC port")
      ->check(CLI::Range(1, 65535))
      ->capture_default_str();
  // p is required: the spec makes it a user input, so there is no hidden default.
  // Range(1, 1000): zero producers would do nothing, and the upper bound
  // catches typos (e.g. 10000 threads) with a readable error message.
  app.add_option("-p,--producers", cfg.producers, "p: number of producer threads")
      ->required()
      ->check(CLI::Range(1, kMaxThreads));
  app.add_option("--videos-dir", videos_dir,
                 "Folder containing one subfolder per producer thread: 1/, 2/, ... p/")
      ->required();

  if (!run_parser(app, argc, argv, exit_code)) return false;
  cfg.videos_dir = videos_dir;

  // ---- Validation that depends on the filesystem (CLI11 can't do this) ----
  // We collect ALL problems before failing, so the user can fix them in one go
  // instead of discovering them one run at a time.
  std::vector<std::string> problems;
  if (!fs::is_directory(cfg.videos_dir)) {
    problems.push_back("--videos-dir '" + cfg.videos_dir.string() + "' is not a folder");
    return fail(problems, exit_code);
  }

  // Build the thread -> folder mapping. Thread 0 reads videos/1, thread 1 reads
  // videos/2, and so on. Each thread therefore has its OWN folder, as the spec requires.
  for (int i = 1; i <= cfg.producers; ++i) {
    fs::path dir = cfg.videos_dir / std::to_string(i);
    if (!fs::is_directory(dir)) {
      problems.push_back("missing folder for producer thread " + std::to_string(i) +
                         ": " + dir.string());
    } else if (count_videos(dir) == 0) {
      // A producer with nothing to upload is almost certainly a setup mistake.
      problems.push_back("no video files in " + dir.string());
    } else {
      cfg.thread_dirs.push_back(dir);
    }
  }
  if (!problems.empty()) return fail(problems, exit_code);
  return true;
}

void print_config(const ProducerConfig& cfg) {
  std::cout << "[producer config]\n"
            << "  consumer      : " << cfg.host << ":" << cfg.port << "\n"
            << "  producers (p) : " << cfg.producers << "\n";
  for (size_t i = 0; i < cfg.thread_dirs.size(); ++i)
    std::cout << "  thread " << i + 1 << " folder : " << cfg.thread_dirs[i].string() << "\n";
}

// ---------------------------------------------------------------------------
// Consumer
// ---------------------------------------------------------------------------

bool parse_consumer_config(int argc, char** argv, ConsumerConfig& cfg, int& exit_code) {
  CLI::App app{"Media upload consumer: receives videos, saves them, serves the GUI"};

  // Defaults are assigned BEFORE add_option so that --help can display them.
  std::string uploads_dir = "./uploads";
  cfg.port = 50051;
  cfg.gui_port = 8080;
  app.add_option("--port", cfg.port, "gRPC port to listen on")
      ->check(CLI::Range(1, 65535))
      ->capture_default_str();
  app.add_option("-c,--consumers", cfg.consumers, "c: number of consumer worker threads")
      ->required()
      ->check(CLI::Range(1, kMaxThreads));
  // q must be >= 1: a bucket that holds nothing would drop every upload.
  app.add_option("-q,--queue", cfg.queue_size, "q: max queue length (leaky bucket)")
      ->required()
      ->check(CLI::Range(1, kMaxQueue));
  app.add_option("--uploads-dir", uploads_dir, "Folder where all uploaded videos are saved")
      ->capture_default_str();
  app.add_option("--gui-port", cfg.gui_port, "Port for the browser GUI")
      ->check(CLI::Range(1, 65535))
      ->capture_default_str();

  if (!run_parser(app, argc, argv, exit_code)) return false;
  cfg.uploads_dir = uploads_dir;

  std::vector<std::string> problems;

  // Two servers can't listen on the same port.
  if (cfg.port == cfg.gui_port)
    problems.push_back("--port and --gui-port must be different (both are " +
                       std::to_string(cfg.port) + ")");

  // Create the uploads folder if needed, then PROVE we can write to it by
  // creating and deleting a small file. Finding out now is much better than
  // having every upload fail later with a confusing error.
  std::error_code ec;
  fs::create_directories(cfg.uploads_dir, ec);
  if (ec) {
    problems.push_back("cannot create uploads folder '" + cfg.uploads_dir.string() +
                       "': " + ec.message());
  } else {
    fs::path probe = cfg.uploads_dir / ".write_test";
    std::ofstream out(probe);
    if (!out) {
      problems.push_back("uploads folder '" + cfg.uploads_dir.string() + "' is not writable");
    } else {
      out.close();
      fs::remove(probe, ec);
    }
  }
  if (!problems.empty()) return fail(problems, exit_code);
  return true;
}

void print_config(const ConsumerConfig& cfg) {
  std::cout << "[consumer config]\n"
            << "  gRPC port       : " << cfg.port << "\n"
            << "  consumers (c)   : " << cfg.consumers << "\n"
            << "  queue size (q)  : " << cfg.queue_size << "\n"
            << "  uploads folder  : " << cfg.uploads_dir.string() << "\n"
            << "  GUI port        : " << cfg.gui_port << "\n";
}

}  // namespace config