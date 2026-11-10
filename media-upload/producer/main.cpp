// producer/main.cpp -- entry point. Module 2 will add the upload threads.
#include <iostream>

#include "config.h"
#include "upload.grpc.pb.h"  // generated from proto/upload.proto (used from Module 2 on)

int main(int argc, char** argv) {
  config::ProducerConfig cfg;
  int exit_code = 0;
  // After this call the config is validated; everything below can trust it.
  if (!config::parse_producer_config(argc, argv, cfg, exit_code)) return exit_code;

  config::print_config(cfg);
  std::cout << "producer: config OK (upload logic comes in Module 2)\n";
  return 0;
}