// consumer/main.cpp -- entry point. Module 3 will add the server, queue and workers.
#include <iostream>

#include "config.h"
#include "upload.grpc.pb.h"  // generated from proto/upload.proto (used from Module 3 on)

int main(int argc, char** argv) {
  config::ConsumerConfig cfg;
  int exit_code = 0;
  // After this call the config is validated; everything below can trust it.
  if (!config::parse_consumer_config(argc, argv, cfg, exit_code)) return exit_code;

  config::print_config(cfg);
  std::cout << "consumer: config OK (server and queue come in Module 3)\n";
  return 0;
}