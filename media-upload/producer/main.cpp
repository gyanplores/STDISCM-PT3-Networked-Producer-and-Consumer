// producer/main.cpp
#include <iostream>
#include "upload.grpc.pb.h"
int main() {
  mediaupload::UploadResult r;
  r.set_status(mediaupload::DROPPED_QUEUE_FULL);
  std::cout << "producer skeleton OK (" << mediaupload::Status_Name(r.status()) << ")\n";
}