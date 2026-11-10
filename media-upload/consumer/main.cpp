// consumer/main.cpp
#include <iostream>
#include "upload.grpc.pb.h"
int main() {
  mediaupload::FileMeta m;
  m.set_filename("sample.mp4");
  std::cout << "consumer skeleton OK (" << m.filename() << ")\n";
}