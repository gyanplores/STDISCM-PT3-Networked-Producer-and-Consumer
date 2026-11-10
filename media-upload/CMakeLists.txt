cmake_minimum_required(VERSION 3.18)
project(media_upload LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)          # change to 20 if both compilers support it
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Threads REQUIRED)
find_package(Protobuf REQUIRED)
find_package(CLI11 REQUIRED)   # command-line parsing (Module 1)
find_package(gRPC CONFIG QUIET)

if(gRPC_FOUND)
  set(GRPC_CPP_PLUGIN $<TARGET_FILE:gRPC::grpc_cpp_plugin>)
  set(GRPC_LIBS gRPC::grpc++)
else()
  # Fallback for systems (e.g. Ubuntu apt packages) without gRPC CMake configs
  find_package(PkgConfig REQUIRED)
  pkg_check_modules(GRPCPP REQUIRED IMPORTED_TARGET grpc++)
  find_program(GRPC_CPP_PLUGIN grpc_cpp_plugin REQUIRED)
  set(GRPC_LIBS PkgConfig::GRPCPP)
endif()

# ---- Generate C++ code from proto/upload.proto into the build tree ----
set(PROTO_FILE ${CMAKE_CURRENT_SOURCE_DIR}/proto/upload.proto)
set(GEN_DIR ${CMAKE_CURRENT_BINARY_DIR}/generated)
file(MAKE_DIRECTORY ${GEN_DIR})

set(GEN_SRCS
  ${GEN_DIR}/upload.pb.cc ${GEN_DIR}/upload.pb.h
  ${GEN_DIR}/upload.grpc.pb.cc ${GEN_DIR}/upload.grpc.pb.h)

add_custom_command(
  OUTPUT ${GEN_SRCS}
  COMMAND protobuf::protoc
  ARGS --proto_path=${CMAKE_CURRENT_SOURCE_DIR}/proto
       --cpp_out=${GEN_DIR}
       --grpc_out=${GEN_DIR}
       --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN}
       ${PROTO_FILE}
  DEPENDS ${PROTO_FILE}
  COMMENT "Generating gRPC/Protobuf sources")

# Shared library both programs link against (generated code + common/ code)
add_library(upload_common STATIC ${GEN_SRCS} common/config.cpp)
target_include_directories(upload_common PUBLIC
  ${GEN_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/common)
target_link_libraries(upload_common PUBLIC
  protobuf::libprotobuf ${GRPC_LIBS} Threads::Threads)

add_executable(producer producer/main.cpp)
target_link_libraries(producer PRIVATE upload_common)

add_executable(consumer consumer/main.cpp)
target_link_libraries(consumer PRIVATE upload_common)