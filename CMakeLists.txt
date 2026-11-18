cmake_minimum_required(VERSION 3.20)
project(MediaUploadService CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Protobuf: Windows (vcpkg) provides a CMake config package, but Ubuntu's apt
# package does not. Try the config package first and fall back to CMake's
# built-in finder, so the same file builds on both.
find_package(Protobuf CONFIG QUIET)
if(NOT Protobuf_FOUND)
  find_package(Protobuf REQUIRED)
endif()
find_package(gRPC CONFIG REQUIRED)
find_package(OpenSSL REQUIRED)

get_target_property(PROTOC_EXE protobuf::protoc LOCATION)
get_target_property(GRPC_CPP_PLUGIN_EXE gRPC::grpc_cpp_plugin LOCATION)

set(PROTO_FILE "${CMAKE_CURRENT_SOURCE_DIR}/stream.proto")
set(GENERATED_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated")
file(MAKE_DIRECTORY "${GENERATED_DIR}")

set(PROTO_SRCS "${GENERATED_DIR}/stream.pb.cc")
set(PROTO_HDRS "${GENERATED_DIR}/stream.pb.h")
set(GRPC_SRCS  "${GENERATED_DIR}/stream.grpc.pb.cc")
set(GRPC_HDRS  "${GENERATED_DIR}/stream.grpc.pb.h")

add_custom_command(
    OUTPUT "${PROTO_SRCS}" "${PROTO_HDRS}" "${GRPC_SRCS}" "${GRPC_HDRS}"
    COMMAND ${PROTOC_EXE}
    ARGS --grpc_out="${GENERATED_DIR}"
         --cpp_out="${GENERATED_DIR}"
         -I "${CMAKE_CURRENT_SOURCE_DIR}"
         --plugin=protoc-gen-grpc="${GRPC_CPP_PLUGIN_EXE}"
         "${PROTO_FILE}"
    DEPENDS "${PROTO_FILE}"
)

add_library(stream_proto STATIC ${PROTO_SRCS} ${GRPC_SRCS})
target_include_directories(stream_proto PUBLIC "${GENERATED_DIR}")
target_link_libraries(stream_proto PUBLIC gRPC::grpc++ protobuf::libprotobuf)

# Producer (Client). The source folder in the repo is producer/.
# The program it builds is still called "client" (client.exe on Windows).
add_executable(client producer/main.cpp)
target_link_libraries(client PRIVATE stream_proto OpenSSL::Crypto)

# Consumer (Server). Run it from the server/ folder: it reads index.html,
# cert.pem and key.pem from the current directory.
add_executable(server server/main.cpp)
target_include_directories(server PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/server")
target_link_libraries(server PRIVATE stream_proto)