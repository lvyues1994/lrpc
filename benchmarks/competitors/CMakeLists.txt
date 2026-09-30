cmake_minimum_required(VERSION 3.22)
project(lrpc_comparison LANGUAGES C CXX)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(COMPARISON_FRAMEWORK "" CACHE STRING "grpc, brpc, or coro_rpc")
set(COMPARISON_SOURCE "" CACHE PATH "Pinned upstream source directory")
set(COMPARISON_BRPC_BUILD "" CACHE PATH "Prepared brpc build directory")
find_package(Threads REQUIRED)

if(COMPARISON_FRAMEWORK STREQUAL "grpc")
    add_subdirectory("${COMPARISON_SOURCE}" grpc EXCLUDE_FROM_ALL)
    # brpc requires generic services; grpc_cpp_plugin rejects that option.
    file(READ "${CMAKE_CURRENT_SOURCE_DIR}/echo.proto" grpc_schema)
    string(REPLACE "cc_generic_services = true" "cc_generic_services = false" grpc_schema "${grpc_schema}")
    file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/echo.proto" "${grpc_schema}")
    add_custom_command(OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/echo.pb.cc" "${CMAKE_CURRENT_BINARY_DIR}/echo.pb.h"
        "${CMAKE_CURRENT_BINARY_DIR}/echo.grpc.pb.cc" "${CMAKE_CURRENT_BINARY_DIR}/echo.grpc.pb.h"
        COMMAND $<TARGET_FILE:protobuf::protoc> --proto_path=${CMAKE_CURRENT_BINARY_DIR}
        --cpp_out=${CMAKE_CURRENT_BINARY_DIR} --grpc_out=${CMAKE_CURRENT_BINARY_DIR}
        --plugin=protoc-gen-grpc=$<TARGET_FILE:grpc_cpp_plugin> "${CMAKE_CURRENT_BINARY_DIR}/echo.proto"
        DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/echo.proto" protobuf::protoc grpc_cpp_plugin VERBATIM)
    add_executable(comparison_bench grpc.cpp "${CMAKE_CURRENT_BINARY_DIR}/echo.pb.cc"
        "${CMAKE_CURRENT_BINARY_DIR}/echo.grpc.pb.cc")
    target_link_libraries(comparison_bench PRIVATE grpc++ protobuf::libprotobuf)
elseif(COMPARISON_FRAMEWORK STREQUAL "brpc")
    find_package(Protobuf REQUIRED)
    find_package(OpenSSL REQUIRED)
    find_package(ZLIB REQUIRED)
    find_path(COMPARISON_GFLAGS_INCLUDE gflags/gflags.h REQUIRED)
    find_library(COMPARISON_GFLAGS_LIBRARY gflags REQUIRED)
    find_path(COMPARISON_LEVELDB_INCLUDE leveldb/db.h REQUIRED)
    find_library(COMPARISON_LEVELDB_LIBRARY leveldb REQUIRED)
    add_library(comparison_brpc STATIC IMPORTED)
    set_target_properties(comparison_brpc PROPERTIES
        IMPORTED_LOCATION "${COMPARISON_BRPC_BUILD}/output/lib/libbrpc.a"
        INTERFACE_INCLUDE_DIRECTORIES "${COMPARISON_SOURCE}/src;${COMPARISON_BRPC_BUILD};${COMPARISON_GFLAGS_INCLUDE};${COMPARISON_LEVELDB_INCLUDE}"
        INTERFACE_LINK_LIBRARIES "${COMPARISON_GFLAGS_LIBRARY};${COMPARISON_LEVELDB_LIBRARY};protobuf::libprotobuf;protobuf::libprotoc;Threads::Threads;OpenSSL::SSL;OpenSSL::Crypto;ZLIB::ZLIB;${CMAKE_DL_LIBS};rt")
    protobuf_generate_cpp(proto_sources proto_headers echo.proto)
    add_executable(comparison_bench brpc.cpp ${proto_sources} ${proto_headers})
    target_link_libraries(comparison_bench PRIVATE comparison_brpc)
elseif(COMPARISON_FRAMEWORK STREQUAL "coro_rpc")
    add_subdirectory("${COMPARISON_SOURCE}" yalantinglibs EXCLUDE_FROM_ALL)
    add_executable(comparison_bench coro_rpc.cpp)
    target_link_libraries(comparison_bench PRIVATE yalantinglibs::yalantinglibs)
else()
    message(FATAL_ERROR "Select COMPARISON_FRAMEWORK=grpc|brpc|coro_rpc")
endif()
target_include_directories(comparison_bench PRIVATE "${CMAKE_CURRENT_BINARY_DIR}")
target_link_libraries(comparison_bench PRIVATE Threads::Threads)
target_compile_options(comparison_bench PRIVATE -Wall -Wextra -Werror=return-type)
