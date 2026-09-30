add_library(rpc_v2 STATIC src/core.cpp src/engine.cpp src/stream_core.cpp src/client.cpp src/client_stream.cpp
            src/channel.cpp src/server.cpp src/transport.cpp)
add_library(lrpc::v2 ALIAS rpc_v2)
target_include_directories(rpc_v2 PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/include")
target_link_libraries(rpc_v2 PUBLIC lrpc::wire net::net PRIVATE lrpc::message)
lrpc_configure_target(rpc_v2)
