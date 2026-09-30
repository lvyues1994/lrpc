add_library(rpc_runtime STATIC src/channel.cpp src/runtime.cpp src/resolver.cpp src/dispatch.cpp)
add_library(lrpc::runtime ALIAS rpc_runtime)
target_include_directories(rpc_runtime PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/include"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../unary/src")
target_link_libraries(rpc_runtime PUBLIC lrpc::unary)
lrpc_configure_target(rpc_runtime)
