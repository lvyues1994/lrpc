# lrpc

基于 [net](https://github.com/lvyues1994/net) 的 C++14 RPC 库，目标是小消息一元 RPC 的低 p99。
支持一元调用和三种流式调用；一个服务可定义多个方法，同一连接可并发处理多个调用。

## 模块

| 模块 | 用途 |
| --- | --- |
| `lrpc::wire` | 无分配的帧和 head 编解码、协议校验；视图借用输入，编码输入不得与输出重叠 |
| `lrpc::compression` | 可选的 zstd/LZ4 流消息压缩 |
| `lrpc::rpc` | shard、client、server、channel、流式调用、类型化调用与共享服务契约 |
| `lrpc::protobuf` | 可选的 protobuf codec 及每调用 Arena，带 2 KiB 内联初始块 |
| `lrpc::json` | 可选的普通结构体 JSON 映射、严格 SAX 校验和有界单次编码 |
| `protoc-gen-rpc` | 生成服务端接口、每方法容量配置、binding 和客户端 Stub |

公开接口在 [include/rpc](include/rpc)：[client.hpp](include/rpc/client.hpp)、[server.hpp](include/rpc/server.hpp)、
[channel.hpp](include/rpc/channel.hpp)、[stream.hpp](include/rpc/stream.hpp)、[typed.hpp](include/rpc/typed.hpp)、
[service.hpp](include/rpc/service.hpp)。结构、线上行为和实测见 [设计说明](docs/design.md)。

## 构建与运行

需要支持 C++14 的编译器、CMake 3.21+、Ninja，以及 net 和 co2。原始字节与自定义 codec 构建无需 protobuf。
在本项目根目录执行，源码路径按实际位置调整：

```sh
cmake --preset debug \
  -DLRPC_NET_SOURCE_DIR=/home/lvyues/code/net/net \
  -DNET_CO2_DIR=/home/lvyues/code/coro/coro \
  -DCMAKE_DISABLE_FIND_PACKAGE_co2=ON
cmake --build --preset debug
ctest --preset debug
./build/debug/examples/unary_echo
```

[原始字节示例](examples/unary_echo.cpp) 在同一个事件循环中启动回环 TCP 服务端与客户端，输出 `hello lrpc` 后关闭并排空。
集成测试和示例需要本机回环监听权限。

父项目也可提供 `net::net`，或省略源码路径使用已安装的 net 包。本地源码接入接受已有 co2 target、安装包或源码目录；
上面的 `CMAKE_DISABLE_FIND_PACKAGE_co2=ON` 用于明确选择本地 co2 源码。配置不会自动下载依赖，默认关闭 net 的 TLS、测试、示例和基准。

仅构建和测试协议与压缩模块，无需 net：

```sh
cmake -S . -B build/wire-only -G Ninja -DLRPC_BUILD_RPC=OFF -DLRPC_BUILD_EXAMPLES=OFF
cmake --build build/wire-only
ctest --test-dir build/wire-only --output-on-failure
```

### protobuf 与代码生成

需要同一供应来源的 protobuf 头文件、libprotobuf、libprotoc 和 protoc，已验证版本为 **3.21.12**；生成器测试还需要 Python 3。
私有安装通过 `CMAKE_PREFIX_PATH` 指定：

```sh
cmake --preset protobuf \
  -DLRPC_NET_SOURCE_DIR=/home/lvyues/code/net/net \
  -DNET_CO2_DIR=/home/lvyues/code/coro/coro \
  -DCMAKE_DISABLE_FIND_PACKAGE_co2=ON \
  -DCMAKE_PREFIX_PATH=/path/to/protobuf/prefix
cmake --build --preset protobuf
ctest --preset protobuf
./build/protobuf/examples/protobuf_echo
```

[protobuf 示例](examples/protobuf_echo.cpp) 根据 [echo.proto](examples/proto/echo.proto) 生成代码，演示类型化调用与 metadata 往返，
输出 `hello protobuf, metadata=1`。消息代码 `.pb.h/.pb.cc` 与 RPC 代码 `.rpc.hpp/.rpc.cpp` 分别由 `--cpp_out` 和 `--rpc_out` 生成，
项目内可用 `lrpc_generate_cpp` 一起生成并链接。生成的接口、Arena 生命周期和错误映射见 [protobuf 用法](docs/protobuf.md)。

### 普通结构体 JSON

启用 `LRPC_BUILD_JSON=ON`，提供独立 RapidJSON 源码目录（`LRPC_RAPIDJSON_SOURCE_DIR`）或安装包，链接 `lrpc::json`。
`RPC_JSON_FIELDS(Type, ...)` 映射字段，`json_method<Request, Reply>` 声明方法，`rpc::bind_method` 注册服务端，
`rpc::bind(client, method)` 得到客户端调用。字段策略、容量限制和基线见 [JSON 用法](docs/json.md)。

多方法服务可在创建共享契约时选择 JSON 或显式映射的 protobuf，server 与 client stub 使用同一契约，
业务保留普通结构体参数；同时启用两模块和 codegen 后 `json_users protobuf` 使用 protobuf，见 [共享服务契约](docs/service-contract.md)。

## 调用与资源约定

- 每个 `io_context` 建一个 `rpc::shard`，其上的 server、client、channel 及其全部调用都在该 context 的线程执行，包括析构。
- client 先等待 `connect()` 成功再调用；多端点、后台重连、负载均衡和未执行调用的重试用 `rpc::channel`。
- `client.bind(name)` 做一次名字查找，得到只适用于该 client/channel 的 `method_ref`；之后 `call(method, request, response, &spec, &trailer)`
  返回可直接等待的 `unary_call`，结果是 `call_result{code, size, not_executed}`。
- 请求、回复缓冲、`call_spec`、`response_trailer` 和请求 metadata 须保活到调用完成；binding 借用的 handler/service 须保活到服务端排空。
- `call_spec::deadline` 是绝对截止，`timeout` 是额外的相对上限，两者取较早值；下游调用传入 `server_context::deadline` 即可继承截止。
- 调用继承等待方协程的 stop token，允许其它线程请求停止。
- handler 用 `context.stop_requested()` 检查停止。要交给可能活过 handler 的工作，用 `context.stop_token()` 取 token；
  从协程环境读到的 token 不能留到 handler 结束后，因为没被请求停止的停止状态会交给后续调用。
- 每个方法声明 `max_response_bytes` 和 `max_trailer_bytes`。handler 用 `set_trailer(message, metadata)` 设置状态说明和响应 metadata；
  客户端在 `response_trailer::storage` 提供缓冲，放不下时 `truncated` 为真。状态说明只随错误状态发送。
- `server.drain()` 发 GOAWAY、完成已接纳的调用后关闭连接；`close()` 取消全部。关闭后仍须运行事件循环消费完成事件，再销毁 context。
- 流式调用默认启用（`connection_options::receive.features` 含 `wire::streaming`）；压缩需 `-DLRPC_ENABLE_COMPRESSION=ON`、
  zstd/LZ4 开发包及双方 SETTINGS 协商，默认关闭。

## 测试

| Preset | 配置 |
| --- | --- |
| `debug` / `release` | 核心、原始字节、类型化调用、channel、流式和零分配门槛；Release 同样保留测试检查 |
| `protobuf` / `protobuf-release` / `protobuf-sanitize` | 增加 protobuf、生成代码、生成器负例及对应示例 |
| `json` / `json-release` / `json-sanitize` | 普通结构体 JSON、类型化调用及字段/资源校验 |
| `sanitize` | Clang AddressSanitizer + UBSan，包含本地 net 源码 |
| `tsan` | ThreadSanitizer；不构建替换全局分配器的零分配门槛测试 |
| `fuzz` | Clang libFuzzer 协议测试，无需 net |

切换构建目录时需重新传入依赖路径。网络测试按 epoll/poll/select/io_uring 各跑一遍，不可用的后端明确跳过。

运行协议模糊测试：

```sh
cmake --preset fuzz
cmake --build --preset fuzz
./build/fuzz/tests/wire_fuzz -runs=100000 -max_len=4096
```

## 性能

[基准用法](benchmarks/README.md) 以裸 net 固定长度回显为对照，覆盖同线程和跨进程回环 TCP、闭环与开放负载、逐请求延迟和分配诊断，
另测与批量流共用连接时的一元延迟。
当前实测与零分配门槛见 [设计说明](docs/design.md#实测)。
