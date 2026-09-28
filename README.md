# lrpc

基于 [net](https://github.com/lvyues1994/net) 的 C++14 RPC 库，目标是小消息一元 RPC 的低 p99。
一元（unary）表示一次调用发送一个请求、返回一个响应；一个服务可定义多个方法，同一连接可并发处理多个调用。

## 当前能力

| 模块 | 用途 |
| --- | --- |
| `lrpc::wire` | 无分配的帧和 head 编解码、协议校验；视图借用输入，编码输入不得与输出重叠 |
| `lrpc::codec` | 可特化的消息 codec 及擦除类型的编解码入口，不依赖 protobuf |
| `lrpc::unary` | 单分片 TCP、原始字节与类型化调用、SETTINGS 握手、方法名驻留、写合并、取消与截止、双向 metadata、过载拒绝及 GOAWAY 排空 |
| `lrpc::protobuf` | 可选的 protobuf codec、类型化服务适配器及每调用 Arena，带 2 KiB 内联初始块 |
| `protoc-gen-rpc` | 生成客户端 Stub、服务端接口、方法描述符、绑定和每方法容量配置 |

公开接口见 [unary.hpp](unary/include/rpc/unary.hpp) 和 [typed.hpp](unary/include/rpc/typed.hpp)。
完整设计与当前实现边界见 [设计稿](docs/rpc-design.md)，其中 §17.1 记录已经落地的部分。

## 构建与运行

需要支持 C++14 的编译器、CMake 3.21+、Ninja，以及 net 和 co2。以下命令在 Linux 本地环境验证；
使用的依赖版本为 net `7dff859`、co2 `a265e57`。原始字节与自定义 codec 构建无需 protobuf。

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

仅构建和测试协议模块，无需 net；使用独立目录避免改变普通构建的配置：

```sh
cmake -S . -B build/wire-only -G Ninja \
  -DLRPC_BUILD_UNARY=OFF -DLRPC_BUILD_NET_SMOKE=OFF
cmake --build build/wire-only
ctest --test-dir build/wire-only --output-on-failure
```

### protobuf 与代码生成

需要同一供应来源的 protobuf 头文件、libprotobuf、libprotoc 和 protoc，已验证版本为 **3.21.12**。
生成器测试还需要 Python 3。系统安装可直接查找，私有安装通过 `CMAKE_PREFIX_PATH` 指定：

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
输出 `hello protobuf, metadata=1`。生成器支持一个 service 内的多个方法及一个文件内的多个 service；
[测试 proto](tests/proto/echo.proto) 包含 `RoundTrip`、`Required`、`delete` 三个方法。
每个方法都需实现服务端函数，并在生成的 Limits 中设置响应容量。

消息代码 `.pb.h/.pb.cc` 与 RPC 代码 `.rpc.hpp/.rpc.cpp` 分别由 `--cpp_out` 和 `--rpc_out` 生成，
项目内可用 `lrpc_generate_cpp` 一起生成并链接。支持 proto2、proto3、optional、lite、导入和嵌套消息。
手动生成命令、CMake 接入、Arena 生命周期以及私有依赖目录的运行环境见 [protobuf 与 metadata 用法](docs/protobuf.md)。

## 调用与资源约定

- 原始字节和自定义类型接口链接 `lrpc::unary`；protobuf 接口链接 `lrpc::protobuf` 或生成的协议 target。
  先等待 `connect()` 成功，再调用 RPC；没有隐式连接、重连或自动重试。
- 请求、回复及请求 metadata 的借用存储须保活到调用完成。生成的 binding 持有适配器，用户 service 则须保活到服务端工作排空。
- 每方法必须显式设置 `max_response_bytes`；`0` 表示空响应。服务端执行前预留响应存储，handler 超限写入返回 `internal`。
- 请求 metadata 由 `call_options.metadata` 发送；响应由 `server_context.response_metadata.assign(...)` 立即复制。
  发送响应 metadata 时需增大默认值为 2 的 `max_response_head_bytes`；客户端通过 `call_options.response_metadata` 提供接收缓冲，
  在 `call_result.response_metadata` 中取得视图。默认丢弃，容量不足返回 `resource_exhausted`。
- `call_options.deadline` 是绝对截止，`timeout` 是额外的相对上限，两者取较早值。下游调用传入上游 `server_context.deadline` 即可继承截止。
- 调用继承父 task 的 stop token，允许其它线程请求停止。其余 API，包括析构，均在所属 `io_context` 分片线程执行。
- `drain()` 停止服务端准入并完成已接纳调用；`close()` 请求取消。关闭后仍须运行事件循环消费完成事件，再销毁 context。
  `drain()` 没有自动宽限期，忽略取消且一直不结束的 handler 会继续持有资源。

请求、响应和控制帧使用固定池，预算按整块容量加描述符计费，同时受分片和连接额度限制；响应占额持续到写完成。
响应池块大小取所有方法中最大的 `16 + max_response_head_bytes + max_response_bytes`，小响应也按该整块收费。
`stats().storage_bytes` 包含空闲池块，不包含接收缓冲、方法表、调用对象、Arena、分配器开销及传输层内存，不能当作进程总内存。

`connection_options.receive_buffer_bytes` 可独立设置每连接接收窗口，允许一次读取容纳多帧。
默认 `0` 使用 `max_frame_size + 16` 字节，显式值介于该下限与 `16 MiB + 16` 之间；增大窗口会增加每连接固定内存。
公共结构和虚接口已有扩展，库与调用方需一起重新编译。

## 测试与诊断

| Preset | 配置 |
| --- | --- |
| `debug` / `release` | 原始字节、自定义 codec 和 metadata；Release 同样保留测试检查 |
| `protobuf` / `protobuf-release` | 增加 protobuf、生成代码及对应示例测试 |
| `sanitize` / `protobuf-sanitize` | Clang AddressSanitizer + UBSan，包含本地 net 源码 |
| `tsan` | ThreadSanitizer；不构建与其全局分配器冲突的故障注入程序 |
| `fuzz` | Clang libFuzzer 协议测试，无需 net |

切换构建目录时需重新传入依赖路径。测试覆盖协议边界、部分读写、粘包、缓冲寿命、预算回收、并发取消、超时、过载、
分配失败及生成器负例；TCP 与取消压力按 epoll/poll/select/io_uring 运行，不可用后端明确跳过。
protobuf Debug、Release 和 ASan/UBSan 各 23 项测试通过，关闭 protobuf 后 17 项通过；发行版 protobuf 二进制本身未插桩。

运行协议模糊测试：

```sh
cmake --preset fuzz
cmake --build --preset fuzz
./build/fuzz/tests/wire_fuzz -runs=100000 -max_len=4096
```

## 性能进展与已知限制

[基准用法](benchmarks/README.md) 提供裸 net 对照，覆盖原始字节、每端单线程 epoll 的同线程和跨进程回环 TCP，
包含闭环、开放负载、逐请求延迟、分配栈与发送队列诊断。
结果依次记录在 [首轮实测](benchmarks/results/2026-09-27.md)、[P0 验证](benchmarks/results/2026-09-27-p0.md)、
[开放负载复测](benchmarks/results/2026-09-27-open-load.md) 和 [调度诊断](benchmarks/results/2026-09-27-scheduler.md)。
严格性能验收仍未通过，不能将诊断运行当作达标结果；protobuf 路径的 p99 尚未验收。

当前仍使用每调用定时器、`run_async` 和动态调用对象，尚未达到设计中的分配目标；Arena 也可继续向堆分配。
调用对象池、时间轮、多分片、有限准入排队和流式 RPC 尚未实现；协议当前仅接受单帧、无压缩的一元调用及控制帧。
依赖存在已复现但未修改的 [net 定时器扩容 OOM 限制](docs/net-timer-oom.md)，常规测试通过不表示该故障可恢复。
