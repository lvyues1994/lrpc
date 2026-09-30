# protobuf 与代码生成

`lrpc::protobuf` 提供 protobuf 消息的 codec 和每调用 Arena，`protoc-gen-rpc` 根据 `.proto` 生成服务接口与客户端 Stub。
两者都是可选模块；原始字节与自定义 codec 不依赖 protobuf。

普通 C++ 结构体也可通过显式 `protobuf_mapping<T>` 使用生成消息，初始化时选择 JSON 或 protobuf，
见 [共享服务契约](service-contract.md)。

## 构建和生成

需要同一供应来源的 protobuf 头文件、libprotobuf、libprotoc 和 protoc，已验证版本为 **3.21.12**。
CMake 使用 `protobuf::libprotobuf`、`protobuf::libprotoc`、`protobuf::protoc`，接受父项目提供的 target，
或通过 `find_package(Protobuf 3.21)` 查找，不自动下载。`LRPC_BUILD_PROTOBUF` 默认关闭；`LRPC_BUILD_CODEGEN` 默认随之开启。

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

系统安装的依赖可省略 `CMAKE_PREFIX_PATH`。另有 `protobuf-release` 和 `protobuf-sanitize` preset；
sanitizer 覆盖本项目和本地 net 源码，外部 protobuf 库本身未插桩。

手动生成消息和 RPC 代码：

```sh
protoc -I examples/proto --cpp_out=generated --rpc_out=generated \
  --plugin=protoc-gen-rpc=build/protobuf/codegen/protoc-gen-rpc echo.proto
```

输出目录须先创建。`echo.proto` 生成 `.pb.h/.pb.cc` 和 `.rpc.hpp/.rpc.cpp`，保留相对目录。
项目内可使用以下 CMake 函数；`PROTOS` 必须列出需要生成的导入文件，单纯导入不会自动生成其 C++ 消息代码：

```cmake
lrpc_generate_cpp(echo_protocol
    IMPORT_DIR "${CMAKE_CURRENT_SOURCE_DIR}/proto"
    PROTOS echo.proto)
target_link_libraries(my_app PRIVATE echo_protocol)
```

## 生成的代码

每个 `service Echo` 生成：

| 名字 | 用途 |
| --- | --- |
| `EchoService` | 服务端接口，每个方法一个纯虚函数 |
| `EchoLimits` | 每方法一个 `rpc::method_limits`（回复上限默认 64 KiB，trailer 上限默认 256 B） |
| `Echo_bindings(service, limits = {})` | 返回交给 `rpc::server` 的 `std::vector<rpc::method_binding>` |
| `EchoStub(rpc::call_target)` | 客户端，借用一个 `rpc::client` 或 `rpc::channel` |

线上名字为 `package.Service/Method`。RPC 方法名是 C++ 关键字时加 `_`，转换后重名、与生成的名字冲突时明确报错。
proto2、proto3、optional、lite、导入和嵌套消息已覆盖；生成器支持 unary、client streaming、server streaming 和双向流。
`cc_generic_services` 和未知插件参数被拒绝。

## 服务端与客户端

完整可运行示例见 [protobuf_echo.cpp](../examples/protobuf_echo.cpp) 和 [echo.proto](../examples/proto/echo.proto)。
服务实现的虚函数返回 `net::task<rpc::status_code>`，通常转发到捕获指针的 co2 自由协程：

```cpp
struct echo_service final : example::unary::EchoService {
    net::task<rpc::status_code> Echo(rpc::server_context &context, example::unary::Request const &request,
                                     example::unary::Reply &reply) override;
};

rpc::shard shard{context};
echo_service service;
example::unary::EchoLimits limits;
limits.Echo = {1024, 128}; // 回复编码上限；END head（状态说明与 metadata）上限。
rpc::server server{shard, example::unary::Echo_bindings(service, limits)};
rpc::client client{shard};
example::unary::EchoStub stub{client};
// 在 co2 协程中，先等待 connect，再调用：
CO2_AWAIT_SET(result, stub.Echo(request, reply, &spec, &trailer));
```

一元方法返回 `rpc::unary_call`，`spec` 和 `trailer` 可省略。Stub 构造时绑定全部方法（每个方法一次名字查找），可能分配或抛异常；
之后的调用不分配。生成的适配器由 binding 持有，随在飞调用保活；用户 service 须活到服务端排空，
Stub 借用的 client/channel、请求和回复对象须活到各自调用完成。

请求在发送时直接编码进帧，回复在读协程里直接解码进调用方的对象，没有中间缓冲；客户端回复可以由调用方自己的 Arena 分配。
服务端每次调用拥有一个 Arena，使用 **2 KiB 内联初始块**，请求和响应处于同一 Arena，
覆盖解码、业务挂起及响应编码；handler 结束前（包括服务端对象已销毁时）不会释放。
Arena 仍可向堆扩容，字符串等字段也不保证零分配。

codec 契约为 `size(message)`、`encode(message, output)`、`decode(input, message)`：先计算大小，再写入恰好该大小的缓冲，
两步之间不得修改消息。protobuf 实现使用缓存大小编码，拒绝未初始化的 required 字段；解码前验证 `INT_MAX` 和指针边界。
codec 是同步回调，不得等待所属 `io_context`。错误映射：

- 客户端编码失败（如缺 required 字段）：发送前返回 `invalid_argument`，`not_executed` 为真；
- 服务端解码失败：`invalid_argument`，业务函数不运行；
- 服务端编码失败或回复超过 `max_response_bytes`、handler 抛异常：`internal`；
- 客户端解码回复失败：`internal`；错误状态不解码 body，回复对象保持原样。

不用 protobuf 时，特化 `rpc::codec<Message>`，使用 `rpc::method<Request, Response>{"完整名"}`、
`rpc::bind(client, method)` 和 `rpc::bind_method(...)`。

## 状态说明与 metadata

请求 metadata 放在 `call_spec::metadata`，服务端从 `server_context::metadata` 读取；描述符及键值在调用完成前保持有效。
保留二进制值、顺序和重复键，不强加 HTTP 字段规则。

handler 用 `context.set_trailer(message, metadata)` 设置状态说明和响应 metadata，立即复制，
总大小受该方法的 `max_trailer_bytes` 约束。metadata 随任何状态发送，状态说明只随错误状态发送。
客户端传 `rpc::response_trailer*` 并在 `storage` 里提供缓冲，完成后从 `message` 和 `metadata` 读取，
可用 `wire::decode_metadata_entry` 依次解码；缓冲放不下时 `truncated` 为真。

## 流式接口

例如 `rpc Chat(stream EchoRequest) returns (stream EchoReply)` 生成：

```cpp
// 服务端实现：
net::task<rpc::status_code> Chat(rpc::server_context &,
    rpc::typed_server_stream<EchoRequest, EchoReply, rpc::default_codec_policy> &) override;
// 客户端协程中的使用：
rpc::typed_open_result<EchoRequest, EchoReply, rpc::default_codec_policy> call;
CO2_AWAIT_SET(call, stub.Chat(&spec));
if (call.code != rpc::status_code::ok) CO2_RETURN();
CO2_AWAIT_SET(code, call.stream.write(request));
CO2_AWAIT_SET(read, call.stream.read(reply)); // read.code / read.ended
CO2_AWAIT_SET(code, call.stream.writes_done());
CO2_AWAIT_SET(result, call.stream.finish(&trailer));
```

`write` 先把消息编码进流自己的缓冲再发送，编码失败返回 `invalid_argument` 且不发送；`read` 解码进调用方的对象。
`writes_done` 半关闭发送方向，最终状态和 trailer 由 `finish` 返回；stream 析构请求取消。流不自动重试。
服务端 handler 可用 `stream.set_trailer(...)` 设置 trailer。四种调用形态的完整用法见 [protobuf_tests.cpp](../tests/protobuf_tests.cpp)。
