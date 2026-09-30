# protobuf、类型化调用与 metadata

P2 提供可选的 `lrpc::protobuf`、`protoc-gen-rpc` 及双向 metadata API，保留 C++14、单分片一元调用、取消、截止和执行前响应预留。
普通原始字节构建不依赖 protobuf；`lrpc::codec` 和 `<rpc/typed.hpp>` 也可用于自定义消息。
后续运行时升级已引入固定调用槽、共享精确截止、冷方法绑定、server_builder、状态说明和四种 RPC 接口；完整性能验收仍待完成。

普通 C++ 结构体也可通过显式 `protobuf_mapping<T>` 使用生成消息，保留同一个命名 stub 和业务 handler，
初始化时选择 JSON/protobuf；映射、服务契约和格式匹配要求见 [共享服务契约](service-contract.md)。

## 构建和生成

需要同一供应来源的 protobuf 头文件、libprotobuf、libprotoc 和 protoc；已验证版本为 Ubuntu 的 **3.21.12-8.2ubuntu0.3**。
CMake 使用 `protobuf::libprotobuf`、`protobuf::libprotoc`、`protobuf::protoc`，接受父项目提供的 target，或通过 `find_package(Protobuf 3.21)` 查找，不自动下载。
普通构建默认关闭 `LRPC_BUILD_PROTOBUF`；`LRPC_BUILD_CODEGEN` 可单独开关。

```sh
cmake --preset protobuf \
  -DLRPC_NET_SOURCE_DIR=/home/lvyues/code/net/net \
  -DNET_CO2_DIR=/home/lvyues/code/coro/coro \
  -DCMAKE_PREFIX_PATH=/path/to/protobuf/prefix
cmake --build --preset protobuf
ctest --preset protobuf
./build/protobuf/examples/protobuf_echo
```

系统安装的依赖可省略 `CMAKE_PREFIX_PATH`。本次本地验证将发行版 deb 解包到 `build/deps/protobuf/usr`，未安装系统包；
该私有目录的 protoc 运行还需将其 `lib/x86_64-linux-gnu` 加入 `LD_LIBRARY_PATH`。
另有 `protobuf-release` 和 `protobuf-sanitize` preset。sanitizer 覆盖本项目和本地 net 源码，发行版 protobuf 二进制本身未插桩。
本阶段 protobuf Release 与 ASan/UBSan/LSan 各 40 项、TSan 28 项通过，四种生成接口覆盖 epoll/poll/select/io_uring；
TSan 配置关闭 protobuf，发行版 protobuf 库也没有 sanitizer 插桩。
另已验证父项目 `add_subdirectory` 后生成、编译并运行示例，以及不依赖 net 的独立 codegen 构建。

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

每个 `service Echo` 生成 `EchoService`、借用 `rpc::client` 的 `EchoStub`、`EchoLimits`、
`Echo_bindings(service, limits)`、`add_Echo_service(builder, service, limits)` 与 `Echo_service_descriptor()`。
线上名字为 `package.Service/Method`；描述符包含调用种类、幂等性、codec 操作和 service 内 ordinal。表内位置不是线上方法 ID，
方法驻留仍由每条连接分配；幂等性不会自动启用重试。RPC 方法的 C++ 关键字加 `_`，转换后的重名明确报错。
proto2、proto3、optional、lite、导入和嵌套消息已覆盖；生成器支持 unary、client streaming、server streaming 和 bidi。
`cc_generic_services`、未知插件参数和 C++ 名称冲突仍被拒绝。

## 服务端与客户端

完整可运行示例见 [protobuf_echo.cpp](../examples/protobuf_echo.cpp) 和 [echo.proto](../examples/proto/echo.proto)。
服务实现的虚函数返回 `net::task<rpc::status_code>`，通常转发到捕获指针的 co2 自由协程：

```cpp
example::unary::EchoLimits limits{};
limits.Echo = {1024, 128}; // 最大编码 body；最大完整 END head。
rpc::server_builder builder{context};
example::unary::add_Echo_service(builder, service, limits);
auto server = builder.build();
example::unary::EchoStub stub{*client};
// 在 co2 协程中，先等待 connect，再调用：
CO2_AWAIT_SET(result, stub.Echo(request, reply, options));
```

每方法 `max_response_bytes` 必须显式设置，`0` 代表空编码响应。生成适配器由 binding 持有，进入 server 后随在飞调用保活；
用户 service 和客户端借用的 client、请求、回复对象仍需按各自调用寿命保活。新增公共字段和虚入口要求库与调用方一起重新编译。
原 bindings/make_server 入口保留。builder 成功 build 后不可再使用，失败可重试。
非空 Stub 构造时批量绑定整个 service，可能分配或抛异常；client 注册上限默认 4096。
handle 绑定所属 client，warm 调用使用缓存位置；线上方法定义仍在 writer 提交时发布。

客户端经 `codec_ops` 直接编码到已取得预算的请求块，响应在接收回调内直接解码到调用方对象，没有额外的整帧临时 vector。
相对超时在调用协程开始时、序列化前折算为绝对截止。客户端回复可以由调用方自己的 Arena 分配。
服务端每次类型化调用拥有一个 Arena，使用 **2 KiB 内联初始块**，请求和响应处于同一 Arena；
该资源位于非移动 handler 协程帧，覆盖解码、业务挂起及响应编码。取消不会提前销毁仍未退出的 handler。
`method_limits.message_cache_entries` 可显式配置有界 Arena 缓存；零保留上述路径。缓存满返回 `resource_exhausted`，
调用退出后 Reset 释放扩展块并重建消息，持有业务协程期间不会复用。缓存容量是每个 adapter 的配置，需匹配允许的并发度。
此缓存仅适用于 protobuf 一元适配器；流式接口中的消息由业务持有，可自行放入 Arena。
Arena 可继续向堆申请空间，字符串等字段也不保证零分配；固定缓存计入 `storage_bytes`，活动 Arena 扩展块和用户对象内存不计入。

codec 契约为 `size(message)`、`encode(message, output)`、`decode(input, message)`。
先计算大小、再写入恰好该大小的缓冲，两步之间不得修改消息；protobuf 实现使用缓存大小编码并拒绝未初始化的 required 字段。
解码前验证 `INT_MAX` 和指针边界；codec 的 decode 不得将接收字节视图保存到返回对象。
codec 是同步回调，不得递归驱动所属 `io_context` 或等待同上下文工作；允许同步关闭 client。
服务端在请求解码后再次检查取消和截止时间，已失效的调用不会进入业务函数。
坏请求返回 `invalid_argument`，成功 END 的坏响应返回 `data_loss`，远端非 OK 状态不解码 body。
解码失败时回复对象可能已经改变；编码超过方法声明上限返回 `internal`。

不用 protobuf 时，特化 `rpc::codec<Message>`，使用 `rpc::method<Request, Response>{"完整名"}`、
`rpc::call(client, method, request, reply, options)` 和 `rpc::bind_method(...)`。
也可手写 `method_descriptor` 并用 `client::bind` / `rpc::call(client, handle, ...)`，空或外来 handle 返回 invalid_argument。
连接与 pending 状态不随消息类型重复实例化；按 A 保留公共 `net::task<call_result>`。
固定槽、worker 与截止的回收边界见 [运行时升级](runtime-upgrade.md)。

## 状态说明

`call_result.code` 与 `.message` 分别为状态码和拥有的错误说明，后者支持嵌入 NUL，接收窗口复用后仍有效。
生成 service 保留 `task<status_code>`，handler 可用 `rpc::set_status(context, {code, message})` 立即复制说明再返回码。
手写 `bind_method` 同时接受 `task<status_code>` 与 `task<status>`。
说明与 metadata 共用 `max_response_head_bytes`；OK 不允许说明，超限返回空 head 的 internal。
客户端说明复制 OOM 返回无说明 resource_exhausted；错误 END 不解码业务 body。
`std::error_code ec = result.code` 使用 RPC category，取消/超时与 net 条件等价；status 本身不隐式转换。

## metadata

请求通过 `call_options.metadata` 传入 `wire::metadata_list`，服务端通过 `server_context.metadata` 读取。
描述符及键值在调用完成前保持有效且不可修改；服务端请求视图借用本次调用持有的请求块。
保留二进制值、顺序和重复键，不强加 HTTP 字段规则。

响应由 handler 调用 `context.response_metadata.assign(entries)` 设置。
setter 立即编码复制，临时字符串可在返回后销毁。注册时 `max_response_head_bytes` 必须容纳完整 END head，
包括空错误消息和 metadata count 前缀，范围为 **2–65535**，默认 `2` 表示仅能发送空 metadata。
响应块容量为所有方法中最大的 `16 + max_response_head_bytes + max_response_bytes`，加内部描述符按整块计入分片及连接预算。
准入前也检查 head 上限加 body 上限是否符合对端帧限制。head/body 超限时使用已有预留发送空 metadata 的 `END(internal)`。
实际 head 比预留短时，body 会在同一块内前移；默认空 head 路径无需这次移动。

客户端用 `call_options.response_metadata` 提供可选的有界缓冲，`call_result.response_metadata` 返回其中的视图，
可用 `wire::decode_metadata_entry` 依次读取。缓冲存的是编码后的条目，不含 count 前缀；成功和错误 END 都可返回 metadata。
默认 `{nullptr, 0}` 丢弃 metadata；提供的缓冲不足则返回 `resource_exhausted`，不返回截断视图，也不关闭连接。
该缓冲须与回复存储分离，调用期间不能另作他用，完成后也须活得比返回视图更久。
收到 CANCEL、本地取消或未收到 END 时不保证有响应 metadata。

## 流式接口

例如 `rpc Chat(stream EchoRequest) returns (stream EchoReply)` 生成：

```cpp
// 服务端实现：
net::task<rpc::status_code> Chat(rpc::server_context&,
    rpc::server_stream<EchoRequest, EchoReply>&) override;
// 客户端协程中的使用：
rpc::stream_call<EchoRequest, EchoReply> stream;
CO2_AWAIT_SET(stream, stub.Chat(options));
if (!stream) CO2_RETURN();
CO2_AWAIT_SET(code, stream.write(request));
CO2_AWAIT_SET(read, stream.read(reply));
CO2_AWAIT_SET(code, stream.writes_done());
CO2_AWAIT_SET(result, stream.finish());
```

`read` 含 code/ended，空 protobuf 消息与 EOF 区分。`write` 的请求对象借用到该操作结束；`writes_done` 半关闭发送方向，
最终状态和响应 metadata 由 `finish` 返回。每方向最多一个在飞操作，允许同时读和写；stream 析构请求取消。
开流 options 中的请求 metadata 和响应 metadata 存储保活到整个流终止。流式不自动重试。
客户端需显式启用 streaming/window，服务端注册流式方法后自动启用；混合 service 的一元方法也使用扩展 profile。
生成的四种服务及跨分片完整调用见 [protobuf_stream_tests.cpp](../tests/protobuf_stream_tests.cpp)；
协商、窗口、容量和关闭契约见 [运行时升级](runtime-upgrade.md)。
