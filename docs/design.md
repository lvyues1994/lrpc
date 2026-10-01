# lrpc 设计

lrpc 在 net 之上实现：一个 shard 对应一个 `io_context`，连接、调用和定时都在所属 shard 的线程上运行；只有取消可以从任意线程发出。

## 结构

- **shard**：每个 `io_context` 一份。分级 slab（64 B–64 KiB，按需取块、空闲块按类限量缓存），1 ms 时间轮和它唯一的 `steady_timer`（惰性撤销），跨线程取消用的 stop hook 池，以及按需创建的压缩器。
- **connection**：客户端和服务端共用。读协程在一个固定窗口里就地解析帧，放不下的大帧临时换大缓冲；写协程把一轮里提交的帧从连续的发送块链一次写出。发送积压超过高水位时暂停读，把压力经 TCP 推回对端。
- **client**：`unary_call` 是等待者本身，调用的全部状态住在等待方协程帧里；请求直接编码进发送缓冲；方法首次调用时驻留，之后只发编号。
- **server**：调用对象池化；handler 在读协程里就地启动；准入按方法声明的响应上限记账，不预占物理内存。

## 用法

```cpp
rpc::shard shard{context};
rpc::server server{shard, {{"demo/Echo", &handler, 4096}}};
auto const endpoint = server.listen({net::ip::address_v4::loopback(), 0});
rpc::client client{shard};

// 在 shard 线程的协程里：
CO2_AWAIT_SET(connected, client.connect(endpoint));
echo = client.bind("demo/Echo"); // 一次字符串查找
CO2_AWAIT_SET(result, client.call(echo, request, response, &spec)); // result.code、result.size
```

`spec`（截止、相对超时、请求 metadata）可省略；取消来自等待方的 stop token，可以从任意线程发出。请求、响应缓冲、`spec` 和 `response_trailer` 借用到调用完成。

`channel` 面向一组固定端点或具名目标（`channel_target{host, service}`），每个端点或目标维持 `connections_per_endpoint` 条连接，断开后在后台按指数退避重连。具名目标在建连前经 context 的解析线程调用 getaddrinfo 解析，也可以用 `channel_options::resolve` 换成自己的查询（例如服务发现）。同一目标的各条连接从不同地址起步，连接失败就换下一个地址；上次结果超过 `resolve_interval`，或结果里的地址都失败过，才重新解析，解析失败时沿用上次的结果。每次调用发往在飞调用最少的就绪连接。调用失败且能证明服务端没有执行（发送前就失败、END 带 `not_executed`、GOAWAY 表明服务端没处理该流）时，换一条连接透明重试，最多 `max_attempts` 次，截止时间沿用第一次算出的绝对时刻；没有别的连接可换时，调用以最后一次尝试的状态结束。`call_result::not_executed` 把同样的判断交给调用方。

```cpp
rpc::channel channel{shard, {endpoint_a, endpoint_b}};
CO2_AWAIT_SET(ready, channel.wait_ready(deadline));
echo = channel.bind("demo/Echo");
CO2_AWAIT_SET(result, channel.call(echo, request, response));
```

## 流式调用

覆盖 client streaming、server streaming 和双向流：

- **消息：** 按 `stream_fragment_bytes`（默认 16 KiB）和对端帧上限中较小的一个分片，首片带 9 字节描述符；空的 MESSAGE|END_STREAM 表示半关闭。
- **流控：** 每条流一个窗口，一条消息按 `max(1, 大小)` 计费；读方拿到的消息是视图，到下一次读时释放并返还 WINDOW_UPDATE，写方额度不足时等待；大于整个窗口的消息直接判 `resource_exhausted`。
- **发送轮转：** 发送缓冲里待写的字节少于 `stream_send_budget`（默认 256 KiB）时，流才能继续分片，否则排队；写协程每写出一批，就从排队的流里轮流各取一片。REQUEST、END 和控制帧不受这个预算限制，所以其它调用在用户态最多排在约一个预算的流数据后面。写操作借用调用方的字节，整条消息分完片才完成。
- **轮次：** 套接字读写可能同步完成，所以读协程每解析约一个接收缓冲区的流消息字节就让出一次；有流在排队时，写协程每写一批也让出一次，免得批量数据压住同线程上的另一个方向。一元调用的帧只按 `frames_per_turn` 计数，回复照常成批写出。
- **缓冲上限：** 每条流缓存的消息条数上限是 `max_buffered_messages`。

```cpp
CO2_AWAIT_SET(opened, client.open(chat, rpc::method_kind::bidirectional, &spec));
CO2_AWAIT_SET(wrote, opened.stream.write(message));
CO2_AWAIT_SET(read, opened.stream.read());      // read.message / read.ended
CO2_AWAIT_SET(wrote, opened.stream.writes_done());
CO2_AWAIT_SET(result, opened.stream.finish(&trailer));
```

流消息可以压缩（构建时开 `LRPC_ENABLE_COMPRESSION`，依赖 zstd 和 LZ4）：两端都在 `receive.features` 里打开 `wire::message_compression`，在 `receive.compression` 里声明支持的算法，发送方用 `preferred_compression` 选算法。超过 `compression_threshold` 且压缩后变小的消息才压缩；接收方按描述符里的解码长度精确解压，这个长度同样受消息上限和窗口约束。一元单帧的线上语法不带压缩。

服务端用 `stream_method_handler` 接收 `server_stream&`，handler 返回时发 END；client streaming 必须恰好写一条消息，server streaming 必须读到一条消息和半关闭，否则以 `invalid_argument` 结束。按流发送的一元调用由服务端交给普通的一元 handler。开流时的截止和等待方的 stop token 覆盖整条流。

响应 metadata 和状态说明走 END head：handler 调 `response.set_trailer(message, metadata)` 或 `context.set_trailer(...)`，上限是方法的 `max_trailer_bytes`；客户端给 `call()` 传 `response_trailer*` 并提供存储，放不下时 `truncated` 为真。按线协议，状态说明只随错误状态发送。

## 类型化调用

核心只认两种 body，类型化层建立在它们之上，不给核心增加分配或拷贝：

- `request_body`：一段字节，或一个消息加编码器。精确编码器（如 protobuf）拿到恰好 `size()` 的空间；一次编码的编码器（如 JSON）拿到至多上界的空间，帧里剩余更少时只拿剩余部分，写不下判 `resource_exhausted`。
- `response_body`：一段缓冲，或一个消息加解码器；END 到达时在读协程里就地解码进调用方的消息。

`<rpc/typed.hpp>` 提供：

- `method<Request, Response, Policy>{name}`，`Policy` 决定编码（`default_codec_policy` 走 `codec<T>` 特化，另有 `json_codec_policy`、`mapped_protobuf_codec_policy`）；
- `bind(client_or_channel, method)` 得到 `bound_method`，`operator()` 返回 `unary_call`；
- `bind_method(method, service, &Service::Fn, limits)` 生成服务端 binding，适配器由 binding 持有；
- 流式的 `bound_stream`、`typed_client_stream`、`typed_server_stream` 和 `bind_stream_method`。

类型化一元调用两端稳态都是 0 次堆分配。编码失败在发送前判 `invalid_argument`（`not_executed`）；服务端解码失败判 `invalid_argument`，内存不足判 `resource_exhausted`；服务端编码失败或回复超出 `max_response_bytes` 判 `internal`；客户端解码失败判 `internal`。

### 编码标识与协商

同一个方法名可以按编码各注册一个 binding（`method_binding::codec`，例如 `"json"`、`"proto"`）。编码标签挂在方法定义上，不随每次调用发送：

- **协商：** 双方在 SETTINGS 的 features 里都声明 `wire::method_codecs`（0x08，默认开启）才启用；任一方没有声明时，连接按原来的语法工作，标签被忽略。
- **线上格式：** 启用后，带 NEW_METHOD 的 REQUEST head 在方法名之后多一个长度前缀的标签字段，可以为空，至多 64 字节可见 ASCII。之后同一连接上的调用只带方法 ID，没有额外字节和解析。
- **服务端选择：** 带标签的调用交给同名同标签的 binding，没有则交给同名的无标签 binding（它兜底接收任何编码）；不带标签的调用交给无标签 binding，没有则交给最先注册的那个。都没有时，这个方法 ID 在该连接上返回 `unimplemented` 并标记未执行，调用方可以换一种编码重试。
- **标签来源：** 类型化层由 policy 给出（`rpc::codec_label<Message, Policy>()`）：`json_codec_policy` 是 `json`；生成的 protobuf 消息和 `mapped_protobuf_codec_policy` 是 `proto`，因为两者在线上是同样的字节；自定义 `codec<T>` 可以提供 `static char const *label()`。原始字节的 `bind(name)` 和 `method_binding` 默认不带标签。

请求只能按客户端选定的编码发送，因为方法定义和第一次调用在同一帧里，没有往返。所以“协商”就是客户端声明编码、服务端按规则选择或明确拒绝，不由服务端替客户端换编码。

`<rpc/service.hpp>` 的 `service_contract<Policy, Methods...>` 把一组方法的名字和类型放在一起，服务端 `bindings(...)` 与客户端 `bind_service(...)` 共用，见 [共享服务契约](service-contract.md)。protobuf 服务由 `protoc-gen-rpc` 生成，见 [protobuf 用法](protobuf.md)；普通结构体的 JSON 见 [JSON 用法](json.md)。

## 实测

同线程回环，epoll，CPU 钉在一个核上，5 轮交错取中位数（`benchmarks/unary_bench --transport net|rpc`）：

| 场景 | 裸 net | lrpc |
| --- | --- | --- |
| 64 B，1 在飞，p50 | 3.25 µs | 3.41 µs |
| 64 B，1 在飞，用户态 CPU/调用 | 772 ns | 1240 ns |
| 64 B，64 在飞，吞吐 | 3.22 M/s | 3.16 M/s |
| 64 B，64 在飞，p50 | 14.6 µs | 14.3 µs |
| 4 KiB，1 在飞，p50 | 3.57 µs | 3.95 µs |
| 4 KiB，64 在飞，吞吐 | 1.03 M/s | 0.88 M/s |
| 峰值 RSS | 5.8 MB | 5.9 MB |

1 在飞时比裸 net 多的约 0.16 µs 来自结构：每端一个读协程、一个写协程，以及经 `any_stream` 类型擦除的读写。

同一条连接上另有一条批量流时（`benchmarks/mixed_bench`：两端各一个线程，回环 TCP，客户端持续写批量消息，同时逐个发 64 B 一元调用），3 轮取中位数：

| 批量消息 / 流窗口 | 一元 p50 | 一元 p99 | 批量吞吐 |
| --- | --- | --- | --- |
| 无批量流 | 7.0 µs | 8.2 µs | — |
| 64 KiB / 256 KiB | 21.6 µs | 33.7 µs | 8.0 GiB/s |
| 1 MiB / 1 MiB（默认窗口） | 87.7 µs | 96.4 µs | 9.2 GiB/s |
| 1 MiB / 4 MiB | 276 µs | 382 µs | 10.6 GiB/s |

在用户态，一元帧最多排在约一个发送预算之后。剩下的等待来自窗口内已写进内核缓冲的批量数据，略小于窗口除以吞吐。改成分片轮转之前，4 MiB 窗口那一行是 p50 1.1 ms、p99 1.9 ms、2.1 GiB/s。

`gate_tests` 把客户端和服务端放在两个线程上，稳态下逐项断言：

- 每次调用两端都是 0 次堆分配，原始字节、带截止和类型化调用都一样；一条完整的原始字节流（打开、各一条消息、半关闭、结束）也是 0 次：流对象和收件箱取自 shard 的 slab，服务端没被请求停止、handler 也没取走 token 的停止状态留给下一条流；
- 不带截止时 0 次时钟读取，带截止时每端 1 次；
- 64 在飞时两端都是每 64 次调用 1 次写；4 KiB 调用一批超出一个接收缓冲区，就绪型后端下服务端仍是每 64 次 1 次写（io_uring 每读完一个接收缓冲区写一次）。

## 尚未覆盖

- 写进内核收发缓冲的字节无法再调整顺序：与批量流共用一条连接的调用，仍要排在流窗口内已写出的数据之后（见实测）。对延迟敏感的调用，宜给批量流用较小的窗口或单独的连接。
- 类型化流的编码缓冲每条流分配一次。
- 已发出且结果未知的调用不重试。同一目标的各条连接各自解析，不共享结果；`close()` 打断不了进行中的 getaddrinfo，context 要等它返回才能排空。
- 从 handler 协程环境读到的 stop token（`net::this_coro::stop_token`）不能留到 handler 结束后：没被请求停止的停止状态会交给后续调用。要留下来用的 token 须经 `server_context::stop_token()` 取，取过的停止状态不再交出去。
- 客户端事先不知道服务端支持哪些编码：标签不匹配要等第一次调用，才以 `unimplemented`（未执行）暴露。
