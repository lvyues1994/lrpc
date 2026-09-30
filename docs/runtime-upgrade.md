# RPC 运行时升级

第一阶段完成架构升级的 1–4 步：调用生命周期与 API 比较、固定调用槽与服务端 worker、共享精确截止调度器、方法绑定与状态 API。
公开调用继续返回 `net::task<call_result>`；用户选择先保留现有 `CO2_AWAIT`、`run_async` 和 `run` 用法，后续再评估专用 `unary_call`。

## 调用与缓冲的所有权

| 资源 | 持有者 | 可复用时刻 |
| --- | --- | --- |
| 客户端调用槽 | client 的固定槽池 | 完成通知被消费、取消 callback 注销并等待返回之后 |
| 已排队请求 | 请求块中的槽下标与 generation | writer 检查 generation；失效帧可丢弃，已提交帧须写完或关闭连接 |
| 服务端调用槽 | server 的固定槽池 | handler task 及其 Arena 析构、请求 lease 释放之后 |
| 响应块与连接占额 | handler，随后移交 tx / writer | 部分写、取消或关闭的 I/O 完成被消费之后 |
| 截止节点 | 调用槽内嵌节点 | 完成、取消或到期时先从共享堆摘除，再允许槽复用 |
| worker / 截止 driver | 激活的端点 | 关闭或排空后，没有可接受工作的 listener、连接与 handler 时退出 |

客户端 pending 流号索引使用固定开放寻址表，删除后移补洞，不累计 tombstone。
完成、截止、取消和关闭竞争同一个原子阶段；获胜者只投递一次 continuation。
外线程取消只竞争阶段并投递通知，流表、截止堆、CANCEL 帧和槽回收仍在所属分片执行。
`.stats().active_calls` 统计 pending 流号索引；已通知但尚未消费结果的槽仍占用池容量。

服务端每个槽有一条可休眠、可重新启动的长期 worker，调用只向它发信号。
worker 用公开 `task::await_suspend` 启动用户 handler，槽内 child environment 传播执行器、帧分配器和本次 stop token。
每次仍新建 `StopState`；用户持有的旧 token 不会关联下一代调用。忽略取消且不退出的 handler 继续占额。

构造 client / server 不启动后台工作。实际连接或监听时激活，关闭后须继续运行 context 排空。
监听启动失败会回滚已启动的后台工作；accept 意外失败会关闭服务。正常 `drain()` 保留已接纳调用直到完成。
API、配置和析构限所属分片线程；只有请求 stop token 可来自其它线程。codec 禁止递归驱动同一 context。

## 截止调度

同一 `io_context` 的 client 与 server 共用一份固定容量最小堆、一条 driver 和一个 `steady_timer`。
冷构造按各端点最大调用槽数预留配额，构造失败由 RAII 归还；准入、摘除和到期不分配堆节点。
插入与删除 O(log N)，使用原始绝对时刻，不按毫秒桶取整。到期不得提前；实际执行仍可能因调度停顿而延后。

新较早截止或堆变空会取消当前 wait；只有 driver 消费旧 wait 的完成后才能重新武装。
到期节点先摘除再回调，每处理 64 个到期项主动让出。无有限截止时不武装 timer。
timer 的非取消错误使调度器失效，已有有限截止以 `unavailable` 完成，后续有限截止准入拒绝。
已有端点的无截止调用仍可执行；新端点不能激活该失效调度器。所有持有者释放后可创建新调度器。
net 的冷定时器堆扩容 OOM 仍可能直接终止进程，见 [依赖限制](net-timer-oom.md)。

## 冷注册与状态

`client::bind(method_descriptor)` 返回只适用于该 client 的 `method_handle`；批量 service 绑定失败时回滚全部新增项。
`client_options.max_registered_methods` 默认 4096，独立于每连接线上 method ID 上限。
描述符 `ordinal` 是 service 内位置，handle 是 client 内稠密位置，线上 ID 则在第一次成功准入时分配。
绑定调用经过首次驻留后直接使用缓存位置。字符串接口可与 handle 调用混用，NEW_METHOD 仍在 writer 提交时发布。
空、外来或 codec 不匹配的 handle 返回 `invalid_argument`。

生成的非空 Stub 在构造时批量绑定 service，可能分配或抛异常。`server_builder` 收集 bindings，在 `build()` 验证并冻结；
成功后不可复用，失败可重试。生成的 `add_<Service>_service` 接入 builder；原 `make_server` 和 `<Service>_bindings` 保留。

`status` 拥有 `.code` 与二进制 `.message`，`call_result` 继承这两个字段，没有第二份可写 code。
生成服务仍返回 `task<status_code>`；可调用 `set_status(context, {code, message})` 立即复制说明。
手写 `bind_method` 同时支持 `task<status_code>` 和 `task<status>`。
说明与 metadata 共用 `max_response_head_bytes`，默认 2 只容纳空 head；超限是 sticky 的 `END(internal)`。
OK 不允许说明；未知码或非法成功说明不会发出非法 END。客户端复制错误说明，返回值不借用接收窗口；复制 OOM 返回无说明的 `resource_exhausted`。
`std::error_code ec = result.code` 使用 RPC category，取消与超时分别匹配 net 条件；`status` 本身不隐式转换。
handler 异常映射为 `internal`，不向对端发送异常说明；日志钩子仍待实现。

本轮保留 task 与原 `.code` 用法，但公共结构和虚接口的 ABI 已改变，库与调用方须一起重新编译。
自定义 `rpc::client` 派生实现需补新增 bind 和 handle 调用入口；service、请求与回复的借用寿命约定不变。

## 验证与性能边界

第一阶段验证：Debug 18 项；protobuf Debug、Release、ASan/UBSan/LSan 各 24 项；TSan 17 项全部通过，四个网络后端均执行。
新增覆盖代际复用、堆容量/早插入/取消重挂/失效退出、监听启动和构造 OOM 回滚、绑定批量回滚、状态说明寿命与复制 OOM、builder 重试、生成器符号遮蔽。
发行版 protobuf 二进制自身未插桩；TSan 排除全局 new 故障注入程序。

第一阶段的 `api_probe` 只比较无网络表示成本：当时 co2 内联空间 192 B，原型直接 awaiter 144 B，A 每次 1 次包装分配、B 为 0。
后续 call_options 扩展后需重新运行，不能沿用 144 B 作为当前接口尺寸。
该原型没有完整 RPC 生命周期，不能证明网络 p99 收益，也不是已交付的 B 接口。
字节/protobuf 配对结果、分配统计与仍未通过的验收项见 [运行时实测](../benchmarks/results/2026-09-30-runtime.md)。

## 后续阶段：5、6、7、8、10、11

### 缓存与性能选择（5）

公开调用仍是 `net::task<call_result>`。默认使用系统帧分配器、每调用 protobuf Arena；
`method_limits.message_cache_entries` 为零。显式设置非零值可冷分配有界 Arena 槽，调用持有独占槽直到
解码、业务挂起及编码全部退出；归还时 Reset 并重建消息，扩展块释放。满额返回 `resource_exhausted`。
共享 adapter 的槽管理使用 mutex；不同分片需要独立缓存时应提供独立 bindings。
固定缓存容量计入 `storage_bytes`，活动 Arena 的扩展块和用户对象仍不属于该统计。
帧回收通过 net 的分配器显式配置，寿命必须覆盖该 context 的全部协程；不默认启用。
五轮配对实验比较帧缓存、Arena 缓存和不同入口，见 [后续阶段实测](../benchmarks/results/2026-09-30-upgrade.md)。
小消息 p99 未显示稳定缓存收益；Arena 缓存减少测得的申请字节，显式选择时仍需承担冷缓存内存。
默认 client 仅捕获实际使用的截止/metadata 字段，避免复制 channel 专用治理参数；64 B/64 在飞的回退尚未消除。

### channel、分片与跨执行器调用（6、7）

`<rpc/channel.hpp>` 提供单分片连接池，`<rpc/runtime.hpp>` 提供固定线程/执行器、分片 channel 与 server。
它们链接 `lrpc::runtime`。最小配置如下，方法注册仍在 `start()` 前完成：

```cpp
auto owner = rpc::make_runtime({2, net::backend_kind::epoll});
rpc::channel_options options;
options.resolve = rpc::make_static_resolver({endpoint});
options.max_waiting_calls = 64;
options.max_waiting_bytes = 1024 * 1024;
auto channel = rpc::make_channel(*owner, options);
auto method = channel->bind(descriptor);
owner->start();
// 外部执行器的协程中：
rpc::call_options call;
call.timeout = std::chrono::seconds{1};
call.wait_for_ready = true;
CO2_AWAIT_SET(result, rpc::call(*channel, method, request, reply, call));
CO2_AWAIT(channel->shutdown(std::chrono::milliseconds{100}));
// 外部线程，在调用协程排空后：
owner->shutdown();
```

单分片 channel 只能在 owner 上使用；runtime facade 可从外部执行器进入。owner 分片上发起的调用使用本分片 actor，
外部调用轮转选 actor，并在完成后回到原执行器。跨线程帧用系统资源，避免依赖 caller 的线程本地缓存。
`call_sync(runtime, target, ...)` 供外部同步线程使用；从任意 runtime worker 调用都返回 `failed_precondition`。
service 若有可变状态，必须能并发使用，或为各分片提供独立实现/bindings。
server 在接入分片 accept，按 round-robin 移交 socket；没有实现 SO_REUSEPORT 模式。

静态和 DNS resolver 提供 backend 集，channel 限制实际连接数；DNS 使用 net resolver，停止仍需等后台解析结束。
连接按需建立，失败后指数退避并加入 jitter；支持 round-robin 和 power-of-two 选择。
`connections_per_backend`、`max_connections`、`max_retired_connections` 共同限制连接数量，额度包含尚未排空的 retired session。
idle timeout 仅回收空闲连接，后续需求可重新连接；max age/GOAWAY 使旧连接排空并轮换。
keepalive 在 PING 实际写完后开始等待 PONG，排队中的 PING 不消耗响应超时。
单分片 `warmup()` 在至少一个连接 READY 后成功；runtime facade 则等待每个 actor 至少一个 READY，共用一次绝对截止。

`channel::shutdown(grace)` 停止新准入，宽限内等待既有调用，届满再取消并排空；借用存储不能随取消请求提前销毁。
`server::shutdown(grace)` 到期后请求关闭；其返回（或等待被取消）不保证所有 handler/I/O 已排空，仍须继续驱动 context。
runtime 停止数据投递后，仍使用冷预留的控制投递关闭资源，排空完成才 join；不通过 `io_context::stop()` 丢弃在飞协程。
不响应取消的 handler 和后台 DNS 可能延长实际返回时间。启动线程失败会关闭冷注册资源、排空并 join 已启动的 worker。

### 准入、重试与观测（8）

默认快速拒绝。`wait_for_ready` 等待可用连接，`wait_for_capacity` 等待容量；须同时配置有限队列且有有限截止。
FIFO 同时计费等待调用数和编码请求 head/body 字节；超限返回 `resource_exhausted`，队列中的取消与截止立即退出。
`max_logical_calls` 还限制活跃、排队和重试退避中的逻辑调用。默认单帧一元 server 另支持执行前的有界 FIFO，
过期或取消的排队项不会进入业务函数。流式 server 当前直接准入，不接受非零 `max_queued_calls`。

`call_options.retry.max_attempts` 默认 1。更多尝试只在配置的状态码上重试，沿用原始截止和 stop token。
未知/非幂等方法只有明确未执行证明才允许重试；可能已经执行的请求必须带已冷绑定的幂等描述符。
`NOT_EXECUTED` 是 SETTINGS 协商后的拒绝证明，GOAWAY 只证明截止流号之后的调用未执行。
请求只编码一次，拥有型重放、临时回复和 metadata 受 `replay_bytes` 限制；最终选定响应才解码到用户对象。
结果提供 `attempts`；流式调用拒绝 `max_attempts != 1`，不重放消息。

interceptor 在 owner 上执行逻辑调用和实际 attempt 的 before/after；after 按进入顺序逆序执行。
before 可本地拒绝；hook 异常映射为 internal 并交给 `on_exception`，异常文本不发送到 peer。
`make_trace_interceptor(sink)` 输出调用 ID、attempt、起止与状态；callback 内视图需复制才能保留。
`channel::metrics()` 与 `export_metrics(ostream, snapshot)` 提供计数、状态分类、排队/调用耗时及资源快照的 JSON 导出。
runtime 查询通过 owner 获取完整快照，无法投递时返回上次完整缓存；关闭后查询已 join 的状态。
流式逻辑调用的 after/指标在流终止时触发，而不是 open 返回时。hooks、sink 须保活到 channel 排空。

### 流式协议与生成接口（10）

默认单帧一元路径保留。客户端显式设置 `receive.features |= wire::streaming` 和正数 `initial_stream_window`；
服务端存在非 unary binding 时自动启用 streaming profile。扩展客户端面对未协商 streaming 的旧服务端返回 unimplemented；
扩展服务端仍接受旧客户端的小消息 unary REQUEST/END。profile 在握手后固定，不能在同一连接切换。

生成器支持四种 method kind，包括混合 service。流式 Stub 返回 `task<stream_call<Request, Response>>`；
服务函数接收 `server_stream<Request, Response>&` 并返回 `task<status_code>`，用法见 [protobuf 流式示例](protobuf.md#流式接口)。
流固定在开流 session 上，runtime facade 将每次 read/write/half-close/finish 投递到该 owner。
每方向最多一个在飞操作，可以同时读和写。`writes_done()` 只半关闭发送方向；`finish()` 等最终 END。
空消息是 `read{ok, ended=false}`；EOF 为 `ended=true`。client-streaming 必须恰好返回一条响应，server-streaming 必须恰好接收一条请求。
unary 两方向各一条消息，响应在 END(OK) 后才解码。同步或协程内 handler 异常只结束当前流，其他流继续使用连接。
客户端 typed stream 析构请求取消；取消仍须驱动 owner 到完成。
开流的截止、父 stop token、请求 metadata 和响应 metadata 缓冲覆盖整个 RPC，须保活到终止结果。

扩展 REQUEST 不携带 body；每条 MESSAGE 的第一片有 9 字节描述符：`u32 encoded_size, u32 decoded_size, u8 algorithm`。
后续片不带 head；MORE 必须与描述符剩余长度一致，COMPRESSED 只出现在压缩消息第一片。
半关闭使用没有 head/body 的 MESSAGE|END_STREAM。END 仅携带状态及 metadata；成功 END 不允许中断未完整重组的消息。
每条连接一条写链，以每流至多一片待写实现 FIFO 交错；控制帧优先，REQUEST 与对应 CANCEL 保持线序。

非 unary 流窗口扣除整条消息的 `max(encoded_size, decoded_size, 1)`；消息超过初始窗口直接拒绝。扩展 unary 不扣逐流窗口。
额度在最终接收 buffer/切片释放后返还，空消息同样有 receipt；`max_buffered_messages` 另外限制数量。
读取到同一个 byte_buffer 时会先释放上一条结果；若业务仍持有副本/切片，额度继续被占用。
压缩、分片和保留接收结果因此不会绕过背压。

### 拥有型缓冲与压缩（11）

`lrpc::message` 不依赖 net。`byte_buffer` 的 copy/adopt、slice/join 持有不可变共享存储；切片按底层容量计费，
同一存储在 retained_capacity 中去重，join 默认最多 64 段。builder 只在 finish 前允许写入；adopt 后旧可写别名不能再使用。
codec 的借用 decode 必须复制；拥有型 codec 可保留 buffer。owned 发送走 gather，不为每条消息再做完整拷贝；
非同预算来源的存储按完整保留容量预留额度。部分写或取消后，帧所有者仍保留到 I/O 完成。

接收 header 先校验长度，再分配拥有型帧；单片消息可直接移交该存储，分片消息预留重组容量后复制。
每连接 payload budget 可挂接 server 的共享父 budget，受两级物理容量约束。
`connection_options.auxiliary_bytes` 约束接收窗口、source 缓冲、压缩工作区和 receipt/取消通知预留；
server 的 `auxiliary_bytes` 限制这些连接的共享额度，空消息 receipt 也占通知配额。
固定辅助额度在握手后激活数据面时校验；接收窗口和工作区此前已经分配，握手峰值另受 `max_connections` 限制。
流结束后保留的 buffer 仍计费，可晚于连接及 context 析构，并在其他线程释放；关闭封住通知入口，晚释放不会触碰已销毁 executor。
`storage_bytes` 汇总 payload、控制池、接收窗口、source 缓冲、压缩工作区及固定 Arena 缓存，
不包括所有 C++ 对象/容器/分配器开销和用户内存，不是 RSS 或完整堆上限。
流式 server 的 storage_bytes 只汇总仍登记的 session；已回收 session 的外部保留 buffer 仍计入共享
request/response_bytes_in_use，但不再计入该汇总，不能仅凭 storage_bytes 判断所有保留存储已释放。

`use_receive_source` 接管 TCP 读方向，固定 4×16 KiB；握手残留先消费，pull 后按字节 consume。
当前 net 无可分离租约，因此复制到 RPC 存储后立即归还后端缓冲；没有实现内核接收至业务的端到端零拷贝。
常规 owned read 和 receive_source 两条路径均覆盖四个后端。

`LRPC_ENABLE_COMPRESSION=ON` 需要 zstd 和 LZ4 开发包；关闭时不引入两者依赖，协商 bitmap 为零。
双方开启 `wire::message_compression` 并公布算法位图后，`preferred_compression` 选择 zstd(1) 或 LZ4(2)，
`compression_threshold` 控制起点，小消息默认不压缩。zstd 只接受一帧，拒绝尾随/拼接/跳过帧；LZ4 使用 block 格式。
解压前校验编码长度、声明的解压长度、消息上限和配额；编码块与解压块同时存活时都计费。
压缩工作区冷分配并属于单连接 owner，不在消息间共享可变压缩状态。

本阶段没有接入 Unix 域/TLS（9）、SO_REUSEPORT、专用 unary_call 或第三方追踪后端；§13 的全部性能场景仍需分别验收。

本阶段验证：Debug 30 项，protobuf Release、ASan/UBSan/LSan 各 40 项，TSan 28 项，四个后端全部执行；wire libFuzzer 10 万轮通过。
TSan 关闭 protobuf，并排除全局 new 注入与 pthread_create 拦截；发行版 protobuf 本身未插桩。
线程启动失败回归在 Release 和 ASan 均执行；ASan 下成功创建委托 sanitizer interceptor，避免绕过线程登记。
