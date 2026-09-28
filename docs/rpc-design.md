# 基于 net 的高性能 RPC 库：架构与实现设计

本文是一个新库（下文称 rpc，命名空间 `rpc`）的设计规格：它建在 net 之上，给出从线协议到代码生成的
全部组件、组件之间的契约、热路径上每一步的做法与代价，以及实现顺序。

## 0. 前提

以下五项是设计的出发点；改变其中任一项时，括号里的章节要重做：

1. **自有二进制协议**，只与自己互通；协议层留接缝，以后可以加 gRPC（HTTP/2）适配（§4、§16）。
2. **编解码可插拔**，protobuf 是第一个实现（§9）。
3. **一元调用 + 双向流**（§4.7、§6.5、§7.3）。
4. **与 net 一致：C++14 + co2 宏协程**，命名沿用 net 的 snake_case（全文代码）。
5. **第一版范围**：点对点 + 客户端连接池 / 负载均衡 / 服务发现接口；限流熔断链路追踪只留拦截器接缝（§7、§12）。

## 1. 问题与性能契约

**问题**：让一个进程里的协程以函数调用的形式调用另一个进程的方法——带类型的请求与响应、截止时间、取消、
流式消息——并在多核上把每次调用的额外开销压到接近裸套接字往返。

**首要验收目标**：在明确的请求速率和错误率约束下，降低 64 B / 4 KiB 一元调用的 p99。吞吐、分配次数与多核扩展
作为辅助指标；过载行为按 §5.6 的准入规则执行。具体负载点、p99 门槛和允许的拒绝率待首轮基准后确定。

**性能预算**（以下均为待验证目标，不是已达到的指标）：基线来自 net 在 i7-13700KF / Linux 7.0 / GCC 13 上的
同进程、两端同线程回环测试。3.1 µs 是该条件下的往返基线，不是跨进程 p99；RPC 与裸 net 按 §13 在相同拓扑下比较。

| 指标 | 基线（net 实测） | 目标 |
| --- | --- | --- |
| 64 B 一元调用往返，单连接、一个在飞 | 裸 `read_some` / `write_some` 往返 3.1 µs | ≤ 3.8 µs（额外 ≤ 0.7 µs） |
| 每次一元调用的堆分配（稳态） | — | 客户端 0；服务端 ≤ 1（§11） |
| 小消息吞吐，单分片服务端，流水线 64 个在飞 | — | ≥ 100 万次 / 秒 |
| 多核扩展 | 同一 `io_context` 4 线程只有 1.8×（32 连接 ping-pong） | 分片数 × 单分片吞吐的 90% 以上 |
| 截止时间的每次调用成本 | `net::timeout()` +153 ns、1 次分配；io_uring 上 +580 ns | ≤ 20 ns、0 次分配 |

## 2. 组件

```
            ┌──────────────────────── 生成代码（codegen）────────────────────────┐
            │   XxxStub（客户端存根）            XxxService（服务端接口 + 方法表）  │
            └───────────┬───────────────────────────────────┬──────────────────┘
                        │ 调用                               │ 注册
                ┌───────▼────────┐                  ┌────────▼────────┐
                │ client          │                  │ server           │
                │ channel / LB /  │                  │ 接入 / 分派 /    │
                │ resolver / 重试 │                  │ 调用上下文池     │
                └───────┬────────┘                  └────────┬────────┘
                        │           ┌──────────────┐          │
                        └──────────►│ connection   │◄─────────┘
                                    │ 读协程 / 写协程│
                                    │ 流表 / 收发缓冲│
                                    └──┬────────┬──┘
                           编解码 codec │        │ 帧 wire
                                    ┌──▼──┐  ┌──▼──┐
                                    │codec│  │wire │   ← 纯函数，无 I/O
                                    └─────┘  └─────┘
            runtime：N 个分片（每个 = 一个 io_context + 一个线程），截止时间轮、池、分片间投递
            ─────────────────────────────── net ───────────────────────────────
```

| 组件 | 职责 | 依赖 |
| --- | --- | --- |
| `wire` | 帧头与各帧 head 段的编码 / 解码：缓冲区进、结构体出，不做 I/O | 无 |
| `codec` | 消息 ⇄ 字节：静态的 `codec<M>` 特化（存根用）+ 擦除的方法处理器（服务端分派用） | 无（protobuf 实现依赖 libprotobuf） |
| `connection` | 一条传输连接上的多路复用：读协程解帧、写协程合并发送、流表、背压、保活、关闭 | wire、net |
| `server` | 接入与分片分配、方法表、调用上下文池、启动处理协程、取消 / 截止 / 过载、优雅关闭 | connection、codec |
| `client` | channel（每分片子通道）、解析器与负载均衡接口、一元与流式调用、重试 | connection、codec |
| `runtime` | 分片（线程 + `io_context`）、截止时间轮、每分片的池与分配器、分片间投递 | net |
| `codegen` | protoc 插件：生成存根、服务接口与方法描述符 | libprotoc |

分成七块的理由：`wire` 与 `codec` 是可以单测、模糊测试的纯函数，放进 `connection` 就只能通过套接字测；
`connection` 是客户端与服务端共用的热路径，拆成两份会让写合并、流控各实现一遍；`runtime` 独立，是因为
线程模型（§3）是可替换的决定，客户端与服务端都不该知道分片怎么建；`codegen` 是构建期工具，不进运行时。
再合并任何两块都会让一个修改点波及两种职责；再拆（例如把流控单列）得不到新的测试或替换接缝。

## 3. 线程模型：每核一个分片

**决定**：`runtime` 起 N 个分片，每个分片是一个线程独占运行的 `net::io_context{kind, net::single_thread_hint}`。
一条连接终生属于一个分片；连接上的读、写、流表、调用上下文、处理协程都在这个分片上，热路径没有锁、没有原子
读改写（除了跨线程取消，§7.4）。

依据（net 实测）：

- 同一 `io_context` 多线程 `run()`：32 连接 ping-pong 在 4 线程上 1.8×，io_uring 只有 1.42×——反应器锁、调度器锁
  与线程交接占掉了剩下的部分。每线程一个 `io_context` 才能线性扩展。
- `single_thread_hint` 让 io_uring 用 `SINGLE_ISSUER | DEFER_TASKRUN`；就绪型后端走单线程就地执行路径。
- 分片内的 `post` 一跳 13 ns，调用完成与处理协程启动都走它（§5.5）。

规则：

- **亲和**：服务端连接由接入分片分给某个分片（§6.1）；客户端在哪个分片上发起调用，就用这个分片自己的连接（§7.1）。
- **分片间**只有两种交互：投递（`io_context::post` 本身线程安全）与只读共享（启动后不可变的方法表、解析结果快照）。
- **重活移出**：处理协程里的 CPU 密集段用 `CO2_AWAIT(net::run(pool.get_executor())(heavy(...)))` 切到
  `net::thread_pool` 再回来，分片线程不被占住。
- **帧分配器**：每个分片 `ctx.set_frame_allocator(&ctx.recycling_frame_allocator())`，分片内的协程帧（处理协程、
  内部协程）走回收式分配器；分片线程上 malloc 不是瓶颈时保持默认也可以，基准决定。

## 4. 线协议

全部多字节整数小端；变长整数是 uint64 无符号 LEB128（下文 `varint`），只接受最短表示，最多 10 字节，
第 10 字节只能是 1；非最短表示、溢出和未结束的第 10 字节均非法。定长字段按字节读取或 `memcpy` 后转换字节序，
不依赖结构体布局、主机端序或对齐，不做指针别名转换。

### 4.1 连接前导与 SETTINGS

客户端连上后先发 8 字节魔数 `"NRPC" 00 01 0D 0A`（最后两字节防误连文本协议，`00 01` 是协议版本），紧跟一个
SETTINGS 帧；服务端回一个 SETTINGS。客户端收到并验证服务端 SETTINGS 后才发送 REQUEST，以对端公布的限制
约束首个请求；该等待只发生在建立连接时，也计入调用的绝对截止。

首版每端只发送一次 SETTINGS，参数在连接存续期间固定，变更需新建连接。服务端按线序先处理客户端 SETTINGS，
再准入后续 REQUEST；§5.9 的响应预留以这组对端接收限制为依据。重复 SETTINGS 或在它之前收到 REQUEST 属于连接错误。

SETTINGS 的 head 是若干 `varint key, varint value`：

| key | 含义 | 默认 |
| --- | --- | --- |
| 1 `max_frame_size` | 本端愿意接收的最大帧载荷 | 4 MiB |
| 2 `max_message_size` | 本端愿意接收的最大消息（分片重组后） | 64 MiB |
| 3 `max_concurrent_streams` | 本端同时处理的流数（服务端的过载界限） | 1024 |
| 4 `initial_stream_window` | 流式调用每流的初始接收窗口（字节） | 1 MiB |
| 5 `max_method_ids` | 驻留方法表的上限（§4.5） | 4096 |
| 6 `compression` | 支持的压缩算法位图（bit0 zstd、bit1 lz4） | 0 |

空 SETTINGS head 使用全部默认值；省略的键使用其默认值。未知键及其重复项解析后忽略，重复已知键拒绝。
已知值限定为 uint32，wire 层允许 0（例如不接纳调用、只接受空消息）；连接层另行验证本地配置和协商结果是否可用。
第一阶段本端公布 `compression = 0`。

### 4.2 帧头（16 字节）

| 偏移 | 大小 | 字段 | 说明 |
| --- | --- | --- | --- |
| 0 | 4 | `length` | 载荷字节数（不含帧头），≤ 对端的 `max_frame_size` |
| 4 | 4 | `stream_id` | 0 = 连接级帧 |
| 8 | 1 | `type` | 见 §4.3 |
| 9 | 1 | `flags` | `END_STREAM 0x01`、`COMPRESSED 0x02`、`MORE 0x04`（消息未完，续在下一帧）、`NEW_METHOD 0x08` |
| 10 | 2 | `head_length` | 载荷开头的 head 段长度；载荷其余部分是 body（消息字节） |
| 12 | 4 | `aux` | 按类型解释的定长字段，一元热路径上常用的值都放这里，免得解 varint |

帧头定长，解码按上述偏移取字段并检查范围；一次 `read_some` 读进来的多个帧在同一缓冲区里顺序解析。

### 4.3 帧类型

| type | 名称 | `stream_id` | `aux` | head 段 | body |
| --- | --- | --- | --- | --- | --- |
| 1 | SETTINGS | 0 | 0 | 键值对 | 空 |
| 2 | REQUEST | 新流 | 方法引用（§4.5） | 截止、驻留名、元数据（§4.6） | 第一条请求消息（可空） |
| 3 | MESSAGE | 已有流 | 0 | 空 | 一条消息（或其一片） |
| 4 | END | 已有流 | 状态码（§10） | 状态消息、尾部元数据 | 一元：响应消息；流式：空 |
| 5 | CANCEL | 已有流 | 原因码 | 空 | 空 |
| 6 | WINDOW_UPDATE | 已有流 | 窗口增量（字节） | 空 | 空 |
| 7 | PING | 0 | 不透明值，PONG 原样带回 | 空 | 空 |
| 8 | PONG | 0 | 同上 | 空 | 空 |
| 9 | GOAWAY | 0 | 已处理的最大 `stream_id` | 原因（UTF-8） | 空 |

REQUEST 的 head 段：

```
uint64_le timeout_us              // 固定 8 字节，0 = 没有截止
[NEW_METHOD] varint name_len, name  // 驻留名，例如 "demo.Echo/Echo"
varint metadata_count
  repeat: varint key_len, key, varint value_len, value
```

END 的 head 段：`varint message_len, message`（状态为 OK 时 0 长度）+ 同上的元数据块。

每个 head 必须恰好消费完，不能把 body 或下一帧的字节当作缺失字段。元数据数量先按剩余字节数验证上界，
每个键值对至少占两个长度字节；所有字段长度均先检查后切片。方法名（NEW_METHOD 时非空）、状态说明和元数据
在 wire 层按不透明字节处理；GOAWAY 的原因需是有效 UTF-8。

### 4.4 一元调用：每个方向恰好一帧

请求 = 一个 `REQUEST | END_STREAM`（head 带截止与元数据，body 是请求消息）；响应 = 一个 `END`（`aux` 是状态码，
body 是响应消息）。一元调用没有单独的"开流 / 数据 / 结束"三步，连接上的帧数与系统调用数都按一帧计。对比 gRPC 的
HTTP/2 映射：请求是 HEADERS + DATA，响应是 HEADERS + DATA + HEADERS（trailers），再加 5 字节的消息前缀与
HPACK 状态。

### 4.5 方法引用：按连接驻留

每次请求带完整方法名（二三十字节，服务端还要查字符串表）太贵；32 位哈希有碰撞时会静默调用到错误的方法。
这里用按连接驻留：客户端第一次在某条连接上调用某方法时，在 REQUEST 上置 `NEW_METHOD`、head 里带完整名、`aux`
是它为这个名字选的新编号；之后同一连接上只发编号。帧在一条连接上按序处理，驻留一定先于使用。

- 客户端：生成代码给每个方法描述符一个进程内稠密下标；每条连接一个 `std::vector<std::uint32_t>`，下标 → 线上
  编号（0 = 未驻留），O(1)。
- 服务端：每条连接一个 `std::vector<method_handler const*>`，编号 → 处理器，O(1)；超过 `max_method_ids` 是连接错误。

### 4.6 截止时间

客户端在首次等待调用时确定本地单调时钟上的绝对截止，连接等待、准入排队、编码、发送排队与重试共用这一截止。
REQUEST 带本次发送时的剩余超时（微秒），不带绝对时刻，免得依赖两端时钟同步；预算已耗尽时在本地完成为
`deadline_exceeded`，不得把它编码成表示“没有截止”的 0。正的不足 1 µs 的剩余时间向上取整为 1 µs。

服务端以收到帧的时刻加上剩余超时作为本端截止；网络延迟仍计入客户端的本地截止。处理协程调用下游时，
`call_options` 从 `server_context` 继承绝对截止，不能重新给予原始超时时长；同时设置显式相对超时时取两者中较早的截止。

### 4.7 流量控制

- **一元调用不设逐流窗口**：执行并发由 `max_concurrent_streams` 约束，准入与发送队列按 §5.6 管理。
  TCP 窗口不能替代应用层的内存预算。
- **流式调用每流一个接收窗口**：初值 `initial_stream_window`；发送方的 MESSAGE body 字节数扣窗口，窗口不足时
  `write` 挂起（§6.5、§7.3）；接收方的消费者每读走一条消息累计信用，累计到窗口一半发一个 WINDOW_UPDATE。
  超窗发送是流错误（CANCEL 原因码 `flow_control`）。
- 当前协议未定义连接级窗口；是否需要增加它，待发送预算与流式重组的内存边界确定后再决定。

### 4.8 流 ID 与连接轮换

客户端从 1 起单调递增分配 `stream_id`，不复用：复用会让迟到的帧（例如取消之后服务端才发出的 END）落到新调用上。
编号到 2³¹ 时客户端把这条连接标为排空——不再发新调用，在飞的做完后关闭——并由子通道新建一条（§7.5）。每秒 100 万次
调用时约 36 分钟一次，代价是一次连接建立。

GOAWAY：服务端要关闭时发 GOAWAY，`aux` 是它已经开始处理的最大流号；客户端据此把编号更大的在飞调用判为"未处理"，
可以按 §7.6 的策略在别的连接上重试。服务端发出后不得再执行流号大于 `aux` 的请求，包括已经接收但仍在准入队列中
等待的请求；这些请求必须出队，不能在客户端重试之后又开始执行。

### 4.9 大消息分片

本节是后续阶段的方向；第一阶段按 §4.11 只支持单帧 unary。分片与 END 的组合语义需在启用前另行定稿。
后续消息大于对端 `max_frame_size` 时拆成多帧，除最后一帧外都置 `MORE`。写协程每次只把一片放进发送批次、再把剩余部分
排回队尾（§5.3），其它流的帧因此能插在两片之间，一条 100 MiB 的消息不会独占连接。接收方按流重组，重组后超过
`max_message_size` 是流错误。

### 4.10 错误的两级

- **流错误**只终止一个流：方法未驻留、消息解码失败、超窗、消息过大。回 CANCEL 或 END（状态码说明原因），连接照常。
- **连接错误**终止整条连接：魔数 / 版本不符、帧头非法（长度超限、未知类型）、驻留表溢出、协议状态违例。
  控制帧额度允许时发送 GOAWAY；无法发送时直接关闭，在飞的调用以 `unavailable` 完成（§5.9）。

### 4.11 第一阶段的 wire 子集与边界

`wire` 是无 I/O、无堆分配的编解码模块，只验证当前帧；握手顺序、帧方向、流号单调、方法驻留及迟到帧处理属于连接层。

- REQUEST flags 只允许 `END_STREAM` 或 `END_STREAM | NEW_METHOD`，方法编号非零；END flags 为 0，状态码为 0–16，
  非 OK 时 body 必须为空。最小错误 END 为 16 字节帧头加两个零字节 head，共 18 字节。
- 非连接级帧的流号为 1–2³¹−1。SETTINGS / PING / PONG / GOAWAY 的流号为 0；GOAWAY 的 aux 允许 0–2³¹−1。
  CANCEL 原因码尚未定枚举，wire 保留 aux 原值。其它字段约束按 §4.3。
- MESSAGE、WINDOW_UPDATE、流式 REQUEST、MORE 和 COMPRESSED 返回“当前不支持”；未知 type / 未定义 flag 位返回格式错误。
  连接层收到任一类不能处理的帧均终止连接；该行为不代表未来版本不能支持这些已定义能力。
- 接收上限分别约束载荷（head + body）和单帧消息 body；先校验帧头再等待完整载荷，长度相加须防溢出。
  不完整帧返回 `need_more`，失败不消费输入；已完整 head 内缺字段是格式错误。
- 解码结果借用输入存储，其有效期不超过输入且要求期间字节不变。编码输入视图、元数据描述符及参数对象必须与输出区
  不重叠；编码失败后丢弃可能已写入的前缀。编解码结果显式携带错误与成功时的消费 / 写入长度。

## 5. 连接（热路径核心）

### 5.1 结构

```cpp
namespace rpc {
namespace detail {

struct connection {                       // 只在所属分片上被访问
    shard* home;                           // 所属分片
    std::unique_ptr<transport> link;       // 传输（§5.8）
    frame_reader rx;                       // 接收缓冲 + 解帧状态（§5.2）
    tx_queue tx;                           // 发送段队列 + 发送 slab（§5.3）
    stream_table streams;                  // stream_id → stream_state*（§5.4）
    method_table methods;                  // 驻留表：客户端 下标→编号 / 服务端 编号→处理器
    settings local, peer;
    std::uint32_t next_stream_id = 1;      // 客户端
    std::uint32_t refs = 0;                // 分片内引用计数：在飞调用、读协程、写协程各持一份
    connection_state state;                // open / draining / closed
    writer_signal writer_wake;             // 写协程的唤醒（单等待者，非原子）
};

} // namespace detail
} // namespace rpc
```

每条连接两条长期协程：读协程与写协程。net 的契约是"同一流同一方向同一时刻只能有一个未完成的操作"，TLS 流允许一读
一写并发——一条连接恰好一个读者一个写者，满足两者。

### 5.2 读协程

```
for (;;) {
    确保 rx 里有 16 字节帧头；不够就 fill()
    解帧头、校验（length ≤ local.max_frame_size，type 已知，head_length ≤ length）
    if (帧头 + length > rx 容量)                       // 大帧
        分配专用缓冲（≤ 1 MiB 走分片池，否则堆），拷入已到部分，
        CO2_AWAIT_SET(r, net::read(link->stream(), 余下部分))  // 直接读进专用缓冲，不过 rx
    else
        while (rx 不足一整帧) fill()
    handle_frame(帧头, head 视图, body 视图)             // 同步；不 co_await
    rx.consume(16 + length)
}
fill(): CO2_AWAIT_SET(r, link->stream().read_some(rx.free_tail()))
```

- **一次读尽量多**：`rx` 默认 64 KiB，`read_some` 给出全部空闲尾部，一次系统调用读进多个帧，逐个解析。尾部空间
  小于一个帧头时把未消费字节 `memmove` 到头部（通常只有几十字节）。
- **同步处理**：`handle_frame` 只做准入、解码与登记，不挂起。获准执行的请求在这里解码进调用上下文的 arena（§6.3）、
  响应在这里解码进调用方给的对象（§7.3），之后 `rx` 里的字节就可以丢弃。显式排队的请求须在 §5.6 的预算内保存
  所需字节，不能让借用 `rx` 的视图跨越消费；读协程不等待处理协程。
- **调度预算（候选）**：限制每轮解析与分派的帧数、字节数，耗尽后投递续体让出执行权，即使 `rx` 仍有完整帧也一样。
  机制与阈值由小消息 p99 基准验证后确定；单条大消息的同步解码耗时仍需单独验证，轮次预算无法中断一次解码。
- **io_uring 的零拷贝接收**（第二阶段，§17）：用 `net::receive_source`（多发 RECV + 提供缓冲环）代替 `rx`。帧可能跨块：
  帧头跨块时拷进 16 字节暂存；body 跨块时，小消息拷进暂存再解码，大消息把块的所有权交给 `rpc::byte_buffer`
  （§9.3），用完 `consume` 归还。

### 5.3 写协程与写合并

发送端不直接写套接字：一元调用、处理协程、流的 `write` 都只是把编码好的帧追加进 `tx`，再唤醒写协程。

```
for (;;) {
    while (tx.empty()) { CO2_AWAIT(writer_wake.wait()); if (state == closed) CO2_RETURN(); }
    批次 = tx.gather(最多 max_iovec 段、最多 256 KiB)
    CO2_AWAIT_SET(w, net::write(link->stream(), 批次))   // 部分写由 net::write 继续，直到写完或出错
    if (w.ec) { fail(w.ec); CO2_RETURN(); }
    tx.release(w.value)                                  // 解除本批引用；按块实际可复用的容量归还额度（§5.9）
}
```

- **发送 slab**：`tx` 持一串 64 KiB slab（分片池分配）。编码时先 `reserve(n)`（n = 16 + head + body，protobuf 的 body
  长度由 `ByteSizeLong()` 先算出），在 slab 里就地写帧头、head、body，再 `commit(n)`。连续的小帧在同一 slab 里首尾
  相接，`gather` 把它们合成一个段——一批几十个响应常常只是一个 iovec，`sendmsg` 退化成 `send`。
  一元响应的 `reserve` 兑现 §5.9 已取得的预留，不再与新请求竞争额度；slab 合并不得侵占其它调用已预留的容量。
- **大消息**的 body 超过 slab 的四分之一时不拷进 slab：帧头进 slab，body 作为单独一段挂进队列（用户给的
  `rpc::byte_buffer` 零拷贝，或编码进专用缓冲）；超过 `max_frame_size` 的按 §4.9 分片，每批只取一片。
- **写的时候继续追加**：写协程取批次时记下每段的长度；写在飞期间追加进同一 slab 的字节不在本批，下一轮再发。slab
  地址不变，不需要锁（同一分片）。
- **唤醒时机**：队列由空变非空时，追加方 `post` 写协程的续体，而不是就地写。本轮调度里后续追加的帧（同一次读进来的
  其它请求的响应）在写协程真正运行前都已入队，一次写出。单个在飞调用多付一跳 `post`（13 ns），换来流水线下每批一次
  系统调用。只合并当时已经就绪的数据，不设置等待凑批的延时；批次上限也按小消息 p99 验证。

REQUEST 尚未提交给传输前需检查截止，并使其 head 携带 §4.6 的剩余预算；过期的未发送请求应撤出队列。
`timeout_us` 固定为 head 开头的 8 字节小端整数，写协程只在整帧尚未提交时更新这 8 字节，帧长度和其余字段偏移不变；
已经提交或部分发送的字节不能修改。过期请求的撤销须同时维护方法驻留的先后关系，连接层实现时验证。

### 5.4 流表

`stream_id → stream_state*` 的开放寻址哈希表：容量 2 的幂、线性探测、删除用后移（不留墓碑），键是单调递增的 u32，
低位直接做哈希。稳态下不分配；负载因子超过 0.5 时翻倍（冷路径）。`stream_state` 是客户端调用状态（住在调用方协程帧里，
§7.3）或服务端调用上下文（池里，§6.3）的公共前缀：

```cpp
struct stream_ops {                         // 每种流一张静态表：分派不查类型标签
    void (*on_frame)(stream_state*, frame_view const&) noexcept;
    void (*on_abort)(stream_state*, status const&) noexcept;       // 连接关闭 / 取消 / 截止
};

struct stream_state {
    std::uint32_t id;
    stream_kind kind;                       // client_unary / client_stream / server_call（调试与统计用）
    stream_ops const* ops;
    deadline_node deadline;                 // 截止时间轮的侵入式节点（§8）
};
```

### 5.5 完成与恢复的时机

读协程完成一个客户端调用时，把调用方的续体 `post` 到调用方的执行器，而不是在读协程中途嵌套恢复它：

- 安全：被恢复的调用方可能关闭 channel、销毁连接；嵌套在读协程的循环中间恢复它，读协程会在自己的帧被销毁之后继续跑。
- 批量：处理当前批次并投递完成，随后让调用方运行；批次边界按 §5.2 的候选预算验证，新发的调用由写协程合并（§5.3）。

服务端启动处理协程同理：解码完请求后 `post` 处理协程的初始句柄（§6.3）。

### 5.6 准入、有限排队与背压

- **默认快速失败**：一元调用的准入等待队列容量默认为 0。客户端没有 READY 连接时返回 `unavailable`；可用连接的
  并发额度或发送预算不足时返回 `resource_exhausted`。服务端不能接纳新请求时直接拒绝，不等待资源恢复。
  已接纳帧在单写协程中等待发送属于正常传输排队，仍受发送预算和调用截止约束。
- **显式有限排队**：业务开启准入等待或 `wait_for_ready` 时，必须配置有限队列长度，且入队调用必须有有限截止。
  队列满立即返回 `resource_exhausted`；配置缺失则拒绝启用该策略。等待期间的取消或到期由所属分片处理并出队，恢复为
  `cancelled` 或 `deadline_exceeded`，之后不得再发送或执行。保存请求内容的队列还须受字节预算约束。
- **发送端**：新请求与流式写在编码之前检查加入本次数据后的预算。高水位暂定 8 MiB、低水位 2 MiB；降至低水位后，
  对显式排队的一元调用重新检查截止与准入条件，再按序接纳。单次提交本身超过可用总容量时直接拒绝，不能永久排队。
  已建立流的 `write` 仍可因流窗口或发送背压挂起（§6.5），不因此改变为一元调用的快速失败语义。
  这些高低水位不拦截已取得预留的一元响应，也不能让其它写者借用其预留容量。
- **服务端一元响应**：执行前按方法上限取得可兑现的响应预留，完成后将存储所有权交给 `tx`，核算与回收见 §5.9。
  接收重组、消息对象与 handler 临时内存仍需独立预算，不能据响应预算宣称连接的全部内存已经有界。

### 5.7 保活、关闭与错误传播

- **保活**：连接空闲（无收发）超过 `keepalive_interval`（默认 30 s）时发 PING；`keepalive_timeout`（默认 10 s）内没有
  PONG 视为断开。计时走截止时间轮（§8），不为每条连接各开一个定时器。
- **关闭**：`fail(ec)` 把状态置 closed、唤醒写协程让它退出、`link->stream()` 上的在飞读由关闭传输取消；流表里每个
  状态调 `on_abort(unavailable)`；两条长期协程各放下一份引用，引用归零时销毁连接对象。
- **在飞调用持引用**：客户端调用登记进流表时 `++refs`，完成时 `--refs`，连接对象因此活到最后一个调用完成（都在同一
  分片，非原子）。

### 5.8 传输

```cpp
struct transport {
    virtual ~transport() = default;
    virtual net::any_stream& stream() = 0;             // 读写都经它
    virtual net::socket_base* socket() noexcept = 0;   // 设选项、建 receive_source；非套接字传输为空
    virtual net::task<net::io_result<>> shutdown() = 0;  // TCP：shutdown(send)；TLS：close_notify
};

std::unique_ptr<transport> make_tcp_transport(net::tcp_socket socket);
std::unique_ptr<transport> make_local_transport(net::local_stream_socket socket);
std::unique_ptr<transport> make_tls_transport(net::tcp_socket socket, net::tls::context const& ctx, tls_role role);
```

连接代码经 `net::any_stream` 读写，只编译一次、放在 `.cpp` 里。擦除的代价是每次 `read_some` / `write_some`
多约 17 ns（net 实测 19.0 对 2.5 ns），相对一次 1–2 µs 的系统调用、再被批量摊薄，可以忽略。TLS 流的每次读写是一个
task 帧（net 的 `docs/tls.md`：加密往返 4 次分配），走分片的回收式帧分配器。

### 5.9 一元响应的预留与回收

**方法上限**：每个一元方法在启动前必须配置有限的 `max_response_bytes`，表示未压缩 body 的编码上限；不从协议的
`max_message_size` 隐式取得默认值。服务端另配置有限的响应 head 上限，覆盖状态说明与尾部元数据，并满足 §4 的字段与帧限制。
框架结合固定的对端接收限制，计入所有帧头、分片描述符、分配粒度与碎片上界，得到本连接的最坏发送存储需求 `R`；
`R` 至少能容纳一个无 body、无尾部元数据的固定错误 END，且该错误头也满足配置的 head 限制。
分片、压缩和零拷贝路径启用前，都必须验证各自的
最坏存储需求，不能只按 body 长度扣费；对端的消息与帧上限若使该连接无法兑现方法声明，也须在执行业务前拒绝。

**两级预算**：每连接与每分片分别设置业务响应容量 `B`。在所属分片内，准入同时取得两级容量；任一层失败则完整回滚，
按 §5.6 拒绝或显式排队，尚未进入处理协程。按本地支持布局计算的 `R` 大于任一层总容量属于启动配置错误，不能让调用永久等待；
连接准入时再据对端限制计算 `R` 并校验能否兑现，单次需求超过总容量时直接拒绝。
两层均保持 `reserved + occupied <= B`：`reserved` 是尚未兑现的存储承诺，`occupied` 是已分配且被编码、排队或在飞写持有的
块容量与发送描述符。一次转交只改变两者的归属，不重复扣费；不同连接的预留合计受分片容量限制。

**可兑现的预留**：取得预留时必须从有界缓冲池锁定足够的块或等价的存储保障；池补充或分配失败也须在执行业务前处理。
不能仅减少一个逻辑字节计数器，然后等 handler 返回再尝试无保障的分配。共享 slab 按实际保留容量记一次账，尾部、空洞
和仍被引用的部分均不能提前归还；未用预留也只有在对应容量确实可以重用时才能释放。

分片还保持 `locked + live + cached <= B_shard`，三项分别是为预留锁定且尚未使用的存储、已经被编码或 `tx` 持有的存储
（包括导入的外部块）、未指派的空闲池缓存，均按实际分配容量计入描述符等开销。每块只归入一项，共享块只计一次。
这与准入公式使用同一个分片容量上限；回池只把占用变成缓存，不等于向操作系统释放。分配策略须先给出保守容量上界，
再用基准决定是否优化共享与碎片利用率。

| 阶段 | 响应存储与额度的所有者 | 允许的处理 |
| --- | --- | --- |
| 等待准入 | 尚未持有响应预留 | 仅受等待队列预算约束，出队时重新尝试完整准入 |
| 准入通过、执行与编码 | `server_call` 的预留句柄 | 编码只能兑现已有预留，不能因其它调用占用而再次等待发送额度 |
| 编码完成、排队与在飞写 | `tx` 持有的块及其预算凭据 | 转交独立所有权后才可复位调用 arena；可复用的多余预留可归还 |
| 最后一个使用者释放 | 缓冲池 | 归还两级占用额度并触发等待者重新准入 |

响应实际 body 或 head 超过声明上限时，不编码超限内容；记录方法及实际尺寸，用本调用预留生成固定、有界的
`END(internal)`。这是服务端响应契约违约，不能标为“未执行”，也不触发额外的自动重试；业务副作用不会因该错误回滚。
预留保证预算不被其它调用抢走，不保证网络交付，也不约束 handler 在构造响应时已经分配的任意对象。

**控制帧额度**：未准入拒绝与连接控制帧使用独立、同样有连接和分片上限的保留池，业务响应不可占用它。
已准入调用的正常或错误 END 使用自身预留，不再重复取得控制额度。拒绝消息采用有界 head；控制额度也耗尽时关闭对应连接，
不再分配更多应答或等待它恢复。该策略不保证慢客户端一定收到拒绝或 GOAWAY。

**取消与关闭**：尚未提交传输的完整帧可以撤销；在飞缓冲必须等写操作完成或取消完成后才能回收。若一个帧已部分发出，
必须完成该帧或关闭连接，不能跳过剩余字节继续发下一帧。收到取消只改变结果处置，不能据此销毁仍运行的 handler、
复位其 arena 或释放仍有引用的缓冲；确定不会再编码时，才能归还尚未兑现的响应预留。

本节约束 RPC 持有的响应编码存储及发送描述符。请求 / 响应对象、Arena、用户临时内存，以及 TLS 和内核缓冲另行核算；
编码时消息对象和编码结果同时存在的峰值也不能漏算。

## 6. 服务端

### 6.1 接入与分片分配

默认：分片 0 上一个 `tcp_acceptor`，接受后按"连接数最少"选目标分片，`socket.release()` 交出描述符，`post` 到目标分片，
在那里 `assign` 到它的 `io_context` 并创建连接。每条连接多两次投递，与一次 TCP 握手相比可以忽略；分配均匀，可移植到
IOCP（net 的 `release()` 会解绑完成端口）。

Linux 上可选：每个分片一个接受器，开 `SO_REUSEPORT`，由内核按四元组哈希分配：

```cpp
net::tcp_acceptor acceptor{shard.context()};
acceptor.open(net::ip::tcp::v4());
acceptor.set_option(net::socket_option::reuse_address{true});
acceptor.set_option(net::socket_option::boolean<SOL_SOCKET, SO_REUSEPORT>{true});
acceptor.bind(endpoint);
acceptor.listen();
```

连接数少时哈希分配可能不均，所以不作默认。

### 6.2 服务注册与方法表

生成代码为每个方法给一个 `method_handler` 的具体子类（§9.4），`server_builder` 在启动时把"完整方法名 → 处理器"
收进一张表；`start()` 之后表只读，所有分片共享，不加锁。连接上的驻留表（§4.5）指向这张表里的处理器。

注册配置同时保存 §5.9 的一元响应上限；生成描述符提供方法身份，部署配置提供上限，不要求把部署容量写死在 proto 中。
启动时验证上限、响应 head 限制及连接 / 分片预算的可兑现性；缺失或不可能满足的配置直接报告错误。

```cpp
struct method_handler {                    // 生成代码为每个方法实现一个 final 子类
    virtual ~method_handler() = default;
    virtual method_descriptor const& descriptor() const noexcept = 0;
    // 在调用上下文的 arena 里建请求 / 响应对象并解码请求；失败返回 invalid_argument。
    virtual status prepare(server_call& call, net::const_buffer request_bytes) const = 0;
    virtual net::task<status> invoke(server_call& call) const = 0;         // 调用用户的实现
    virtual std::size_t response_size(server_call const& call) const = 0;
    virtual void encode_response(server_call const& call, net::mutable_buffer out) const = 0;
};
```

每次调用四次虚调用，约 2 ns 一次；换来的是分派代码对所有方法只编译一份。

### 6.3 调用上下文与处理协程的启动

`server_call` 从分片的空闲链取，调用结束归还，稳态不分配：

```cpp
struct server_call : stream_state {
    connection* conn;
    method_handler const* handler;
    response_reservation response_storage; // RAII 预留句柄；编码后将存储及占用凭据转交 tx
    call_arena arena;                     // 首块内联 2 KiB；protobuf 用 Arena 的"用户初始块"指向它
    void* request;  void* response;       // arena 里的消息
    server_context context;               // 暴露给用户：截止、元数据、取消、对端地址
    net::stop_source stop;                // 取消 / 截止 / 连接关闭都经它请求停止
    net::io_env env;                      // 分片执行器 + stop.get_token() + 分片帧分配器
    net::task<status> task;
    net::detail::completion_frame done;   // 处理协程完成时对称转移到这里
    net::continuation start;
};
```

读协程先解析方法引用并取得其上限，再按 §6.4 检查准入、取得 §5.9 的响应预留；通过后取 `server_call` →
`handler->prepare(...)` 解码 → 登记进流表与截止时间轮 →
`call.task = handler->invoke(call)` → 把 `task` 的续体设为 `done` 的句柄、环境设为 `&call.env` → `post(call.start)`。

这正是 `net::run_async` 做的事，但 `run_async` 每次 `new` 一个启动状态（2 次分配、46 ns）；这里的状态嵌在池化的
`server_call` 里。已有公开 `task.await_suspend(done_handle, &call.env)` 可设置续体与环境并返回启动句柄；
完成后用 `task.await_resume()` 取结果，再销毁已完成的 task，无需新增 task 启动入口。

`done` 的回调先取结果并检查终止状态：已收到 CANCEL 或连接关闭则丢弃结果；本端截止则生成 `deadline_exceeded`；
其余路径检查响应上限并编码正常或固定错误 END（抛出的异常 → `internal`）。需要发送时，使用已有预留，
把编码存储的独立所有权与预算凭据转交 `tx`（§5.9），随后销毁已完成的 task 帧、摘流表与时间轮、复位 arena、归还调用上下文。
`tx` 不能留下借用该 arena 或 task 帧的段；零拷贝响应也必须先转交独立的块所有权。无需发送时归还可复用的预留并完成同样的清理。

准入后的 `prepare` 或启动失败也走上述资源收尾，不等待一个尚未启动的 handler；必要的错误 END 使用已有预留。
若已经生成部分但尚未提交的编码，先丢弃该编码再复用预留生成错误，不能额外借用未计费的存储；所有出口只清理一次。

调用上下文持有连接引用直到清理完成；handler 返回可以释放执行并发名额，但排队响应的占用额度继续由 `tx` 持有。
启动时 `on_work_started`，上述清理完成后才 `on_work_finished`；发送链自身的工作与引用继续保护未发送的响应。

处理协程的帧从分片的帧分配器来（启动时线程局部槽位就是读协程链的分配器）。

### 6.4 取消、截止与过载

尚在服务端准入队列中的请求也须可按流号定位；取消、截止或连接关闭时直接摘队列并释放保存内容，不再启动处理协程。
其中截止以 `deadline_exceeded` 收尾，取消或连接关闭时丢弃请求。下列 `call.stop` 路径适用于已经进入 §6.3 的调用。

- **CANCEL 帧**：查流表 → `call.stop.request_stop()` → 处理协程里在飞的 net 操作以 `operation_aborted` 结束，之后
  再发起的操作也不执行（net 在开始前就查停止）→ 完成时不再发 END。
- **截止**：时间轮到期 → 同样 `request_stop()`，完成时发 `END(deadline_exceeded)`（客户端多半已经放弃，这帧只是让它
  尽早释放状态）。
- **连接关闭**：流表里每个调用 `request_stop()`，完成时丢弃结果。
- **过载**：在处理的调用数达到 `max_concurrent_streams` 或准入预算不足时，默认直接回 `END(resource_exhausted)`，
  不解码业务消息、不创建处理协程；拒绝帧使用 §5.9 的控制额度，额度耗尽则关闭连接。执行并发上限写进 SETTINGS；
  显式启用的服务端等待队列另按 §5.6 限制，
  出队时重新检查截止和资源，满足条件才进入 §6.3 的处理流程。未执行拒绝的线上可识别语义仍待确定（§7.6）。
- **每次调用一个 `StopState`**：`net::stop_source` 构造就 `new` 一个（17 ns）。池化 `stop_source` 不安全——处理协程
  可能把 token 交给活得更久的后台链，复用会让它收到下一个调用的停止请求。要省掉这次分配，需要 co2 提供"没被请求过
  且只剩自己一份引用时可以重置"的 `stop_source`（§15）。

### 6.5 流式处理

```cpp
// 生成的接口：
virtual net::task<rpc::status> chat(rpc::server_context& ctx, rpc::server_stream<ChatIn, ChatOut>& stream) = 0;

// 用户在协程里：
CO2_AWAIT_SET(got, stream.read(&in));     // rpc::read_result{status, bool ended}
CO2_AWAIT_SET(st, stream.write(out));     // 窗口不足或发送端超过高水位时挂起
```

- 读协程把 MESSAGE 解码成 arena 里的消息，挂进流的接收队列（界限是接收窗口）；`read` 有消息就同步返回，没有就挂起
  到下一条到达或对端 `END_STREAM`。
- `write` 编码进 `tx`、扣发送窗口；窗口不够时挂起，直到对端 WINDOW_UPDATE。
- 消息在 arena 里，arena 在一个长流上会一直长：流式调用的消息用分块 arena，每读走一条就把整块已读消息的空间标记为
  可回收（按块回收，块内是 bump 分配）。

### 6.6 优雅关闭

`server::shutdown(grace)`：停止接入 → 每条连接发 GOAWAY（`aux` = 已开始处理的最大流号），按 §4.8 清理不再执行的
排队请求，之后的 REQUEST 回 `unavailable`（客户端仅在重试策略允许时重试）→ 等执行中的调用结束并排空响应，最多 `grace` →
对仍未结束的调用 `request_stop()` → 关闭传输 →
分片的 `run()` 在工作计数归零后返回。

## 7. 客户端

### 7.1 channel 与每分片子通道

```cpp
struct channel_config {
    std::string target;                          // "static:///10.0.0.1:8080,10.0.0.2:8080" / "dns:///svc:8080"
    lb_policy lb = lb_policy::power_of_two;      // round_robin / power_of_two / consistent_hash
    std::uint32_t connections_per_endpoint = 1;  // 每个分片、每个后端的连接数
    std::chrono::milliseconds idle_timeout{300000};
    retry_policy retry;                          // §7.6
    tls_options const* tls = nullptr;
};

std::shared_ptr<channel> make_channel(runtime& rt, channel_config const& config);
```

`channel` 是可以跨线程持有的句柄；它内部为每个分片保存一份子通道集合与负载均衡器，只由那个分片访问。调用从哪个分片
发起就用哪个分片的集合（分片线程上的线程局部 `current_shard` 给出下标），热路径不加锁。

代价是连接数 = 分片数 × 后端数 × `connections_per_endpoint`；连接按需建立、空闲超时关闭。后端很多、分片很多时可以
把 `connections_per_endpoint` 设为 0，表示"只在一个指定分片上建连接，其它分片的调用投递过去"（§7.7 的路径）。

### 7.2 解析器与负载均衡

```cpp
struct resolver {                                // 在 channel 的控制分片上运行
    virtual ~resolver() = default;
    // 首次与每次变化时给出端点集合；stop_token 请求停止时以 operation_aborted 结束。
    virtual net::task<net::io_result<endpoint_set>> next() = 0;
};

struct load_balancer {                           // 每分片一个实例，只在该分片上调用
    virtual ~load_balancer() = default;
    virtual void update(std::shared_ptr<endpoint_set const> endpoints) = 0;   // 冷路径
    virtual subchannel* pick(pick_request const& request) noexcept = 0;       // 热路径，只选 READY 的
    virtual void on_finished(subchannel* chosen, call_outcome const& outcome) noexcept {}
};
```

- 解析结果是不可变快照，控制分片拿到新快照后 `post` 给每个分片，由各分片的 `load_balancer::update` 重建自己的子通道集合。
- 内置：`static`、`dns`（`net::resolver`，按 TTL 或固定间隔重解析）；etcd / consul 之类由用户实现 `resolver`。
- `power_of_two`：随机取两个 READY 子通道，选在飞调用少的那个；在飞数是分片内计数，不跨分片汇总。
- `consistent_hash`：`pick_request` 带调用方给的哈希键，环上取点。

### 7.3 一元调用：状态住在调用方的帧里

```cpp
// 生成的存根：
rpc::unary_call echo(EchoRequest const& request, EchoReply* reply, rpc::call_options const& options = {});

// 调用方：
auto call_echo(demo::EchoStub* stub, EchoRequest const* req, EchoReply* reply)
    CO2_BEG(net::task<rpc::status>, (stub, req, reply), rpc::status st;) {
    CO2_AWAIT_SET(st, stub->echo(*req, reply, rpc::call_options{}.timeout(std::chrono::milliseconds{50})));
    CO2_RETURN(st);
}
CO2_END
```

`unary_call` 是一个 IoAwaitable，住在调用方协程帧的 awaiter 槽里（co2 的 `CO2_AWAIT_STORAGE_SIZE`，net 设为 192
字节），里面就是这次调用的全部状态；与 net 的组合 awaiter 一样只在启动前移动（进槽），登记进流表之后地址不再变：

```cpp
struct unary_call : stream_state {       // stream_state：流号、种类、操作表指针、截止节点，约 40 字节
    union { channel* chan; connection* conn; };  // 选定连接之前 / 之后
    method_descriptor const* method;     // 含编解码函数（§9.1）
    void const* request;  void* reply;
    std::chrono::microseconds timeout;   // 从 options 拷出的显式相对超时
    std::chrono::steady_clock::time_point inherited_deadline; // 若 options 继承截止则原样保留
    metadata const* extra;               // 调用方拥有，须活到调用结束
    net::continuation cont;  net::io_env const* env;
    std::atomic<std::uint8_t> phase{0};  // pending / completing / done（§7.4）
    status_code code = status_code::ok;
    std::uint8_t flags = 0;              // wait_for_ready、相对超时 / 继承截止是否启用等
    unary_call* cancel_next = nullptr;   // 分片取消收件箱的链（§7.4）
    stop_hook* on_stop = nullptr;        // 仅当 stop_token 可能被请求时，从分片小对象池取
};
static_assert(sizeof(net::detail::env_awaiter<unary_call>) <= CO2_AWAIT_STORAGE_SIZE, "unary_call must fit the await slot");
```

`await_suspend(h, env)` 的启动契约：

1. 首次进入时按 §4.6 确定绝对截止，继承的截止保持不变；已经取消或到期则本地完成。
   当前线程不是分片线程时转入 §7.7，跨线程投递也消耗预算。
2. 在进入连接或准入等待前建立取消与截止处理；等待态即使没有连接也必须能退出。注册 `stop_hook` 可能同步触发回调，
   初始化尚未完成时只能记录取消，不能提前恢复调用方。等待态字段与完成发布的具体布局仍需生命周期设计验证。
3. 从本分片选择子通道并检查 §5.6 的准入条件；默认直接返回失败，显式开启有限等待才入队。取得连接并获准发送后，
   才分配 `stream_id`、登记进流表、`++conn->refs`，沿用已有截止。
4. `reserve` → 就地写帧头、head（剩余超时、首次调用时的驻留名、元数据）、body → `commit` → 唤醒写协程。
   提交传输前的截止检查与 head 刷新见 §5.3。

完成（读协程收到 END）：`phase` 从 pending 抢到 completing → 解码响应进 `reply` → 摘下流表与时间轮 → `--refs` →
`env->executor.post(cont)`。`await_resume` 注销并归还 `stop_hook`、返回 `status`（错误时状态消息见 §10）。

上述字段是热路径草图；计入绝对截止、等待队列与完成发布所需状态后，须重新验证大小与对齐，不能据此认定已经满足
192 字节槽内零 malloc 的目标。`stop_callback` 暂不放在调用状态里，是因为它一个就占 72 字节，而不可取消的调用
（`stop_token` 不可能被请求）根本不需要它。另一种做法是把
`NET_AWAIT_STORAGE_SIZE` 调到 256：调用状态可以把 `stop_callback` 内联，代价是进程里每个协程帧都多 64 字节。`static_assert`
守住大小——超出时 co2 会退回一次堆分配，语义不变但悄悄多一次分配。

**流式调用**（`stub.chat(options)` 返回 `rpc::stream_call<In, Out>`）：流跨越多个 `co_await`，状态不能住在某一个 awaiter
里，由调用方持有的 `stream_call` 对象承载（它本身是流表里的 `stream_state`，调用方协程帧里的一个成员）：

```cpp
CO2_AWAIT_SET(st, call.start());          // 发 REQUEST（不带 END_STREAM），等不到服务端也立即返回
CO2_AWAIT_SET(st, call.write(out));       // 扣发送窗口，不够或超过高水位时挂起
CO2_AWAIT_SET(got, call.read(&in));       // rpc::read_result{status, bool ended}
CO2_AWAIT_SET(st, call.writes_done());    // 空 MESSAGE | END_STREAM：半关闭
CO2_AWAIT_SET(st, call.finish());         // 等服务端的 END，给出最终状态
```

接收队列、窗口与 arena 的做法与服务端（§6.5）对称；`stream_call` 析构时流还没结束就发 CANCEL。

### 7.4 取消与截止的竞态

完成、取消、截止三条路径可能同时到达，用 `phase` 的一次 CAS 决定谁收尾：

下面的连接收尾描述适用于已发送的调用。仍在连接或准入等待队列中的调用只需出队并本地完成；未提交传输的 REQUEST
撤出发送队列，不发 CANCEL。已经提交给在飞写操作的缓冲不能提前回收，其取消与引用释放顺序仍需在生命周期设计中补齐。

- **完成**在分片上：CAS 成功才解码与恢复；失败说明取消或截止先到，丢掉这个 END。
- **截止**在分片上（时间轮）：CAS 成功 → 摘流表、发 CANCEL、以 `deadline_exceeded` 恢复调用方。
- **取消**来自 `stop_token`，回调可能在任意线程：CAS 成功后，若当前线程就是连接所属分片则直接收尾；否则把调用压进
  分片的取消收件箱（经 `cancel_next` 串起的无锁栈），收件箱由空变非空时 `post` 分片唯一的一个排空续体；排空时在分片上
  逐个摘流表、发 CANCEL、以 `cancelled` 恢复。连接这时即使已经关闭也还活着：调用持有它的一份引用，引用在这里才放下。
- `await_resume` 在调用方线程注销 `stop_hook`；co2 的 `stop_callback` 析构会等正在另一线程上执行的回调返回，回调只做
  CAS 与一次入栈，等待很短。

### 7.5 连接管理

每个子通道（一个分片 × 一个后端 × 一条连接）是一个小状态机：

```
idle ──首次 pick──► connecting ──握手 + 前导成功──► ready ──GOAWAY / 流号用尽──► draining ──在飞归零──► idle
          ▲              │ 失败                      │ 传输错误 / 保活超时
          └── backoff ◄──┴───────────────────────────┘
```

- `connecting`：`tcp_socket::connect`（解析出多个地址时依次尝试），连上后设 `no_delay`，TLS 则握手，再发前导与 SETTINGS。
  整个建立过程套 `net::timeout(..., connect_timeout)`——这是冷路径，一个定时器没关系。
- 失败后指数退避（初值 100 ms，倍率 1.6，上限 30 s，±20% 抖动）。
- 没有 READY 子通道时：`wait_for_ready` 默认关闭，立即返回 `unavailable`；显式开启后遵守 §5.6 的有限排队规则，
  连接变为 READY 只触发重新准入，不能绕过并发与发送预算。
- 状态只有五个、转移固定，用 `enum class` 加转移函数就够；每个状态的行为（连、等、退避）是子通道协程里顺序写出的循环，
  不需要状态类层次。

### 7.6 重试

```cpp
struct retry_policy {
    std::uint32_t max_attempts = 1;                   // 1 = 不重试
    std::chrono::milliseconds initial_backoff{10};
    std::chrono::milliseconds max_backoff{1000};
    float multiplier = 2.0f;
    status_code_set retryable{status_code::unavailable};
};
```

- **默认关闭**：`max_attempts = 1`，过载也不自动重试；允许重试的方法或失败原因不意味着默认开启重试。
- **一定安全**的情形不看方法：能够证明请求尚未提交给传输；或 GOAWAY 表明服务端不会执行它（流号大于 `aux`，§4.8）。
  普通 `resource_exhausted` 也可能由已经执行的业务返回，不能单凭状态码证明“未执行”；框架准入拒绝的可识别编码待定。
- **其余情形**只对在 proto 里标了 `idempotency_level = IDEMPOTENT` 的方法按策略重试。
- 可重试的调用把 body 编码进一块单独的、带引用计数的缓冲（不是发送 slab），每次尝试只重写帧头与 head（新流号、新连接上的
  驻留），body 作为独立的一段挂进队列。不可重试的调用不付这份代价。
- 重试在剩余截止之内进行，不重置截止。

### 7.7 在分片之外发起调用

调用方运行在线程池、别的 `io_context` 或非协程线程上时：`await_suspend` 先确定截止，再把 `unary_call` 自身的一个续体
`post` 到选定分片，在分片上继续 §7.3 的准入与发送流程（`stop_hook` 这时用 malloc：它在调用方线程上归还，不能进分片池）；
完成时 `env->executor.post(cont)` 回到调用方的执行器。两次跨线程投递
（net 的线程池一跳 23 ns，外加唤醒），适合非热路径；热路径应让调用方本身跑在分片上。

同步代码可以用 `net::test::run_blocking` 式的包装（`rpc::call_sync`）阻塞等待，只用于工具与测试。

## 8. 截止时间轮

客户端调用的截止、服务端调用的截止、连接保活都挂在每个分片的一个时间轮上：

- 单层哈希时间轮：4096 个桶 × 1 ms（覆盖 4 s），更远的放在溢出链，每转一圈重新分桶一次。节点侵入在 `stream_state`
  与连接里（双向链），插入 / 摘除 O(1)、零分配。
- 一个分片一个 `net::steady_timer`，只在轮非空时武装到下一个非空桶的时刻；到期处理完当前桶再武装下一个。轮空了定时器
  就停，第一次插入时重新武装。io_uring 上每次武装是一个 SQE，但频率是每个非空毫秒最多一次，不是每次调用一次。
- 精度：到期最多晚一个桶（1 ms，可配置）。RPC 截止通常是毫秒级；要求亚毫秒截止的调用可以在调用方自己套
  `net::timeout()`（每次 +153 ns、1 次分配）。

## 9. 编解码与代码生成

### 9.1 两种形态

客户端存根知道消息的具体类型，用静态特化，能内联：

```cpp
template <class Message, class = void> struct codec;   // 由各实现特化

template <class M>
struct codec<M, typename std::enable_if<std::is_base_of<google::protobuf::MessageLite, M>::value>::type> {
    static std::size_t size(M const& m) { return m.ByteSizeLong(); }            // 缓存长度
    static void encode(M const& m, net::mutable_buffer out) {                    // 用缓存长度写，不再算一遍
        m.SerializeWithCachedSizesToArray(static_cast<std::uint8_t*>(out.data()));
    }
    static bool decode(net::const_buffer in, M& m) {
        return m.ParseFromArray(in.data(), static_cast<int>(in.size()));
    }
};
```

`unary_call` 这类与类型无关的对象经 `codec_ops`（一组由 `codec<M>` 实例化出来的函数指针，每个方法一份静态常量）调用，
避免把调用状态做成模板而让连接代码按消息类型重复实例化。服务端分派用 §6.2 的 `method_handler`，同理。

### 9.2 protobuf 的内存

- 服务端请求 / 响应建在 `google::protobuf::Arena` 上，Arena 的初始块指向 `server_call` 里内联的 2 KiB（Arena 支持
  用户提供初始块）；小消息的所有字段分配都是 bump 指针，调用结束整块复位。
- 客户端的响应对象由调用方提供；调用方可以自己用 Arena，库不强加。
- 编码两遍（`ByteSizeLong` 一遍、`SerializeWithCachedSizesToArray` 一遍）但只写一次内存，直接写进发送 slab。

### 9.3 原始字节与零拷贝

`rpc::byte_buffer`：一串带引用计数的只读片（分片内非原子计数）。作为请求 / 响应类型时：

- 发送：各片直接作为 `tx` 的段，不拷贝（§5.3）。
- 接收：大帧读进专用缓冲后，缓冲的所有权交给 `byte_buffer`（§5.2）；io_uring 的 `receive_source` 路径交出提供缓冲环的块。
- 小帧仍从 `rx` 拷出——拷几十字节比管理一块共享缓冲的寿命便宜。

作为一元响应的外部块还须满足 §5.9 的容量核算：视图长度不能代表它固定住的底层块容量。只有能确定所有权、保留容量
并纳入预留的块才走零拷贝；替换预留时还须计入池中仍保留的空闲块，不能把外部块与池缓存的双份存储漏算。
无法满足时编码 / 拷贝到已预留的有界存储，不以“小视图”绕过响应预算。

### 9.4 生成代码

`protoc --rpc_out=...` 为每个 service 生成：

```cpp
namespace demo {

rpc::service_descriptor const& echo_service_descriptor();   // 方法描述符表：完整名、种类、幂等性、稠密下标

struct EchoService {                                        // 服务端接口：用户实现它
    virtual ~EchoService() = default;
    virtual net::task<rpc::status> echo(rpc::server_context& ctx, EchoRequest const& req, EchoReply& reply) = 0;
    virtual net::task<rpc::status> chat(rpc::server_context& ctx, rpc::server_stream<ChatIn, ChatOut>& stream) = 0;
};
void add_echo_service(rpc::server_builder& builder, EchoService* impl);   // 注册；impl 由组合根持有

struct EchoStub {                                           // 客户端存根：值类型，借用 channel
    explicit EchoStub(rpc::channel& channel);
    rpc::unary_call echo(EchoRequest const& req, EchoReply* reply, rpc::call_options const& options = {});
    rpc::stream_call<ChatIn, ChatOut> chat(rpc::call_options const& options = {});
  private:
    rpc::channel* channel_;
};

}
```

co2 的协程是自由函数（`CO2_BEG` 捕获参数到帧里），虚成员函数把 `this` 转交给它，参数以指针传入——与 net 的
`openssl_stream` 驱动协程同一写法：

```cpp
// echo_service_impl.hpp
struct EchoServiceImpl final : demo::EchoService {
    net::task<rpc::status> echo(rpc::server_context& ctx, EchoRequest const& req, EchoReply& reply) override;
    // ...
};

// echo_service_impl.cpp
namespace {
auto echo_impl(EchoServiceImpl* self, rpc::server_context* ctx, EchoRequest const* req, EchoReply* reply)
    CO2_BEG(net::task<rpc::status>, (self, ctx, req, reply)) {
    reply->set_text(req->text());
    CO2_RETURN(rpc::status{});
}
CO2_END
} // namespace

net::task<rpc::status> EchoServiceImpl::echo(rpc::server_context& ctx, EchoRequest const& req, EchoReply& reply) {
    return echo_impl(this, &ctx, &req, &reply);
}
```

不用 protobuf 的场合，`rpc::method<Request, Response>` 模板加一个完整名字符串即可手写描述符与处理器，编解码走对应的
`codec` 特化。

### 9.5 压缩

SETTINGS 协商双方都支持的算法；发送方对超过阈值（默认 4 KiB）的 body 压缩进 slab 并置 `COMPRESSED`，接收方解压进
arena（服务端）或临时缓冲（客户端）。小消息不压缩：压缩与解压的 CPU 大于省下的字节。

## 10. 状态与错误模型

```cpp
enum class status_code : std::uint8_t {
    ok = 0, cancelled = 1, unknown = 2, invalid_argument = 3, deadline_exceeded = 4, not_found = 5,
    already_exists = 6, permission_denied = 7, resource_exhausted = 8, failed_precondition = 9, aborted = 10,
    out_of_range = 11, unimplemented = 12, internal = 13, unavailable = 14, data_loss = 15, unauthenticated = 16,
};

struct status {                          // 值类型：成功时不分配
    status_code code() const noexcept;
    char const* message() const noexcept; // 错误说明；成功时 ""
    bool ok() const noexcept;
    // 错误时的说明按需分配（错误路径），成功路径只是一个字节的码
};
```

- 码值与 gRPC 一致，将来做 gRPC 适配时直接映射。
- 提供 `rpc::status_category()`（`std::error_category`），`status` 能转成 `std::error_code`；`deadline_exceeded` 与 net 的
  `cond::timeout` 等价，`cancelled` 与 `cond::canceled` 等价——调用方可以用同一套条件判断 net 与 rpc 的错误。
- 热路径不抛异常；处理协程抛出的异常在 `done` 回调里变成 `internal`，异常说明不发给对端（避免泄露内部信息），只记日志。

## 11. 内存与每次调用的分配预算

| 对象 | 放在哪 | 一元调用的稳态分配 |
| --- | --- | --- |
| 客户端调用状态 | 调用方协程帧的 awaiter 槽 | 0（≤ 184 字节时） |
| 请求 / 响应编码 | 连接的发送 slab（分片池） | 0 |
| 接收缓冲 | 连接的 64 KiB `rx` | 0 |
| 流表 | 开放寻址表 | 0 |
| 截止节点 | 嵌在调用状态里 | 0 |
| 取消回调（仅可取消的调用） | 分片小对象池 | 0 次 malloc |
| 服务端调用上下文 | 分片空闲链 | 0 |
| 响应预留与预算凭据 | 编码前由调用上下文持有，之后随 tx 块持有 | 目标 0；池容量与回收规则见 §5.9 |
| 服务端请求 / 响应消息 | protobuf Arena，初始块内联在上下文里 | 0（小消息） |
| 服务端处理协程帧 | 分片的回收式帧分配器 | 0 次 malloc |
| 服务端 `stop_source` | 每次新建 | 1（co2 支持重置后为 0，§15） |
| 客户端响应消息 | 调用方提供 | 由调用方决定 |

对照：用 `run_async` 启动处理协程（2 次分配、46 ns）、`net::timeout()` 做截止（1 次分配、153 ns），每次调用要多
3 次分配、约 200 ns。

## 12. 拦截器与可观测性

- **拦截器**：客户端与服务端各一条链，同步钩子，不在热路径上建协程：

  ```cpp
  struct server_interceptor {
      virtual ~server_interceptor() = default;
      virtual status on_request(server_context& ctx) { return {}; }   // 非 OK 则直接回 END，不调处理器
      virtual void on_finished(server_context const& ctx, status const& result) {}
  };
  ```

  需要 I/O 的准入（例如远程鉴权）由用户在处理协程开头做，或实现一个可以挂起的 `admission` 钩子（第二版）。
- **指标**：每分片一组非原子计数器与对数分桶的延迟直方图；导出时向每个分片 `post` 一次快照，合并后给出。热路径上
  没有原子操作。
- **追踪**：追踪上下文作为元数据（REQUEST 的 head）传递，`server_context` 暴露，拦截器负责接到具体的追踪系统。

## 13. 性能预算与基准

64 B 一元调用、单连接、一个在飞，在裸往返（3.1 µs）之上的额外开销。已测的原语用 net 的数字，其余是估算，由下面的
基准验证：

| 步骤 | 一侧 | 估计 |
| --- | --- | --- |
| 选子通道、分流号、登记流表 | 客户端 | 10 ns |
| 编码帧头 + head + 请求（protobuf 小消息） | 客户端 | 60 ns |
| 唤醒写协程（`post`）+ 取批次 | 两侧各一次 | 2 × 20 ns（`post` 实测 13 ns） |
| `any_stream` 擦除 | 两侧各一读一写 | 4 × 17 ns（实测） |
| 解帧头、查驻留表、取调用上下文 | 服务端 | 15 ns |
| `StopState` + 截止节点 | 服务端 | 20 ns（`stop_source` 实测 17 ns） |
| 解码请求进 Arena | 服务端 | 50 ns |
| 处理协程帧 + 启动 `post` + 完成 | 服务端 | 40 ns |
| 编码响应 | 服务端 | 50 ns |
| 解帧、查流表、解码响应、`post` 恢复调用方 | 客户端 | 80 ns |
| **合计** | | **约 0.45 µs**（目标 ≤ 0.7 µs） |

基准放在 rpc 的 `benchmarks/`。沿用 net 的分配计数与用户态 / 内核态统计，并增加逐请求的延迟记录；整轮耗时除以
请求数不能代替请求级 p50 / p99。原语成本与上表合计用于解释开销，不作为尾延迟预测。
首轮原始字节、同线程 epoll 回环对照见 [实测报告](../benchmarks/results/2026-09-27.md)；它没有验证上表的 protobuf 路径，
也未证明额外开销 ≤ 0.7 µs。当前测量口径和复现步骤见 [基准说明](../benchmarks/README.md)。

1. **首要验收：小消息一元延迟**。64 B / 4 KiB 指编码后的业务载荷，不含 RPC 帧头与 head；原始字节和 protobuf 分开报告。
   先用单连接一个在飞比较裸 net 与 RPC，再按独立于响应完成的计划持续发起请求，逐步提高请求速率并加入突发负载。
   延迟从调用发起计至完成，报告成功调用 p50 / p99、完成吞吐、拒绝率、超时率及排队时间；拒绝与其它失败的延迟分开统计。
   同时记录计划发起与实际发起的偏差，负载发生器跟不上时不能据该轮声称满足目标负载。比较时固定错误率约束，
   不以提高拒绝率换取成功调用的低 p99。默认快速拒绝与显式有限排队分别测量。
2. 流水线吞吐：单连接 1 / 8 / 64 个在飞，报告每秒调用数与每次调用的系统调用数（`strace -c`）。
3. 多分片扩展：1 / 2 / 4 / 8 个分片，每分片 32 条连接，服务端吞吐与扩展比；`taskset -c 4-11`（多线程行不能绑一个 CPU）。
4. 扇出：一个调用方 `when_all` 16 个下游调用。
5. 流式：单流 64 KiB 消息吞吐、1000 条流各 64 B 消息。
6. 截止：每次调用带 50 ms 截止与不带的差（验证 ≤ 20 ns）。
7. 对照：同机同负载下 gRPC C++（callback API）与 brpc 的 1–3 项。

响应预算的两组对照纳入基准 1：声明上限大而实际响应小；慢读大响应连接与正常小响应连接共存。除延迟与拒绝率外，
分别记录逻辑编码字节、预留容量、实际持有块容量及池缓存，验证保守预留的成本和慢连接的占用边界。

同一会话交替跑改动前后的二进制，固定后端、编译配置、CPU 亲和与负载；同线程回环、跨线程和跨进程分开报告。
每轮保存请求级分布与成功 / 失败计数，汇总多轮中位数及波动，不以最好一轮代替 p99 验收；不同日期的绝对值不能直接横比。

## 14. 测试

- `wire`：编解码往返的单元测试 + libFuzzer（帧头、各 head 段、varint 边界、截断输入），所有解码函数对任意输入不越界、
  不分配超过声明的上限。
- `connection`：用 `net::test::memory_stream`（`max_read_size` 模拟帧被拆成任意小块到达）与 `net::test::fuse`（每次
  操作前注入错误）驱动读写协程，覆盖每一条错误路径；写合并用计数的内存流断言"N 个响应一次写出"。
- 端到端：回环 TCP 上的一元、流式、取消、截止、GOAWAY 重试、背压、过载；四个后端（epoll / poll / select / io_uring）
  各跑一遍，Windows 上 IOCP。
- 准入：默认饱和时不进入等待队列；显式排队的长度 / 字节上限、无截止配置拒绝、满队列拒绝、取消 / 到期出队、
  READY 后重新准入及 GOAWAY 后禁止执行超出截止流号的排队请求；覆盖慢读响应时的发送预算，验证过载后可恢复。
- 响应预算：方法上限缺失 / 不可兑现的启动拒绝，两级预留失败的完整回滚、池分配失败发生在 invoke 前、超限响应的固定错误、
  handler 完成后 tx 继续占额、共享 slab 尾部不提前归还、控制额度耗尽关闭连接。用可手动完成的异步测试流验证部分写、
  取消与关闭后的最后引用回收，确保部分帧不会被截断后接上其它帧；立即完成的内存流不足以覆盖这些时序。
- 并发：多分片 + 跨线程取消的压力测试在 TSan 与 ASan 下反复跑（2 核与 8 核），与 net 的 CI 矩阵一致。

## 15. 需要 net / co2 补的能力

| 能力 | 用在哪 | 现状 |
| --- | --- | --- |
| 可重置的 `stop_source`（没被请求过、只剩一份引用时复用 `StopState`） | 服务端每次调用省一次分配（§6.4） | co2 没有 |
| `SO_REUSEPORT` 的具名选项 | 每分片接受器（§6.1） | 可用 `socket_option::boolean<SOL_SOCKET, SO_REUSEPORT>` 代替 |
| 帧不分配的 TLS 读写驱动 | TLS 连接的每次读写（§5.8） | 每次读写一个 task 帧 |
| io_uring `SEND_ZC` | 大消息发送（§5.3） | 未使用 |
| io_uring 用户态定时器堆 | 与 rpc 无直接关系（时间轮已避开） | 每次限时一个内核超时，+580 ns |

第一阶段每次新建 `stop_source`，其复用能力属于后续分配优化，不阻塞实现。task 启动直接使用 §6.3 的已有公开接口。

## 16. 否决的方案

| 方案 | 否决理由 |
| --- | --- |
| 以 gRPC（HTTP/2）为主协议 | 一元调用每个方向 2–3 帧、HPACK 状态、每消息 5 字节前缀、两级流控的 WINDOW_UPDATE；互通需求可以由协议层的适配器满足（连接之上换一套帧读写），不必让所有调用付这份代价 |
| 所有线程 `run()` 同一个 `io_context` | net 实测 4 线程 1.8×、io_uring 1.42×；锁与线程交接在每次完成上付费 |
| 方法名每次随请求发送 / 32 位哈希 | 前者每次多几十字节和一次字符串查找；后者碰撞时静默调用错误的方法 |
| 每次调用 `net::timeout()` 做截止 | +153 ns、1 次分配，io_uring 上 +580 ns；时间轮每次约 10 ns、零分配 |
| 每次调用 `run_async` 启动处理协程 | 2 次分配、46 ns；池化上下文里嵌完成帧是 0 次 |
| 调用方直接写套接字（加锁串行化） | 失去写合并，每个调用一次系统调用；锁在多核上争用 |
| 读协程完成调用时嵌套恢复调用方 | 调用方可能销毁连接；并且打断批量处理（§5.5） |
| 流号 64 位、永不轮换 | 帧头多 4 字节；轮换的代价是约半小时一次连接建立 |
| 连接按消息类型模板化 | 连接代码按传输与消息类型重复实例化；`any_stream` 的擦除只多约 17 ns / 次 I/O |

## 17. 实现顺序

1. **wire + 单分片一元**：`wire` 编解码与模糊测试；`connection`（TCP，`rx` 读、写合并）；原始字节 codec；最小的
   客户端（固定端点）与服务端（单分片接入）；`status`、取消与截止、按方法上限预留响应存储及默认快速拒绝。
   验证 §5.9 的容量不变量并补齐接收与调用对象的资源边界，再用基准 1、2 对照裸 net；可选有限排队仅在截止、
   取消出队与队列容量测试通过后开启。
2. **protobuf 与生成代码**：`codec` 的 protobuf 特化、protoc 插件、Arena；服务端调用上下文池；截止时间轮与分配优化
   按实测收益引入，保持第一阶段的取消、截止和准入语义。
3. **多分片**：`runtime`、接入分配（默认 + `SO_REUSEPORT`）、每分片子通道、`round_robin` / `power_of_two`、
   `static` / `dns` 解析器、子通道状态机与退避、GOAWAY 与排空。基准 3、4。
4. **流式与治理接缝**：流式调用与每流窗口、背压、重试、拦截器、Unix 域与 TLS 传输。基准 5、6。
5. **io_uring 专门路径与其它**：`receive_source` 零拷贝接收、`byte_buffer`、压缩、指标导出；对照基准 7。

### 17.1 当前实现边界

当前已落地 `lrpc::wire`、`lrpc::codec`、`lrpc::unary` 和可选的 `lrpc::protobuf`：C++14、TCP、单分片的一元调用；公开接口在
`unary/include/rpc/unary.hpp`，运行示例在 `examples/unary_echo.cpp`。实现以取消、截止、准入及关闭时的资源所有权为基线，
已建立原始字节、同线程及跨进程 epoll 回环的测量工具，尚未完成 §13 的完整性能验收。下列条目记录当前实现，前文的对象池、时间轮等仍是后续目标。

- 连接各有一条读链和写链。读链每处理 64 帧主动让出；写链每批最多 16 帧、256 KiB，单帧超过 256 KiB 时独占一批。
  取消已开始写入的请求不会截断帧；写失败则关闭连接，最后一份缓冲引用在 I/O 完成被消费后释放。
- 方法定义在 writer 提交时生效；尚未提交就取消的调用不会让后续请求引用未发送的定义。服务端即使拒绝请求，也保留其有效方法定义。
  准入分配失败会回滚本次新增的方法槽，避免耗尽尚未发送的定义名额。客户端等待服务端 SETTINGS 后准入，默认无等待队列、重连或重试。
- 请求、响应、控制帧分别使用预分配定长块池，每块按 payload 容量加内部 descriptor 收费，池缓存也计入配置预算。
  服务端请求块为 `max_frame_size` 字节，客户端请求块另含 16 字节帧头；响应块为全部方法中最大的 `16 + max_response_head_bytes + max_response_bytes`，
  控制块为 64 字节。响应同时取得分片存储及连接额度后才启动 handler，转入 tx 后仍占额。控制池耗尽会关闭连接。
- 每个方法必须显式声明响应上限，`0` 表示空响应；缺失、重复方法名或连一个块都无法兑现的预算在启动时拒绝。
  peer 的响应消息/帧上限在执行前检查；handler 超出自身声明上限返回 `internal`。
- 本实现允许的本端 `max_frame_size` 为 36 字节到 16 MiB。`connection_options.receive_buffer_bytes` 控制每连接固定接收窗口：
  默认 `0` 使用 `max_frame_size + 16`；显式值必须在 `[max_frame_size + 16, 16 MiB + 16]` 内。
  增大窗口可在一次读取中接收多帧，不改变 SETTINGS、允许的帧大小或请求/响应池预算。
  `max_connections` 包含已关闭但仍被 I/O、handler 或 deadline task 持有的连接，因此服务端接收缓冲总量不超过
  `max_connections × 实际接收窗口字节数`。连接名额在接收缓冲和其它连接资源析构后归还；窗口分配失败回滚该名额。
- 服务端方法槽由 `max_connections × (max_method_ids + 1)` 限制，方法 ID 上限最多 65536；客户端方法名长度和驻留数量也受配置限制。
  服务端活跃调用受 `max_active_calls`、每连接 stream 上限及请求/响应池共同约束；取消后尚未退出的 handler 仍占用调用名额。
  客户端 pending 表受本端与 peer 较小的 stream 上限以及请求池约束。调用对象、协程帧和标准容器仍有动态分配。
- `resource_stats` 的三个 `*_bytes_in_use` 是当前整块占用，`storage_bytes` 是池的实际 payload 与 descriptor 容量之和，含空闲缓存。
  它不覆盖接收缓冲、方法表、调用对象、分配器元数据、传输层和用户 handler 的内存，不是 RSS 总量。
- 截止沿用绝对时间，发送前编码剩余微秒，已过期的有限截止不得编码为无限；服务端从收到请求时计算截止。
  当前以 `steady_timer` 与 `run_async` 建立正确性路径，未宣称达到时间轮或零分配目标。
- API 与析构运行在所属分片线程；父 task 的 stop token 可从其它线程请求停止。`close()` 后继续运行 context 排空，
  `drain()` 不带自动宽限期，handler 若不响应取消且不结束会继续占额。
- P2 的 `codec_ops` 与 `client::call_encoded` 将类型化消息接入相同的准入/取消/截止路径；请求直接编码到池块，响应直接解码到调用方对象。
  protoc 插件生成一元 service/stub、带幂等性和 codec 的描述符及自持有适配器的 bindings；部署时显式配置各方法响应上限。
  当前使用 `net::task<call_result>` 和 `make_server(..., bindings)`，尚未引入前文的 `channel`、`server_builder`、池化 `unary_call` 或进程全局稠密下标。
  每次类型化 handler 的非移动协程帧拥有带 2 KiB 初始块的 protobuf Arena，覆盖业务挂起及编码；可向堆扩容，未宣称零分配。
  C++14 生成、proto2/3、optional、lite、嵌套/导入已验证；流式方法明确拒绝生成。
- metadata 沿用既有 REQUEST/END 语法：请求通过 `call_options.metadata`，响应通过 `server_context.response_metadata.assign` 立即复制。
  `max_response_head_bytes` 默认 2，范围 2–65535，计入执行前预留和对端帧上限；超限使用原预留生成空 metadata 的 `END(internal)`。
  客户端提供 `call_options.response_metadata` 有界缓冲，在 `call_result.response_metadata` 中取得视图；默认丢弃，容量不足返回 `resource_exhausted`。
  成功及错误 END 均可携带 metadata。实际 head 变短时，响应 body 在预留块内前移。完整 API 和生成示例见 [P2 用法](protobuf.md)。

测试已覆盖真实回环 TCP 上的并发逆序完成、跨线程取消、超时、快速拒绝与恢复、GOAWAY，以及手动异步传输上的部分写、
延迟取消完成、关闭后的缓冲/名额回收、异常握手、协议错误和拆包粘包。新增完成/取消/截止/关闭的确定顺序测试、
客户端部分写取消后的帧完整性、多连接请求/响应/控制及调用额度隔离、准入与任务分配故障注入。
epoll/poll/select/io_uring 的 TCP 和外部取消线程压力矩阵已在 Debug、Release、ASan/UBSan/LSan 与独立 TSan 配置验证；
不可用后端以跳过报告，TSan 不构建与其全局 new 拦截冲突的故障注入程序。

基准覆盖 64 B / 4 KiB、1/8/64 个在飞、开放负载与突发，并分开记录成功、拒绝、超时及发生器丢弃。
分配栈与可选 tx 队列直方图仅用于诊断，不参与延迟验收；后者只统计实际提交帧的入队至首次写提交等待。
跨进程每端各一执行器线程，其 CPU 统计只覆盖客户端。加长开放负载观察已出现调度迟到和拒绝，短轮通过不能外推持续稳定达标。
分配归因见 [P0 验证报告](../benchmarks/results/2026-09-27-p0.md)；接收窗口优化及仍未通过的开放负载门槛见
[后续复测](../benchmarks/results/2026-09-27-open-load.md)。帧缓存减少分配但未稳定改善 p99，当前保留默认系统分配器，
调用对象池、时间轮及其余 §13 场景仍待完成。

依赖 net 的冷定时器堆扩容 OOM 会在 `noexcept` 内终止进程，RPC 无法捕获；按本轮约定保持依赖不变，
复现与故障测试覆盖边界见 [已知限制](net-timer-oom.md)。常规测试通过不表示所有分配失败均可恢复。
