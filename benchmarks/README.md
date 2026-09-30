# 单分片 TCP 基准

`unary_bench` 对照原始 net 固定长度回显与 `lrpc::unary` 原始字节或 protobuf Echo。默认双方同进程、同线程、同一个
`io_context`；也支持两进程各一个执行器线程。两种拓扑均使用一条回环 TCP 并启用 `TCP_NODELAY`。
64 B / 4 KiB 是编码后的业务载荷；RPC 额外携带协议头。protobuf 单 bytes 字段分别放入 62/4093 B 内容。
工具目前面向 Linux，各拓扑分别报告，不能外推生产网络的 p99。
首轮数据与帧缓存实验结论见 [2026-09-27 实测报告](results/2026-09-27.md)。
加长观察、跨进程与分配归因见 [P0 验证报告](results/2026-09-27-p0.md)。
接收窗口优化、主机停顿诊断及尚未通过的严格门槛见 [开放负载复测](results/2026-09-27-open-load.md)。
非自愿抢占、运行队列等待与内核跟踪进展见 [调度诊断](results/2026-09-27-scheduler.md)。
本轮固定调用槽、共享截止与 protobuf 配对数据见 [运行时报告](results/2026-09-30-runtime.md)。
与 gRPC、brpc、Alibaba coro_rpc 的本机真实 API 对照见 [四库性能报告](results/2026-09-30-libraries.md)。

构建和运行：

```sh
cmake --preset bench \
  -DLRPC_NET_SOURCE_DIR=/home/lvyues/code/net/net \
  -DNET_CO2_DIR=/home/lvyues/code/coro/coro \
  -DCMAKE_DISABLE_FIND_PACKAGE_co2=ON
cmake --build --preset bench
taskset -c 2 ./build/bench/benchmarks/unary_bench \
  --transport rpc --bytes 64 --inflight 1 --iterations 100000 --warmup 5000
uv run --offline python benchmarks/run_suite.py \
  --binary build/bench/benchmarks/unary_bench --out build/bench-results/closed \
  --suite closed --rounds 5 --iterations 100000 --warmup 5000 --cpu 2
```

`run_suite.py` 串行运行各配置，隔轮反转配置顺序，保存环境与源码/二进制哈希、逐轮 JSON、逐请求 CSV.gz 和汇总表。
输出目录必须尚不存在。脚本独立重算 CSV 的分类计数和分位数，检查时间戳顺序及开放负载的计划时刻；任何不一致使运行失败。
同时核对输出窗口和配置、汇总计数及有效轮标记；校验器自身的边界测试可运行
`uv run --offline python benchmarks/test_run_suite.py`。
CPU 亲和不等于独占 CPU，请同时查看环境文件中的 SMT、频率策略，并避开其它编译或负载。

`--suite closed` 覆盖 64 B / 4 KiB 和 1/8/64 个在飞；`open` 覆盖每秒 2 万、10 万、30 万、100 万次计划发起，
另加 16 个请求一组的突发；`cache` 在闭环上交替系统分配器与 net 帧缓存，并加入 50 ms 截止；`smoke` 用于快速检查正常、
丢弃、拒绝及超时路径。自检建议 `--rounds 1 --iterations 1000 --warmup 100`，不将该结果当作性能证据。
`core` 聚焦两种载荷的单在飞闭环和每秒 30 万次开放负载，可用 `--iterations 600000 --rounds 5` 延长观察区间。
`open-core` 只运行其中的每秒 30 万次开放负载。

基准的协议帧上限固定为 8192 B，两端 RPC 接收窗口现在默认 65536 B，与 raw 的固定读取窗口相同。
这只扩大一次读取可容纳的帧数；库的默认配置未变。P0 使用的 RPC 窗口为 8208 B，单次可用
`--receive-buffer-bytes 8208` 重现；raw 只接受 65536。suite 用 `--rpc-receive-buffer-bytes 8208 65536`
在同一二进制下交替对照两个窗口，并将相同配置传给跨进程服务端。窗口大小写入每轮 JSON 和汇总表。

跨进程对照在同一命令上增加 `--topology separate-process --cpu 2 --server-cpu 4`。脚本每轮启动新的服务端，
读取有截止保护的 ready 消息后才启动客户端；结束后发送 SIGTERM 并等待关闭、排空，强制终止会使该轮失败。
两进程各一个执行器线程，CPU 应选择不同物理核；当前机器的 2/3 是 SMT 同核，2/4 是不同核。
单独运行可使用 `--role server`，其输出临时回环端口，再以 `--role client --port PORT` 连接。
跨进程的 CPU、分配及队列统计只覆盖客户端；`cpu_scope` 明确该边界，不将其与同线程的双方合计直接比较。

测量边界：

- 连接、SETTINGS、首次方法定义及预热不计入正式区间。带截止的实验以不短于 1 秒的截止预热相同路径，正式阶段再切换到配置值。
  截止从实际进入调用 API 起算，未从计划时刻扣除调度迟到。
- 每条请求保存 `scheduled / offered / started / completed`。`started` 在公共请求协程实际调用 API 前，`completed` 在 API 返回后、
  内容验证前。成功 API 延迟为 `completed-started`；`schedule_lag` 为 `started-scheduled`；计划起点总延迟为 `completed-scheduled`。
  普通构建的库内 tx 排队输出为 `null`，另以 `bench-queue` 诊断；调度迟到不代表库内排队。
- 闭环完成一个再补一个；开放负载始终按 `floor(i/burst)*burst/rate` 的绝对计划发起，追赶时不重置起点。
  驱动槽满时记为 `generator_drop`，不伪装成 RPC 拒绝。拒绝合并本地和服务端的 `resource_exhausted`，超时及其它错误单列。
- 预分配请求槽和样本；payload 内嵌序号，成功响应检查长度及全部内容。不匹配使整轮无效。
  两侧使用相同的每调用 `run_async` 公共驱动；其启动/验证开销仍影响吞吐和 CPU。raw 闭环单在飞直接 `write/read`，
  其余配置用固定长度 FIFO、单读链和单写链。raw 是没有 RPC 准入、截止和取消语义的传输基线。
- 分位数由逐请求样本计算，取 `ceil(p*N)-1`；成功、拒绝、超时、其它错误互不混合。汇总表取逐轮 p99 的中位数并保留最小/最大值。
  `success_per_second` 与其它 `*_per_second` 使用包含尾部排空的整轮时间；`actual_start_span_rate` 单独按首末实际调用间隔计算。
- 开放负载的 `load_faithful` 要求无驱动丢弃，且每次实际发起相对计划的迟到不超过 `--max-schedule-lag-us`（默认 100 µs）。
  这是本工具明确的有效性阈值，可按目标修改；不是服务 SLO。`eligible_for_zero_error_p99` 还要求全部请求成功。
  未通过的轮次仍保留用于诊断；汇总表的中位数包含这些轮次，须同时检查 `Eligible rounds`，
  不能用未通过轮次宣称满足对应负载下的低 p99。
  插桩构建的 `diagnostic_only=true`，不计入可用于延迟验收的轮次。

常用单次选项：`--transport net|rpc|v2`（v2 只支持 bytes codec 和 client 入口）、`--codec bytes|protobuf`、`--bytes 64|4096`、`--inflight N`、`--backend epoll|poll|select|io_uring`、
`--rate R`（0 为闭环）、`--burst N`、`--rpc-streams N`（服务端 stream 上限）、`--deadline-us N`、
`--frame-allocator system|recycling`、`--receive-buffer-bytes N`、`--samples path.csv`。raw 不实现逐调用截止。单次运行阶段含预热有 30 秒看门狗，
随后关闭并用 `context.run()` 排空，该清理阶段没有内部截止；suite 另对客户端进程设 60 秒超时。

protobuf 基准要求同时开启 `LRPC_BUILD_PROTOBUF`、`LRPC_BUILD_CODEGEN` 和 `LRPC_BUILD_BENCHMARKS`；raw net 仅支持 bytes。
protobuf prepare 在计时前设置请求字段，API 区间包含 codec 编码、服务端解码/编码和客户端解码，内容验证在计时结束后。
为与旧实现保持相同驱动，该基准使用兼容的字符串 typed 入口，不测生成 Stub 的冷绑定收益。

`compare_runtime.py` 配对两个二进制：closed 默认覆盖两种 codec、64/4096 B、1/8/64 在飞和 0/50 ms 截止；
open-core 使用每秒 30 万次计划发起、128 个驱动槽。隔轮反转版本和配置顺序，支持同线程/跨进程。
它分别记录旧库归档、共用驱动快照、旧构建配置和新源码，逐轮核对输出配置与 CSV，保留失败或无效轮次。

```sh
uv run --offline python benchmarks/compare_runtime.py \
  --before build/runtime-baseline/unary_bench \
  --after build/protobuf-release/benchmarks/unary_bench \
  --baseline-source build/runtime-baseline \
  --out build/bench-results/runtime-closed --rounds 3 --iterations 20000 --warmup 2000
```

旧二进制必须在改实现前冻结；baseline-source 包含 implementation.tar（旧库）、harness.tar（共用基准/CMake）和 config/。
当前工作树不能替代旧二进制来源。`--inflight 1` 可用于延长首要场景；分配插桩二进制另跑，延迟不参与验收。
`bench-alloc` 的 `api_probe` 仅比较无网络的 task/直接 awaiter 表示，不是端到端 RPC 性能证据。

后续阶段的缓存/入口实验使用 `--arena-cache N`（每 unary adapter 的 protobuf Arena 槽数，默认 0）、
`--rpc-entry client|channel|runtime`（默认 client）、`--runtime-cpu0/1 CPU`。
channel 与 client 共用单执行器和单连接；runtime 为两分片、两连接、三个线程，驱动线程之外使用两个显式绑定的 worker CPU。
runtime 结果衡量整套配置的成本，不能直接称为 facade 开销或多核扩展比。非 client 入口暂不支持跨进程或 recycling 模式。

```sh
uv run --offline python benchmarks/upgrade_suite.py \
  --binary build/protobuf-release/benchmarks/unary_bench \
  --before build/runtime-baseline/steps-1-4-unary-bench \
  --out build/bench-results/steps5-11 \
  --rounds 5 --iterations 30000 --warmup 3000 --cpu 2 --worker-cpus 4 6
```

此脚本交替比较冻结的 1–4 步二进制与当前默认路径、system/recycling、每调用 Arena/64 槽缓存、三种入口。
逐请求 CSV 独立验证，错误/无效轮原样保留并标注 eligible，不用于成功 p99 验收。
本轮中间基线只冻结了二进制和哈希，没有对应的中间源码归档，不能宣称该基线可由当前源码完全重建；
完整来源要求见前述 compare_runtime。实测与缓存选择见 [升级阶段报告](results/2026-09-30-upgrade.md)。

帧缓存和分配诊断：

`--frame-allocator recycling` 使用 net 的 `recycling_memory_resource`，寿命覆盖 context 和全部任务，默认每尺寸类缓存至多 64 块。
这只复用协程帧，不能消除调用对象、表项、stop state、定时器和 `run_async` 完成状态的所有分配，也不计入 RPC 的 payload 池统计。
只有在相同错误率和负载下取得可重复收益才适合选择该配置；工具默认使用 system。

另用 `bench-alloc` preset 构建带全局 `new/new[]` 计数的二进制，再执行同一 suite。计数含公共驱动，
不计直接 `malloc`、对齐分配或 RSS；其延迟不能代替未插桩结果。诊断构建支持环境变量 `LRPC_BENCH_FAIL_NEW_AT=N`
在第 N 次分配注入一次 `bad_alloc`，用于检查失败后的异步排空。

用户态/内核态 CPU 由 `getrusage` 记录。`perf`、`strace` 应另跑；没有权限时不填造 CPU 热点或系统调用次数。

`diagnose` preset 提供带调试信息、固定代码地址的分配追踪构建。正式区间启用 `--allocation-trace build/diagnose/trace.json` 后，
工具用固定 2,048 个槽、每栈最多 24 帧记录全局 `new/new[]` 的调用位置和请求字节数；满表以 `lost` 显式报告。
将该次运行的 JSON 输出保存为 `build/diagnose/trial.json`，再用以下命令将地址映射回同一个二进制，并核对追踪总数与该轮分配计数：

```sh
uv run --offline python benchmarks/analyze_allocations.py \
  --binary build/diagnose/benchmarks/unary_bench \
  --trace build/diagnose/trace.json --summary build/diagnose/trial.json \
  --out build/diagnose/allocations.json
```

归类依据最靠近分配的调用帧，保留完整解析栈供复核；分配次数和请求字节数不是耗时、存活内存或 RSS。
`allocation_counting`、`allocation_stacks`、`queue_instrumentation`、`sanitizers` 分别标识插桩状态。
服务端角色不支持 allocation-trace，传入会报错。

`bench-queue` preset 只启用发送队列探针，不启用分配计数或 backtrace；应与分配追踪分开运行，以减小诊断间的相互干扰。
队列边界为进入 RPC tx 队列至首次提交写，统计实际提交的帧，排除发送前取消/到期和关闭时丢弃的帧；
它不是全部准入调用的排队时间，也不包括内核发送等待。直方图桶的上界为 `2^index` ns，报告分位数时须标为桶上界。
raw net 未被该探针观测，输出 `null`；跨进程客户端的服务端响应队列也为 `null`。
该开关增加内部块描述符大小，可能改变固定预算下的块数，因此队列诊断也不参与正式性能验收。

## JSON codec 基线

启用 `LRPC_BUILD_JSON=ON` 后，`json_codec_bench [iterations]` 比较有界单次编码、size 后 encode 和 SAX 解码，
使用有效 UTF-8 字符串与结构体字段。输出每组的线上字节、p50/p99/平均纳秒；默认每组 100000 次，预热 1000 次。
解码计时包含目标容器分配与结果比较，不包含 TCP、RPC 调度或 metadata，不能用于网络 p99 验收。
构建与约束见 [JSON 用法](../docs/json.md)。
