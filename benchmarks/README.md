# 单分片 TCP 基准

`unary_bench` 对照原始 net 固定长度回显与 lrpc 原始字节 unary 调用。默认双方同进程、同线程、同一个 `io_context`；
也支持两进程各一个执行器线程。两种拓扑均使用一条回环 TCP 并启用 `TCP_NODELAY`。
64 B / 4 KiB 是业务载荷；RPC 额外携带协议头。工具目前面向 Linux，各拓扑分别报告，不能外推生产网络的 p99。

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
校验器自身的边界测试可运行 `uv run --offline python benchmarks/test_run_suite.py`。
CPU 亲和不等于独占 CPU，请同时查看环境文件中的 SMT、频率策略，并避开其它编译或负载。

`--suite closed` 覆盖 64 B / 4 KiB 和 1/8/64 个在飞；`open` 覆盖每秒 2 万、10 万、30 万、100 万次计划发起，
另加 16 个请求一组的突发；`cache` 在闭环上交替系统分配器与 net 帧缓存，并加入 50 ms 截止；`smoke` 用于快速检查正常、
丢弃、拒绝及超时路径。自检建议 `--rounds 1 --iterations 1000 --warmup 100`，不将该结果当作性能证据。
`core` 聚焦两种载荷的单在飞闭环和每秒 30 万次开放负载；`open-core` 只运行其中的开放负载。

基准的协议帧上限固定为 8192 B，两端 RPC 接收窗口默认 65536 B，与 raw 的固定读取窗口相同。
`--receive-buffer-bytes` 可改 RPC 窗口，raw 只接受 65536；suite 用 `--rpc-receive-buffer-bytes 8208 65536`
在同一二进制下交替对照多个窗口。窗口大小写入每轮 JSON 和汇总表。

跨进程对照在同一命令上增加 `--topology separate-process --cpu 2 --server-cpu 4`。脚本每轮启动新的服务端，
读取有截止保护的 ready 消息后才启动客户端；结束后发送 SIGTERM 并等待关闭、排空，强制终止会使该轮失败。
两进程各一个执行器线程，CPU 应选择不同物理核。单独运行可使用 `--role server`，其输出临时回环端口，
再以 `--role client --port PORT` 连接。跨进程的 CPU 与分配统计只覆盖客户端，`cpu_scope` 标明该边界。

测量边界：

- 连接、SETTINGS、首次方法定义及预热不计入正式区间。带截止的实验以不短于 1 秒的截止预热相同路径，正式阶段再切换到配置值。
  截止从实际进入调用 API 起算，未从计划时刻扣除调度迟到。
- 每条请求保存 `scheduled / offered / started / completed`。`started` 在公共请求协程实际调用 API 前，`completed` 在 API 返回后、
  内容验证前。成功 API 延迟为 `completed-started`；`schedule_lag` 为 `started-scheduled`；计划起点总延迟为 `completed-scheduled`。
- 闭环完成一个再补一个；开放负载始终按 `floor(i/burst)*burst/rate` 的绝对计划发起，追赶时不重置起点。
  驱动槽满时记为 `generator_drop`，不伪装成 RPC 拒绝。拒绝合并本地和服务端的 `resource_exhausted`，超时及其它错误单列。
- 预分配请求槽和样本；payload 内嵌序号，成功响应检查长度及全部内容。不匹配使整轮无效。
  两侧使用相同的每调用 `run_async` 公共驱动；其启动/验证开销仍影响吞吐和 CPU。raw 闭环单在飞直接 `write/read`，
  其余配置用固定长度 FIFO、单读链和单写链。raw 是没有 RPC 准入、截止和取消语义的传输基线。
- 分位数由逐请求样本计算，取 `ceil(p*N)-1`；成功、拒绝、超时、其它错误互不混合。汇总表取逐轮 p99 的中位数并保留最小/最大值。
  `success_per_second` 与其它 `*_per_second` 使用包含尾部排空的整轮时间；`actual_start_span_rate` 单独按首末实际调用间隔计算。
- 开放负载的 `load_faithful` 要求无驱动丢弃，且每次实际发起相对计划的迟到不超过 `--max-schedule-lag-us`（默认 100 µs）。
  这是本工具的有效性阈值，不是服务 SLO。`eligible_for_zero_error_p99` 还要求全部请求成功。
  汇总表的中位数包含未通过的轮次，须同时检查 `Eligible rounds`。插桩构建的 `diagnostic_only=true`，不计入延迟验收。

常用单次选项：`--transport net|rpc`、`--bytes 64|4096`、`--inflight N`、`--backend epoll|poll|select|io_uring`、
`--rate R`（0 为闭环）、`--burst N`、`--rpc-streams N`（服务端 stream 上限）、`--deadline-us N`、
`--frame-allocator system|recycling`、`--receive-buffer-bytes N`、`--samples path.csv`。raw 不实现逐调用截止。
单次运行阶段含预热有 30 秒看门狗，随后关闭并用 `context.run()` 排空；suite 另对客户端进程设 60 秒超时。

## 混合负载

`mixed_bench` 测量与批量流共用一条连接时的一元延迟。服务端和客户端各一个线程，走一条回环 TCP。客户端在一条双向流上持续写
`--bulk-bytes` 大小的消息（默认 1 MiB，0 表示不开批量流），同时逐个发 `--bytes`（默认 64 B）的回显调用。输出一元调用的
p50/p99/p999/max 和批量吞吐。`--window` 设流窗口，也就是在途批量字节的上限；`--fragment-bytes`、`--send-budget`、
`--receive-buffer-bytes` 覆盖对应的连接选项。`bench` preset 会一起构建它：

```sh
taskset -c 2,4 ./build/bench/benchmarks/mixed_bench --bulk-bytes 1048576 --window 4194304 --calls 20000
```

## 分配诊断

`--frame-allocator recycling` 使用 net 的 `recycling_memory_resource` 复用协程帧，工具默认使用 system。

`bench-alloc` preset 构建带全局 `new/new[]` 计数的二进制，输出 `new_calls_per_offered`。计数含公共驱动，
不计直接 `malloc`、对齐分配或 RSS；其延迟不能代替未插桩结果。环境变量 `LRPC_BENCH_FAIL_NEW_AT=N`
在第 N 次分配注入一次 `bad_alloc`，用于检查失败后的异步排空。

`diagnose` preset 提供带调试信息、固定代码地址的分配追踪构建。正式区间启用 `--allocation-trace build/diagnose/trace.json` 后，
工具用固定 2,048 个槽、每栈最多 24 帧记录调用位置和请求字节数；满表以 `lost` 显式报告。
将该次运行的 JSON 输出保存为 `build/diagnose/trial.json`，再将地址映射回同一个二进制：

```sh
uv run --offline python benchmarks/analyze_allocations.py \
  --binary build/diagnose/benchmarks/unary_bench \
  --trace build/diagnose/trace.json --summary build/diagnose/trial.json \
  --out build/diagnose/allocations.json
```

归类依据最靠近分配的调用帧，保留完整解析栈供复核。服务端角色不支持 allocation-trace。
用户态/内核态 CPU 由 `getrusage` 记录；`perf`、`strace` 应另跑。

## JSON codec 基线

启用 `LRPC_BUILD_JSON=ON` 后，`json_codec_bench [iterations]` 比较有界单次编码、size 后 encode 和 SAX 解码，
输出每组的线上字节、p50/p99/平均纳秒；默认每组 100000 次，预热 1000 次。
解码计时包含目标容器分配与结果比较，不包含 TCP 或 RPC 调度，不能用于网络 p99 验收。
