# 2026-09-30：lrpc、gRPC、brpc 与 Alibaba coro_rpc 性能对照

本机固定 CPU 预算的一元 Echo 实测中，lrpc 与 coro_rpc 的单在飞延迟接近，62 B 业务内容的五轮范围重叠；
8/64 在飞时，lrpc 的 p99 更低、吞吐更高。该判断针对下述版本、正常调用 API 和配置，不能外推各库的多核吞吐上限。

## 测试条件

客户端、服务端分属两个进程，各自所有线程分别绑定 CPU 2、CPU 4；驱动/监测进程绑定 CPU 8，三者是不同物理核。
i7-13700KF、GCC 13.3、Linux 7.0.0-34。CPU 2/4 为 powersave / balance_performance，turbo 开启，未隔离其它主机任务。
采用本机明文回环 TCP、持久单连接配置、TCP_NODELAY、闭环一元 RPC；关闭重试、请求截止、TLS 和压缩。
每个配置五轮，先预热 10,000 次，再测 100,000 次。共 120 次正式测试、12,000,000 次正式请求，全部成功并通过全量 CSV 校验。
配置隔轮反转，库顺序逐轮旋转，各库在四个执行位置都出现；第五轮重复第一轮，因此不宣称完全消除顺序影响。

| 库 | 实际接口 / 协议 | 编码与构建 |
| --- | --- | --- |
| lrpc 当前工作树 | 单执行器 client，typed protobuf call，epoll | protobuf 3.21.12；C++14，Release -O3；系统帧分配、每调用 Arena |
| [gRPC C++ 1.84.0](https://github.com/grpc/grpc/releases/tag/v1.84.0) | AsyncEcho + CompletionQueue，服务端一个 CQ poller；HTTP/2 | 固定依赖 protobuf 35.1（C++ 7.35.1）；C++20，Release -O3 |
| [brpc 1.18.0](https://github.com/apache/brpc/releases/tag/1.18.0) | 共享单连接 Channel，常驻 bthread 同步调用；baidu_std | protobuf 3.21.12；Release，库的最终优化选项为上游 -O2，适配器 -O3；4 个 bthread worker |
| [Alibaba yaLanTingLibs coro_rpc 0.6.1](https://github.com/alibaba/yalantinglibs/releases/tag/0.6.1) | 一个 executor、一个 TCP client，send_request 双 await；coro_rpc 原生协议 | 默认 struct_pack；C++20，Release -O3；拥有型 std::string 请求/响应 |

请求内容为 62/4093 B，服务端返回完整等长内容。对 protobuf 而言编码体为 64/4096 B；
coro_rpc 编码体与协议头不同。因此这是相同业务内容、各库正常编码路径的端到端比较，并非相同 wire bytes 或纯传输层比较。
gRPC 与 lrpc/brpc 的 protobuf 版本也不同，不将性能差异完全归因于网络或调度实现。

API 延迟从进入实际调用前到 CQ 交付、同步返回或协程恢复后，包含该路径的编码/解码；完整响应校验在结束时间戳之后。
显式准备请求、Controller Reset、gRPC ClientContext 创建等调用方准备不进入单次 API 延迟；它们、逐请求验证、补发和最终排空仍影响整轮吞吐及客户端 CPU。
连接和预热排除在正式区间之外，每个 slot/lane 完成后立即补发，阶段末尾才统一排空。

## p99 与吞吐

p99 为每轮全部成功请求的 nearest-rank 分位数，再取五轮中位数；方括号是五轮最小/最大值，不是置信区间。
吞吐为每轮成功次数除以含尾部排空的正式阶段耗时，再取中位数。kRPC/s 表示每秒千次请求。

### 业务内容 62 B（protobuf 编码体 64 B）

| 库 | 最大在飞 | p99 µs 中位数 [最小, 最大] | 吞吐 kRPC/s | 客户端 CPU µs/调用 |
| --- | ---: | ---: | ---: | ---: |
| lrpc | 1 | 9.237 [8.760, 9.834] | 129.6 | 4.311 |
| gRPC C++ 1.84.0 | 1 | 41.474 [38.810, 46.328] | 28.7 | 18.664 |
| brpc 1.18.0 | 1 | 14.393 [14.121, 15.783] | 84.6 | 7.119 |
| coro_rpc 0.6.1 | 1 | 9.369 [9.090, 11.882] | 125.5 | 5.069 |
| lrpc | 8 | 15.658 [15.364, 16.352] | 535.8 | 1.007 |
| gRPC C++ 1.84.0 | 8 | 192.573 [188.720, 200.684] | 63.9 | 13.643 |
| brpc 1.18.0 | 8 | 69.915 [62.230, 74.074] | 228.3 | 4.224 |
| coro_rpc 0.6.1 | 8 | 29.907 [29.030, 42.351] | 324.3 | 3.077 |
| lrpc | 64 | 66.896 [62.019, 80.659] | 1021.5 | 0.696 |
| gRPC C++ 1.84.0 | 64 | 1119.804 [1077.884, 1346.205] | 88.4 | 10.192 |
| brpc 1.18.0 | 64 | 391.020 [366.004, 402.459] | 304.7 | 3.160 |
| coro_rpc 0.6.1 | 64 | 206.342 [204.730, 225.530] | 334.8 | 2.986 |

### 业务内容 4093 B（protobuf 编码体 4096 B）

| 库 | 最大在飞 | p99 µs 中位数 [最小, 最大] | 吞吐 kRPC/s | 客户端 CPU µs/调用 |
| --- | ---: | ---: | ---: | ---: |
| lrpc | 1 | 10.946 [10.661, 10.988] | 108.2 | 5.095 |
| gRPC C++ 1.84.0 | 1 | 41.931 [40.783, 42.699] | 27.1 | 19.755 |
| brpc 1.18.0 | 1 | 15.903 [15.652, 19.511] | 74.4 | 8.017 |
| coro_rpc 0.6.1 | 1 | 11.038 [11.017, 12.379] | 107.7 | 5.838 |
| lrpc | 8 | 24.919 [23.746, 31.060] | 336.3 | 1.584 |
| gRPC C++ 1.84.0 | 8 | 209.686 [207.162, 236.581] | 58.7 | 14.617 |
| brpc 1.18.0 | 8 | 81.967 [79.826, 91.176] | 202.3 | 4.779 |
| coro_rpc 0.6.1 | 8 | 35.156 [34.179, 37.962] | 270.1 | 3.701 |
| lrpc | 64 | 113.228 [97.478, 114.718] | 698.3 | 1.342 |
| gRPC C++ 1.84.0 | 64 | 1282.023 [1259.984, 1348.739] | 75.3 | 12.104 |
| brpc 1.18.0 | 64 | 538.426 [514.458, 556.253] | 242.5 | 3.988 |
| coro_rpc 0.6.1 | 64 | 194.932 [185.674, 270.585] | 360.0 | 2.777 |

CPU 数值仅覆盖客户端进程，包含公共驱动，未合并服务端；不能当成库自身的 CPU 成本。

## 观测边界与结论的适用范围

| 库 | 客户端 / 服务端 OS 线程数采样范围 |
| --- | --- |
| lrpc | 1 / 1 |
| gRPC C++ 1.84.0 | 19 / 20 |
| brpc 1.18.0 | 7 / 7–10 |
| coro_rpc 0.6.1 | 2–3 / 3 |

线程/连接每 25 ms 采样，覆盖启动、预热、正式测量及输出。所有采到的线程都符合对应 CPU 亲和；
各轮观测到的最大已建立业务 TCP 连接数为 1，同时配置与实现均为单端点单连接。
这是采样证据，不能证明两次采样之间没有短暂额外线程/连接。CSV 时间区间重建的最大 API 在飞数在所有正式轮都等于配置值。

这组结果适用于计算很轻的单连接 Echo。它未测试服务端长任务隔离、多机网络、多核扩展、TLS、流式调用或故障恢复。
闭环结果不证明固定到达速率下的 p99 SLO；缓存、driver、协议和完成交付方式也不同，不能据表中差异判断某一个内部模块的贡献。
此前 lrpc 同进程同执行器报告采用另一种拓扑，不能把本次跨进程数字与其直接当作回归比较。
本报告也不替代 lrpc 既有严格开放负载验收，相关未通过项仍见 [升级报告](2026-09-30-upgrade.md)。

## 官方公开数字为何不参与本表

- coro_rpc 官方历史简介公布约 2000 万 QPS Pipeline、约 450 万 QPS Ping-pong，后者为 2000 连接；测试机是 96 核 Xeon Platinum 8163。
  该页未给出对应测试 commit 或小消息全量 p99，不能将其标为本次 0.6.1 的重测结果。[官方简介](https://alibaba.github.io/yalantinglibs/zh/coro_rpc/coro_rpc_introduction.html)
- 旧 coro_rpc benchmark_client 读预编码输入、直接写 Asio TCP，默认关闭响应检查且每 1000 次采一次 RTT；本次适配器每次走正常发送/解码 API 并验证完整响应。[官方旧工具源码](https://github.com/alibaba/yalantinglibs/blob/e8d2c89e4fb336a36818a8abed8b6fc90973dda6/src/coro_rpc/benchmark/client.cpp)
- brpc 官方横向 benchmark 明确标注 2015 年，所用 gRPC 为 release-0_11；不能代表当前两库。该测试的长尾隔离场景还把人为慢请求排除于延迟 CDF。[官方 benchmark](https://github.com/apache/brpc/blob/master/docs/cn/benchmark.md)
- gRPC 官方基准说明列举的无竞争延迟和 QPS 场景使用 StreamingCall，通常跨 GKE/GCE 节点，且多数启用 TLS；与这里的小消息明文 unary 不同。[官方基准说明](https://grpc.io/docs/guides/benchmarking/)

本地 fiber1 的旧 bthread/裸 TCP 数据也不属于 brpc RPC，不纳入此次表格。

## 复现与证据

三库真实 API 适配器、固定版本来源与构建命令见 [构建说明](../competitors/README.md)。
正式数据位于 `build/bench-results/libraries-2026-09-30-rotated/`：逐请求 CSV.gz、每轮 stdout/stderr、线程/连接观测、环境和源码/二进制哈希均保留。
初组 `libraries-2026-09-30/` 的执行位置覆盖不充分，作为诊断组保留，未混入正式汇总；较短的连接观测自检失败记录同样保留。
[可保留 JSON](2026-09-30-libraries.json) 汇集全部正式轮、来源、编译配置、CSV 哈希与初组摘要；正式目录另有实现/驱动源码快照。

```sh
uv run --offline python benchmarks/test_compare_libraries.py
uv run --offline python benchmarks/test_run_suite.py
uv run --offline python benchmarks/compare_libraries.py \
  --out build/bench-results/libraries-new --rounds 5 --iterations 100000 --warmup 10000 \
  --cpu 2 --server-cpu 4 --control-cpu 8
```

新增校验器 6 项、原校验器 5 项测试通过；正式轮独立核对每条请求的序号、状态、时间戳、在飞上限和 p99。
本轮只新增测试适配器、比较脚本与报告，没有调整 lrpc/net/co2 实现，也未进行 Git 提交。
