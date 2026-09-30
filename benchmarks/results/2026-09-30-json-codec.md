# JSON codec 首个基线

普通结构体映射为 JSON 对象，text 使用 64/256/1024 B ASCII；另含 uint64 id 与三个整数的 flags。
Release、GCC 13.3.0、i7-13700KF，单进程固定 CPU 2，每组预热 1000 次、测量 100000 次，连续三轮。
下表为各轮 p99 的中位数，单位 ns。原始轮次、内核、固定 RapidJSON 提交、二进制与相关源码哈希见
[数据](2026-09-30-json-codec.json)；测量源码在本地提交 7ae5c3c 后的工作区，JSON 改动尚未提交。

| text 字节 | 单次有界编码 p99 | size 后 encode p99 | SAX 解码 p99 |
| --- | --- | --- | --- |
| 64 | 191 | 365 | 328 |
| 256 | 343 | 619 | 478 |
| 1024 | 920 | 1771 | 1189 |

复现：按 [JSON 构建用法](../../docs/json.md) 开启 Release 和 benchmark，执行
`taskset -c 2 ./build/json-release/benchmarks/json_codec_bench 100000`，重复三轮。
输出缓冲提前分配，upper_bound 在计时区间外计算；解码包含目标容器分配和结果比较，逐次计时包含时钟开销。
没有修改 CPU 频率、调度或其它系统参数，也没有隔离整台机器的后台负载。

单次编码在这三组的 p99 约为 size 后 encode 的 52%–55%，支持避免为测量大小重复序列化。
这不是与 protobuf、gRPC、brpc 或 coro_rpc 的协议对比，也不包含网络、RPC 调度、metadata 或开放负载验收；
不能据此宣称网络 p99 改善。JSON 正确性验证为 Debug、Release、ASan/UBSan/泄漏检查各 34 项通过，
JSON/protobuf 同时启用的 Release 配置 44 项通过。
