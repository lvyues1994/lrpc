# 四库一元 RPC 对照

这里通过各库真实的客户端/服务端 API 测量 Echo：gRPC C++ AsyncService/CompletionQueue、
brpc 共享 Channel 的常驻 bthread 同步调用、coro_rpc 单连接的 `send_request` 双 await。
lrpc 复用上一级 `unary_bench` 的 protobuf 跨进程路径。测试适配器不加入库的默认构建。

固定来源：gRPC `v1.84.0`（`3252a89f10d8e92997862167ca7d095ecda85973`）、brpc `1.18.0`、
Alibaba yaLanTingLibs `0.6.1`。源码、私有开发包和构建产物放在 `build/competitors/`。
gRPC 使用其固定的 protobuf 35.1 源码；brpc/lrpc 使用仓库已有的 protobuf 3.21.12 私有依赖。
各库在独立进程中，不混合两个 protobuf runtime。

业务内容分别为 62/4093 B，protobuf 编码后是 64/4096 B；coro_rpc 默认 struct_pack 编码体不同。
双方返回等长完整内容，payload 带序号，客户端逐次校验。所有库关闭调用截止、重试、TLS 和压缩，
使用持久连接与 TCP_NODELAY。brpc 先设置最小的 4 个 worker；gRPC/coro_rpc 显式指定应用 I/O/CQ worker，
库自身的后台线程也受进程 CPU 亲和约束。测试期间不并行构建。

本机 Ubuntu 开发包只下载并解包，没有系统安装。重建时先准备官方固定源码：

```sh
mkdir -p build/competitors/sources build/competitors/prefix
git clone --depth 1 --branch v1.84.0 https://github.com/grpc/grpc.git build/competitors/sources/grpc
git -C build/competitors/sources/grpc submodule update --init --depth 1 \
  third_party/abseil-cpp third_party/protobuf third_party/re2 third_party/cares/cares \
  third_party/grpc-proto third_party/googleapis third_party/xds third_party/envoy-api
curl -fL https://github.com/apache/brpc/archive/refs/tags/1.18.0.tar.gz -o build/competitors/brpc-1.18.0.tar.gz
curl -fL https://github.com/alibaba/yalantinglibs/archive/refs/tags/0.6.1.tar.gz -o build/competitors/yalantinglibs-0.6.1.tar.gz
tar -xzf build/competitors/brpc-1.18.0.tar.gz -C build/competitors/sources
tar -xzf build/competitors/yalantinglibs-0.6.1.tar.gz -C build/competitors/sources
(cd build/competitors && apt-get download libgflags-dev=2.2.2-2build1 libleveldb-dev=1.23-5build1)
for comparison_package in build/competitors/libgflags-dev*.deb build/competitors/libleveldb-dev*.deb; do
  dpkg-deb -x "$comparison_package" build/competitors/prefix
done
```

以下构建命令对应本机路径和版本，依赖 `build/deps/protobuf/usr` 已准备好。
brpc 使用系统同版本 `libleveldb.so.1d`；其他发行版须指定自己的匹配运行库。

```sh
comparison_root="$PWD"
comparison_pb="$comparison_root/build/deps/protobuf/usr"
export LD_LIBRARY_PATH="$comparison_pb/lib/x86_64-linux-gnu${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
cmake -S benchmarks/competitors -B build/competitors/grpc-adapter -G Ninja \
  -DCMAKE_BUILD_TYPE=Release -DCOMPARISON_FRAMEWORK=grpc \
  -DCOMPARISON_SOURCE="$comparison_root/build/competitors/sources/grpc" \
  -DgRPC_INSTALL=OFF -Dprotobuf_INSTALL=OFF -DABSL_ENABLE_INSTALL=ON \
  -DgRPC_BUILD_TESTS=OFF -DgRPC_SSL_PROVIDER=package -DgRPC_ZLIB_PROVIDER=package \
  -DgRPC_BUILD_GRPC_CSHARP_PLUGIN=OFF -DgRPC_BUILD_GRPC_NODE_PLUGIN=OFF \
  -DgRPC_BUILD_GRPC_OBJECTIVE_C_PLUGIN=OFF -DgRPC_BUILD_GRPC_PHP_PLUGIN=OFF \
  -DgRPC_BUILD_GRPC_PYTHON_PLUGIN=OFF -DgRPC_BUILD_GRPC_RUBY_PLUGIN=OFF
cmake --build build/competitors/grpc-adapter --target comparison_bench -j 8
cmake -S build/competitors/sources/brpc-1.18.0 -B build/competitors/brpc-build -G Ninja \
  -DCMAKE_BUILD_TYPE=Release -DBUILD_BRPC_TOOLS=OFF -DBUILD_UNIT_TESTS=OFF -DWITH_DEBUG_SYMBOLS=OFF \
  -DCMAKE_PREFIX_PATH="$comparison_root/build/competitors/prefix/usr;$comparison_pb" \
  -DProtobuf_PROTOC_EXECUTABLE="$comparison_pb/bin/protoc"
cmake --build build/competitors/brpc-build --target brpc-static -j 6
cmake -S benchmarks/competitors -B build/competitors/brpc-adapter -G Ninja \
  -DCMAKE_BUILD_TYPE=Release -DCOMPARISON_FRAMEWORK=brpc \
  -DCOMPARISON_SOURCE="$comparison_root/build/competitors/sources/brpc-1.18.0" \
  -DCOMPARISON_BRPC_BUILD="$comparison_root/build/competitors/brpc-build" \
  -DCMAKE_PREFIX_PATH="$comparison_root/build/competitors/prefix/usr;$comparison_pb" \
  -DCOMPARISON_LEVELDB_LIBRARY=/usr/lib/x86_64-linux-gnu/libleveldb.so.1d \
  -DProtobuf_PROTOC_EXECUTABLE="$comparison_pb/bin/protoc"
cmake --build build/competitors/brpc-adapter --target comparison_bench -j 2
cmake -S benchmarks/competitors -B build/competitors/coro_rpc-adapter -G Ninja \
  -DCMAKE_BUILD_TYPE=Release -DCOMPARISON_FRAMEWORK=coro_rpc \
  -DCOMPARISON_SOURCE="$comparison_root/build/competitors/sources/yalantinglibs-0.6.1"
cmake --build build/competitors/coro_rpc-adapter --target comparison_bench -j 2
```

gRPC 的生成 schema 只把 `cc_generic_services` 改为 false，消息和方法保持相同；
该选项为 brpc 开启，gRPC 插件拒绝开启时生成。独立适配器的使用参数是
`--role server|client --port N --bytes 64|4096 --inflight N --iterations N --warmup N --samples path.csv`。

串行测量及校验：

```sh
uv run --offline python benchmarks/test_compare_libraries.py
uv run --offline python benchmarks/compare_libraries.py \
  --out build/bench-results/libraries-new --rounds 5 --iterations 100000 --warmup 10000 \
  --cpu 2 --server-cpu 4 --control-cpu 8
```

三个 CPU 要分属不同物理核。脚本隔轮反转配置并轮换库顺序；每轮重启 server/client，连接和预热不计入测量。
每次 API 延迟从实际进入调用前开始，到 CQ 交付、同步返回或协程恢复后结束，包含编码/解码，校验在计时后。
显式请求准备、Controller Reset、gRPC ClientContext 创建不属于单次 API 延迟；整轮吞吐和客户端 CPU 包含驱动与校验。
每条 lane/slot 完成即补发，阶段末尾排空。CPU 统计仅包含客户端，不含服务端，也不是纯库成本。

输出包括环境/源码/二进制哈希、逐轮 JSON、逐请求 CSV.gz、stdout/stderr 和线程/连接观测。
连接计数同时检查 IPv4 与 IPv4-mapped IPv6，并合并两端同一连接。
脚本拒绝错误请求、缺样本、时间戳异常、超出在飞上限、p99 不一致、错误 CPU 亲和及未观测到单 TCP 的轮次。
客户端有进程看门狗和强制终止后备；服务端必须成功响应 SIGTERM 并排空，失败证据保留在对应输出目录。
正式数据与范围限制见 [四库性能报告](../results/2026-09-30-libraries.md)。
