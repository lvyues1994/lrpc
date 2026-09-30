# 普通 C++ 结构体与 JSON RPC

`lrpc::json` 将普通结构体映射为现有 lrpc 消息 body 中的 UTF-8 JSON 对象。
方法名、帧格式、状态、截止、取消与 metadata 沿用 lrpc；双方须为同一方法约定相同 codec。
此模块不提供 HTTP 或 JSON-RPC 2.0 入口，protobuf 与 JSON 可以使用不同方法名共存。

## 构建

模块默认关闭，需要独立的官方 RapidJSON 头文件。验证版本固定在
[`24b5e7a8b27f42fa16b96fc70aade9106cf7102f`](https://github.com/Tencent/rapidjson/commit/24b5e7a8b27f42fa16b96fc70aade9106cf7102f)，
包含 UTF-8 校验路径的越界读取修复。该提交的版本宏仍是 1.1.0，不能仅按版本宏判断是否包含修复。
官方归档 `https://codeload.github.com/Tencent/rapidjson/tar.gz/24b5e7a8b27f42fa16b96fc70aade9106cf7102f`
的 SHA256 为 `2d2601a82d2d3b7e143a3c8d43ef616671391034bc46891a9816b79cf2d3e7a8`。

取得并解压源码后，在项目根目录执行：

```sh
cmake --preset json \
  -DLRPC_NET_SOURCE_DIR=/home/lvyues/code/net/net \
  -DNET_CO2_DIR=/home/lvyues/code/coro/coro \
  -DCMAKE_DISABLE_FIND_PACKAGE_co2=ON \
  -DLRPC_RAPIDJSON_SOURCE_DIR=/path/to/rapidjson
cmake --build --preset json
ctest --preset json
./build/json/examples/json_users
```

也接受父项目的 `RapidJSON`/`rapidjson` target，或 `find_package(RapidJSON CONFIG)` 安装包。
配置不会下载依赖；JSON 的公开头文件不包含 RapidJSON 类型。使用方链接 `lrpc::json`。
保留 C++14；无须 protoc 或 JSON 代码生成器。

## 结构体映射与服务契约

```cpp
#include <rpc/json.hpp>

struct GetUserRequest { std::uint64_t id = 0; };
struct GetUserReply { std::string name; };
RPC_JSON_FIELDS(GetUserRequest, id);
RPC_JSON_FIELDS(GetUserReply, name);

rpc::json_method<GetUserRequest, GetUserReply> const get_user{"users/GetUser"};
```

宏放在全局命名空间，支持 1–16 个字段；映射不侵入结构体，也不改变该类型的其它 codec。
支持 bool、有符号/无符号整数、float/double、std::string、嵌套映射结构体及 std::vector；
暂不支持 optional、map、枚举、指针、字符串视图或 vector<bool>。每对象最多映射 64 个字段。

需要别名、必填字段或超过 16 个字段时手写 traits：

```cpp
template <> struct rpc::json_traits<GetUserRequest> {
    static auto fields() {
        return std::make_tuple(rpc::json_field("user_id", &GetUserRequest::id, true));
    }
};
```

这是对宏定义的替代，同一类型只能定义一次 traits。字段名存储须保活到程序退出。
重复或无效映射在取得 codec 时抛出 invalid_argument；嵌套映射在首次使用时验证。
所有映射字段都会编码。缺失的非必填字段取 `T{}` 的默认值，保留成员默认初始化器；
未知字段完整校验后忽略。null 不会隐式变成字段默认值，已有字段类型必须匹配。

服务端业务函数保持明确的请求与响应类型：

```cpp
struct UserService {
    net::task<rpc::status_code> GetUser(rpc::server_context &,
        GetUserRequest const &, GetUserReply &);
};

std::vector<rpc::method_binding> methods;
methods.push_back(rpc::bind_method(get_user, service, &UserService::GetUser, {1024, 128}));
rpc::server server{shard, std::move(methods)};
```

`{1024, 128}` 分别是该方法的回复编码上限与 END head（状态说明和 metadata）上限，省略时为 64 KiB 和 256 B；
服务对象须保活到服务端排空。handler 签名在编译期检查，错误的请求或响应类型不能绑定到方法。

客户端绑定一次，之后每次调用返回 `rpc::unary_call`：

```cpp
auto const get = rpc::bind(client, get_user); // 或 rpc::bind(channel, get_user)
// 在调用协程内，先等待 connect() 或 channel.wait_ready() 成功：
GetUserRequest request{7};
GetUserReply reply;
CO2_AWAIT_SET(result, get(request, reply, &spec, &trailer));
```

错误的参数类型在编译时拒绝；`json_bound_method` 只能由 `rpc::bind` 创建，不能把任意 `method_ref`
重新标注为其它消息类型。bound_method 借用 client/channel，调用完成前请求、回复、`spec` 和 trailer 均须保活。
多方法服务可使用 `service_contract` 一次绑定，server 与 stub 共用类型和名字定义。
[完整示例](../examples/json_users.cpp) 注册并调用 GetUser、RenameUser，共享结构体和 stub 见 [users.hpp](../examples/users.hpp)；
初始化时选 JSON 或 protobuf、普通结构体的显式 protobuf 映射见 [共享服务契约](service-contract.md)。

## 编码、校验与资源

请求在发送时单次编码进帧，回复同样编码一次并提交实际长度。编码按保守上界预留空间，
上界超过帧的剩余空间或方法的回复上限时只给剩余部分，实际写不下才失败，所以装得下的消息不会因为转义估算而被拒绝。
channel 换连接重试时从同一请求消息重新编码，消息须保持不变直到调用完成。
protobuf 和其它只提供 size/encode 的 codec 使用精确大小路径。

JSON 使用 SAX 直接写入结构体，字符串拥有自己的存储，不保留接收缓冲的借用视图。
输入必须是完整的一个对象，拒绝尾随数据、原始 NUL、重复字段（含转义后同名字段）、非法 UTF-8、
孤立 surrogate、注释、尾随逗号、NaN/Infinity 和整数越界。未知对象内部也执行这些校验。
整数目标拒绝浮点 token；整数到 float/double 直接转换，十进制浮点先按 double 全精度解析后转换到目标类型。
该转换可能舍入，不能将 uint64 JSON 数字交给只提供 double 数字类型的另一语言并期待精确往返。

`json_options<Message>::limits()` 可为根消息类型调整以下限制；嵌套值使用根消息限制：

| 限制 | 默认值 |
| --- | --- |
| 对象/数组嵌套深度 | 32，允许配置 1–64 |
| 单个字符串或字段名 | 64 KiB |
| 单数组元素数量 | 16384 |
| 单对象字段数量，含未知字段 | 256 |
| 解码保留值的逻辑累计字节 | 1 MiB |
| RapidJSON 临时栈预算 | 256 KiB，最小 512 B |

逻辑字节累计计入字符串内容、数组元素 sizeof(T) 和未知字段名追踪；不等于进程内存上限，
不覆盖根结构体、std::string/vector 的多余容量、固定 SAX 帧或分配器开销。
临时栈有 512 B 内联存储，扩容时计入同时存在的新旧块，超限抛出 bad_alloc。
请求/回复的线上大小仍受 RPC 的帧和消息上限约束。保守上界只遍历字段、长度和容器，不序列化。

语法、字段类型、数量/深度限制错误的请求返回 invalid_argument；解码逻辑预算、临时栈或分配失败返回 resource_exhausted。
两种情况下业务函数都不运行。客户端请求编码失败在发送前返回 invalid_argument，超出帧返回 resource_exhausted。
服务端输出无效 JSON 或超出已声明回复上限返回 internal；客户端解码回复失败（包括内存不足）返回 internal。
底层 decode 失败可修改目标对象；encode_bounded 失败的 written 为零，但输出缓冲可能已写入部分内容，调用方必须丢弃。

## 性能验证

`json_codec_bench` 独立比较单次有界编码、兼容的 size 后 encode 以及 SAX 解码：

```sh
cmake --preset json-release -DLRPC_BUILD_BENCHMARKS=ON \
  -DLRPC_NET_SOURCE_DIR=/home/lvyues/code/net/net \
  -DNET_CO2_DIR=/home/lvyues/code/coro/coro \
  -DLRPC_RAPIDJSON_SOURCE_DIR=/path/to/rapidjson
cmake --build --preset json-release
./build/json-release/benchmarks/json_codec_bench 100000
```

缓冲提前分配，upper_bound 在计时区间外计算；逐次计时包含时钟开销，解码包含目标容器分配与结果比较，
不能替代网络 RPC 的 p99 验收。测试覆盖直接连接、channel、上界超过帧与回复上限、metadata 示例及编译期类型检查。
