# 共享服务契约与编码选择

`rpc/service.hpp` 把方法名、请求/响应类型和幂等性放进同一个服务契约。
创建契约时选择 codec policy，server 注册与 client stub 消费同一份定义。
调用直接使用已绑定 handle 和 codec 表，仍返回 `net::task<call_result>`。

## server 与 stub

[User 服务定义](../examples/users.hpp) 只依赖 `lrpc::unary`，业务类型是普通 C++ 结构体。
两个方法通过 `unary_method<Request, Reply>` 声明，命名 stub 把 `GetUser`、`RenameUser`
转发给 `bound_service::call<0/1>`。选择 JSON 的用法是：

```cpp
#include "json_users.hpp"

auto contract = example::users_contract(rpc::json_codec_policy{});
rpc::server_builder builder{context};
example::add_users_service(builder, contract, service);
auto server = builder.build();

example::UserStub users{*client, contract};
// 在调用协程内，先等待 connect() 或 channel.warmup() 成功：
example::GetUserRequest request{7};
example::GetUserReply reply;
CO2_AWAIT_SET(result, users.GetUser(request, reply));
```

库层的服务定义/注册对应：

```cpp
auto contract = rpc::make_service_contract("users", policy,
    rpc::unary_method<GetUserRequest, GetUserReply>{
        "users/GetUser", rpc::idempotency::no_side_effects},
    rpc::unary_method<RenameUserRequest, RenameUserReply>{"users/RenameUser"});

rpc::add_service(builder, contract, service,
    {rpc::method_limits{1024, 128}, rpc::method_limits{128, 2}},
    &UserService::GetUser, &UserService::RenameUser);
auto methods = rpc::bind_service(*client, contract);
CO2_AWAIT_SET(result, methods.call<0>(request, reply));
```

handler 依声明顺序传入，参数签名与 `call<I>` 的请求/响应类型在编译期检查。
服务端先构造全部 adapter，再一次加入 builder；空 handler、无效 cache 或分配失败不会留下部分服务。
客户端一次批量冷绑定；第二方法冲突或注册表满时撤销本次新注册的方法，保留原有注册。
契约构造时拒绝空名、嵌入 NUL、重复方法名、无效幂等性和缺失 codec 回调。

契约拥有名字，可以复制、移动，绑定完成后可以销毁。
stub/adapter 保存选定的静态 codec 表，之后修改原契约不会切换它们的格式；policy 的表须活到程序退出。
stub 借用 client，adapter 借用业务 service，二者须保活到所有调用结束。
请求、响应和 metadata 保持原有异步借用约定。
runtime facade 的 stub 必须在 `runtime::start()` 前构造；业务有可变状态时按分片提供独立 service。

## 普通结构体使用 protobuf

JSON 字段映射不包含 protobuf 的字段号与 presence 信息，不能据此自动推出二进制协议。
保留普通结构体参数需要为每个请求/响应类型显式提供 `protobuf_mapping<T>`：

```cpp
#include <rpc/protobuf_mapping.hpp>
#include "users.pb.h"

template <> struct rpc::protobuf_mapping<GetUserRequest> {
    using message_type = example::wire::GetUserRequest;
    static bool to_protobuf(GetUserRequest const &value, message_type &out) {
        out.set_id(value.id); return true;
    }
    static bool from_protobuf(message_type const &value, GetUserRequest &out) {
        out.id = value.id(); return true;
    }
    static std::size_t upper_bound(GetUserRequest const &) { return 11; }
};
```

`message_type` 必须是生成的 protobuf 消息；转换函数必须返回 bool，false 表示数据不满足映射。
`upper_bound` 返回便宜的保守线上字节上界，不序列化、不转换、不缓存消息；计算必须检查溢出。
例中字段号 1 的 tag 占 1 B，uint64 varint 最多 10 B。
嵌套消息、repeated、默认值及字段 presence 的完整适配见 [protobuf_users.hpp](../examples/protobuf_users.hpp)，
字段号来自 [users.proto](../examples/proto/users.proto)。

选择该 policy 后，服务和业务调用保持上面的写法：

```cpp
auto contract = example::users_contract(rpc::mapped_protobuf_codec_policy{});
example::add_users_service(builder, contract, service);
example::UserStub users{*client, contract};
CO2_AWAIT_SET(result, users.GetUser(request, reply));
```

两种 policy 产生相同的契约 C++ 类型，因此可以按启动配置选择。
原生 protobuf 消息也可用 `default_codec_policy` 建立服务契约，无须映射。
现有 `method`、`json_method`、`bound_method`、`bind_method` 及 protoc 生成的 service/stub 均继续使用。

映射编码在一次回调中建立临时 Arena 消息，转换一次、计算 ByteSizeLong，再写入已预算的容量；
channel 的重试复用编码快照。兼容的独立 size/encode 接口会各自转换，RPC 优先走有界编码。
解码在临时 Arena 中解析，再复制到业务结构体；不得保留 Arena 消息的指针或视图。
普通结构体仍使用每调用独立的业务存储，不自动获得 protobuf 消息缓存。
映射决定缺失字段和 presence 的处理，未知 protobuf 字段不会保留。
转换/解析失败沿用 typed RPC 的错误映射；分配失败返回 resource_exhausted。
上界过小最多导致有界编码失败，不会扩大缓冲或越界写入。
临时 Arena 可向堆扩容；这些扩展分配与业务结构体的容器内存不计入 RPC 收发缓冲预算，
有界编码约束的是输出/准备容量，不是进程总内存硬上限。

## 格式匹配与验证

同一 wire 方法名一次只注册一种编码。server 与 client 必须配置匹配的契约；
不同方法名可以在同一服务端、端口和连接使用不同格式。
目前没有在线 codec 标识、协商或自动转码，不能保证选错格式一定被拒绝。
这里的普通结构体 JSON 映射也不等于 protobuf JSON mapping。

同时启用 `LRPC_BUILD_JSON`、`LRPC_BUILD_PROTOBUF` 和 `LRPC_BUILD_CODEGEN` 后，
同一 [双方法示例](../examples/json_users.cpp) 支持：

```sh
./build/protobuf-release/examples/json_users
./build/protobuf-release/examples/json_users protobuf
```

测试覆盖两种格式、直接 client/channel、多分片 runtime、扩展 profile 的跨帧一元消息、metadata，
以及与原生 protobuf 编码逐字节比较、单次转换、容量失败、契约生命周期、批量回滚和编译期拒绝错误类型。
这些正确性测试不构成 JSON 与 protobuf 的网络 p99 性能对比。
