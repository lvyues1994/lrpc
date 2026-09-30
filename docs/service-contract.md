# 共享服务契约与编码选择

`rpc/service.hpp` 把一组方法的名字和请求/响应类型放进同一个服务契约 `service_contract<Policy, Methods...>`。
编码由模板参数 `Policy` 决定，server 的 binding 与 client 的 stub 从同一份契约生成；调用直接使用绑定好的方法，返回 `rpc::unary_call`。

## server 与 stub

[User 服务定义](../examples/users.hpp) 只依赖 `lrpc::rpc`，业务类型是普通 C++ 结构体。
两个方法通过 `unary_method<Request, Reply>` 声明，`UserContract<Policy>` 是契约类型，命名 stub `UserStub<Policy>`
把 `GetUser`、`RenameUser` 转发给 `bound_service::call<0/1>`。选择 JSON 的用法是：

```cpp
#include "json_users.hpp"

auto const contract = example::users_contract(rpc::json_codec_policy{});
rpc::server server{shard, example::users_bindings(contract, service)};

example::UserStub<rpc::json_codec_policy> const users{client, contract}; // client 或 channel
// 在调用协程内，先等待 connect() 或 channel.wait_ready() 成功：
example::GetUserRequest request{7};
example::GetUserReply reply;
CO2_AWAIT_SET(result, users.GetUser(request, reply, &spec, &trailer));
```

库层的定义与绑定对应：

```cpp
auto const contract = rpc::make_service_contract("users", policy,
    rpc::unary_method<GetUserRequest, GetUserReply>{"users/GetUser"},
    rpc::unary_method<RenameUserRequest, RenameUserReply>{"users/RenameUser"});

auto methods = contract.bindings(service, {{rpc::method_limits{1024, 128}, rpc::method_limits{128, 2}}},
                                 &UserService::GetUser, &UserService::RenameUser);
rpc::server server{shard, std::move(methods)};
auto const bound = rpc::bind_service(client, contract);
CO2_AWAIT_SET(result, bound.call<0>(request, reply));
```

handler 依声明顺序传入，签名与 `call<I>` 的请求/响应类型在编译期检查；空 handler 抛 `invalid_argument`。
契约构造时拒绝空名、嵌入 NUL 和重复方法名。契约拥有名字，可以复制、移动，绑定完成后可以销毁。
stub 借用 client/channel，binding 持有适配器、借用业务 service，二者须保活到所有调用结束；
请求、回复、`spec` 和 trailer 借用到调用完成。

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
auto const contract = example::users_contract(rpc::mapped_protobuf_codec_policy{});
rpc::server server{shard, example::users_bindings(contract, service)};
example::UserStub<rpc::mapped_protobuf_codec_policy> const users{client, contract};
CO2_AWAIT_SET(result, users.GetUser(request, reply));
```

Policy 是契约类型的一部分，所以按启动配置选择编码时，在两个实例化之间分支，
例如 [json_users.cpp](../examples/json_users.cpp) 的 `serve(policy)` 模板。原生 protobuf 消息可直接用 `default_codec_policy` 建立契约，无须映射。

映射编码在一次回调中建立临时 Arena 消息，转换一次、计算 ByteSizeLong，再写入预留的空间。
解码在临时 Arena 中解析，再复制到业务结构体；不得保留 Arena 消息的指针或视图。
映射决定缺失字段和 presence 的处理，未知 protobuf 字段不会保留。
转换/解析失败沿用类型化调用的错误映射；上界过小最多导致编码失败，不会越界写入。

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

测试覆盖两种格式、直接 client 与 channel、trailer，以及与原生 protobuf 编码逐字节比较、单次转换、容量失败和编译期拒绝错误类型。
