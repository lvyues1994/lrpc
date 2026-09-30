#pragma once
#include <rpc/json.hpp>

namespace example {
struct GetUserRequest { std::uint64_t id = 0; };
struct User { std::uint64_t id = 0; std::string name; std::vector<std::string> roles; };
struct GetUserReply { User user; };
struct RenameUserRequest { std::uint64_t id = 0; std::string name; };
struct RenameUserReply { bool updated = false; };
}
RPC_JSON_FIELDS(example::GetUserRequest, id);
RPC_JSON_FIELDS(example::User, id, name, roles);
RPC_JSON_FIELDS(example::GetUserReply, user);
RPC_JSON_FIELDS(example::RenameUserRequest, id, name);
RPC_JSON_FIELDS(example::RenameUserReply, updated);

namespace example {
inline rpc::json_method<GetUserRequest, GetUserReply> const &get_user_method() {
    static rpc::json_method<GetUserRequest, GetUserReply> const value{"users/GetUser", rpc::idempotency::no_side_effects};
    return value;
}
inline rpc::json_method<RenameUserRequest, RenameUserReply> const &rename_user_method() {
    static rpc::json_method<RenameUserRequest, RenameUserReply> const value{"users/RenameUser"};
    return value;
}
// Handwritten service contract; each named method keeps its request/reply types.
class UserStub {
public:
    explicit UserStub(rpc::client &client) : get_(rpc::bind(client, get_user_method())), rename_(rpc::bind(client, rename_user_method())) {}
    net::task<rpc::call_result> GetUser(GetUserRequest const &request, GetUserReply &reply, rpc::call_options options = {}) const {
        return get_(request, reply, options);
    }
    net::task<rpc::call_result> RenameUser(RenameUserRequest const &request, RenameUserReply &reply, rpc::call_options options = {}) const {
        return rename_(request, reply, options);
    }
private:
    rpc::json_bound_method<GetUserRequest, GetUserReply> get_;
    rpc::json_bound_method<RenameUserRequest, RenameUserReply> rename_;
};
}
