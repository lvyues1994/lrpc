#pragma once
#include "users.hpp"
#include <rpc/json.hpp>

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
}
