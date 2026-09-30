#pragma once

#include <rpc/service.hpp>

namespace example {
struct GetUserRequest { std::uint64_t id = 0; };
struct User { std::uint64_t id = 0; std::string name; std::vector<std::string> roles; };
struct GetUserReply { User user; };
struct RenameUserRequest { std::uint64_t id = 0; std::string name; };
struct RenameUserReply { bool updated = false; };

using UserContract = rpc::service_contract<rpc::unary_method<GetUserRequest, GetUserReply>,
                                         rpc::unary_method<RenameUserRequest, RenameUserReply>>;
template <class Policy> UserContract users_contract(Policy policy) {
    return rpc::make_service_contract("users", policy,
        rpc::unary_method<GetUserRequest, GetUserReply>{"users/GetUser", rpc::idempotency::no_side_effects},
        rpc::unary_method<RenameUserRequest, RenameUserReply>{"users/RenameUser"});
}
struct UserLimits {
    rpc::method_limits GetUser{1024, 128};
    rpc::method_limits RenameUser{128, 2};
};
template <class Service>
rpc::server_builder &add_users_service(rpc::server_builder &builder, UserContract const &contract,
                                       Service &service, UserLimits const &limits = {}) {
    return rpc::add_service(builder, contract, service, {limits.GetUser, limits.RenameUser},
                            &Service::GetUser, &Service::RenameUser);
}
class UserStub {
public:
    UserStub(rpc::client &client, UserContract const &contract) : methods_(client, contract) {}
    net::task<rpc::call_result> GetUser(GetUserRequest const &request, GetUserReply &reply, rpc::call_options options = {}) const {
        return methods_.call<0>(request, reply, options);
    }
    net::task<rpc::call_result> RenameUser(RenameUserRequest const &request, RenameUserReply &reply, rpc::call_options options = {}) const {
        return methods_.call<1>(request, reply, options);
    }
private:
    rpc::bound_service<rpc::unary_method<GetUserRequest, GetUserReply>,
                       rpc::unary_method<RenameUserRequest, RenameUserReply>> methods_;
};
}
