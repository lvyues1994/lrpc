#pragma once

#include <rpc/service.hpp>

#include <cstdint>
#include <string>
#include <vector>

namespace example {

struct GetUserRequest { std::uint64_t id = 0; };
struct User { std::uint64_t id = 0; std::string name; std::vector<std::string> roles; };
struct GetUserReply { User user; };
struct RenameUserRequest { std::uint64_t id = 0; std::string name; };
struct RenameUserReply { bool updated = false; };

using GetUserMethod = rpc::unary_method<GetUserRequest, GetUserReply>;
using RenameUserMethod = rpc::unary_method<RenameUserRequest, RenameUserReply>;

// One service, encoded as the policy chooses: JSON or mapped protobuf.
template <class Policy> using UserContract = rpc::service_contract<Policy, GetUserMethod, RenameUserMethod>;
template <class Policy> UserContract<Policy> users_contract(Policy policy) {
    return rpc::make_service_contract("users", policy, GetUserMethod{"users/GetUser"},
                                      RenameUserMethod{"users/RenameUser"});
}

struct UserLimits {
    rpc::method_limits GetUser{1024, 128};
    rpc::method_limits RenameUser{128, 2};
};

template <class Policy, class Service>
std::vector<rpc::method_binding> users_bindings(UserContract<Policy> const &contract, Service &service,
                                                UserLimits const &limits = {}) {
    return contract.bindings(service, {{limits.GetUser, limits.RenameUser}}, &Service::GetUser, &Service::RenameUser);
}

template <class Policy> class UserStub {
public:
    UserStub(rpc::call_target target, UserContract<Policy> const &contract) : methods_(target, contract) {}
    rpc::unary_call GetUser(GetUserRequest const &request, GetUserReply &reply, rpc::call_spec const *spec = nullptr,
                            rpc::response_trailer *trailer = nullptr) const noexcept {
        return methods_.template call<0>(request, reply, spec, trailer);
    }
    rpc::unary_call RenameUser(RenameUserRequest const &request, RenameUserReply &reply,
                               rpc::call_spec const *spec = nullptr, rpc::response_trailer *trailer = nullptr) const noexcept {
        return methods_.template call<1>(request, reply, spec, trailer);
    }

private:
    rpc::bound_service<Policy, GetUserMethod, RenameUserMethod> methods_;
};

} // namespace example
