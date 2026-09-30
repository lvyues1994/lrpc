#pragma once

#include "users.hpp"
#include "users.pb.h"
#include <rpc/protobuf_mapping.hpp>

namespace example {
inline std::size_t protobuf_bound_add(std::size_t a, std::size_t b) {
    if (b > std::numeric_limits<std::size_t>::max() - a) throw std::invalid_argument{"protobuf bound overflow"};
    return a + b;
}
}
namespace rpc {
template <> struct protobuf_mapping<example::GetUserRequest> {
    using message_type = example::wire::GetUserRequest;
    static bool to_protobuf(example::GetUserRequest const &value, message_type &out) { out.set_id(value.id); return true; }
    static bool from_protobuf(message_type const &value, example::GetUserRequest &out) { out.id = value.id(); return true; }
    static std::size_t upper_bound(example::GetUserRequest const &) { return 11; }
};
template <> struct protobuf_mapping<example::GetUserReply> {
    using message_type = example::wire::GetUserReply;
    static bool to_protobuf(example::GetUserReply const &value, message_type &out) {
        auto &user = *out.mutable_user();
        user.set_id(value.user.id); user.set_name(value.user.name);
        for (auto const &role : value.user.roles) user.add_roles(role);
        return true;
    }
    static bool from_protobuf(message_type const &value, example::GetUserReply &out) {
        if (!value.has_user()) return false;
        auto const &user = value.user();
        out.user.id = user.id(); out.user.name = user.name();
        out.user.roles.assign(user.roles().begin(), user.roles().end());
        return true;
    }
    static std::size_t upper_bound(example::GetUserReply const &value) {
        // One-byte tags, <=10-byte uint64/length prefixes, including the nested envelope.
        auto bytes = example::protobuf_bound_add(33, value.user.name.size());
        for (auto const &role : value.user.roles)
            bytes = example::protobuf_bound_add(bytes, example::protobuf_bound_add(11, role.size()));
        return bytes;
    }
};
template <> struct protobuf_mapping<example::RenameUserRequest> {
    using message_type = example::wire::RenameUserRequest;
    static bool to_protobuf(example::RenameUserRequest const &value, message_type &out) {
        out.set_id(value.id); out.set_name(value.name); return true;
    }
    static bool from_protobuf(message_type const &value, example::RenameUserRequest &out) {
        out.id = value.id(); out.name = value.name(); return true;
    }
    static std::size_t upper_bound(example::RenameUserRequest const &value) {
        return example::protobuf_bound_add(22, value.name.size());
    }
};
template <> struct protobuf_mapping<example::RenameUserReply> {
    using message_type = example::wire::RenameUserReply;
    static bool to_protobuf(example::RenameUserReply const &value, message_type &out) { out.set_updated(value.updated); return true; }
    static bool from_protobuf(message_type const &value, example::RenameUserReply &out) { out.updated = value.updated(); return true; }
    static std::size_t upper_bound(example::RenameUserReply const &) { return 2; }
};
}
