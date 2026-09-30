#pragma once
#include <rpc/channel.hpp>
#include <functional>
namespace rpc { namespace detail {
std::size_t registration_checkpoint(channel &actor) noexcept;
void rollback_registration(channel &actor, std::size_t checkpoint) noexcept;
void observe_connectivity(channel &actor, std::function<void(bool)> callback);
bool is_runtime_thread() noexcept;
} }
