#pragma once

#include <cstdint>

namespace rpc {

// Same values as gRPC, so an adapter can map them directly.
enum class status_code : std::uint8_t {
    ok = 0, cancelled = 1, unknown = 2, invalid_argument = 3, deadline_exceeded = 4,
    not_found = 5, already_exists = 6, permission_denied = 7, resource_exhausted = 8,
    failed_precondition = 9, aborted = 10, out_of_range = 11, unimplemented = 12,
    internal = 13, unavailable = 14, data_loss = 15, unauthenticated = 16,
};

char const *to_string(status_code code) noexcept;

} // namespace rpc
