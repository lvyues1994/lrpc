#pragma once

#include <cstdint>
#include <string>
#include <system_error>
#include <type_traits>

namespace rpc {

enum class status_code : std::uint8_t {
    ok = 0, cancelled = 1, unknown = 2, invalid_argument = 3, deadline_exceeded = 4,
    not_found = 5, already_exists = 6, permission_denied = 7, resource_exhausted = 8,
    failed_precondition = 9, aborted = 10, out_of_range = 11, unimplemented = 12,
    internal = 13, unavailable = 14, data_loss = 15, unauthenticated = 16,
};

// Owns error text, including embedded NUL bytes. Successful statuses carry no
// text on the wire. Only error completion may allocate to copy received text.
struct status {
    status_code code = status_code::ok;
    std::string message{};
};

std::error_category const &status_category() noexcept;
inline std::error_code make_error_code(status_code code) noexcept {
    return {static_cast<int>(code), status_category()};
}

} // namespace rpc
namespace std {
template <> struct is_error_code_enum<::rpc::status_code> : true_type {};
} // namespace std
