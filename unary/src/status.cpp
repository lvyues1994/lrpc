#include <rpc/status.hpp>
#include <net/error.hpp>

namespace rpc {
namespace {
struct rpc_category final : std::error_category {
    char const *name() const noexcept override { return "rpc"; }
    std::string message(int value) const override {
        static char const *const names[] = {"ok", "cancelled", "unknown", "invalid_argument", "deadline_exceeded",
            "not_found", "already_exists", "permission_denied", "resource_exhausted", "failed_precondition",
            "aborted", "out_of_range", "unimplemented", "internal", "unavailable", "data_loss", "unauthenticated"};
        return value >= 0 && value <= 16 ? names[value] : "invalid RPC status";
    }
    std::error_condition default_error_condition(int value) const noexcept override {
        if (value == static_cast<int>(status_code::cancelled)) return net::make_error_condition(net::cond::canceled);
        if (value == static_cast<int>(status_code::deadline_exceeded)) return net::make_error_condition(net::cond::timeout);
        return {value, *this};
    }
};
}
std::error_category const &status_category() noexcept { static rpc_category const category; return category; }
} // namespace rpc
