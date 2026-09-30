#pragma once

#include <rpc/v2/client.hpp>

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace rpc {
namespace v2 {

struct channel_options {
    client_options client{};
    std::size_t connections_per_endpoint = 1;
    // Reconnection after a failed attempt; a connection that was ready
    // reconnects at once.
    std::chrono::milliseconds initial_backoff{20};
    std::chrono::milliseconds max_backoff{2000};
    // Attempts per call, including the first. A call is retried, on another
    // connection when there is one, only if it provably did not execute.
    std::uint32_t max_attempts = 3;
};

namespace detail {
struct channel_core;
}

// Calls over connections to fixed endpoints, kept connected in the
// background. Each call goes to the ready connection with the fewest calls in
// flight. As with client, every member runs on the shard's thread and the
// channel must outlive its calls.
class channel {
public:
    channel(shard &owner, std::vector<net::ip::tcp::endpoint> endpoints, channel_options options = {});
    ~channel();
    channel(channel const &) = delete;
    channel &operator=(channel const &) = delete;

    // ok once a connection is ready; unavailable after close; deadline_exceeded.
    net::task<status_code> wait_ready(clock::time_point deadline = clock::time_point::max());
    method_ref bind(std::string const &name);
    unary_call call(method_ref method, wire::bytes_view request, wire::mutable_bytes_view response,
                    call_spec const *spec = nullptr, response_trailer *trailer = nullptr) noexcept;
    // On the least loaded ready connection; streams are not retried.
    open_operation open(method_ref method, method_kind kind, call_spec const *spec = nullptr) noexcept;
    std::size_t ready_connections() const noexcept;
    void close() noexcept;

private:
    std::shared_ptr<detail::channel_core> core_;
};

} // namespace v2
} // namespace rpc
