#pragma once

#include <rpc/client.hpp>

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace rpc {

// A host name or address, and a port number or service name.
struct channel_target {
    std::string host;
    std::string service;
};

struct channel_options {
    client_options client{};
    std::size_t connections_per_endpoint = 1; // Per endpoint or target.
    // Reconnection after a failed attempt; a connection that was ready
    // reconnects at once.
    std::chrono::milliseconds initial_backoff{20};
    std::chrono::milliseconds max_backoff{2000};
    // Attempts per call, including the first. A call is retried, on another
    // connection when there is one, only if it provably did not execute.
    std::uint32_t max_attempts = 3;
    // A connection to a named target resolves it again before connecting
    // once its answer is older than this, or every address in it failed.
    std::chrono::milliseconds resolve_interval{30000};
    // Resolves named targets instead of getaddrinfo, for example from service
    // discovery. It runs on the shard's thread and must not throw; an empty
    // answer is a failed lookup.
    std::function<net::task<std::vector<net::ip::tcp::endpoint>>(channel_target const &)> resolve{};
};

namespace detail {
struct channel_core;
}

// Calls over connections kept up in the background, to fixed endpoints or to
// named targets. Each call goes to the ready connection with the fewest calls
// in flight. As with client, every member runs on the shard's thread and the
// channel must outlive its calls.
class channel {
public:
    channel(shard &owner, std::vector<net::ip::tcp::endpoint> endpoints, channel_options options = {});
    // Names are resolved on the context's resolver thread when a connection
    // is made; the connections of one target start at different addresses
    // and move to the next address when one fails. close() cannot interrupt
    // a lookup in progress: the context drains once it returns.
    channel(shard &owner, std::vector<channel_target> targets, channel_options options = {});
    ~channel();
    channel(channel const &) = delete;
    channel &operator=(channel const &) = delete;

    // ok once a connection is ready; unavailable after close; deadline_exceeded.
    net::task<status_code> wait_ready(clock::time_point deadline = clock::time_point::max());
    method_ref bind(std::string const &name);
    unary_call call(method_ref method, request_body request, response_body response, call_spec const *spec = nullptr,
                    response_trailer *trailer = nullptr) noexcept;
    // On the least loaded ready connection; streams are not retried.
    open_operation open(method_ref method, method_kind kind, call_spec const *spec = nullptr) noexcept;
    std::size_t ready_connections() const noexcept;
    void close() noexcept;

private:
    std::shared_ptr<detail::channel_core> core_;
};

} // namespace rpc
