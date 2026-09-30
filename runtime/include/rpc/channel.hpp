#pragma once

#include <rpc/typed.hpp>
#include <array>
#include <iosfwd>

namespace rpc {

struct resolution {
    status result{};
    std::vector<net::ip::tcp::endpoint> endpoints{};
};

// A resolver instance may be shared by channels. Implementations create their
// operation state on the supplied owner; no operation accesses another shard.
struct resolver {
    virtual ~resolver() = default;
    virtual net::task<resolution> resolve(net::io_context &owner) = 0;
};
std::shared_ptr<resolver> make_static_resolver(std::vector<net::ip::tcp::endpoint> endpoints);
std::shared_ptr<resolver> make_dns_resolver(std::string host, std::string service);

enum class balancing { round_robin, power_of_two };
enum class connectivity { idle, connecting, ready, draining, backoff, closed };

struct call_info {
    std::string const *method = nullptr; // Borrowed only for the callback.
    clock::time_point deadline = clock::time_point::max();
    wire::metadata_list metadata{};
    std::uint32_t attempt = 0; // Zero denotes the logical call.
    std::uint64_t call_id = 0; // Monotonic within this shard's channel.
    clock::time_point began{};
};

// Owner-thread hooks, once per logical call and once per actual attempt. A
// before hook may refuse locally; callbacks must not run/poll the context.
// Borrowed until channel shutdown. Exceptions become internal and are reported
// to on_exception; exception contents never go to the peer.
struct interceptor {
    virtual ~interceptor() = default;
    virtual status before(call_info const &) { return {}; }
    virtual void after(call_info const &, call_result const &) {}
    virtual void on_exception(std::exception_ptr) noexcept {}
};

struct trace_event {
    call_info call{}; // Callback-only views; a sink retaining text must copy it.
    bool finished = false;
    status_code code = status_code::ok;
    std::uint64_t elapsed_nanoseconds = 0;
};
struct trace_sink {
    virtual ~trace_sink() = default;
    virtual void record(trace_event const &) = 0;
};
// Borrowed sink, owner-thread callbacks. Payload and metadata values are not
// copied/exported by the built-in tracer.
std::unique_ptr<interceptor> make_trace_interceptor(trace_sink &sink);

struct metrics_snapshot {
    std::uint64_t calls = 0;
    std::uint64_t attempts = 0;
    std::uint64_t retries = 0;
    std::uint64_t queued = 0;
    std::uint64_t rejected = 0;
    std::uint64_t reconnects = 0;
    std::uint64_t queue_nanoseconds = 0;
    std::uint64_t call_nanoseconds = 0;
    std::array<std::uint64_t, 17> completed{};
    std::size_t waiting_calls = 0;
    std::size_t waiting_bytes = 0;
    std::size_t live_sessions = 0; // Includes connecting and retired sessions.
    std::size_t replay_bytes_in_use = 0;
    resource_stats resources{};
};
void export_metrics(std::ostream &output, metrics_snapshot const &snapshot);

struct channel_options {
    std::shared_ptr<resolver> resolve{};
    client_options connection{}; // Limits PER session, including retired sessions.
    std::size_t connections_per_backend = 1;
    std::size_t max_connections = 16;
    std::size_t max_retired_connections = 2; // Additional, explicitly budgeted slots.
    std::size_t max_waiting_calls = 0; // Fast rejection is the default.
    std::size_t max_waiting_bytes = 0;
    std::size_t max_logical_calls = 1024; // Includes active, queued and retry-backoff calls.
    std::size_t replay_bytes = 16U * 1024U * 1024U; // Owned request replay, attempt reply and metadata.
    std::chrono::milliseconds connect_timeout{5000};
    std::chrono::milliseconds initial_backoff{100};
    std::chrono::milliseconds max_backoff{30000};
    double backoff_multiplier = 1.6;
    double backoff_jitter = 0.2;
    std::chrono::milliseconds idle_timeout{0};
    std::chrono::milliseconds max_connection_age{0};
    std::chrono::milliseconds resolver_refresh{30000};
    balancing load_balance = balancing::round_robin;
    std::vector<interceptor *> interceptors{};
};

struct channel : client {
    // Resolves and starts bounded parallel connection attempts. Completion
    // means at least one READY connection on this shard. A runtime channel
    // warms each shard under one common deadline before returning success.
    virtual net::task<status_code> warmup(call_options options = {}) = 0;
    virtual net::task<> shutdown(std::chrono::milliseconds grace) = 0;
    virtual metrics_snapshot metrics() const noexcept = 0;
};

// Like client, this single-shard channel is used/destructed on its owner.
// make_channel(runtime&, ...) provides the cross-thread entry point.
std::unique_ptr<channel> make_channel(net::io_context &owner, channel_options options);

} // namespace rpc
