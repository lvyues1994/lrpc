#pragma once

#include "storage.hpp"

#include <net/continuation.hpp>
#include <net/timer.hpp>

#include <array>
#include <limits>
#include <atomic>

namespace rpc {
namespace detail {

// Single-shard, single-waiter event for the private writer chain. Closing a
// connection always signals it; it never borrows a user's cancellation token.
class event {
public:
    struct awaiter {
        event *owner;
        net::continuation continuation{};
        bool await_ready() noexcept;
        net::coroutine_handle<> await_suspend(net::coroutine_handle<> handle, net::io_env const *env) noexcept;
        void await_resume() noexcept {}
    };
    awaiter wait() noexcept;
    void signal() noexcept;
private:
    bool signaled_ = false;
    net::continuation *waiter_ = nullptr;
    net::executor_ref executor_{};
};

struct yield_awaiter {
    net::continuation continuation{};
    bool await_ready() const noexcept { return false; }
    net::coroutine_handle<> await_suspend(net::coroutine_handle<> handle, net::io_env const *env) noexcept {
        continuation.h = handle;
        env->executor.post(continuation);
        return net::noop_coroutine();
    }
    void await_resume() const noexcept {}
};

struct connection_observer {
    virtual ~connection_observer() = default;
    virtual void on_frame(wire::frame_view const &frame) = 0;
    virtual void on_close() noexcept = 0;
    virtual bool prepare_request(block &frame) noexcept = 0;
    virtual void on_idle() noexcept = 0;
};

enum class phase { idle, handshaking, ready, draining, closed };

// Stable owner of one transport's read and write state; I/O chains retain the
// enclosing observer, which owns this object. No pending I/O survives its owner.
struct connection {
    connection(net::io_context &context, connection_options options,
               block_pool &controls, byte_budget &control_budget, connection_observer &observer);
    void close() noexcept;
    void enqueue(block_lease frame) noexcept;
    bool control(wire::frame_type type, std::uint32_t stream, std::uint32_t aux, std::uint8_t flags = 0) noexcept;

    net::io_context &context;
    connection_options options;
    block_pool &controls;
    byte_budget &control_budget;
    connection_observer &observer;
    std::unique_ptr<transport> link{};
    phase state = phase::idle;
    wire::settings peer{};
    std::vector<std::uint8_t> rx{};
    std::size_t rx_begin = 0;
    std::size_t rx_end = 0;
    tx_queue tx{};
    event outbound{};
    bool writing = false;
    std::size_t io_chains = 0;
    clock::time_point last_activity = clock::now();
    net::steady_timer keepalive;
    std::uint32_t ping_sequence = 0;
    clock::time_point ping_sent_at{};
    bool ping_queued = false, awaiting_pong = false;
};

struct client_state;
std::uint64_t next_client_identity();
struct client_call;
struct cancel_notice {
    client_call *call;
    void operator()() const noexcept;
};
enum class completion_phase { pending, publishing, published, cancelled };
struct client_call {
    client_state *owner = nullptr;
    slot_handle handle{};
    net::continuation continuation{};
    net::executor_ref executor{};
    deadline_node timeout{};
    inplace<net::stop_callback<cancel_notice>> cancellation{};
    std::atomic<completion_phase> phase{completion_phase::published};
    std::uint32_t id = 0;
    std::size_t method_index = 0;
    clock::time_point deadline = clock::time_point::max();
    net::stop_token token{};
    wire::mutable_bytes_view response{};
    decoded_response decoded{};
    wire::mutable_bytes_view response_metadata{};
    std::size_t method_bytes = 0;
    call_result result{};
    bool submitted = false;
};

void validate_options(connection_options const &options);
clock::time_point resolve_deadline(call_options options) noexcept;
clock::time_point resolve_deadline(clock::time_point deadline, std::chrono::microseconds timeout) noexcept;
clock::time_point deadline_after(std::uint64_t microseconds) noexcept;
clock::time_point deadline_after(clock::time_point start, std::uint64_t microseconds) noexcept;
std::uint64_t remaining_timeout(clock::time_point deadline) noexcept;
status_code io_status(std::error_code error) noexcept;
net::task<net::io_result<wire::frame_view>> read_frame(connection &value);
net::task<net::io_result<>> handshake(connection &value, bool server_side);
void start_io(connection &value, std::shared_ptr<connection_observer> owner);
bool encode_end(block &storage, std::uint32_t id, status_code code, std::size_t body_size,
                wire::settings const &peer, std::size_t head_size = 2, std::size_t body_offset = 18) noexcept;

} // namespace detail
} // namespace rpc
