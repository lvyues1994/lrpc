#pragma once

#include <rpc/channel.hpp>
#include <net/backend.hpp>
#include <functional>

namespace rpc {

struct runtime_options {
    std::size_t shards = 1;
    net::backend_kind backend = net::backend_kind::epoll;
    std::size_t max_pending_posts = 4096;
    std::vector<unsigned> cpu_affinity{}; // Empty: let the OS schedule threads.
};

// Owns one io_context and one fixed thread per shard. Configure resources
// before start(), or use post() afterwards. close hooks execute on their owner;
// shutdown stops admission, invokes hooks, drains I/O, then joins. It never
// abandons coroutine frames through io_context::stop().
struct runtime {
    virtual ~runtime() = default;
    virtual std::size_t size() const noexcept = 0;
    virtual net::io_context &context(std::size_t shard) = 0;
    virtual bool running_in_this_thread() const noexcept = 0;
    virtual bool accepting() const noexcept = 0;
    virtual bool started() const noexcept = 0;
    virtual bool stopped() const noexcept = 0; // All workers joined, including failed startup rollback.
    virtual void start() = 0;
    virtual bool post(std::size_t shard, std::function<void()> operation) = 0;
    virtual void on_shutdown(std::size_t shard, std::function<void()> operation) = 0; // Cold registration.
    virtual void shutdown() = 0; // External thread only; can wait for cooperative handlers/DNS.
    virtual bool acquire_call(bool control = false) noexcept = 0; // Control work bypasses data capacity.
    virtual void release_call() noexcept = 0;
};
std::unique_ptr<runtime> make_runtime(runtime_options options = {});
std::unique_ptr<channel> make_channel(runtime &owner, channel_options options);

// A borrowed service may run concurrently on shards; provide a separate
// binding set per shard when its implementation has mutable shard-local state.
std::unique_ptr<server> make_server(runtime &owner, std::vector<std::vector<method_binding>> methods,
                                  server_options options = {});

// External threads only. Storage remains borrowed until this function returns.
// Rejects calls from any runtime thread to avoid blocking an event loop.
call_result call_sync(runtime &owner, client &target, std::string method,
                      wire::bytes_view request, wire::mutable_bytes_view response, call_options options = {});

} // namespace rpc
