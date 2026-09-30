#include <rpc/runtime.hpp>
#include <rpc/stream.hpp>
#include "connection.hpp"
#include "channel_private.hpp"
#include <net/detail/completion_frame.hpp>
#include <net/run.hpp>
#include <net/run_async.hpp>
#include <net/this_coro.hpp>
#include <net/timeout.hpp>
#include <future>
#include <mutex>
#include <atomic>
#include <cstring>
#if defined(_WIN32)
#include <winsock2.h>
#else
#include <unistd.h>
#endif

namespace rpc {
namespace detail {

struct dispatch_lifetime {
    runtime *owner = nullptr;
    ~dispatch_lifetime() { if (owner) owner->release_call(); }
};
// Prepared while cold; drain/close never compete with the bounded data queue
// and never allocate a posting node. Each monotonic operation is posted once.
struct control_post {
    control_post(runtime &owner, std::size_t shard, std::function<void()> action)
        : owner(owner), executor(owner.context(shard).get_executor()), action(std::move(action)), frame(&resume, this) {
        continuation.h = frame.handle();
    }
    void request(std::shared_ptr<void> lifetime) noexcept {
        if (requested.exchange(true)) return;
        if (!owner.started()) { action(); return; }
        if (!owner.acquire_call(true)) return; // Runtime shutdown has its own prepared close hooks.
        keepalive = std::move(lifetime); executor.on_work_started(); executor.post(continuation);
    }
    static net::coroutine_handle<> resume(void *value) noexcept {
        auto &self = *static_cast<control_post *>(value);
        auto executor = self.executor; auto *owner = &self.owner;
        self.action(); self.keepalive.reset(); // May destroy this node.
        owner->release_call(); executor.on_work_finished(); return nullptr;
    }
    runtime &owner;
    net::io_context::executor_type executor;
    std::function<void()> action;
    net::detail::completion_frame frame;
    net::continuation continuation{};
    std::atomic<bool> requested{false};
    std::shared_ptr<void> keepalive{};
};
template <class State> struct shutdown_cleanup {
    State *state = nullptr;
    std::shared_ptr<State> keepalive{};
    bool completed = false;
    ~shutdown_cleanup() {
        if (state && !completed) {
            state->closed.store(true);
            for (auto const &post : state->close_posts) post->request(keepalive);
        }
    }
};
struct runtime_channel_state {
    explicit runtime_channel_state(runtime &owner) : owner(owner), identity(next_client_identity()) {}
    std::size_t pick(net::execution_context const &source) noexcept {
        for (std::size_t i = 0; i < actors.size(); ++i) if (&owner.context(i) == &source) return i;
        return cursor.fetch_add(1, std::memory_order_relaxed) % actors.size();
    }
    method_handle bind(method_descriptor const &descriptor) {
        if (owner.started()) throw std::logic_error{"bind runtime methods before start"};
        if (!descriptor.name) throw std::invalid_argument{"null method name"};
        for (std::size_t i = 0; i < methods.size(); ++i) if (methods[i].name == descriptor.name) {
            if (methods[i].request != descriptor.request_codec || methods[i].response != descriptor.response_codec ||
                methods[i].semantics != descriptor.semantics || methods[i].kind != descriptor.kind) throw std::invalid_argument{"conflicting runtime method"};
            return {identity, i};
        }
        registered_method entry; entry.name = descriptor.name; entry.request = descriptor.request_codec;
        entry.response = descriptor.response_codec; entry.semantics = descriptor.semantics;
        entry.kind = descriptor.kind;
        entry.handles.reserve(actors.size());
        std::vector<std::size_t> checkpoints; checkpoints.reserve(actors.size());
        for (auto const &actor : actors) checkpoints.push_back(registration_checkpoint(*actor));
        try {
            for (auto const &actor : actors) entry.handles.push_back(actor->bind(descriptor));
            methods.push_back(std::move(entry));
        } catch (...) { for (std::size_t i = 0; i < actors.size(); ++i) rollback_registration(*actors[i], checkpoints[i]); throw; }
        return {identity, methods.size() - 1};
    }
    method_handle lookup(method_handle value, std::size_t shard) const noexcept {
        return value.owner_ == identity && value.index_ < methods.size() ? methods[value.index_].handles[shard] : method_handle{};
    }
    struct registered_method {
        std::string name{};
        codec_ops const *request = nullptr, *response = nullptr;
        idempotency semantics = idempotency::unknown;
        method_kind kind = method_kind::unary;
        std::vector<method_handle> handles{};
    };
    runtime &owner;
    std::vector<std::shared_ptr<channel>> actors{};
    std::vector<registered_method> methods{}; // Frozen before start; no hot-path lock.
    std::uint64_t identity;
    std::atomic<std::size_t> cursor{0};
    std::atomic<bool> closed{false};
    std::atomic<bool> drained{false};
    std::vector<std::unique_ptr<control_post>> close_posts{}, drain_posts{};
    std::vector<std::unique_ptr<std::atomic<bool>>> ready{};
    mutable std::mutex snapshot_mutex;
    metrics_snapshot cached{};
};

struct routed_buffers {
    wire::bytes_view request{};
    wire::mutable_bytes_view response{};
    encoded_request encoded{};
    decoded_response decoded{};
    bool typed = false;
};
auto routed_call(std::shared_ptr<runtime_channel_state> s, std::string method, method_handle handle,
                 routed_buffers buffers, call_options options)
    CO2_BEG(net::task<call_result>, (s, method, handle, buffers, options),
            dispatch_lifetime lifetime; net::io_env const *env = nullptr; std::size_t shard = 0;
            method_handle selected; call_result result; net::task<call_result> operation;) {
    CO2_AWAIT_SET(env, net::this_coro::environment);
    options.deadline = resolve_deadline(options); options.timeout = std::chrono::microseconds{0};
    if (env->stop_token.stop_requested()) CO2_RETURN((call_result{status_code::cancelled}));
    if (clock::now() >= options.deadline) CO2_RETURN((call_result{status_code::deadline_exceeded}));
    if (s->closed.load() || !s->owner.started() || !s->owner.acquire_call()) {
        result.code = s->closed.load() || !s->owner.accepting() ? status_code::unavailable : status_code::resource_exhausted;
        result.not_executed = true; CO2_RETURN(std::move(result));
    }
    lifetime.owner = &s->owner; shard = s->pick(env->executor.context());
    if (handle) {
        selected = s->lookup(handle, shard);
        if (!selected) CO2_RETURN((call_result{status_code::invalid_argument}));
    }
    // Construction only captures arguments; client state is accessed in the
    // owner task. Cross-shard frames use process-lifetime new/delete storage.
    operation = buffers.typed ? (handle ? s->actors[shard]->call_encoded(selected, buffers.encoded, buffers.decoded, options) :
        s->actors[shard]->call_encoded(std::move(method), buffers.encoded, buffers.decoded, options)) :
        s->actors[shard]->call(std::move(method), buffers.request, buffers.response, options);
    CO2_AWAIT_SET(result, net::run(s->owner.context(shard).get_executor(), env->stop_token, net::new_delete_resource())(std::move(operation)));
    // Do not read owner-thread state on the returned caller executor.
    CO2_RETURN(std::move(result));
}
CO2_END

template <class Result> struct stream_dispatch {
    using task_type = net::task<Result>;
    static auto run(runtime *owner, std::size_t shard, task_type operation)
        CO2_BEG(task_type, (owner, shard, operation), dispatch_lifetime lifetime; net::io_env const *env; Result result;) {
        CO2_AWAIT_SET(env, net::this_coro::environment);
        if (!owner->acquire_call()) CO2_RETURN((Result{status_code::unavailable}));
        lifetime.owner = owner;
        CO2_AWAIT_SET(result, net::run(owner->context(shard).get_executor(), env->stop_token, net::new_delete_resource())(std::move(operation)));
        CO2_RETURN(std::move(result));
    }
    CO2_END
};
struct routed_stream final : byte_stream {
    routed_stream(runtime &owner, std::size_t shard, std::shared_ptr<byte_stream> stream) : owner(owner), shard(shard), stream(std::move(stream)) {}
    net::task<stream_read_result> read(byte_buffer &message) override { return stream_dispatch<stream_read_result>::run(&owner, shard, stream->read(message)); }
    net::task<status_code> write(byte_buffer message) override { return stream_dispatch<status_code>::run(&owner, shard, stream->write(std::move(message))); }
    net::task<status_code> write_encoded(encoded_request message) override { return stream_dispatch<status_code>::run(&owner, shard, stream->write_encoded(message)); }
    net::task<status_code> writes_done() override { return stream_dispatch<status_code>::run(&owner, shard, stream->writes_done()); }
    net::task<call_result> finish() override { return stream_dispatch<call_result>::run(&owner, shard, stream->finish()); }
    void cancel() noexcept override { stream->cancel(); }
    runtime &owner;
    std::size_t shard;
    std::shared_ptr<byte_stream> stream;
};
struct routed_stream_cleanup {
    std::shared_ptr<byte_stream> stream;
    ~routed_stream_cleanup() { if (stream) stream->cancel(); }
};
auto routed_open_stream(std::shared_ptr<runtime_channel_state> s, std::string method, method_kind kind, method_handle handle, call_options options)
    CO2_BEG(net::task<stream_open_result>, (s, method, kind, handle, options), dispatch_lifetime lifetime;
            net::io_env const *env; std::size_t shard = 0; stream_open_result result; net::task<stream_open_result> operation; method_handle selected; routed_stream_cleanup cleanup;
            std::shared_ptr<routed_stream> wrapper;) {
    CO2_AWAIT_SET(env, net::this_coro::environment);
    if (s->closed.load() || !s->owner.started() || !s->owner.acquire_call()) CO2_RETURN((stream_open_result{status_code::unavailable}));
    lifetime.owner = &s->owner; shard = s->pick(env->executor.context());
    options.deadline = resolve_deadline(options); options.timeout = std::chrono::microseconds{0};
    if (handle) { selected = s->lookup(handle, shard); if (!selected) CO2_RETURN((stream_open_result{status_code::invalid_argument})); }
    wrapper = std::make_shared<routed_stream>(s->owner, shard, std::shared_ptr<byte_stream>{});
    operation = handle ? s->actors[shard]->open_stream(selected, options) : s->actors[shard]->open_stream(std::move(method), kind, options);
    CO2_AWAIT_SET(result, net::run(s->owner.context(shard).get_executor(), env->stop_token, net::new_delete_resource())(std::move(operation)));
    cleanup.stream = result.stream;
    if (result.stream) { wrapper->stream = std::move(result.stream); result.stream = std::move(wrapper); }
    cleanup.stream.reset();
    CO2_RETURN(std::move(result));
}
CO2_END
auto ready_on_owner(std::shared_ptr<runtime_channel_state> s, std::size_t shard, call_options options)
    CO2_BEG(net::task<status_code>, (s, shard, options), status_code result;) {
    CO2_AWAIT_SET(result, s->actors[shard]->warmup(options));
    s->ready[shard]->store(s->actors[shard]->ready(), std::memory_order_relaxed);
    CO2_RETURN(result);
}
CO2_END
auto routed_warmup(std::shared_ptr<runtime_channel_state> s, call_options options)
    CO2_BEG(net::task<status_code>, (s, options), dispatch_lifetime lifetime;
            net::io_env const *env = nullptr; std::size_t shard = 0; status_code result = status_code::unavailable;) {
    CO2_AWAIT_SET(env, net::this_coro::environment);
    options.deadline = resolve_deadline(options); options.timeout = std::chrono::microseconds{0};
    if (s->closed.load() || !s->owner.started() || !s->owner.acquire_call()) CO2_RETURN(status_code::unavailable);
    lifetime.owner = &s->owner;
    for (shard = 0; shard < s->actors.size(); ++shard) {
        CO2_AWAIT_SET(result, net::run(s->owner.context(shard).get_executor(), env->stop_token, net::new_delete_resource())(ready_on_owner(s, shard, options)));
        if (result != status_code::ok) break;
    }
    CO2_RETURN(result);
}
CO2_END
auto routed_shutdown(std::shared_ptr<runtime_channel_state> s, std::chrono::milliseconds grace)
    CO2_BEG(net::task<>, (s, grace), dispatch_lifetime lifetime; std::size_t shard = 0;
            shutdown_cleanup<runtime_channel_state> cleanup;
            net::io_env const *env = nullptr; clock::time_point until; std::chrono::milliseconds remaining;) {
    CO2_AWAIT_SET(env, net::this_coro::environment);
    if (!s->owner.started() || !s->owner.acquire_call(true)) CO2_RETURN();
    lifetime.owner = &s->owner; s->closed.store(true);
    cleanup.state = s.get(); cleanup.keepalive = s;
    until = clock::now() + std::max(grace, std::chrono::milliseconds::zero());
    for (shard = 0; shard < s->actors.size(); ++shard) {
        remaining = clock::now() < until ? std::chrono::duration_cast<std::chrono::milliseconds>(until - clock::now()) : std::chrono::milliseconds{0};
        CO2_AWAIT(net::run(s->owner.context(shard).get_executor(), env->stop_token, net::new_delete_resource())(s->actors[shard]->shutdown(remaining)));
        s->ready[shard]->store(false, std::memory_order_relaxed);
    }
    s->drained.store(true, std::memory_order_release);
    cleanup.completed = true;
    CO2_RETURN();
}
CO2_END

void add_snapshot(metrics_snapshot &total, metrics_snapshot const &s) noexcept {
    total.calls += s.calls; total.attempts += s.attempts; total.retries += s.retries; total.queued += s.queued;
    total.rejected += s.rejected; total.reconnects += s.reconnects; total.queue_nanoseconds += s.queue_nanoseconds;
    total.call_nanoseconds += s.call_nanoseconds;
    for (std::size_t i = 0; i < total.completed.size(); ++i) total.completed[i] += s.completed[i];
    total.waiting_calls += s.waiting_calls; total.waiting_bytes += s.waiting_bytes;
    total.live_sessions += s.live_sessions; total.replay_bytes_in_use += s.replay_bytes_in_use;
    total.resources.connections += s.resources.connections; total.resources.active_calls += s.resources.active_calls;
    total.resources.request_bytes_in_use += s.resources.request_bytes_in_use;
    total.resources.response_bytes_in_use += s.resources.response_bytes_in_use;
    total.resources.control_bytes_in_use += s.resources.control_bytes_in_use; total.resources.storage_bytes += s.resources.storage_bytes;
    total.resources.queued_calls += s.resources.queued_calls; total.resources.queued_bytes += s.resources.queued_bytes;
}
struct runtime_channel_impl final : channel {
    explicit runtime_channel_impl(std::shared_ptr<runtime_channel_state> s) : state_(std::move(s)) {}
    ~runtime_channel_impl() override { close(); }
    net::task<status_code> connect(net::ip::tcp::endpoint) override { return unsupported_attach(); }
    net::task<status_code> attach(std::unique_ptr<transport>) override { return unsupported_attach(); }
    static auto unsupported_attach() CO2_BEG(net::task<status_code>, ()) { CO2_RETURN(status_code::unimplemented); } CO2_END
    net::task<call_result> call(std::string method, wire::bytes_view request, wire::mutable_bytes_view response, call_options options) override {
        return routed_call(state_, std::move(method), {}, {request, response}, options);
    }
    net::task<call_result> call_encoded(std::string method, encoded_request request, decoded_response response, call_options options) override {
        return routed_call(state_, std::move(method), {}, {{}, {}, request, response, true}, options);
    }
    net::task<call_result> call_encoded(method_handle method, encoded_request request, decoded_response response, call_options options) override {
        return routed_call(state_, {}, method, {{}, {}, request, response, true}, options);
    }
    method_handle bind(method_descriptor const &method) override { return state_->bind(method); }
    net::task<stream_open_result> open_stream(std::string method, method_kind kind, call_options options) override { return routed_open_stream(state_, std::move(method), kind, {}, options); }
    net::task<stream_open_result> open_stream(method_handle method, call_options options) override { return routed_open_stream(state_, {}, method_kind::unary, method, options); }
    std::vector<method_handle> bind(service_descriptor const &service) override {
        if (service.size && !service.methods) throw std::invalid_argument{"null service methods"};
        std::vector<method_handle> result; result.reserve(service.size);
        auto const previous = state_->methods.size();
        std::vector<std::size_t> checkpoints; checkpoints.reserve(state_->actors.size());
        for (auto const &actor : state_->actors) checkpoints.push_back(registration_checkpoint(*actor));
        try { for (std::size_t i = 0; i < service.size; ++i) result.push_back(state_->bind(service.methods[i])); }
        catch (...) {
            state_->methods.resize(previous);
            for (std::size_t i = 0; i < state_->actors.size(); ++i) rollback_registration(*state_->actors[i], checkpoints[i]);
            throw;
        }
        return result;
    }
    bool ready() const noexcept override { for (auto const &ready : state_->ready) if (ready->load(std::memory_order_relaxed)) return true; return false; }
    resource_stats stats() const noexcept override { return metrics().resources; }
    metrics_snapshot metrics() const noexcept override {
        if (state_->owner.stopped()) {
            metrics_snapshot total;
            for (auto const &actor : state_->actors) add_snapshot(total, actor->metrics());
            return total;
        }
        // External snapshots execute reads on their owners. A shard must never
        // block waiting for another shard, so it receives the last full snapshot.
        if (!state_->owner.started() || !state_->owner.accepting() || state_->owner.running_in_this_thread()) {
            std::lock_guard<std::mutex> lock(state_->snapshot_mutex); return state_->cached;
        }
        metrics_snapshot total;
        try {
            for (std::size_t i = 0; i < state_->actors.size(); ++i) {
                auto promise = std::make_shared<std::promise<metrics_snapshot>>(); auto future = promise->get_future();
                auto actor = state_->actors[i];
                if (!state_->owner.post(i, [actor, promise] { promise->set_value(actor->metrics()); })) {
                    std::lock_guard<std::mutex> lock(state_->snapshot_mutex); return state_->cached;
                }
                add_snapshot(total, future.get());
            }
            std::lock_guard<std::mutex> lock(state_->snapshot_mutex); state_->cached = total;
        } catch (...) { std::lock_guard<std::mutex> lock(state_->snapshot_mutex); return state_->cached; }
        return total;
    }
    bool quiescent() const noexcept override {
        if (state_->drained.load(std::memory_order_acquire)) return true;
        if (!state_->closed.load()) return false;
        try {
            if (!state_->owner.started() || state_->owner.stopped()) {
                for (auto const &actor : state_->actors) if (!actor->quiescent()) return false;
            } else {
                if (!state_->owner.accepting() || state_->owner.running_in_this_thread()) return false;
                for (std::size_t i = 0; i < state_->actors.size(); ++i) {
                    auto promise = std::make_shared<std::promise<bool>>(); auto future = promise->get_future();
                    auto actor = state_->actors[i];
                    if (!state_->owner.post(i, [actor, promise] { promise->set_value(actor->quiescent()); })) return false;
                    if (!future.get()) return false;
                }
            }
            state_->drained.store(true, std::memory_order_release); return true;
        } catch (...) { return false; }
    }
    void close() noexcept override {
        state_->closed.store(true);
        for (std::size_t i = 0; i < state_->actors.size(); ++i) {
            state_->ready[i]->store(false, std::memory_order_relaxed);
            state_->close_posts[i]->request(state_);
        }
    }
    void drain() noexcept override {
        for (std::size_t i = 0; i < state_->actors.size(); ++i) {
            state_->drain_posts[i]->request(state_);
        }
    }
    net::task<status_code> warmup(call_options options) override { return routed_warmup(state_, options); }
    net::task<> shutdown(std::chrono::milliseconds grace) override { return routed_shutdown(state_, grace); }
private:
    std::shared_ptr<runtime_channel_state> state_;
};

class native_socket {
public:
    native_socket() noexcept = default;
    ~native_socket() { close(); }
    native_socket(native_socket const &) = delete;
    native_socket &operator=(native_socket const &) = delete;
    auto get() const noexcept -> net::tcp_socket::native_handle_type { return fd_; }
    void adopt(net::tcp_socket::native_handle_type fd) noexcept { fd_ = fd; valid_ = true; }
    void release() noexcept { valid_ = false; }
private:
    void close() noexcept {
        if (!valid_) return;
#if defined(_WIN32)
        closesocket(fd_);
#else
        ::close(fd_);
#endif
    }
    net::tcp_socket::native_handle_type fd_{};
    bool valid_ = false;
};
struct runtime_server_state {
    explicit runtime_server_state(runtime &runtime) : owner(runtime) {}
    runtime &owner;
    std::vector<std::shared_ptr<server>> actors{};
    std::unique_ptr<net::tcp_acceptor> acceptor{};
    std::atomic<bool> closed{false}, draining{false};
    std::vector<std::unique_ptr<control_post>> close_posts{}, drain_posts{};
    std::size_t cursor = 0;
    mutable std::mutex mutex;
    resource_stats cached{};
};
void transfer_socket(std::shared_ptr<runtime_server_state> s, std::size_t shard,
                     std::shared_ptr<native_socket> fd, net::ip::tcp protocol) {
    s->owner.post(shard, [s, shard, fd, protocol] {
        if (s->closed.load() || s->draining.load()) return;
        net::tcp_socket socket{s->owner.context(shard)};
        if (socket.assign(protocol, fd->get())) return;
        fd->release(); s->actors[shard]->attach(make_tcp_transport(std::move(socket)));
    });
}
void stop_accepting(std::shared_ptr<runtime_server_state> const &s) {
    s->drain_posts[0]->request(s);
}
auto accept_sharded(std::shared_ptr<runtime_server_state> s)
    CO2_BEG(net::task<>, (s), net::io_result<net::tcp_socket> accepted; std::size_t shard = 0;
            std::shared_ptr<native_socket> fd; net::ip::tcp protocol = net::ip::tcp::v4(); std::error_code error;) {
    while (!s->closed.load() && !s->draining.load()) {
        CO2_AWAIT_SET(accepted, s->acceptor->accept());
        if (accepted.ec) break;
        protocol = accepted.value.local_endpoint(error).protocol();
        if (error) { accepted.value.close(); continue; }
        if (accepted.value.set_option(net::socket_option::no_delay{true})) { accepted.value.close(); continue; }
        fd = std::make_shared<native_socket>(); fd->adopt(accepted.value.release()); shard = s->cursor++ % s->actors.size();
        transfer_socket(s, shard, fd, protocol);
    }
    CO2_RETURN();
}
CO2_END
auto shutdown_sharded_server(std::shared_ptr<runtime_server_state> s, std::chrono::milliseconds grace)
    CO2_BEG(net::task<>, (s, grace), dispatch_lifetime lifetime; std::size_t shard = 0;
            shutdown_cleanup<runtime_server_state> cleanup;
            net::io_env const *env = nullptr; clock::time_point until; std::chrono::milliseconds remaining;) {
    CO2_AWAIT_SET(env, net::this_coro::environment);
    if (!s->owner.started() || !s->owner.acquire_call(true)) CO2_RETURN();
    lifetime.owner = &s->owner; s->draining.store(true);
    cleanup.state = s.get(); cleanup.keepalive = s;
    stop_accepting(s);
    until = clock::now() + std::max(grace, std::chrono::milliseconds::zero());
    for (shard = 0; shard < s->actors.size(); ++shard) {
        remaining = clock::now() < until ? std::chrono::duration_cast<std::chrono::milliseconds>(until - clock::now()) : std::chrono::milliseconds{0};
        CO2_AWAIT(net::run(s->owner.context(shard).get_executor(), env->stop_token, net::new_delete_resource())(s->actors[shard]->shutdown(remaining)));
    }
    s->closed.store(true); cleanup.completed = true; CO2_RETURN();
}
CO2_END
struct runtime_server_impl final : server {
    explicit runtime_server_impl(std::shared_ptr<runtime_server_state> s) : state_(std::move(s)) {}
    ~runtime_server_impl() override { close(); }
    net::ip::tcp::endpoint listen(net::ip::tcp::endpoint endpoint) override {
        if (state_->owner.started() || state_->acceptor) throw std::logic_error{"listen before runtime start, once"};
        state_->acceptor = std::make_unique<net::tcp_acceptor>(state_->owner.context(0), endpoint);
        std::error_code ec; auto bound = state_->acceptor->local_endpoint(ec); if (ec) throw std::system_error{ec};
        auto state = state_;
        net::run_async(state_->owner.context(0).get_executor(), [] {}, [state](std::exception_ptr) {
            state->closed.store(true);
            for (auto const &post : state->close_posts) post->request(state);
        })([state] { return accept_sharded(state); });
        return bound;
    }
    bool attach(std::unique_ptr<transport>) override { return false; } // An arbitrary transport cannot change its context.
    void drain() noexcept override {
        state_->draining.store(true);
        for (std::size_t i = 0; i < state_->actors.size(); ++i) {
            state_->drain_posts[i]->request(state_);
        }
    }
    resource_stats stats() const noexcept override {
        if (state_->owner.stopped()) {
            resource_stats total;
            for (auto const &actor : state_->actors) {
                auto value = actor->stats(); total.connections += value.connections; total.active_calls += value.active_calls;
                total.request_bytes_in_use += value.request_bytes_in_use; total.response_bytes_in_use += value.response_bytes_in_use;
                total.control_bytes_in_use += value.control_bytes_in_use; total.storage_bytes += value.storage_bytes;
                total.queued_calls += value.queued_calls; total.queued_bytes += value.queued_bytes;
            }
            return total;
        }
        if (!state_->owner.started() || !state_->owner.accepting() || state_->owner.running_in_this_thread()) {
            std::lock_guard<std::mutex> lock(state_->mutex); return state_->cached;
        }
        resource_stats total;
        try {
            for (std::size_t i = 0; i < state_->actors.size(); ++i) {
                auto promise = std::make_shared<std::promise<resource_stats>>(); auto future = promise->get_future(); auto actor = state_->actors[i];
                if (!state_->owner.post(i, [actor, promise] { promise->set_value(actor->stats()); })) {
                    std::lock_guard<std::mutex> lock(state_->mutex); return state_->cached;
                }
                auto value = future.get(); total.connections += value.connections; total.active_calls += value.active_calls;
                total.request_bytes_in_use += value.request_bytes_in_use; total.response_bytes_in_use += value.response_bytes_in_use;
                total.control_bytes_in_use += value.control_bytes_in_use; total.storage_bytes += value.storage_bytes;
                total.queued_calls += value.queued_calls; total.queued_bytes += value.queued_bytes;
            }
            std::lock_guard<std::mutex> lock(state_->mutex); state_->cached = total;
        } catch (...) { std::lock_guard<std::mutex> lock(state_->mutex); return state_->cached; }
        return total;
    }
    void close() noexcept override {
        state_->closed.store(true);
        for (std::size_t i = 0; i < state_->actors.size(); ++i) {
            state_->close_posts[i]->request(state_);
        }
    }
    net::task<> shutdown(std::chrono::milliseconds grace) override { return shutdown_sharded_server(state_, grace); }
private:
    std::shared_ptr<runtime_server_state> state_;
};
} // namespace detail

std::unique_ptr<channel> make_channel(runtime &owner, channel_options options) {
    if (owner.started()) throw std::logic_error{"create channels before runtime start"};
    auto state = std::make_shared<detail::runtime_channel_state>(owner);
    state->actors.reserve(owner.size()); state->ready.reserve(owner.size());
    for (std::size_t i = 0; i < owner.size(); ++i) {
        auto actor = std::shared_ptr<channel>{make_channel(owner.context(i), options)};
        state->actors.push_back(actor); state->ready.push_back(std::make_unique<std::atomic<bool>>(false));
        auto *raw = state.get();
        state->close_posts.push_back(std::make_unique<detail::control_post>(owner, i, [actor, raw, i] { raw->ready[i]->store(false); actor->close(); }));
        state->drain_posts.push_back(std::make_unique<detail::control_post>(owner, i, [actor, raw, i] { raw->ready[i]->store(false); actor->drain(); }));
        std::weak_ptr<detail::runtime_channel_state> weak = state;
        detail::observe_connectivity(*actor, [weak, i](bool ready) noexcept { if (auto s = weak.lock()) s->ready[i]->store(ready); });
        owner.on_shutdown(i, [actor] { actor->close(); });
    }
    return std::make_unique<detail::runtime_channel_impl>(std::move(state));
}
std::unique_ptr<server> make_server(runtime &owner, std::vector<std::vector<method_binding>> methods, server_options options) {
    if (owner.started() || methods.size() != owner.size()) throw std::invalid_argument{"one cold server registry per shard is required"};
    auto state = std::make_shared<detail::runtime_server_state>(owner);
    state->actors.reserve(owner.size());
    for (std::size_t i = 0; i < owner.size(); ++i) {
        auto actor = std::shared_ptr<server>{make_server(owner.context(i), std::move(methods[i]), options)};
        state->actors.push_back(actor);
        auto *raw = state.get();
        state->close_posts.push_back(std::make_unique<detail::control_post>(owner, i, [raw, actor, i] { if (i == 0 && raw->acceptor) raw->acceptor->close(); actor->close(); }));
        state->drain_posts.push_back(std::make_unique<detail::control_post>(owner, i, [raw, actor, i] { if (i == 0 && raw->acceptor) raw->acceptor->close(); actor->drain(); }));
        owner.on_shutdown(i, [state, actor, i] { state->closed.store(true); if (i == 0 && state->acceptor) state->acceptor->close(); actor->close(); });
    }
    return std::make_unique<detail::runtime_server_impl>(std::move(state));
}
call_result call_sync(runtime &owner, client &target, std::string method, wire::bytes_view request,
                      wire::mutable_bytes_view response, call_options options) {
    if (detail::is_runtime_thread()) return {status_code::failed_precondition};
    if (!owner.accepting() || !owner.started()) return {status_code::unavailable};
    net::io_context caller; call_result result; std::exception_ptr error;
    net::run_async(caller.get_executor(), [&](call_result value) { result = std::move(value); },
        [&](std::exception_ptr value) { error = value; })([&] { return target.call(std::move(method), request, response, options); });
    caller.run(); if (error) std::rethrow_exception(error); return result;
}
} // namespace rpc
