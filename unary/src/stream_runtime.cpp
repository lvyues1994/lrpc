#include "stream_runtime.hpp"
#include "connection.hpp"
#include <net/detail/completion_frame.hpp>
#include <net/receive_source.hpp>
#include <net/run_async.hpp>
#include <net/this_coro.hpp>
#include <net/timeout.hpp>
#include <algorithm>
#include <array>
#include <cstring>
#include <deque>
#include <functional>
#include <mutex>
#include <unordered_map>

namespace rpc { namespace detail {
struct stream_peer;
struct stream_state;
struct stream_service;

// Sealed before socket cancellation/drain. A late buffer release can safely
// destroy its storage after the context has been destroyed.
struct budget_reservation {
    budget_reservation(std::shared_ptr<buffer_budget> budget, std::size_t bytes) : budget(std::move(budget)), bytes(bytes) {
        if (!this->budget->acquire(bytes)) throw std::bad_alloc{};
    }
    ~budget_reservation() { budget->release(bytes); }
    std::shared_ptr<buffer_budget> budget; std::size_t bytes;
};
struct notification_gate {
    explicit notification_gate(net::io_context &ctx) : executor(ctx.get_executor()) {}
    std::mutex mutex;
    bool open = true;
    net::io_context::executor_type executor;
    std::weak_ptr<stream_peer> owner;
    std::atomic<std::size_t> pending{0};
};
struct notice final : std::enable_shared_from_this<notice> {
    notice(std::shared_ptr<notification_gate> gate, std::shared_ptr<buffer_budget> budget, std::function<void(stream_peer &)> action)
        : reservation(std::move(budget), 512), gate(std::move(gate)), action(std::move(action)), completion(&resume, this) { continuation.h = completion.handle(); }
    void request() noexcept {
        auto g = gate;
        std::lock_guard<std::mutex> lock(g->mutex);
        if (requested || !g->open) return;
        auto owner = g->owner.lock(); if (!owner) return;
        requested = true; pin = std::move(owner); self = shared_from_this();
        ++g->pending; g->executor.on_work_started(); g->executor.post(continuation);
    }
    static net::coroutine_handle<> resume(void *pointer) {
        auto *n = static_cast<notice *>(pointer);
        auto g = n->gate; auto ex = g->executor; auto self = std::move(n->self);
        try { n->action(*n->pin); } catch (...) { /* action implementations close on failure */ }
        n->pin.reset(); --g->pending; self.reset(); ex.on_work_finished(); return {};
    }
    budget_reservation reservation;
    std::shared_ptr<notification_gate> gate;
    std::function<void(stream_peer &)> action;
    net::detail::completion_frame completion;
    net::continuation continuation{};
    std::shared_ptr<notice> self;
    std::shared_ptr<stream_peer> pin;
    bool requested = false;
};
struct receipt {
    std::shared_ptr<notice> notification;
    ~receipt() { notification->request(); }
};

struct cancel_stream { std::shared_ptr<notice> notification; void operator()() const noexcept { notification->request(); } };
template <class Result> struct safe_stream_task {
    using task_type = net::task<Result>;
    struct awaiter {
        task_type task;
        bool await_ready() const noexcept { return task.await_ready(); }
        net::coroutine_handle<> await_suspend(net::coroutine_handle<> h, net::io_env const *env) noexcept { return task.await_suspend(h, env); }
        Result await_resume() noexcept {
            try { return task.await_resume(); }
            catch (std::bad_alloc const &) { return Result{status_code::resource_exhausted}; }
            catch (...) { return Result{status_code::internal}; }
        }
    };
    static auto run(task_type task) CO2_BEG(task_type, (task), Result result;) {
        CO2_AWAIT_SET(result, (awaiter{std::move(task)})); CO2_RETURN(std::move(result));
    }
    CO2_END
};

struct owned_frame {
    std::array<std::uint8_t, 16> header{};
    byte_buffer head, body;
    event completed;
    std::weak_ptr<stream_state> waiting;
    status_code result = status_code::unknown;
};
struct stream_state final : byte_stream, std::enable_shared_from_this<stream_state> {
    net::task<stream_read_result> read(byte_buffer &) override;
    net::task<status_code> write(byte_buffer) override;
    net::task<status_code> write_encoded(encoded_request) override;
    net::task<status_code> writes_done() override;
    net::task<call_result> finish() override;
    void cancel() noexcept override;
    bool observe_completion(std::function<void(call_result const &)> callback) override {
        if (completion_observer) return false;
        if (ended) callback(result); else completion_observer = std::move(callback);
        return true;
    }
    std::weak_ptr<stream_peer> owner;
    std::uint32_t id = 0;
    method_kind kind = method_kind::bidirectional;
    bool client_side = false, legacy = false, ended = false, remote_half = false, local_half = false;
    bool reading = false, writing = false, finishing = false;
    std::size_t received_messages = 0, sent_messages = 0, buffered = 0;
    std::uint32_t send_credit = 0, receive_credit = 0;
    std::deque<byte_buffer> inbox;
    std::unique_ptr<buffer_builder> assembly;
    std::unique_ptr<buffer_builder> send_descriptor, send_compressed;
    wire::message_descriptor descriptor{};
    std::size_t assembly_bytes = 0;
    std::uint32_t assembly_cost = 0;
    event readable, credit, finished, transmitted;
    call_result result{status_code::unknown};
    call_options options{};
    clock::time_point deadline = clock::time_point::max();
    deadline_node timeout;
    net::stop_source stop;
    net::stop_token parent_stop;
    inplace<net::stop_callback<cancel_stream>> cancellation;
    std::shared_ptr<notice> cancel_notice;
    std::function<void(call_result const &)> completion_observer;
    byte_buffer request_head;
    method_binding const *binding = nullptr;
};

struct registered_stream_method { std::string name; method_kind kind; codec_ops const *request; codec_ops const *response; };
struct stream_peer final : connection_observer, std::enable_shared_from_this<stream_peer> {
    stream_peer(net::io_context &ctx, connection_options config, std::size_t send_bytes, std::size_t receive_bytes,
                std::size_t controls_bytes, bool server_side)
        : controls(64, controls_bytes), control_budget{controls_bytes, 0}, conn(ctx, config, controls, control_budget, *this),
          send_budget(std::make_shared<buffer_budget>(send_bytes)), receive_budget(std::make_shared<buffer_budget>(receive_bytes)),
          gate(std::make_shared<notification_gate>(ctx)), auxiliary_budget(std::make_shared<buffer_budget>(config.auxiliary_bytes)), deadlines(shard_deadlines(ctx)),
          reservation(*deadlines, config.receive.max_concurrent_streams), server_side(server_side) {
        registry.reserve(config.receive.max_method_ids); wire_methods.reserve(config.receive.max_method_ids);
        streams.reserve(config.receive.max_concurrent_streams);
        for (unsigned a = 1; a <= 2; ++a) if (config.receive.compression & (1U << (a - 1))) compressors[a - 1] = make_message_compressor(static_cast<compression_algorithm>(a));
    }
    ~stream_peer() { if (activated) deactivate_deadlines(*deadlines); }
    void on_frame(wire::frame_view const &) override;
    void on_close() noexcept override;
    bool prepare_request(block &) noexcept override { return true; }
    void on_idle() noexcept override;
    void complete(std::shared_ptr<stream_state> const &, call_result result, bool send_cancel = false) noexcept;
    void return_credit(std::uint32_t id, std::uint32_t bytes) noexcept;
    void receive_message(std::shared_ptr<stream_state> const &, wire::frame_view const &);
    std::shared_ptr<stream_state> admit(std::uint32_t id, method_kind kind, clock::time_point deadline, call_options options = {});
    void arm(std::shared_ptr<stream_state> const &, net::stop_token);
    std::shared_ptr<owned_frame> enqueue(wire::frame_header h, byte_buffer head = {}, byte_buffer body = {});
    method_handle bind(method_descriptor const &);
    std::vector<method_handle> bind(service_descriptor const &);
    registered_stream_method const *lookup(method_handle h) const { return h.owner_ == identity && h.index_ < registry.size() ? &registry[h.index_] : nullptr; }
    wire::limits outgoing() const { return {conn.peer.max_frame_size, conn.peer.max_message_size, conn.options.receive.features & conn.peer.features}; }
    resource_stats stats() const noexcept;
    block_pool controls;
    byte_budget control_budget;
    connection conn;
    std::shared_ptr<buffer_budget> send_budget, receive_budget;
    std::shared_ptr<notification_gate> gate;
    std::shared_ptr<buffer_budget> auxiliary_budget;
    std::shared_ptr<budget_reservation> fixed_auxiliary;
    std::shared_ptr<deadline_scheduler> deadlines;
    deadline_reservation reservation;
    bool activated = false, server_side = false, received_goaway = false;
    std::uint64_t identity = next_client_identity();
    std::uint32_t next_id = 1, last_request = 0, last_admitted = 0, last_submitted = 0;
    std::vector<registered_stream_method> registry;
    std::vector<std::string> wire_methods;
    std::unordered_map<std::uint32_t, std::shared_ptr<stream_state>> streams;
    std::deque<std::shared_ptr<owned_frame>> frames;
    byte_buffer current_rx;
    std::unique_ptr<net::receive_source> source;
    std::unique_ptr<net::tcp_socket> connecting;
    std::array<std::unique_ptr<message_compressor>, 2> compressors;
    std::weak_ptr<stream_service> service;
    client_events *events = nullptr;
    std::size_t max_registry = 4096, handlers = 0;
    std::shared_ptr<void> control_reservation;
};

void validate_stream_options(connection_options const &options) {
    auto copy = options; copy.receive.compression = 0; validate_options(copy);
    if (!(options.receive.features & wire::streaming) || options.receive.initial_stream_window == 0 ||
        options.receive.max_message_size == 0 || options.max_buffered_messages == 0 || options.max_buffered_messages > 65536 ||
        options.receive.compression & ~compression_algorithms() || options.preferred_compression > 2 ||
        (options.receive.compression && !(options.receive.features & wire::message_compression)) ||
        (options.preferred_compression && !(options.receive.compression & (1U << (options.preferred_compression - 1)))))
        throw std::invalid_argument{"invalid streaming connection limits"};
}
std::shared_ptr<owned_frame> stream_peer::enqueue(wire::frame_header h, byte_buffer head, byte_buffer body) {
    if (conn.state == phase::closed || frames.size() >= conn.options.receive.max_concurrent_streams + 1U) return {};
    if (head.size() > 65535 || body.size() > UINT32_MAX - head.size()) return {};
    auto frame = std::make_shared<owned_frame>();
    h.head_length = static_cast<std::uint16_t>(head.size()); h.length = static_cast<std::uint32_t>(head.size() + body.size());
    if (wire::encode_header(h, {frame->header.data(), 16}, outgoing()).code != wire::error::none) return {};
    frame->head = std::move(head); frame->body = std::move(body); frames.push_back(frame); conn.outbound.signal(); return frame;
}
void stream_peer::complete(std::shared_ptr<stream_state> const &s, call_result result, bool send_cancel) noexcept {
    if (s->ended) return;
    s->ended = true; s->result = std::move(result); unschedule(*deadlines, s->timeout);
    streams.erase(s->id); // Remove before synchronous stop callbacks or close.
    s->stop.request_stop(); s->cancellation.reset();
    s->assembly.reset();
    if (send_cancel && conn.state != phase::closed) {
        try {
            wire::frame_header h{0, s->id, server_side ? wire::frame_type::end : wire::frame_type::cancel, 0, 0,
                server_side ? static_cast<std::uint32_t>(s->result.code) : 0};
            std::array<std::uint8_t, 2> empty{};
            if (!enqueue(h, server_side ? byte_buffer::copy({empty.data(), 2}) : byte_buffer{})) conn.close();
        } catch (...) { conn.close(); }
    }
    s->readable.signal(); s->credit.signal(); s->finished.signal(); s->transmitted.signal();
    if (s->completion_observer) { auto callback = std::move(s->completion_observer); try { callback(s->result); } catch (...) {} }
    if (events) events->on_capacity_change();
    on_idle();
}
void stream_peer::on_close() noexcept {
    { std::lock_guard<std::mutex> lock(gate->mutex); gate->open = false; }
    if (activated) { deactivate_deadlines(*deadlines); activated = false; }
    if (source) source->cancel();
    if (connecting) connecting->close();
    while (!frames.empty()) { auto frame = std::move(frames.front()); frames.pop_front(); frame->result = status_code::unavailable; frame->completed.signal(); }
    while (!streams.empty()) { auto s = streams.begin()->second; complete(s, call_result{status_code::unavailable}); }
    if (events) events->on_connectivity_change();
}
void stream_peer::on_idle() noexcept {
    if (conn.state == phase::draining && streams.empty() && handlers == 0 && frames.empty() && conn.tx.empty() && !conn.writing) conn.close();
}
resource_stats stream_peer::stats() const noexcept {
    resource_stats out{}; out.connections = conn.state == phase::ready || conn.state == phase::draining;
    out.active_calls = streams.size(); out.request_bytes_in_use = server_side ? receive_budget->used() : send_budget->used();
    out.response_bytes_in_use = server_side ? send_budget->used() : receive_budget->used(); out.control_bytes_in_use = controls.in_use();
    out.storage_bytes = conn.rx.capacity() + controls.allocated() + send_budget->used() + receive_budget->used();
    if (source) out.storage_bytes += 65536; // TCP receive_source's fixed backing buffer.
    for (auto const &compressor : compressors) if (compressor) out.storage_bytes += compressor->workspace_bytes();
    return out;
}
std::shared_ptr<stream_state> stream_peer::admit(std::uint32_t id, method_kind kind, clock::time_point deadline, call_options options) {
    if (conn.state != phase::ready || streams.size() >= std::min(conn.peer.max_concurrent_streams, conn.options.receive.max_concurrent_streams)) return {};
    auto s = std::make_shared<stream_state>(); s->owner = shared_from_this(); s->id = id; s->kind = kind; s->client_side = !server_side;
    s->send_credit = conn.peer.initial_stream_window; s->receive_credit = conn.options.receive.initial_stream_window;
    s->deadline = deadline; s->options = options;
    streams.emplace(id, s);
    if (!schedule(*deadlines, s->timeout, deadline, [](void *p, status_code code) {
        auto *s = static_cast<stream_state *>(p); if (auto peer = s->owner.lock()) peer->complete(s->shared_from_this(), call_result{code}, true);
    }, s.get())) { streams.erase(id); return {}; }
    return s;
}
void stream_peer::arm(std::shared_ptr<stream_state> const &s, net::stop_token token) {
    s->parent_stop = token;
    s->cancel_notice = std::make_shared<notice>(gate, auxiliary_budget, [weak = std::weak_ptr<stream_state>{s}](stream_peer &peer) {
        if (auto stream = weak.lock()) peer.complete(stream, call_result{status_code::cancelled}, true);
    });
    s->cancellation.emplace(std::move(token), cancel_stream{s->cancel_notice});
}
void stream_state::cancel() noexcept { if (cancel_notice) cancel_notice->request(); }
void stream_peer::return_credit(std::uint32_t id, std::uint32_t bytes) noexcept {
    auto found = streams.find(id); if (found == streams.end()) return;
    auto &s = *found->second;
    if (s.buffered) --s.buffered;
    if (bytes > conn.options.receive.initial_stream_window - s.receive_credit) { conn.close(); return; }
    s.receive_credit += bytes;
    if (s.kind != method_kind::unary) conn.control(wire::frame_type::window_update, id, bytes);
}

// Copy buffered handshake bytes first. Optional receive_source owns the read
// direction from this point on; backend buffers are consumed only after copy.
auto read_exact(std::shared_ptr<stream_peer> p, wire::mutable_bytes_view bytes)
    CO2_BEG((net::task<net::io_result<>>), (p, bytes),
            std::size_t copied = 0; std::size_t count = 0; std::array<net::const_buffer, 16> scratch{};
            net::io_result<std::size_t> read; net::io_result<net::const_buffer_span> pulled;) {
    count = std::min(bytes.size, p->conn.rx_end - p->conn.rx_begin);
    if (count) std::memcpy(bytes.data, p->conn.rx.data() + p->conn.rx_begin, count);
    p->conn.rx_begin += count; copied = count;
    while (copied < bytes.size) {
        if (!p->source) {
            CO2_AWAIT_SET(read, net::read(p->conn.link->stream(), net::mutable_buffer{bytes.data + copied, bytes.size - copied}));
            CO2_RETURN((net::io_result<>{read.ec}));
        }
        CO2_AWAIT_SET(pulled, p->source->pull(net::const_buffer_span{scratch.data(), scratch.size()}));
        if (pulled.ec) CO2_RETURN((net::io_result<>{pulled.ec}));
        count = 0;
        for (auto const &chunk : pulled.value) {
            auto n = std::min(chunk.size(), bytes.size - copied);
            if (n) std::memcpy(bytes.data + copied, chunk.data(), n);
            copied += n; count += n; if (copied == bytes.size) break;
        }
        if (count == 0) CO2_RETURN((net::io_result<>{net::make_error_code(net::error::eof)}));
        p->source->consume(count);
    }
    CO2_RETURN((net::io_result<>{{}}));
}
CO2_END
auto owned_reader(std::shared_ptr<stream_peer> p)
    CO2_BEG(net::task<>, (p), std::array<std::uint8_t, 16> header{}; wire::decode_result<wire::frame_header> h;
            net::io_result<> result; std::unique_ptr<buffer_builder> buffer; wire::decode_result<wire::frame_view> frame;
            std::unique_ptr<buffer_builder> complete; unsigned turn = 0;) {
    while (p->conn.state != phase::closed) {
        CO2_AWAIT_SET(result, read_exact(p, {header.data(), header.size()}));
        if (result.ec) { p->conn.close(); break; }
        h = wire::decode_header({header.data(), header.size()}, {p->conn.options.receive.max_frame_size, p->conn.options.receive.max_message_size,
            p->conn.options.receive.features & p->conn.peer.features});
        if (h.code != wire::error::none) { p->conn.close(); break; }
        buffer = std::make_unique<buffer_builder>(16 + h.value.length, p->receive_budget);
        std::memcpy(buffer->buffer().data, header.data(), 16);
        CO2_AWAIT_SET(result, read_exact(p, {buffer->buffer().data + 16, h.value.length}));
        if (result.ec) { p->conn.close(); break; }
        p->current_rx = buffer->finish(); buffer.reset();
        frame = wire::decode_frame(p->current_rx.segment(0), {p->conn.options.receive.max_frame_size, p->conn.options.receive.max_message_size,
            p->conn.options.receive.features & p->conn.peer.features});
        if (frame.code != wire::error::none) { p->conn.close(); break; }
        p->conn.last_activity = clock::now();
        p->on_frame(frame.value); p->current_rx.clear();
        if (++turn == 64) { turn = 0; CO2_AWAIT((yield_awaiter{})); }
    }
    p->current_rx.clear(); CO2_RETURN();
}
CO2_END
struct writer_cleanup {
    std::shared_ptr<stream_peer> peer;
    std::shared_ptr<owned_frame> *frame = nullptr;
    ~writer_cleanup() {
        if (!peer) return;
        peer->conn.writing = false;
        if (frame && *frame) { (*frame)->result = status_code::unavailable; (*frame)->completed.signal(); if (auto s = (*frame)->waiting.lock()) s->transmitted.signal(); }
        peer->conn.close();
    }
};
auto owned_writer(std::shared_ptr<stream_peer> p)
    CO2_BEG(net::task<>, (p), std::shared_ptr<owned_frame> frame; block_lease control;
            std::array<net::const_buffer, 130> spans{}; std::size_t count = 0; net::io_result<std::size_t> sent; writer_cleanup cleanup;) {
    cleanup.peer = p; cleanup.frame = &frame;
    while (p->conn.state != phase::closed) {
        if (p->frames.empty() && p->conn.tx.empty()) { p->on_idle(); if (p->conn.state == phase::closed) break; CO2_AWAIT(p->conn.outbound.wait()); continue; }
        count = 0;
        if (!p->conn.tx.empty()) {
            control = p->conn.tx.pop(); spans[count++] = net::const_buffer{control.get()->data, control.get()->size};
        } else {
            frame = std::move(p->frames.front()); p->frames.pop_front(); spans[count++] = net::buffer(frame->header);
            if (frame->header[8] == static_cast<std::uint8_t>(wire::frame_type::request))
                p->last_submitted = wire::decode_header({frame->header.data(), 16}, p->outgoing()).value.stream_id;
            for (std::size_t i = 0; i < frame->head.segment_count(); ++i) { auto b = frame->head.segment(i); spans[count++] = {b.data, b.size}; }
            for (std::size_t i = 0; i < frame->body.segment_count(); ++i) { auto b = frame->body.segment(i); spans[count++] = {b.data, b.size}; }
        }
        p->conn.writing = true;
        CO2_AWAIT_SET(sent, net::write(p->conn.link->stream(), net::const_buffer_span{spans.data(), count}));
        p->conn.writing = false;
        if (frame) { frame->result = io_status(sent.ec); frame->head.clear(); frame->body.clear(); frame->completed.signal(); if (auto s = frame->waiting.lock()) s->transmitted.signal(); frame.reset(); }
        if (control.get() && !sent.ec && p->conn.ping_queued && control.get()->data[8] == static_cast<std::uint8_t>(wire::frame_type::ping)) {
            p->conn.ping_queued = false; p->conn.awaiting_pong = true; p->conn.ping_sent_at = clock::now(); p->conn.keepalive.cancel();
        }
        control.reset();
        if (sent.ec) { p->conn.close(); break; }
        p->conn.last_activity = clock::now(); p->on_idle();
    }
    CO2_RETURN();
}
CO2_END
auto stream_keepalive(std::shared_ptr<stream_peer> p)
    CO2_BEG(net::task<>, (p), net::io_result<> waited;) {
    while (p->conn.state != phase::closed) {
        p->conn.keepalive.expires_at(p->conn.awaiting_pong ? p->conn.ping_sent_at + p->conn.options.keepalive_timeout :
            p->conn.ping_queued ? clock::time_point::max() : p->conn.last_activity + p->conn.options.keepalive_interval);
        CO2_AWAIT_SET(waited, p->conn.keepalive.wait());
        if (p->conn.state == phase::closed) break;
        if (waited.ec == net::cond::canceled) continue;
        if (waited.ec || p->conn.awaiting_pong) { p->conn.close(); break; }
        if (clock::now() < p->conn.last_activity + p->conn.options.keepalive_interval) continue;
        p->conn.ping_queued = true; if (!p->conn.control(wire::frame_type::ping, 0, ++p->conn.ping_sequence)) break;
    }
    CO2_RETURN();
}
CO2_END
void activate_peer(std::shared_ptr<stream_peer> const &p) {
    std::size_t fixed = p->conn.rx.capacity() + (p->conn.options.use_receive_source && p->conn.link->socket() ? 65536U : 0U);
    for (auto const &compressor : p->compressors) if (compressor) fixed += compressor->workspace_bytes();
    p->fixed_auxiliary = std::make_shared<budget_reservation>(p->auxiliary_budget, fixed);
    p->gate->owner = p; activate_deadlines(p->deadlines); p->activated = true;
    if (p->conn.options.use_receive_source && p->conn.link->socket()) p->source = std::make_unique<net::receive_source>(*p->conn.link->socket(), 4, 16U * 1024U);
    auto done = [p] { --p->conn.io_chains; };
    auto fail = [p](std::exception_ptr) { --p->conn.io_chains; p->conn.close(); };
    ++p->conn.io_chains; try { net::run_async(p->conn.context.get_executor(), done, fail)([p] { return owned_reader(p); }); } catch (...) { --p->conn.io_chains; p->conn.close(); throw; }
    ++p->conn.io_chains; try { net::run_async(p->conn.context.get_executor(), done, fail)([p] { return owned_writer(p); }); } catch (...) { --p->conn.io_chains; p->conn.close(); throw; }
    if (p->conn.options.keepalive_interval.count()) { ++p->conn.io_chains; try { net::run_async(p->conn.context.get_executor(), done, fail)([p] { return stream_keepalive(p); }); } catch (...) { --p->conn.io_chains; p->conn.close(); throw; } }
}

struct operation_guard { bool *flag = nullptr; ~operation_guard() { if (flag) *flag = false; } bool enter(bool &value) { if (value) return false; value = true; flag = &value; return true; } };
auto stream_read_task(std::shared_ptr<stream_state> s, byte_buffer *message)
    CO2_BEG(net::task<stream_read_result>, (s, message), std::shared_ptr<stream_peer> p; net::io_env const *env; operation_guard guard;) {
    p = s->owner.lock(); CO2_AWAIT_SET(env, net::this_coro::environment);
    if (!p || &env->executor.context() != &p->conn.context || !guard.enter(s->reading)) CO2_RETURN((stream_read_result{status_code::failed_precondition}));
    message->clear(); // Release a previous owned result before waiting for credit.
    while (s->inbox.empty() && !s->remote_half && !s->ended) CO2_AWAIT(s->readable.wait());
    if (!s->inbox.empty()) { *message = std::move(s->inbox.front()); s->inbox.pop_front(); CO2_RETURN((stream_read_result{})); }
    CO2_RETURN((stream_read_result{s->ended ? s->result.code : status_code::ok, true}));
}
CO2_END
auto send_frame(std::shared_ptr<stream_peer> p, wire::frame_header h, byte_buffer head, byte_buffer body, std::shared_ptr<stream_state> s = {})
    CO2_BEG(net::task<status_code>, (p, h, head, body, s), std::shared_ptr<owned_frame> frame;) {
    try { frame = p->enqueue(h, std::move(head), std::move(body)); }
    catch (...) { CO2_RETURN(status_code::resource_exhausted); }
    if (!frame) CO2_RETURN(status_code::resource_exhausted);
    frame->waiting = s;
    if (s) { while (frame->result == status_code::unknown && !s->ended) CO2_AWAIT(s->transmitted.wait()); CO2_RETURN(s->ended ? s->result.code : frame->result); }
    CO2_AWAIT(frame->completed.wait()); CO2_RETURN(frame->result);
}
CO2_END
struct write_cleanup {
    std::shared_ptr<stream_peer> peer;
    std::shared_ptr<stream_state> stream;
    bool armed = false;
    ~write_cleanup() { if (armed && peer && !stream->ended) peer->complete(stream, call_result{status_code::internal}, true); }
};
auto stream_write_task(std::shared_ptr<stream_state> s, byte_buffer message, encoded_request encoded, bool use_codec, bool half)
    CO2_BEG(net::task<status_code>, (s, message, encoded, use_codec, half),
            std::shared_ptr<stream_peer> p; net::io_env const *env; operation_guard guard;
            std::unique_ptr<buffer_builder> builder; std::unique_ptr<buffer_builder> compressed_builder;
            wire::message_descriptor descriptor; std::array<std::uint8_t, 9> descriptor_bytes{};
            byte_buffer head; byte_buffer fragment; std::size_t size = 0, offset = 0, count = 0; std::uint32_t cost = 0;
            wire::encode_result compressed_result; message_compressor *compressor = nullptr; status_code result; wire::frame_header h; write_cleanup cleanup;) {
    p = s->owner.lock(); CO2_AWAIT_SET(env, net::this_coro::environment);
    if (!p || &env->executor.context() != &p->conn.context || !guard.enter(s->writing)) CO2_RETURN(status_code::failed_precondition);
    if (s->ended) CO2_RETURN(s->result.code == status_code::ok ? status_code::failed_precondition : s->result.code);
    if (s->parent_stop.stop_requested() || env->stop_token.stop_requested()) { p->complete(s, call_result{status_code::cancelled}, true); CO2_RETURN(status_code::cancelled); }
    if (clock::now() >= s->deadline) { p->complete(s, call_result{status_code::deadline_exceeded}, true); CO2_RETURN(status_code::deadline_exceeded); }
    if (s->local_half) CO2_RETURN(status_code::failed_precondition);
    if (half) {
        cleanup.peer = p; cleanup.stream = s; cleanup.armed = true;
        h = {0, s->id, wire::frame_type::message, wire::end_stream, 0, 0};
        CO2_AWAIT_SET(result, send_frame(p, h, {}, {}, s)); if (result == status_code::ok) s->local_half = true;
        cleanup.armed = false; CO2_RETURN(result);
    }
    if ((s->client_side && s->kind == method_kind::server_streaming && s->sent_messages != 0) ||
        (!s->client_side && s->kind == method_kind::client_streaming && s->sent_messages != 0) ||
        (s->kind == method_kind::unary && s->sent_messages != 0)) CO2_RETURN(status_code::failed_precondition);
    try {
        if (use_codec) {
            if (!encoded.message || !encoded.operations || !encoded.operations->size || !encoded.operations->encode ||
                (encoded.operations->owned && !encoded.operations->owned->encode)) CO2_RETURN(status_code::invalid_argument);
            if (encoded.operations->encode_bounded && !encoded.operations->owned) {
                if (!s->send_descriptor) s->send_descriptor = std::make_unique<buffer_builder>(9, p->send_budget);
                auto const &ops = *encoded.operations;
                size = ops.upper_bound ? ops.upper_bound(encoded.message) : ops.size(encoded.message);
                size = std::min<std::size_t>(size, p->conn.peer.max_message_size);
                size = std::min(size, p->send_budget->limit() - p->send_budget->used());
                if (!s->client_side && s->binding) size = std::min(size, s->binding->max_response_bytes);
                builder = std::make_unique<buffer_builder>(size, p->send_budget);
                auto const encoded_result = ops.encode_bounded(encoded.message, builder->buffer());
                if (encoded_result.code == wire::error::output_too_small || encoded_result.written > size) CO2_RETURN(status_code::resource_exhausted);
                if (encoded_result.code != wire::error::none) CO2_RETURN(status_code::invalid_argument);
                message = builder->finish(encoded_result.written); builder.reset();
            } else {
                size = encoded.operations->size(encoded.message);
                if (size > p->conn.peer.max_message_size) CO2_RETURN(status_code::resource_exhausted);
                if (encoded.operations->owned) {
                    if (!encoded.operations->owned->encode(encoded.message, message) || message.size() != size) CO2_RETURN(status_code::invalid_argument);
                } else {
                    builder = std::make_unique<buffer_builder>(size, p->send_budget);
                    if (!encoded.operations->encode(encoded.message, builder->buffer())) CO2_RETURN(status_code::invalid_argument);
                    message = builder->finish(); builder.reset();
                }
            }
        }
        size = message.size();
        if (size > p->conn.peer.max_message_size || message.segment_count() > 64 || message.retained_capacity() > p->send_budget->limit()) CO2_RETURN(status_code::resource_exhausted);
        if (!s->client_side && s->binding && size > s->binding->max_response_bytes) CO2_RETURN(status_code::resource_exhausted);
        // Share external owned segments, charging all pinned physical capacity.
        if (!message.charged_to(p->send_budget.get())) {
            message.retain(std::make_shared<budget_reservation>(p->send_budget, message.retained_capacity()));
        }
        descriptor = {static_cast<std::uint32_t>(size), static_cast<std::uint32_t>(size), 0};
        if (size && p->conn.options.preferred_compression && size >= p->conn.options.compression_threshold &&
            (p->conn.peer.compression & (1U << (p->conn.options.preferred_compression - 1))) &&
            (p->outgoing().features & wire::message_compression)) {
            compressor = p->compressors[p->conn.options.preferred_compression - 1].get();
            if (message.segment_count() > 1) {
                builder = std::make_unique<buffer_builder>(size, p->send_budget);
                message.copy_to(builder->buffer()); message = builder->finish(); builder.reset();
            }
            compressed_builder = std::move(s->send_compressed);
            if (!compressed_builder) compressed_builder = std::make_unique<buffer_builder>(compressor->bound(size), p->send_budget);
            compressed_result = compressor->compress(message.segment(0), compressed_builder->buffer());
            if (compressed_result.code != wire::error::none) CO2_RETURN(status_code::internal);
            if (compressed_result.written < size) {
                descriptor.encoded_size = static_cast<std::uint32_t>(compressed_result.written); descriptor.algorithm = p->conn.options.preferred_compression;
                message = compressed_builder->finish(compressed_result.written);
            }
            compressed_builder.reset();
        }
        if (wire::encode_message_descriptor(descriptor, {descriptor_bytes.data(), 9}, p->outgoing()).code != wire::error::none) CO2_RETURN(status_code::resource_exhausted);
        if (s->send_descriptor) {
            std::memcpy(s->send_descriptor->buffer().data, descriptor_bytes.data(), 9); head = s->send_descriptor->finish(); s->send_descriptor.reset();
        } else head = byte_buffer::copy({descriptor_bytes.data(), 9}, p->send_budget);
        cost = std::max<std::uint32_t>(1, std::max(descriptor.encoded_size, descriptor.decoded_size));
        if (s->kind != method_kind::unary && cost > p->conn.peer.initial_stream_window) CO2_RETURN(status_code::resource_exhausted);
    } catch (std::bad_alloc const &) { p->complete(s, call_result{status_code::resource_exhausted}, true); CO2_RETURN(status_code::resource_exhausted); }
    catch (...) { p->complete(s, call_result{status_code::internal}, true); CO2_RETURN(status_code::internal); }
        cleanup.peer = p; cleanup.stream = s; cleanup.armed = true;
        while (s->kind != method_kind::unary && cost > s->send_credit && !s->ended) CO2_AWAIT(s->credit.wait());
        if (s->ended) CO2_RETURN(s->result.code);
        if (s->parent_stop.stop_requested() || env->stop_token.stop_requested()) { p->complete(s, call_result{status_code::cancelled}, true); CO2_RETURN(status_code::cancelled); }
        if (clock::now() >= s->deadline) { p->complete(s, call_result{status_code::deadline_exceeded}, true); CO2_RETURN(status_code::deadline_exceeded); }
        if (s->kind != method_kind::unary) s->send_credit -= cost;
        offset = 0;
        do {
            count = std::min(message.size() - offset, p->conn.peer.max_frame_size - head.size());
            fragment = message.slice(offset, count); offset += count;
            h = {0, s->id, wire::frame_type::message, static_cast<std::uint8_t>((offset < message.size() ? wire::more : 0) | (head.size() && descriptor.algorithm ? wire::compressed : 0)), 0, 0};
            CO2_AWAIT_SET(result, send_frame(p, h, std::move(head), std::move(fragment), s));
            if (result != status_code::ok) { p->complete(s, call_result{result}, true); CO2_RETURN(result); }
            if (s->ended) CO2_RETURN(s->result.code);
        } while (offset < message.size());
        ++s->sent_messages; cleanup.armed = false; CO2_RETURN(status_code::ok);

}
CO2_END
auto stream_finish_task(std::shared_ptr<stream_state> s)
    CO2_BEG(net::task<call_result>, (s), std::shared_ptr<stream_peer> p; net::io_env const *env; operation_guard guard;) {
    p = s->owner.lock(); CO2_AWAIT_SET(env, net::this_coro::environment);
    if (!p || &env->executor.context() != &p->conn.context || !guard.enter(s->finishing)) CO2_RETURN((call_result{status_code::failed_precondition}));
    if (!s->client_side) CO2_RETURN((call_result{status_code::failed_precondition}));
    while (!s->ended) { CO2_AWAIT(s->finished.wait()); }
    CO2_RETURN(s->result);
}
CO2_END
net::task<stream_read_result> stream_state::read(byte_buffer &m) { return stream_read_task(shared_from_this(), &m); }
net::task<status_code> stream_state::write(byte_buffer m) { return safe_stream_task<status_code>::run(stream_write_task(shared_from_this(), std::move(m), {}, false, false)); }
net::task<status_code> stream_state::write_encoded(encoded_request m) { return safe_stream_task<status_code>::run(stream_write_task(shared_from_this(), {}, m, true, false)); }
net::task<status_code> stream_state::writes_done() { return safe_stream_task<status_code>::run(stream_write_task(shared_from_this(), {}, {}, false, true)); }
net::task<call_result> stream_state::finish() { return stream_finish_task(shared_from_this()); }

void stream_peer::receive_message(std::shared_ptr<stream_state> const &s, wire::frame_view const &frame) {
    if (s->remote_half) { conn.close(); return; }
    if (frame.header.flags == wire::end_stream) {
        if (s->assembly) { conn.close(); return; }
        s->remote_half = true; s->readable.signal(); return;
    }
    if (frame.head.size) {
        if (s->assembly) { conn.close(); return; }
        auto descriptor = wire::decode_message_descriptor(frame.head, {conn.options.receive.max_frame_size, conn.options.receive.max_message_size,
            conn.options.receive.features & conn.peer.features});
        if (descriptor.code != wire::error::none) { conn.close(); return; }
        s->descriptor = descriptor.value;
        s->assembly_cost = std::max<std::uint32_t>(1, std::max(s->descriptor.encoded_size, s->descriptor.decoded_size));
        if (s->kind != method_kind::unary && s->assembly_cost > s->receive_credit) { conn.close(); return; }
        if (s->buffered == conn.options.max_buffered_messages ||
            ((s->kind == method_kind::unary || (s->client_side && s->kind == method_kind::client_streaming) ||
              (!s->client_side && s->kind == method_kind::server_streaming)) && s->received_messages != 0)) {
            complete(s, call_result{status_code::resource_exhausted}, true); return;
        }
        if (s->descriptor.algorithm && (!(conn.options.receive.compression & conn.peer.compression & (1U << (s->descriptor.algorithm - 1))))) { conn.close(); return; }
        if (s->kind != method_kind::unary) s->receive_credit -= s->assembly_cost;
            if (frame.header.flags & wire::more) s->assembly = std::make_unique<buffer_builder>(s->descriptor.encoded_size, receive_budget);
        s->assembly_bytes = 0;
    } else if (!s->assembly) { conn.close(); return; }
    if (frame.body.size > s->descriptor.encoded_size - s->assembly_bytes) { conn.close(); return; }
    if (s->assembly && frame.body.size) std::memcpy(s->assembly->buffer().data + s->assembly_bytes, frame.body.data, frame.body.size);
    s->assembly_bytes += frame.body.size;
    if ((frame.header.flags & wire::more) != 0) {
        if (s->assembly_bytes >= s->descriptor.encoded_size) conn.close();
        return;
    }
    if (s->assembly_bytes != s->descriptor.encoded_size) { conn.close(); return; }
    auto message = s->assembly ? s->assembly->finish() : current_rx.slice(16 + frame.head.size, frame.body.size); s->assembly.reset();
    if (s->descriptor.algorithm) {
        buffer_builder decoded(s->descriptor.decoded_size, receive_budget);
        auto const input = message.segment(0);
        if (compressors[s->descriptor.algorithm - 1]->decompress_exact(input, decoded.buffer()).code != wire::error::none) { conn.close(); return; }
        message = decoded.finish();
    }
    if (s->kind != method_kind::unary) {
        auto notification = std::make_shared<notice>(gate, auxiliary_budget, [id = s->id, cost = s->assembly_cost](stream_peer &p) { p.return_credit(id, cost); });
        auto lifetime = std::make_shared<receipt>(); lifetime->notification = std::move(notification); message.retain(lifetime);
    }
    ++s->received_messages; ++s->buffered; s->inbox.push_back(std::move(message)); s->readable.signal();
}

method_handle stream_peer::bind(method_descriptor const &m) {
    if (!m.name || !*m.name || std::strlen(m.name) > conn.options.max_method_name_bytes || static_cast<unsigned>(m.kind) > 3 || !m.request_codec || !m.response_codec ||
        !m.request_codec->size || !m.request_codec->encode || !m.request_codec->decode || !m.response_codec->size || !m.response_codec->encode || !m.response_codec->decode)
        throw std::invalid_argument{"invalid method descriptor"};
    for (std::size_t i = 0; i < registry.size(); ++i) if (registry[i].name == m.name) {
        if (registry[i].kind != m.kind || registry[i].request != m.request_codec || registry[i].response != m.response_codec) throw std::invalid_argument{"conflicting method descriptor"};
        return {identity, i};
    }
    if (registry.size() == max_registry) throw std::invalid_argument{"method registry full"};
    registry.push_back({m.name, m.kind, m.request_codec, m.response_codec}); return {identity, registry.size() - 1};
}
std::vector<method_handle> stream_peer::bind(service_descriptor const &s) {
    if (s.size && !s.methods) throw std::invalid_argument{"null methods"};
    auto previous = registry.size(); std::vector<method_handle> handles; handles.reserve(s.size);
    try { for (std::size_t i = 0; i < s.size; ++i) handles.push_back(bind(s.methods[i])); }
    catch (...) { registry.resize(previous); throw; } return handles;
}

auto stream_open_task(std::shared_ptr<stream_peer> p, std::string method, method_kind kind, method_handle handle, call_options options)
    CO2_BEG(net::task<stream_open_result>, (p, method, kind, handle, options), net::io_env const *env; clock::time_point deadline;
            std::shared_ptr<stream_state> s; std::unique_ptr<buffer_builder> builder; wire::encode_result head_size, encoded;
            wire::request_head head; status_code sent; wire::frame_header h; std::size_t method_index = 0; bool new_definition = false; std::shared_ptr<owned_frame> frame; write_cleanup cleanup;) {
    CO2_AWAIT_SET(env, net::this_coro::environment);
    if (&env->executor.context() != &p->conn.context) CO2_RETURN((stream_open_result{status_code::failed_precondition}));
    if (handle) { auto m = p->lookup(handle); if (!m) CO2_RETURN((stream_open_result{status_code::invalid_argument})); method = m->name; kind = m->kind; }
    if (static_cast<unsigned>(kind) > 3 || method.empty() || method.size() > p->conn.options.max_method_name_bytes ||
        (options.response_metadata.size && !options.response_metadata.data)) CO2_RETURN((stream_open_result{status_code::invalid_argument}));
    if (p->conn.state != phase::ready) CO2_RETURN((stream_open_result{status_code::unavailable}));
    if (!(p->outgoing().features & wire::streaming)) CO2_RETURN((stream_open_result{status_code::unimplemented}));
    if (p->next_id > wire::max_stream_id) { p->conn.state = phase::draining; CO2_RETURN((stream_open_result{status_code::unavailable})); }
    deadline = resolve_deadline(options);
    if (clock::now() >= deadline) CO2_RETURN((stream_open_result{status_code::deadline_exceeded}));
    if (env->stop_token.stop_requested()) CO2_RETURN((stream_open_result{status_code::cancelled}));
    try {
        method_index = 0;
        while (method_index < p->wire_methods.size() && p->wire_methods[method_index] != method) ++method_index;
        new_definition = method_index == p->wire_methods.size();
        if (new_definition && method_index >= std::min(p->conn.peer.max_method_ids, p->conn.options.receive.max_method_ids)) CO2_RETURN((stream_open_result{status_code::resource_exhausted}));
        head = {remaining_timeout(deadline), {reinterpret_cast<std::uint8_t const *>(method.data()), method.size()}, options.metadata};
        if (!new_definition) head.method_name = {};
        head_size = wire::request_head_size(head, new_definition);
        if (head_size.code != wire::error::none) CO2_RETURN((stream_open_result{status_code::invalid_argument}));
        if (head_size.written > p->conn.peer.max_frame_size || head_size.written > 65535) CO2_RETURN((stream_open_result{status_code::resource_exhausted}));
        builder = std::make_unique<buffer_builder>(head_size.written, p->send_budget);
        encoded = wire::encode_request_head(head, new_definition, builder->buffer());
        if (encoded.code != wire::error::none) CO2_RETURN((stream_open_result{status_code::invalid_argument}));
        s = p->admit(p->next_id, kind, deadline, options); if (!s) CO2_RETURN((stream_open_result{status_code::resource_exhausted}));
        ++p->next_id; p->arm(s, env->stop_token);
        cleanup.peer = p; cleanup.stream = s; cleanup.armed = true;
        h = {0, s->id, wire::frame_type::request, static_cast<std::uint8_t>(new_definition ? wire::new_method : 0), 0, static_cast<std::uint32_t>(method_index + 1)};
        frame = p->enqueue(h, builder->finish()); builder.reset();
        if (frame) frame->waiting = s;
        if (!frame) { p->complete(s, call_result{status_code::resource_exhausted}); CO2_RETURN((stream_open_result{status_code::resource_exhausted})); }
        if (new_definition) p->wire_methods.push_back(std::move(method));
    } catch (std::bad_alloc const &) { if (s) p->complete(s, call_result{status_code::resource_exhausted}, true); CO2_RETURN((stream_open_result{status_code::resource_exhausted})); }
    catch (...) { if (s) p->complete(s, call_result{status_code::internal}, true); CO2_RETURN((stream_open_result{status_code::internal})); }
    while (frame->result == status_code::unknown && !s->ended) CO2_AWAIT(s->transmitted.wait());
    sent = s->ended ? s->result.code : frame->result;
    if (sent != status_code::ok || s->ended) { if (!s->ended) p->complete(s, call_result{sent}); CO2_RETURN((stream_open_result{s->result.code})); }
    cleanup.armed = false; CO2_RETURN((stream_open_result{status_code::ok, s}));
}
CO2_END

struct unary_stream_buffers { encoded_request request; decoded_response response; wire::bytes_view raw; wire::mutable_bytes_view output; bool typed; };
// Unary on the extended profile uses exactly one MESSAGE in each direction.
// Reply decoding is held until the final successful END.
auto large_unary_task(std::shared_ptr<stream_peer> p, std::string method, method_handle handle, unary_stream_buffers buffers, call_options options)
    CO2_BEG(net::task<call_result>, (p, method, handle, buffers, options),
            stream_open_result opened; std::shared_ptr<stream_state> s; status_code sent; stream_read_result read;
            byte_buffer reply; byte_buffer extra; std::unique_ptr<buffer_builder> flattened; call_result result;) {
    if (handle) { auto m = p->lookup(handle); if (!m || m->kind != method_kind::unary || m->request != buffers.request.operations || m->response != buffers.response.operations) CO2_RETURN((call_result{status_code::invalid_argument})); }
    if ((buffers.typed && (!buffers.request.message || !buffers.response.message || !buffers.request.operations || !buffers.response.operations ||
        !buffers.request.operations->size || !buffers.request.operations->encode || !buffers.response.operations->decode ||
        (buffers.request.operations->owned && !buffers.request.operations->owned->encode) ||
        (buffers.response.operations->owned && !buffers.response.operations->owned->decode))) ||
        (!buffers.typed && ((buffers.raw.size && !buffers.raw.data) || (buffers.output.size && !buffers.output.data))) ||
        (options.response_metadata.size && !options.response_metadata.data)) CO2_RETURN((call_result{status_code::invalid_argument}));
    if (buffers.output.size && options.response_metadata.size) {
        auto a = reinterpret_cast<std::uintptr_t>(buffers.output.data), b = reinterpret_cast<std::uintptr_t>(options.response_metadata.data);
        if (a <= b ? b - a < buffers.output.size : a - b < options.response_metadata.size) CO2_RETURN((call_result{status_code::invalid_argument}));
    }
    CO2_AWAIT_SET(opened, stream_open_task(p, method, method_kind::unary, handle, options));
    if (!opened.stream) { result.code = opened.code; result.not_executed = true; result.capacity_rejected = opened.code == status_code::resource_exhausted; CO2_RETURN(result); }
    s = std::static_pointer_cast<stream_state>(opened.stream);
    if (buffers.typed) CO2_AWAIT_SET(sent, s->write_encoded(buffers.request));
    else { try { reply = byte_buffer::copy(buffers.raw); } catch (...) { p->complete(s, call_result{status_code::resource_exhausted}, true); CO2_RETURN(s->result); } CO2_AWAIT_SET(sent, s->write(std::move(reply))); }
    if (sent != status_code::ok) { p->complete(s, call_result{sent}, true); CO2_RETURN(s->result); }
    CO2_AWAIT_SET(sent, s->writes_done());
    if (sent != status_code::ok) { p->complete(s, call_result{sent}, true); CO2_RETURN(s->result); }
    CO2_AWAIT_SET(read, s->read(reply));
    CO2_AWAIT_SET(result, s->finish());
    if (result.code != status_code::ok) CO2_RETURN(result);
    if (read.ended || s->received_messages != 1 || !s->inbox.empty()) CO2_RETURN((call_result{status_code::data_loss}));
    try {
        if (buffers.typed) {
            if (buffers.response.operations->owned) { if (!buffers.response.operations->owned->decode(reply, buffers.response.message)) CO2_RETURN((call_result{status_code::data_loss})); }
            else { auto b = reply.size() ? reply.segment(0) : wire::bytes_view{}; if (!buffers.response.operations->decode(b, buffers.response.message)) CO2_RETURN((call_result{status_code::data_loss})); }
        } else { if (reply.size() > buffers.output.size) CO2_RETURN((call_result{status_code::resource_exhausted})); if (!reply.copy_to({buffers.output.data, reply.size()})) CO2_RETURN((call_result{status_code::data_loss})); }
        result.response_size = reply.size(); CO2_RETURN(result);
    } catch (std::bad_alloc const &) { CO2_RETURN((call_result{status_code::resource_exhausted})); }
    catch (...) { CO2_RETURN((call_result{status_code::data_loss})); }
}
CO2_END

struct connect_stream_cleanup {
    std::shared_ptr<stream_peer> peer;
    bool done = false;
    ~connect_stream_cleanup() { if (peer && !done) peer->conn.close(); }
};
auto connect_stream_peer(std::shared_ptr<stream_peer> p, net::ip::tcp::endpoint endpoint, std::unique_ptr<transport> link)
    CO2_BEG(net::task<status_code>, (p, endpoint, link), net::io_env const *env; clock::time_point deadline; net::io_result<> result; connect_stream_cleanup cleanup;) {
    CO2_AWAIT_SET(env, net::this_coro::environment);
    if (&env->executor.context() != &p->conn.context || p->conn.state != phase::idle) CO2_RETURN(status_code::failed_precondition);
    cleanup.peer = p;
    p->conn.state = phase::handshaking; deadline = clock::now() + p->conn.options.handshake_timeout;
        if (!link) {
            p->connecting = std::make_unique<net::tcp_socket>(p->conn.context);
            CO2_AWAIT_SET(result, net::timeout(p->connecting->connect(endpoint), deadline));
            if (result.ec || p->conn.state == phase::closed) { p->conn.close(); CO2_RETURN(io_status(result.ec)); }
            if (p->connecting->set_option(net::socket_option::no_delay{true})) { p->conn.close(); CO2_RETURN(status_code::unavailable); }
            link = make_tcp_transport(std::move(*p->connecting)); p->connecting.reset();
        }
        p->conn.link = std::move(link);
        CO2_AWAIT_SET(result, net::timeout(handshake(p->conn, false), deadline));
        if (result.ec || p->conn.state == phase::closed) { p->conn.close(); CO2_RETURN(result.ec ? io_status(result.ec) : status_code::unavailable); }
        if ((p->outgoing().features & wire::streaming) && p->conn.peer.initial_stream_window == 0) { p->conn.close(); CO2_RETURN(status_code::failed_precondition); }
        p->conn.state = phase::ready; activate_peer(p); cleanup.done = true;
        if (p->events) p->events->on_connectivity_change();
        CO2_RETURN(status_code::ok);
}
CO2_END

struct stream_client final : client {
    explicit stream_client(std::shared_ptr<stream_peer> p) : p(std::move(p)) {}
    ~stream_client() override { close(); }
    net::task<status_code> connect(net::ip::tcp::endpoint e) override { return safe_stream_task<status_code>::run(connect_stream_peer(p, e, {})); }
    net::task<status_code> attach(std::unique_ptr<transport> link) override { if (!link) return failed_stream_operation(status_code::invalid_argument); return safe_stream_task<status_code>::run(connect_stream_peer(p, {}, std::move(link))); }
    net::task<call_result> call(std::string m, wire::bytes_view a, wire::mutable_bytes_view b, call_options o) override { return large_unary_task(p, std::move(m), {}, { {}, {}, a, b, false }, o); }
    net::task<call_result> call_encoded(std::string m, encoded_request a, decoded_response b, call_options o) override { return large_unary_task(p, std::move(m), {}, {a, b, {}, {}, true}, o); }
    net::task<call_result> call_encoded(method_handle m, encoded_request a, decoded_response b, call_options o) override { return large_unary_task(p, {}, m, {a, b, {}, {}, true}, o); }
    method_handle bind(method_descriptor const &m) override { return p->bind(m); }
    std::vector<method_handle> bind(service_descriptor const &s) override { return p->bind(s); }
    net::task<stream_open_result> open_stream(std::string m, method_kind k, call_options o) override { return safe_stream_task<stream_open_result>::run(stream_open_task(p, std::move(m), k, {}, o)); }
    net::task<stream_open_result> open_stream(method_handle m, call_options o) override { return safe_stream_task<stream_open_result>::run(stream_open_task(p, {}, method_kind::unary, m, o)); }
    bool ready() const noexcept override { return p->conn.state == phase::ready; }
    bool draining() const noexcept override { return p->conn.state == phase::draining; }
    bool quiescent() const noexcept override { return p->conn.io_chains == 0 && p->streams.empty() && p->gate->pending == 0; }
    bool has_capacity(std::size_t bytes) const noexcept override { return ready() && p->streams.size() < std::min(p->conn.peer.max_concurrent_streams, p->conn.options.receive.max_concurrent_streams) && bytes <= p->send_budget->limit() - p->send_budget->used(); }
    resource_stats stats() const noexcept override { return p->stats(); }
    void close() noexcept override { p->conn.close(); }
    void drain() noexcept override { if (ready()) p->conn.state = phase::draining; if (p->events) p->events->on_connectivity_change(); p->on_idle(); }
    std::shared_ptr<stream_peer> p;
};
std::unique_ptr<client> make_stream_client(net::io_context &ctx, client_options options) {
    validate_stream_options(options.connection); if (!options.max_registered_methods) throw std::invalid_argument{"empty method registry"};
    auto peer = std::make_shared<stream_peer>(ctx, options.connection, options.request_bytes, options.request_bytes, options.control_bytes, false);
    peer->events = options.events; peer->max_registry = options.max_registered_methods; return std::make_unique<stream_client>(std::move(peer));
}

struct stream_service final : std::enable_shared_from_this<stream_service> {
    stream_service(net::io_context &ctx, std::vector<method_binding> methods, server_options options)
        : context(ctx), methods(std::move(methods)), options(options), request_budget(std::make_shared<buffer_budget>(options.request_bytes)),
          response_budget(std::make_shared<buffer_budget>(options.response_bytes)), auxiliary_budget(std::make_shared<buffer_budget>(options.auxiliary_bytes)), control_budget(std::make_shared<buffer_budget>(options.control_bytes)) { sessions.reserve(options.max_connections); }
    bool attach(std::unique_ptr<transport>);
    void close() noexcept { closed = true; if (acceptor) acceptor->close(); for (auto const &p : sessions) p->conn.close(); }
    void drain() noexcept { draining = true; if (acceptor) acceptor->close(); for (auto const &p : sessions) if (p->conn.state == phase::ready) { p->conn.state = phase::draining; p->conn.control(wire::frame_type::goaway, 0, p->last_admitted); p->on_idle(); } }
    void reap() { sessions.erase(std::remove_if(sessions.begin(), sessions.end(), [](auto const &p) { return p->conn.state == phase::closed && p->conn.io_chains == 0 && p->handlers == 0 && p->gate->pending == 0; }), sessions.end()); }
    net::io_context &context;
    std::vector<method_binding> methods;
    server_options options;
    std::shared_ptr<buffer_budget> request_budget, response_budget, auxiliary_budget, control_budget;
    std::vector<std::shared_ptr<stream_peer>> sessions;
    std::unique_ptr<net::tcp_acceptor> acceptor;
    std::size_t active = 0;
    bool closed = false, draining = false;
};

struct handler_task_awaiter {
    net::task<status_code> task;
    bool await_ready() const noexcept { return task.await_ready(); }
    net::coroutine_handle<> await_suspend(net::coroutine_handle<> h, net::io_env const *env) noexcept { return task.await_suspend(h, env); }
    status_code await_resume() noexcept {
        try { return task.await_resume(); } catch (...) { return status_code::internal; }
    }
};
auto invoke_stream_handler(std::shared_ptr<stream_state> s, server_context *context, wire::bytes_view request, response_writer *writer)
    CO2_BEG(net::task<status_code>, (s, context, request, writer), net::task<status_code> child; status_code result;) {
    try {
        child = s->kind == method_kind::unary ? s->binding->handler->invoke(*context, request, *writer)
                                            : s->binding->stream_handler->invoke(*context, s);
    } catch (...) { CO2_RETURN(status_code::internal); }
    if (!child) CO2_RETURN(status_code::internal);
    CO2_AWAIT_SET(result, (handler_task_awaiter{std::move(child)}));
    CO2_RETURN(result);
}
CO2_END

auto serve_stream(std::shared_ptr<stream_peer> p, std::shared_ptr<stream_state> s, std::shared_ptr<stream_service> service)
    CO2_BEG(net::task<>, (p, s, service), server_context context; std::unique_ptr<buffer_builder> response;
            std::unique_ptr<buffer_builder> end_head; std::unique_ptr<response_writer> writer; stream_read_result read;
            byte_buffer request; byte_buffer extra; status_code code = status_code::unknown; status_code sent;
            wire::decode_result<wire::request_head_view> parsed; wire::frame_header h; byte_buffer body;) {
        end_head = std::make_unique<buffer_builder>(s->binding->max_response_head_bytes, p->send_budget);
        context = {s->deadline, s->stop.get_token(), {}, metadata_writer{end_head->buffer()}};
        parsed = wire::decode_request_head(s->request_head.segment(0), true); context.metadata = parsed.value.metadata;
        if (s->kind == method_kind::unary) {
            CO2_AWAIT_SET(read, s->read(request));
            if (read.code != status_code::ok || read.ended) code = read.code == status_code::ok ? status_code::invalid_argument : read.code;
            else {
                CO2_AWAIT_SET(read, s->read(extra));
                if (read.code != status_code::ok || !read.ended || s->received_messages != 1) code = read.code == status_code::ok ? status_code::invalid_argument : read.code;
                else {
                    response = std::make_unique<buffer_builder>(s->binding->max_response_bytes, p->send_budget);
                    if (!s->legacy) {
                        s->send_descriptor = std::make_unique<buffer_builder>(9, p->send_budget);
                        auto algorithm = p->conn.options.preferred_compression;
                        if (algorithm && (p->conn.peer.compression & (1U << (algorithm - 1))))
                            s->send_compressed = std::make_unique<buffer_builder>(p->compressors[algorithm - 1]->bound(s->binding->max_response_bytes), p->send_budget);
                    }
                    writer = std::make_unique<response_writer>(response->buffer());
                    CO2_AWAIT_SET(code, invoke_stream_handler(s, &context, request.size() ? request.segment(0) : wire::bytes_view{}, writer.get()));
                    if (writer->overflowed()) code = status_code::internal;
                    if (code == status_code::ok) {
                        body = response->finish(writer->size()); response.reset();
                        if (s->legacy) { /* body is emitted in END below */ }
                        else { CO2_AWAIT_SET(sent, s->write(std::move(body))); if (sent != status_code::ok) code = sent; }
                    }
                }
            }
        } else {
            CO2_AWAIT_SET(code, invoke_stream_handler(s, &context, {}, nullptr));
            if (code == status_code::ok && ((s->kind == method_kind::client_streaming && s->sent_messages != 1) ||
                (s->kind == method_kind::server_streaming && (s->received_messages != 1 || !s->remote_half)))) code = status_code::invalid_argument;
        }
        if (static_cast<unsigned>(code) > 16 || context.response_metadata.overflowed() ||
            (code == status_code::ok && context.response_metadata.has_message())) {
            code = status_code::internal; context.response_metadata = metadata_writer{end_head->buffer()};
        }
        if (s->ended || p->conn.state == phase::closed) CO2_RETURN();
        h = {0, s->id, wire::frame_type::end, 0, 0, static_cast<std::uint32_t>(code)};
        if (code != status_code::ok) body.clear();
        CO2_AWAIT_SET(sent, send_frame(p, h, end_head->finish(context.response_metadata.size()), std::move(body)));
        p->complete(s, call_result{sent == status_code::ok ? code : sent});
    CO2_RETURN();
}
CO2_END
void start_handler(std::shared_ptr<stream_peer> const &p, std::shared_ptr<stream_state> const &s, std::shared_ptr<stream_service> const &service) {
    ++p->handlers; ++service->active;
    auto done = [p, service] { --p->handlers; --service->active; p->on_idle(); };
    auto fail = [p, s, service](std::exception_ptr error) {
        auto code = status_code::internal;
        try { std::rethrow_exception(error); } catch (std::bad_alloc const &) { code = status_code::resource_exhausted; } catch (...) {}
        --p->handlers; --service->active; p->complete(s, call_result{code}, true); p->on_idle();
    };
    try { net::run_async(p->conn.context.get_executor(), s->stop.get_token(), nullptr, done, fail)([p, s, service] { return serve_stream(p, s, service); }); }
    catch (...) { --p->handlers; --service->active; throw; }
}
void stream_peer::on_frame(wire::frame_view const &f) {
    try {
        if (f.header.type == wire::frame_type::settings) { conn.close(); return; }
        if (f.header.type == wire::frame_type::ping) { conn.control(wire::frame_type::pong, 0, f.header.aux); return; }
        if (f.header.type == wire::frame_type::pong) {
            if ((conn.awaiting_pong || conn.ping_queued) && f.header.aux == conn.ping_sequence) { conn.awaiting_pong = false; conn.ping_queued = false; conn.keepalive.cancel(); } return;
        }
        if (f.header.type == wire::frame_type::goaway) {
            if (server_side || received_goaway || f.header.aux > last_submitted) { conn.close(); return; }
            received_goaway = true;
            conn.state = phase::draining;
            std::vector<std::shared_ptr<stream_state>> rejected;
            for (auto const &entry : streams) if (entry.first > f.header.aux) rejected.push_back(entry.second);
            for (auto const &s : rejected) { call_result r{status_code::unavailable}; r.not_executed = true; complete(s, r); }
            if (events) events->on_connectivity_change();
            on_idle(); return;
        }
        if ((f.header.type == wire::frame_type::request && !server_side) || (f.header.type == wire::frame_type::end && server_side)) { conn.close(); return; }
        if (f.header.type == wire::frame_type::request && server_side) {
            if (f.header.stream_id <= last_request) { conn.close(); return; } last_request = f.header.stream_id;
            auto head = wire::decode_request_head(f.head, (f.header.flags & wire::new_method) != 0);
            if (head.code != wire::error::none) { conn.close(); return; }
            std::string name;
            if (f.header.flags & wire::new_method) {
                if (f.header.aux != wire_methods.size() + 1 || wire_methods.size() == conn.options.receive.max_method_ids || head.value.method_name.size > conn.options.max_method_name_bytes) { conn.close(); return; }
                name.assign(reinterpret_cast<char const *>(head.value.method_name.data), head.value.method_name.size); wire_methods.push_back(name);
            } else { if (f.header.aux == 0 || f.header.aux > wire_methods.size()) { conn.close(); return; } name = wire_methods[f.header.aux - 1]; }
            auto service = this->service.lock(); if (!service) { conn.close(); return; }
            auto binding = std::find_if(service->methods.begin(), service->methods.end(), [&](method_binding const &m) { return m.name == name; });
            status_code rejection = status_code::ok;
            if (binding == service->methods.end()) rejection = status_code::unimplemented;
            else if (conn.state != phase::ready || service->draining) rejection = status_code::unavailable;
            else if (service->active == service->options.max_active_calls) rejection = status_code::resource_exhausted;
            else if ((f.header.flags & wire::end_stream) && binding->kind != method_kind::unary) rejection = status_code::invalid_argument;
            else if (binding->max_response_bytes > conn.peer.max_message_size || binding->max_response_head_bytes > conn.peer.max_frame_size ||
                ((f.header.flags & wire::end_stream) && binding->max_response_bytes > conn.peer.max_frame_size - binding->max_response_head_bytes)) rejection = status_code::resource_exhausted;
            if (rejection != status_code::ok) { conn.control(wire::frame_type::end, f.header.stream_id, static_cast<std::uint32_t>(rejection), outgoing().features & wire::explicit_rejection ? wire::not_executed : 0); return; }
            auto s = admit(f.header.stream_id, binding->kind, head.value.timeout_us ? deadline_after(head.value.timeout_us) : clock::time_point::max());
            if (!s) { conn.control(wire::frame_type::end, f.header.stream_id, static_cast<std::uint32_t>(status_code::resource_exhausted), outgoing().features & wire::explicit_rejection ? wire::not_executed : 0); return; }
            s->binding = &*binding; arm(s, {}); s->request_head = current_rx.slice(16, f.head.size);
            // Normalize reused method heads for server metadata parsing.
            if (!(f.header.flags & wire::new_method)) {
                std::vector<wire::metadata_entry> entries; auto input = head.value.metadata.entries;
                for (std::size_t i = 0; i < head.value.metadata.count; ++i) { auto e = wire::decode_metadata_entry(input); entries.push_back(e.value); input.data += e.consumed; input.size -= e.consumed; }
                wire::request_head normalized{head.value.timeout_us, {reinterpret_cast<std::uint8_t const *>(name.data()), name.size()}, {entries.data(), entries.size()}};
                auto n = wire::request_head_size(normalized, true); buffer_builder b(n.written, receive_budget); wire::encode_request_head(normalized, true, b.buffer()); s->request_head = b.finish();
            }
            if (f.header.flags & wire::end_stream) { s->legacy = true; s->remote_half = true; s->received_messages = 1; s->inbox.push_back(current_rx.slice(16 + f.head.size, f.body.size)); }
            last_admitted = s->id; start_handler(shared_from_this(), s, service); return;
        }
        auto found = streams.find(f.header.stream_id);
        if (found == streams.end()) {
            if ((server_side && f.header.stream_id <= last_request) || (!server_side && f.header.stream_id < next_id)) return;
            conn.close(); return;
        }
        auto s = found->second;
        if (f.header.type == wire::frame_type::message) { if (s->legacy) { conn.close(); return; } receive_message(s, f); return; }
        if (f.header.type == wire::frame_type::window_update) {
            if (s->kind == method_kind::unary || f.header.aux > conn.peer.initial_stream_window - s->send_credit) { conn.close(); return; }
            s->send_credit += f.header.aux; s->credit.signal(); return;
        }
        if (f.header.type == wire::frame_type::cancel) { complete(s, call_result{status_code::cancelled}); return; }
        if (f.header.type == wire::frame_type::end && !server_side) {
            auto code = static_cast<status_code>(f.header.aux);
            if (f.body.size || (code == status_code::ok && (s->assembly ||
                ((s->kind == method_kind::unary || s->kind == method_kind::client_streaming) && s->received_messages != 1)))) { conn.close(); return; }
            auto head = wire::decode_end_head(f.head); call_result result{code}; result.not_executed = (f.header.flags & wire::not_executed) != 0;
            result.message.assign(reinterpret_cast<char const *>(head.value.message.data), head.value.message.size);
            if (s->options.response_metadata.data) {
                auto entries = head.value.metadata.entries;
                if (entries.size > s->options.response_metadata.size) result.code = status_code::resource_exhausted;
                else { if (entries.size) std::memcpy(s->options.response_metadata.data, entries.data, entries.size); result.response_metadata = {{s->options.response_metadata.data, entries.size}, head.value.metadata.count}; }
            }
            s->remote_half = true; complete(s, std::move(result)); return;
        }
        conn.close();
    } catch (...) { conn.close(); }
}

auto start_stream_session(std::shared_ptr<stream_peer> p)
    CO2_BEG(net::task<>, (p), net::io_result<> result;) {
    CO2_AWAIT_SET(result, net::timeout(handshake(p->conn, true), clock::now() + p->conn.options.handshake_timeout));
    if (result.ec || p->conn.state == phase::closed) { p->conn.close(); CO2_RETURN(); }
    p->conn.state = phase::ready; activate_peer(p); CO2_RETURN();
}
CO2_END
bool stream_service::attach(std::unique_ptr<transport> link) {
    if (!link) return false;
    reap(); if (closed || draining || sessions.size() >= options.max_connections) { link->close(); return false; }
    std::shared_ptr<stream_peer> p;
    try {
        p = std::make_shared<stream_peer>(context, options.connection, options.response_bytes_per_connection, options.request_bytes_per_connection, options.control_bytes_per_connection, true);
        p->receive_budget = std::make_shared<buffer_budget>(options.request_bytes_per_connection, request_budget);
        p->send_budget = std::make_shared<buffer_budget>(options.response_bytes_per_connection, response_budget);
        p->auxiliary_budget = std::make_shared<buffer_budget>(options.connection.auxiliary_bytes, auxiliary_budget);
        p->control_reservation = std::make_shared<budget_reservation>(control_budget, p->controls.allocated());
        p->service = shared_from_this(); p->conn.link = std::move(link); p->conn.state = phase::handshaking; sessions.push_back(p);
        net::run_async(context.get_executor(), [p] {}, [p](std::exception_ptr) { p->conn.close(); })([p] { return start_stream_session(p); }); return true;
    } catch (...) { if (p) p->conn.close(); if (link) link->close(); return false; }
}
auto stream_accept_loop(std::shared_ptr<stream_service> service)
    CO2_BEG(net::task<>, (service), net::io_result<net::tcp_socket> accepted;) {
    while (!service->closed && !service->draining) {
        CO2_AWAIT_SET(accepted, service->acceptor->accept()); if (accepted.ec) break;
        if (accepted.value.set_option(net::socket_option::no_delay{true})) { accepted.value.close(); continue; }
        service->attach(make_tcp_transport(std::move(accepted.value)));
    }
    CO2_RETURN();
}
CO2_END
auto shutdown_stream_service(std::shared_ptr<stream_service> service, std::chrono::milliseconds grace)
    CO2_BEG(net::task<>, (service, grace), std::unique_ptr<net::steady_timer> timer; clock::time_point deadline; net::io_result<> waited;) {
    timer = std::make_unique<net::steady_timer>(service->context); service->drain(); deadline = clock::now() + std::max(grace, std::chrono::milliseconds::zero());
    while (!service->sessions.empty()) {
        service->reap(); if (service->sessions.empty()) break;
        if (clock::now() >= deadline) service->close();
        timer->expires_after(std::chrono::milliseconds{1}); CO2_AWAIT_SET(waited, timer->wait());
        if (waited.ec) { service->close(); break; }
    }
    CO2_RETURN();
}
CO2_END
struct stream_server final : server {
    explicit stream_server(std::shared_ptr<stream_service> service) : service(std::move(service)) {}
    ~stream_server() override { close(); }
    net::ip::tcp::endpoint listen(net::ip::tcp::endpoint e) override {
        if (service->closed || service->draining || service->acceptor) throw std::logic_error{"server already started"};
        service->acceptor = std::make_unique<net::tcp_acceptor>(service->context, e); std::error_code ec;
        auto bound = service->acceptor->local_endpoint(ec); if (ec) throw std::system_error{ec}; auto s = service;
        net::run_async(s->context.get_executor(), [s] { if (!s->draining) s->close(); }, [s](std::exception_ptr) { s->close(); })([s] { return stream_accept_loop(s); }); return bound;
    }
    bool attach(std::unique_ptr<transport> link) override { return service->attach(std::move(link)); }
    void drain() noexcept override { service->drain(); }
    void close() noexcept override { service->close(); }
    net::task<> shutdown(std::chrono::milliseconds grace) override { return shutdown_stream_service(service, grace); }
    resource_stats stats() const noexcept override {
        resource_stats total{}; total.active_calls = service->active;
        for (auto const &method : service->methods) if (method.handler) total.storage_bytes += method.handler->cached_storage_bytes();
        for (auto const &p : service->sessions) { auto s = p->stats(); total.connections += p->conn.io_chains || s.connections; total.control_bytes_in_use += s.control_bytes_in_use; total.storage_bytes += s.storage_bytes; }
        total.request_bytes_in_use = service->request_budget->used(); total.response_bytes_in_use = service->response_budget->used(); return total;
    }
    std::shared_ptr<stream_service> service;
};
std::unique_ptr<server> make_stream_server(net::io_context &ctx, std::vector<method_binding> methods, server_options options) {
    options.connection.receive.features |= wire::streaming;
    if (!options.connection.receive.initial_stream_window) options.connection.receive.initial_stream_window = 1024U * 1024U;
    validate_stream_options(options.connection);
    if (!options.max_connections || !options.max_active_calls || options.max_queued_calls) throw std::invalid_argument{"streaming server requires bounded direct admission"};
    std::unordered_map<std::string, bool> names;
    for (auto const &m : methods) {
        if (m.name.empty() || m.name.size() > options.connection.max_method_name_bytes || !names.emplace(m.name, true).second || static_cast<unsigned>(m.kind) > 3 ||
            (m.kind == method_kind::unary ? m.handler == nullptr : m.stream_handler == nullptr) || m.max_response_head_bytes < 2 || m.max_response_head_bytes > 65535 ||
            m.max_response_bytes > options.connection.receive.max_message_size || (m.owned_handler && m.owned_handler.get() != m.handler) ||
            (m.owned_stream_handler && m.owned_stream_handler.get() != m.stream_handler)) throw std::invalid_argument{"invalid streaming method binding"};
    }
    return std::make_unique<stream_server>(std::make_shared<stream_service>(ctx, std::move(methods), options));
}
} } // namespace rpc::detail
