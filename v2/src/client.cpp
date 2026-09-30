#include <rpc/v2/client.hpp>

#include "engine.hpp"

#include <net/error.hpp>
#include <net/timeout.hpp>

#include <algorithm>
#include <cstring>
#include <new>
#include <stdexcept>
#include <string>
#include <vector>

namespace rpc {
namespace v2 {
namespace detail {

namespace {
// Bounds the table a client allocates for what the server advertises.
constexpr std::size_t max_tracked_streams = 1U << 16;
enum : std::uint8_t { call_idle = 0, call_pending = 1, call_done = 2 };
} // namespace

struct method_slot {
    std::string name;
    bool interned = false; // NEW_METHOD has been queued on this connection.
};

struct client_core final : connection_handler {
    client_core(shard_state &state, client_options const &config) : shard(state), options(config) {
        shard.endpoint_opened();
    }
    ~client_core() { end_endpoint(); }

    bool on_frame(wire::frame_view const &frame) noexcept override;
    void on_ready() noexcept override;
    void on_closed() noexcept override;
    void on_finished() noexcept override {}

    bool ready() const noexcept { return conn && conn->ready() && !going_away; }
    void close() noexcept {
        closed = true;
        if (conn) conn->close();
        end_endpoint();
    }
    void end_endpoint() noexcept {
        if (!endpoint_open) return;
        endpoint_open = false;
        shard.endpoint_closed();
    }
    void settle() noexcept {
        if (going_away && streams.size() == 0 && conn) conn->close_when_flushed();
    }

    shard_state &shard;
    client_options const options;
    std::unique_ptr<connection> conn;
    stream_table streams;
    std::vector<method_slot> methods;
    std::uint32_t next_id = 1;
    bool going_away = false;
    bool handshake_done = false;
    bool closed = false;
    bool endpoint_open = true;
    event ready_event;
    status_code connect_status = status_code::unavailable;
};

struct call_access {
    static unary_call &of(stream_state &stream) noexcept { return static_cast<unary_call &>(stream); }

    // From the reader the caller resumes inline: the reader keeps the core
    // alive and the writer is still posted, so a burst of responses still
    // leaves as one write. Other paths post.
    static void finish(unary_call &call, status_code const code, bool const from_reader = false) noexcept {
        if (call.phase_ != call_pending) return;
        auto &core = *call.core_;
        core.streams.erase(call.id);
        core.shard.unschedule(call);
        call.code_ = code;
        call.phase_ = call_done;
        core.settle();
        if (!from_reader) {
            call.env_->executor.post(call.continuation_);
            return;
        }
        auto const next = call.env_->executor.dispatch(call.continuation_); // `call` may be gone after this.
        if (next != net::noop_coroutine()) net::safe_resume(next);
    }

    static void send_cancel(unary_call &call) noexcept {
        if (auto &conn = call.core_->conn) conn->send_control(wire::frame_type::cancel, call.id, 0, 0);
    }

    static void on_frame(stream_state &stream, wire::frame_view const &frame) noexcept {
        auto &call = of(stream);
        auto code = static_cast<status_code>(frame.header.aux);
        if (code == status_code::ok) {
            if (frame.body.size > call.response_.size) {
                code = status_code::resource_exhausted;
            } else {
                if (frame.body.size != 0) std::memcpy(call.response_.data, frame.body.data, frame.body.size);
                call.size_ = static_cast<std::uint32_t>(frame.body.size);
            }
        }
        finish(call, code, true);
    }

    static void on_abort(stream_state &stream, status_code const code) noexcept { finish(of(stream), code); }

    static void on_deadline(stream_state &stream) noexcept {
        auto &call = of(stream);
        send_cancel(call);
        finish(call, status_code::deadline_exceeded);
    }

    static void on_cancel(stream_state &stream) noexcept {
        auto &call = of(stream);
        if (call.phase_ != call_pending) return;
        send_cancel(call);
        finish(call, status_code::cancelled);
    }

    static status_code start(unary_call &call) noexcept;
};

namespace {
stream_ops const unary_ops{&call_access::on_frame, &call_access::on_abort, &call_access::on_deadline,
                           &call_access::on_cancel};

std::uint64_t remaining_us(clock::duration const remaining) noexcept {
    auto const whole = std::chrono::duration_cast<std::chrono::microseconds>(remaining);
    auto micros = static_cast<std::uint64_t>(whole.count());
    if (whole < remaining) ++micros;
    return std::max<std::uint64_t>(micros, 1);
}
} // namespace

status_code call_access::start(unary_call &call) noexcept {
    auto *const core = call.core_;
    if (core == nullptr || !core->ready()) return status_code::unavailable;
    if (call.method_ == 0 || call.method_ > core->methods.size()) return status_code::invalid_argument;
    auto const &env = *call.env_;
    if (env.stop_token.stop_requested()) return status_code::cancelled;
    auto &conn = *core->conn;
    auto const &peer = conn.peer();
    if (call.method_ > peer.max_method_ids || core->streams.size() >= core->streams.limit() ||
        conn.queued_bytes() > conn.options.tx_high_watermark || call.request_.size > peer.max_message_size)
        return status_code::resource_exhausted;
    if (core->next_id > wire::max_stream_id) {
        core->going_away = true;
        core->settle();
        return status_code::unavailable;
    }

    auto deadline = clock::time_point::max();
    std::uint64_t timeout_us = 0;
    if (auto const *spec = call.spec_) {
        if (spec->deadline != clock::time_point::max() || spec->timeout.count() > 0) {
            auto const current = now();
            deadline = spec->deadline;
            if (spec->timeout.count() > 0 && spec->timeout < deadline - current) deadline = current + spec->timeout;
            if (deadline <= current) return status_code::deadline_exceeded;
            timeout_us = remaining_us(deadline - current);
        }
    }

    auto &slot = core->methods[call.method_ - 1];
    bool const intern = !slot.interned;
    wire::request_head head{};
    head.timeout_us = timeout_us;
    if (intern) head.method_name = {reinterpret_cast<std::uint8_t const *>(slot.name.data()), slot.name.size()};
    if (call.spec_ != nullptr) head.metadata = call.spec_->metadata;
    auto const head_size = wire::request_head_size(head, intern);
    if (head_size.code != wire::error::none || head_size.written > 0xffffU) return status_code::invalid_argument;
    auto const payload = head_size.written + call.request_.size;
    if (payload > peer.max_frame_size) return status_code::resource_exhausted;

    stop_hook *hook = nullptr;
    std::uint8_t *frame = nullptr;
    try {
        if (env.stop_token.stop_possible()) hook = core->shard.acquire_hook();
        frame = conn.reserve(wire::header_size + payload);
    } catch (std::bad_alloc const &) {
        if (hook != nullptr) core->shard.release_hook(*hook);
        return status_code::resource_exhausted;
    }
    auto const id = core->next_id;
    wire::frame_header header{};
    header.length = static_cast<std::uint32_t>(payload);
    header.stream_id = id;
    header.type = wire::frame_type::request;
    header.flags = static_cast<std::uint8_t>(wire::end_stream | (intern ? wire::new_method : 0));
    header.head_length = static_cast<std::uint16_t>(head_size.written);
    header.aux = call.method_;
    if (wire::encode_header(header, {frame, wire::header_size}, conn.outgoing()).code != wire::error::none ||
        wire::encode_request_head(head, intern, {frame + wire::header_size, head_size.written}).code !=
            wire::error::none) {
        if (hook != nullptr) core->shard.release_hook(*hook);
        return status_code::internal;
    }
    if (call.request_.size != 0)
        std::memcpy(frame + wire::header_size + head_size.written, call.request_.data, call.request_.size);
    ++core->next_id;
    slot.interned = true;
    call.id = id;
    call.ops = &unary_ops;
    core->streams.insert(call);
    if (deadline != clock::time_point::max()) core->shard.schedule(call, deadline);
    call.phase_ = call_pending;
    call.hook_ = hook;
    conn.commit(wire::header_size + payload);
    if (hook != nullptr) hook->attach(call, env.stop_token); // Last: the request may already be pending.
    return status_code::ok;
}

bool client_core::on_frame(wire::frame_view const &frame) noexcept {
    auto const &header = frame.header;
    switch (header.type) {
    case wire::frame_type::end:
        if (auto *stream = streams.find(header.stream_id)) stream->ops->on_frame(*stream, frame);
        else if (header.stream_id >= next_id) return false; // Late ENDs of finished calls are expected.
        return true;
    case wire::frame_type::goaway: {
        going_away = true;
        std::vector<stream_state *> unstarted;
        try {
            unstarted = streams.snapshot();
        } catch (std::bad_alloc const &) {
            return false;
        }
        // The server will not run streams above aux; they may be retried elsewhere.
        for (auto *stream : unstarted)
            if (stream->id > header.aux) stream->ops->on_abort(*stream, status_code::unavailable);
        settle();
        return true;
    }
    default: return false;
    }
}

void client_core::on_ready() noexcept {
    try {
        streams = stream_table(std::min<std::size_t>(conn->peer().max_concurrent_streams, max_tracked_streams));
    } catch (std::bad_alloc const &) {
        conn->close();
        return;
    }
    handshake_done = true;
    connect_status = status_code::ok;
    ready_event.signal();
}

void client_core::on_closed() noexcept {
    if (!handshake_done) {
        handshake_done = true;
        connect_status = conn->handshake_expired() ? status_code::deadline_exceeded : status_code::unavailable;
        ready_event.signal();
    }
    while (auto *stream = streams.any()) stream->ops->on_abort(*stream, status_code::unavailable);
    end_endpoint();
}

namespace {

auto attach_task(std::shared_ptr<client_core> core, std::unique_ptr<transport> link)
    CO2_BEG(net::task<status_code>, (core, link), status_code result = status_code::ok;) {
    if (core->conn || core->closed || !link) CO2_RETURN(status_code::failed_precondition);
    try {
        core->conn.reset(new connection(core->shard, core->options.connection, std::move(link), false, *core));
        core->conn->start(core);
    } catch (std::bad_alloc const &) {
        result = status_code::resource_exhausted;
    } catch (std::invalid_argument const &) {
        result = status_code::invalid_argument;
    }
    if (result != status_code::ok) {
        core->close();
        CO2_RETURN(result);
    }
    CO2_AWAIT(core->ready_event.wait());
    CO2_RETURN(core->connect_status);
}
CO2_END

auto connect_task(std::shared_ptr<client_core> core, net::ip::tcp::endpoint endpoint)
    CO2_BEG(net::task<status_code>, (core, endpoint), std::unique_ptr<net::tcp_socket> socket;
            net::io_result<> connected; status_code result = status_code::ok;) {
    if (core->conn || core->closed) CO2_RETURN(status_code::failed_precondition);
    socket.reset(new net::tcp_socket(core->shard.context));
    if (core->options.connection.handshake_timeout.count() > 0) {
        CO2_AWAIT_SET(connected, net::timeout(socket->connect(endpoint), core->options.connection.handshake_timeout));
    } else {
        CO2_AWAIT_SET(connected, socket->connect(endpoint));
    }
    if (connected.ec) CO2_RETURN(connected.ec == net::cond::timeout ? status_code::deadline_exceeded
                                                                     : status_code::unavailable);
    if (core->closed) CO2_RETURN(status_code::unavailable);
    if (socket->set_option(net::socket_option::no_delay{true})) CO2_RETURN(status_code::unavailable);
    CO2_AWAIT_SET(result, attach_task(core, make_tcp_transport(std::move(*socket))));
    CO2_RETURN(result);
}
CO2_END

} // namespace
} // namespace detail

unary_call::unary_call(detail::client_core *core, std::uint32_t method, wire::bytes_view request,
                       wire::mutable_bytes_view response, call_spec const *spec) noexcept
    : core_(core), request_(request), response_(response), spec_(spec), method_(method) {}

unary_call::unary_call(unary_call &&other) noexcept
    : detail::stream_state(), core_(other.core_), request_(other.request_), response_(other.response_),
      spec_(other.spec_), method_(other.method_) {
    assert(other.phase_ == detail::call_idle);
}

net::coroutine_handle<> unary_call::await_suspend(net::coroutine_handle<> handle, net::io_env const *env) noexcept {
    continuation_.h = handle;
    env_ = env;
    auto const code = detail::call_access::start(*this);
    if (code == status_code::ok) return net::noop_coroutine();
    code_ = code;
    phase_ = detail::call_done;
    return handle;
}

call_result unary_call::await_resume() noexcept {
    if (hook_ != nullptr) {
        hook_->detach();
        hook_ = nullptr;
    }
    return call_result{code_, size_};
}

client::client(shard &owner, client_options options)
    : core_(std::make_shared<detail::client_core>(owner.state(), options)) {}

client::~client() { close(); }

net::task<status_code> client::connect(net::ip::tcp::endpoint endpoint) {
    return detail::connect_task(core_, endpoint);
}

net::task<status_code> client::attach(std::unique_ptr<transport> link) {
    return detail::attach_task(core_, std::move(link));
}

method_ref client::bind(std::string const &name) {
    auto &methods = core_->methods;
    for (std::size_t index = 0; index != methods.size(); ++index)
        if (methods[index].name == name) return method_ref{static_cast<std::uint32_t>(index + 1)};
    methods.push_back(detail::method_slot{name, false});
    return method_ref{static_cast<std::uint32_t>(methods.size())};
}

bool client::ready() const noexcept { return core_->ready(); }
std::size_t client::pending() const noexcept { return core_->streams.size(); }
void client::close() noexcept { core_->close(); }

} // namespace v2
} // namespace rpc
