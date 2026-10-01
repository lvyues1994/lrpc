#include "client_core.hpp"

#include <net/error.hpp>
#include <net/timeout.hpp>

#include <algorithm>
#include <cstring>
#include <new>
#include <stdexcept>
#include <string>
#include <vector>

namespace rpc {
namespace detail {

namespace {
// Bounds the table a client allocates for what the server advertises.
constexpr std::size_t max_tracked_streams = 1U << 16;

bool retryable(status_code const code) noexcept {
    return code == status_code::unavailable || code == status_code::resource_exhausted;
}
} // namespace

struct call_access {
    static unary_call &of(stream_state &stream) noexcept { return static_cast<unary_call &>(stream); }

    // Starts attempts until one is in flight. Every failure here happened
    // before sending, so a router may try another connection; with none
    // left, the call fails as the last attempt did.
    static status_code launch(unary_call &call, client_core const *avoid,
                              status_code last = status_code::unavailable) noexcept {
        for (;;) {
            if (call.router_ != nullptr) {
                call.core_ = call.router_->pick(avoid);
                if (call.core_ == nullptr) return last;
            }
            auto const code = start(call);
            if (code == status_code::ok) return code;
            if (call.router_ == nullptr || !retryable(code) || ++call.attempts_ >= call.router_->max_attempts())
                return code;
            avoid = call.core_;
            last = code;
        }
    }

    // From the reader the caller resumes inline: the reader keeps the core
    // alive and the writer is still posted, so a burst of responses still
    // leaves as one write. Other paths post.
    static void finish(unary_call &call, status_code code, bool const not_executed,
                       bool const from_reader = false) noexcept {
        if (call.phase_ != call_pending) return;
        auto &core = *call.core_;
        core.streams.erase(call.id);
        core.shard.unschedule(call);
        core.settle();
        if (not_executed && call.router_ != nullptr && retryable(code) &&
            ++call.attempts_ < call.router_->max_attempts()) {
            call.phase_ = call_idle;
            auto const again = launch(call, &core, code);
            if (again == status_code::ok) return;
            code = again;
        }
        call.code_ = code;
        call.not_executed_ = not_executed;
        call.phase_ = call_done;
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

    static bool on_frame(stream_state &stream, wire::frame_view const &frame) noexcept {
        if (frame.header.type != wire::frame_type::end) return false;
        auto &call = of(stream);
        if (call.trailer_ != nullptr && frame.head.size > 2) keep_trailer(*call.trailer_, frame.head);
        auto code = static_cast<status_code>(frame.header.aux);
        auto const &response = call.response_;
        if (code == status_code::ok) {
            if (response.decode != nullptr) {
                if (!response.decode(response.target, frame.body)) code = status_code::internal;
            } else if (frame.body.size > response.capacity) {
                code = status_code::resource_exhausted;
            } else if (frame.body.size != 0) {
                std::memcpy(response.target, frame.body.data, frame.body.size);
            }
            if (code == status_code::ok) call.size_ = static_cast<std::uint32_t>(frame.body.size);
        }
        finish(call, code, (frame.header.flags & wire::not_executed) != 0, true);
        return true;
    }

    static void on_abort(stream_state &stream, status_code const code, bool const not_executed) noexcept {
        finish(of(stream), code, not_executed);
    }

    static void on_deadline(stream_state &stream) noexcept {
        auto &call = of(stream);
        send_cancel(call);
        finish(call, status_code::deadline_exceeded, false);
    }

    static void on_cancel(stream_state &stream) noexcept {
        auto &call = of(stream);
        if (call.phase_ != call_pending) return;
        send_cancel(call);
        finish(call, status_code::cancelled, false);
    }

    static status_code start(unary_call &call) noexcept;
};

namespace {
stream_ops const unary_ops{&call_access::on_frame, &call_access::on_abort, &call_access::on_deadline,
                           &call_access::on_cancel};
} // namespace

std::uint64_t remaining_us(clock::duration const remaining) noexcept {
    auto const whole = std::chrono::duration_cast<std::chrono::microseconds>(remaining);
    auto micros = static_cast<std::uint64_t>(whole.count());
    if (whole < remaining) ++micros;
    return std::max<std::uint64_t>(micros, 1);
}

void keep_trailer(response_trailer &trailer, wire::bytes_view const head) noexcept {
    if (head.size > trailer.storage.size) {
        trailer.truncated = true;
        return;
    }
    std::memcpy(trailer.storage.data, head.data, head.size);
    auto const decoded = wire::decode_end_head({trailer.storage.data, head.size}).value; // Validated.
    trailer.message = decoded.message;
    trailer.metadata = decoded.metadata;
}

status_code call_access::start(unary_call &call) noexcept {
    auto const &env = *call.env_;
    if (env.stop_token.stop_requested()) return status_code::cancelled;
    auto *const core = call.core_;
    if (core == nullptr || !core->ready()) return status_code::unavailable;
    if (call.method_ == 0 || call.method_ > core->methods.size()) return status_code::invalid_argument;
    auto &conn = *core->conn;
    if (!core->has_room() || conn.queued_bytes() > conn.options.tx_high_watermark)
        return status_code::resource_exhausted;
    if (core->next_id > wire::max_stream_id) {
        core->going_away = true;
        core->settle();
        return status_code::unavailable;
    }

    // The first attempt fixes the absolute deadline; later ones only read the clock.
    std::uint64_t timeout_us = 0;
    if (call.deadline_ == clock::time_point::min()) {
        call.deadline_ = clock::time_point::max();
        auto const *spec = call.spec_;
        if (spec != nullptr && (spec->deadline != clock::time_point::max() || spec->timeout.count() > 0)) {
            auto const current = now();
            call.deadline_ = spec->deadline;
            if (spec->timeout.count() > 0 && spec->timeout < call.deadline_ - current)
                call.deadline_ = current + spec->timeout;
            if (call.deadline_ <= current) return status_code::deadline_exceeded;
            timeout_us = remaining_us(call.deadline_ - current);
        }
    } else if (call.deadline_ != clock::time_point::max()) {
        auto const current = now();
        if (call.deadline_ <= current) return status_code::deadline_exceeded;
        timeout_us = remaining_us(call.deadline_ - current);
    }

    request_plan plan{};
    auto const planned = plan_request(*core, core->methods[call.method_ - 1], timeout_us,
                                      call.spec_ != nullptr ? call.spec_->metadata : wire::metadata_list{},
                                      call.request_.size, call.request_.bounded, plan);
    if (planned != status_code::ok) return planned;

    // A retried call keeps the hook of its first attempt.
    stop_hook *hook = nullptr;
    std::uint8_t *frame = nullptr;
    try {
        if (call.hook_ == nullptr && env.stop_token.stop_possible()) hook = core->shard.acquire_hook();
        frame = conn.reserve(wire::header_size + plan.payload);
    } catch (std::bad_alloc const &) {
        if (hook != nullptr) core->shard.release_hook(*hook);
        return status_code::resource_exhausted;
    }
    auto const id = core->next_id;
    std::size_t length = 0;
    auto const written = write_request(*core, plan, frame, id, call.request_, true, length);
    if (written != status_code::ok) {
        if (hook != nullptr) core->shard.release_hook(*hook);
        return written;
    }
    call.id = id;
    call.ops = &unary_ops;
    core->streams.insert(call);
    if (call.deadline_ != clock::time_point::max()) core->shard.schedule(call, call.deadline_);
    call.phase_ = call_pending;
    conn.commit(length);
    if (hook != nullptr) {
        call.hook_ = hook;
        hook->attach(call, env.stop_token); // Last: the request may already be pending.
    }
    return status_code::ok;
}

status_code plan_request(client_core &core, method_slot &slot, std::uint64_t const timeout_us,
                         wire::metadata_list const metadata, std::size_t const body, bool const bounded,
                         request_plan &plan) noexcept {
    auto const &peer = core.conn->peer();
    plan.slot = &slot;
    plan.intern = slot.wire_id == 0;
    plan.labelled = plan.intern && (core.conn->features() & wire::method_codecs) != 0;
    plan.wire_id = plan.intern ? core.next_method : slot.wire_id;
    if (plan.wire_id > peer.max_method_ids) return status_code::resource_exhausted;
    plan.head.timeout_us = timeout_us;
    if (plan.intern) plan.head.method_name = {reinterpret_cast<std::uint8_t const *>(slot.name.data()), slot.name.size()};
    if (plan.labelled) plan.head.codec = {reinterpret_cast<std::uint8_t const *>(slot.codec.data()), slot.codec.size()};
    plan.head.metadata = metadata;
    auto const head_size = wire::request_head_size(plan.head, plan.intern, plan.labelled);
    if (head_size.code != wire::error::none || head_size.written > 0xffffU) return status_code::invalid_argument;
    plan.head_size = head_size.written;
    if (plan.head_size > peer.max_frame_size) return status_code::resource_exhausted;
    std::size_t room = peer.max_frame_size - plan.head_size;
    if (room > peer.max_message_size) room = peer.max_message_size;
    if (body > room && !bounded) return status_code::resource_exhausted;
    plan.payload = plan.head_size + (body > room ? room : body);
    return status_code::ok;
}

status_code write_request(client_core &core, request_plan const &plan, std::uint8_t *const frame,
                          std::uint32_t const id, request_body const &body, bool const end_stream,
                          std::size_t &length) noexcept {
    auto &conn = *core.conn;
    auto *const out = frame + wire::header_size + plan.head_size;
    auto const room = plan.payload - plan.head_size;
    std::size_t size = body.size;
    if (body.encode != nullptr) {
        size = body.encode(body.source, {out, room});
        if (size == encode_failed || size > room)
            return room < body.size ? status_code::resource_exhausted : status_code::invalid_argument;
    } else if (size != 0) {
        std::memcpy(out, body.source, size);
    }
    wire::frame_header header{};
    header.length = static_cast<std::uint32_t>(plan.head_size + size);
    header.stream_id = id;
    header.type = wire::frame_type::request;
    header.flags = static_cast<std::uint8_t>((end_stream ? wire::end_stream : 0) | (plan.intern ? wire::new_method : 0));
    header.head_length = static_cast<std::uint16_t>(plan.head_size);
    header.aux = plan.wire_id;
    if (wire::encode_header(header, {frame, wire::header_size}, conn.outgoing()).code != wire::error::none ||
        wire::encode_request_head(plan.head, plan.intern, {frame + wire::header_size, plan.head_size}, plan.labelled)
                .code != wire::error::none)
        return status_code::internal;
    ++core.next_id;
    if (plan.intern) {
        plan.slot->wire_id = plan.wire_id;
        ++core.next_method;
    }
    length = wire::header_size + header.length;
    return status_code::ok;
}

bool client_core::on_frame(wire::frame_view const &frame) noexcept {
    auto const &header = frame.header;
    switch (header.type) {
    case wire::frame_type::end:
        if (!(frame.head.size == 2 && frame.head.data[0] == 0 && frame.head.data[1] == 0)) {
            auto const head = wire::decode_end_head(frame.head);
            if (head.code != wire::error::none) return false;
            if (header.aux == 0 && head.value.message.size != 0) return false; // Messages accompany errors only.
        }
        if (auto *stream = streams.find(header.stream_id)) return stream->ops->on_frame(*stream, frame);
        return header.stream_id < next_id; // Late ENDs of finished calls are expected.
    case wire::frame_type::message:
    case wire::frame_type::window_update:
    case wire::frame_type::cancel:
        if (auto *stream = streams.find(header.stream_id)) return stream->ops->on_frame(*stream, frame);
        return header.stream_id < next_id;
    case wire::frame_type::goaway: {
        going_away = true;
        std::vector<stream_state *> unstarted;
        try {
            unstarted = streams.snapshot();
        } catch (std::bad_alloc const &) {
            return false;
        }
        // The server will not run streams above aux.
        for (auto *stream : unstarted)
            if (stream->id > header.aux) stream->ops->on_abort(*stream, status_code::unavailable, true);
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
    while (auto *stream = streams.any()) stream->ops->on_abort(*stream, status_code::unavailable, false);
    end_endpoint();
    closed_event.signal();
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

net::task<status_code> connect_client(std::shared_ptr<client_core> core, net::ip::tcp::endpoint endpoint) {
    return connect_task(std::move(core), endpoint);
}

net::task<status_code> attach_client(std::shared_ptr<client_core> core, std::unique_ptr<transport> link) {
    return attach_task(std::move(core), std::move(link));
}

} // namespace detail

unary_call::unary_call(detail::client_core *core, detail::call_router *router, std::uint32_t method,
                       request_body request, response_body response, call_spec const *spec,
                       response_trailer *trailer) noexcept
    : core_(core), router_(router), request_(request), response_(response), spec_(spec), trailer_(trailer),
      method_(method) {}

unary_call::unary_call(unary_call &&other) noexcept
    : detail::stream_state(), core_(other.core_), router_(other.router_), request_(other.request_),
      response_(other.response_), spec_(other.spec_), trailer_(other.trailer_), method_(other.method_) {
    assert(other.phase_ == detail::call_idle);
}

net::coroutine_handle<> unary_call::await_suspend(net::coroutine_handle<> handle, net::io_env const *env) noexcept {
    continuation_.h = handle;
    env_ = env;
    if (auto *const trailer = trailer_) {
        trailer->message = {};
        trailer->metadata = {};
        trailer->truncated = false;
    }
    auto const code = detail::call_access::launch(*this, nullptr);
    if (code == status_code::ok) return net::noop_coroutine();
    code_ = code;
    not_executed_ = true;
    phase_ = detail::call_done;
    return handle;
}

call_result unary_call::await_resume() noexcept {
    if (hook_ != nullptr) {
        hook_->detach();
        hook_ = nullptr;
    }
    return call_result{code_, size_, not_executed_};
}

client::client(shard &owner, client_options options)
    : core_(std::make_shared<detail::client_core>(owner.state(), options)) {}

client::~client() { close(); }

net::task<status_code> client::connect(net::ip::tcp::endpoint endpoint) {
    return detail::connect_client(core_, endpoint);
}

net::task<status_code> client::attach(std::unique_ptr<transport> link) {
    return detail::attach_client(core_, std::move(link));
}

std::size_t detail::find_or_add(std::vector<method_slot> &slots, std::string const &name, std::string const &codec) {
    for (std::size_t index = 0; index != slots.size(); ++index)
        if (slots[index].name == name && slots[index].codec == codec) return index;
    if (!wire::valid_codec({reinterpret_cast<std::uint8_t const *>(codec.data()), codec.size()}))
        throw std::invalid_argument{"invalid codec label"};
    slots.push_back(method_slot{name, codec, 0});
    return slots.size() - 1;
}

method_ref client::bind(std::string const &name, std::string const &codec) {
    return method_ref{static_cast<std::uint32_t>(detail::find_or_add(core_->methods, name, codec) + 1)};
}

bool client::ready() const noexcept { return core_->ready(); }
std::size_t client::pending() const noexcept { return core_->streams.size(); }
void client::close() noexcept { core_->close(); }

} // namespace rpc
