#include "client_core.hpp"
#include "stream_core.hpp"

#include <cstring>
#include <new>
#include <utility>

namespace rpc {
namespace detail {

// Referenced by its handle and, until it ends, by its connection.
struct client_stream_core final : stream_core {
    client_stream_core(client_core &core, method_kind stream_kind) noexcept
        : stream_core(core.shard, stream_kind, true), owner(&core) {}
    ~client_stream_core() override {
        if (hook != nullptr) hook->detach();
        shard.memory.deallocate(trailer, trailer_capacity);
    }

    void fail(status_code const code) noexcept override {
        cancel_remote();
        end(code, false);
    }
    void cancel_remote() noexcept {
        if (conn != nullptr) conn->send_control(wire::frame_type::cancel, id, 0, 0);
    }
    void end(status_code code, bool not_executed) noexcept;
    bool on_end(wire::frame_view const &frame) noexcept;
    void release() noexcept {
        if (--refs == 0) destroy_in(shard.memory, this);
    }

    client_core *owner;
    stop_hook *hook = nullptr;
    std::uint8_t *trailer = nullptr; // Copy of a non-empty END head.
    std::size_t trailer_size = 0;
    std::size_t trailer_capacity = 0;
    std::uint8_t refs = 0;
    bool unexecuted = false;
};

void client_stream_core::end(status_code const code, bool const not_executed) noexcept {
    if (ended) return;
    ended = true;
    result = code;
    unexecuted = not_executed;
    abandon_write();
    if (owner != nullptr) {
        owner->streams.erase(id);
        owner->settle();
        owner = nullptr;
    }
    shard.unschedule(*this);
    conn = nullptr;
    if (code != status_code::ok) drop_messages(); // After END(ok) the inbox stays readable.
    wake_all();
    release(); // The connection's reference; the handle still holds one.
}

bool client_stream_core::on_end(wire::frame_view const &frame) noexcept {
    auto const code = static_cast<status_code>(frame.header.aux);
    if (frame.body.size != 0) return false;
    if (code == status_code::ok && (assembling || (receives_one() && received != 1))) return false;
    if (frame.head.size > 2) {
        try {
            trailer = shard.memory.allocate(frame.head.size);
            trailer_capacity = slab::block_size(frame.head.size);
            trailer_size = frame.head.size;
            std::memcpy(trailer, frame.head.data, frame.head.size);
        } catch (std::bad_alloc const &) {
        }
    }
    remote_half = true;
    end(code, (frame.header.flags & wire::not_executed) != 0);
    return true;
}

namespace {

client_stream_core &of(stream_state &node) noexcept { return static_cast<client_stream_core &>(node); }

bool stream_frame(stream_state &node, wire::frame_view const &frame) noexcept {
    auto &stream = of(node);
    switch (frame.header.type) {
    case wire::frame_type::message: return stream.on_message(frame);
    case wire::frame_type::window_update: return stream.on_window(frame);
    case wire::frame_type::end: return stream.on_end(frame);
    case wire::frame_type::cancel: stream.end(status_code::cancelled, false); return true;
    default: return false;
    }
}

void stream_abort(stream_state &node, status_code const code, bool const not_executed) noexcept {
    of(node).end(code, not_executed);
}

void stream_deadline(stream_state &node) noexcept {
    auto &stream = of(node);
    stream.cancel_remote();
    stream.end(status_code::deadline_exceeded, false);
}

void stream_cancel(stream_state &node) noexcept {
    auto &stream = of(node);
    if (stream.ended) return;
    stream.cancel_remote();
    stream.end(status_code::cancelled, false);
}

stream_ops const client_stream_ops{&stream_frame, &stream_abort, &stream_deadline, &stream_cancel};

open_result open_stream(client_core *core, call_router *router, std::uint32_t const method, method_kind const kind,
                        call_spec const *spec, net::io_env const &env) noexcept {
    if (router != nullptr) core = router->pick(nullptr);
    if (core == nullptr || !core->ready()) return {status_code::unavailable, {}};
    if (method == 0 || method > core->methods.size() || kind > method_kind::bidirectional)
        return {status_code::invalid_argument, {}};
    if (env.stop_token.stop_requested()) return {status_code::cancelled, {}};
    auto &conn = *core->conn;
    if ((conn.features() & wire::streaming) == 0 || conn.peer().initial_stream_window == 0)
        return {status_code::unimplemented, {}};
    if (!core->has_room() || conn.queued_bytes() > conn.options.tx_high_watermark)
        return {status_code::resource_exhausted, {}};
    if (core->next_id > wire::max_stream_id) {
        core->going_away = true;
        core->settle();
        return {status_code::unavailable, {}};
    }
    auto deadline = clock::time_point::max();
    std::uint64_t timeout_us = 0;
    if (spec != nullptr && (spec->deadline != clock::time_point::max() || spec->timeout.count() > 0)) {
        auto const current = now();
        deadline = spec->deadline;
        if (spec->timeout.count() > 0 && spec->timeout < deadline - current) deadline = current + spec->timeout;
        if (deadline <= current) return {status_code::deadline_exceeded, {}};
        timeout_us = remaining_us(deadline - current);
    }
    request_plan plan{};
    auto const planned = plan_request(*core, core->methods[method - 1], timeout_us,
                                      spec != nullptr ? spec->metadata : wire::metadata_list{}, 0, false, plan);
    if (planned != status_code::ok) return {planned, {}};

    client_stream_core *stream = nullptr;
    stop_hook *hook = nullptr;
    std::uint8_t *frame = nullptr;
    auto &memory = core->shard.memory;
    try {
        stream = make_in<client_stream_core>(memory, *core, kind);
        if (env.stop_token.stop_possible()) hook = core->shard.acquire_hook();
        frame = conn.reserve(wire::header_size + plan.payload);
    } catch (std::bad_alloc const &) {
        if (hook != nullptr) core->shard.release_hook(*hook);
        destroy_in(memory, stream);
        return {status_code::resource_exhausted, {}};
    }
    auto const id = core->next_id;
    std::size_t length = 0;
    auto const written = write_request(*core, plan, frame, id, {}, false, length);
    if (written != status_code::ok) {
        if (hook != nullptr) core->shard.release_hook(*hook);
        destroy_in(memory, stream);
        return {written, {}};
    }
    stream->id = id;
    stream->ops = &client_stream_ops;
    stream->open(conn, conn.peer().max_message_size);
    stream->refs = 2;
    core->streams.insert(*stream);
    if (deadline != clock::time_point::max()) core->shard.schedule(*stream, deadline);
    conn.commit(length);
    if (hook != nullptr) {
        stream->hook = hook;
        hook->attach(*stream, env.stop_token);
    }
    return {status_code::ok, stream_access::make_client_stream(stream)};
}

} // namespace
} // namespace detail

net::coroutine_handle<> open_operation::await_suspend(net::coroutine_handle<> handle, net::io_env const *env) noexcept {
    result_ = detail::open_stream(core_, router_, method_, kind_, spec_, *env);
    return handle;
}

bool finish_operation::await_ready() noexcept { return core_ == nullptr || core_->ended; }

net::coroutine_handle<> finish_operation::await_suspend(net::coroutine_handle<> handle,
                                                        net::io_env const *env) noexcept {
    if (core_->finisher.armed) return handle;
    core_->finisher.arm(handle, env);
    return net::noop_coroutine();
}

call_result finish_operation::await_resume() noexcept {
    if (trailer_ != nullptr) {
        trailer_->message = {};
        trailer_->metadata = {};
        trailer_->truncated = false;
    }
    if (core_ == nullptr) return {status_code::failed_precondition, 0, true};
    if (!core_->ended) return {status_code::failed_precondition, 0, false}; // A second concurrent finish.
    if (trailer_ != nullptr && core_->trailer != nullptr)
        detail::keep_trailer(*trailer_, {core_->trailer, core_->trailer_size});
    return {core_->result, 0, core_->unexecuted};
}

write_operation client_stream::write(wire::bytes_view const message) noexcept {
    return detail::stream_access::write(core_, message, false);
}
write_operation client_stream::writes_done() noexcept { return detail::stream_access::write(core_, {}, true); }
read_operation client_stream::read() noexcept { return detail::stream_access::read(core_); }
finish_operation client_stream::finish(response_trailer *trailer) noexcept {
    return detail::stream_access::finish(core_, trailer);
}

void client_stream::cancel() noexcept {
    if (core_ == nullptr || core_->ended) return;
    core_->cancel_remote();
    core_->end(status_code::cancelled, false);
}

void client_stream::reset() noexcept {
    if (core_ == nullptr) return;
    cancel();
    std::exchange(core_, nullptr)->release();
}

open_operation client::open(method_ref const method, method_kind const kind, call_spec const *spec) noexcept {
    return open_operation{core_.get(), nullptr, method.index, kind, spec};
}

} // namespace rpc
