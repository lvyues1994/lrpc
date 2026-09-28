#include "connection.hpp"
#include "diagnostics.hpp"

#include <net/error.hpp>
#include <net/run_async.hpp>
#include <net/timeout.hpp>

#include <algorithm>
#include <cassert>
#include <cstring>
#include <stdexcept>

namespace rpc {
namespace {

struct tcp_transport final : transport {
    explicit tcp_transport(net::tcp_socket socket) : socket_(std::move(socket)), stream_(&socket_) {}
    net::any_stream &stream() noexcept override { return stream_; }
    void close() noexcept override { socket_.close(); }
private:
    net::tcp_socket socket_;
    net::any_stream stream_;
};

} // namespace

std::unique_ptr<transport> make_tcp_transport(net::tcp_socket socket) {
    return std::make_unique<tcp_transport>(std::move(socket));
}

namespace detail {

bool event::awaiter::await_ready() noexcept { return std::exchange(owner->signaled_, false); }
net::coroutine_handle<> event::awaiter::await_suspend(net::coroutine_handle<> handle,
                                                     net::io_env const *env) noexcept {
    assert(owner->waiter_ == nullptr);
    continuation.h = handle;
    owner->executor_ = env->executor;
    owner->waiter_ = &continuation;
    return net::noop_coroutine();
}
event::awaiter event::wait() noexcept { return {this, {}}; }
void event::signal() noexcept {
    auto *waiter = std::exchange(waiter_, nullptr);
    if (waiter != nullptr) executor_.post(*waiter);
    else signaled_ = true;
}

void validate_options(connection_options const &options) {
    if (options.receive.max_frame_size < 36 || options.receive.max_frame_size > 16U * 1024U * 1024U ||
        options.receive.max_concurrent_streams == 0 || options.receive.max_method_ids == 0 ||
        options.receive.max_method_ids > 65536 || options.receive.compression != 0 ||
        options.max_method_name_bytes == 0 || options.max_method_name_bytes > 65000 ||
        options.handshake_timeout <= std::chrono::milliseconds::zero() ||
        (options.receive_buffer_bytes != 0 &&
         (options.receive_buffer_bytes < static_cast<std::size_t>(options.receive.max_frame_size) + wire::header_size ||
          options.receive_buffer_bytes > 16U * 1024U * 1024U + wire::header_size)))
        throw std::invalid_argument{"invalid unary connection limits"};
}

clock::time_point deadline_after(clock::time_point now, std::uint64_t microseconds) noexcept {
    auto const room = std::chrono::duration_cast<std::chrono::microseconds>(clock::time_point::max() - now).count();
    if (microseconds >= static_cast<std::uint64_t>(room)) return clock::time_point::max();
    return now + std::chrono::microseconds{static_cast<std::chrono::microseconds::rep>(microseconds)};
}
clock::time_point deadline_after(std::uint64_t microseconds) noexcept { return deadline_after(clock::now(), microseconds); }
clock::time_point resolve_deadline(call_options options) noexcept {
    if (options.timeout == std::chrono::microseconds::zero()) return options.deadline;
    if (options.timeout < std::chrono::microseconds::zero()) return clock::now();
    return std::min(options.deadline, deadline_after(static_cast<std::uint64_t>(options.timeout.count())));
}
std::uint64_t remaining_timeout(clock::time_point deadline) noexcept {
    if (deadline == clock::time_point::max()) return 0;
    auto const now = clock::now();
    if (now >= deadline) return 0; // Caller must reject expiration before encoding.
    auto const remaining = deadline - now;
    auto const us = std::chrono::duration_cast<std::chrono::microseconds>(remaining);
    auto const rounded = us.count() + (us < remaining ? 1 : 0);
    return static_cast<std::uint64_t>(rounded);
}
status_code io_status(std::error_code error) noexcept {
    if (!error) return status_code::ok;
    if (error == net::cond::canceled) return status_code::cancelled;
    if (error == net::cond::timeout) return status_code::deadline_exceeded;
    return status_code::unavailable;
}

connection::connection(net::io_context &ctx, connection_options config, block_pool &pool,
                       byte_budget &budget, connection_observer &sink)
    : context(ctx), options(config), controls(pool), control_budget(budget), observer(sink),
      rx(config.receive_buffer_bytes != 0 ? config.receive_buffer_bytes :
         static_cast<std::size_t>(config.receive.max_frame_size) + wire::header_size) {}

void connection::close() noexcept {
    if (state == phase::closed) return;
    state = phase::closed;
    if (link) link->close();
    tx.clear();
    outbound.signal();
    observer.on_close();
}
void connection::enqueue(block_lease frame) noexcept {
    if (state == phase::closed) return;
#ifdef LRPC_ENABLE_DIAGNOSTICS
    frame.get()->queued_at = local_queue_capture().active ? clock::now() : clock::time_point{};
#endif
    tx.push(std::move(frame));
    outbound.signal();
}
bool connection::control(wire::frame_type type, std::uint32_t stream, std::uint32_t aux) noexcept {
    if (state == phase::closed) return false;
    auto storage = controls.acquire(control_budget);
    if (storage.get() == nullptr) { close(); return false; }
    auto &node = *storage.get();
    wire::frame_header h{};
    h.type = type; h.stream_id = stream; h.aux = aux;
    if (type == wire::frame_type::end) { h.length = 2; h.head_length = 2; node.data[16] = 0; node.data[17] = 0; }
    auto const encoded = wire::encode_header(h, {node.data, controls.block_size()},
                                             {peer.max_frame_size, peer.max_message_size});
    if (encoded.code != wire::error::none) { close(); return false; }
    node.size = wire::header_size + h.length;
    enqueue(std::move(storage));
    return true;
}

auto read_frame(connection &value)
    CO2_BEG((net::task<net::io_result<wire::frame_view>>), (value),
            net::io_result<std::size_t> received; wire::decode_result<wire::frame_view> decoded;) {
    for (;;) {
        decoded = wire::decode_frame({value.rx.data() + value.rx_begin, value.rx_end - value.rx_begin},
                                    {value.options.receive.max_frame_size, std::numeric_limits<std::uint32_t>::max()});
        if (decoded.code == wire::error::none) {
            value.rx_begin += decoded.consumed;
            CO2_RETURN((net::io_result<wire::frame_view>{{}, decoded.value}));
        }
        if (decoded.code != wire::error::need_more)
            CO2_RETURN((net::io_result<wire::frame_view>{std::make_error_code(std::errc::protocol_error), {}}));
        if (value.rx_begin != 0) {
            std::memmove(value.rx.data(), value.rx.data() + value.rx_begin, value.rx_end - value.rx_begin);
            value.rx_end -= value.rx_begin;
            value.rx_begin = 0;
        }
        CO2_AWAIT_SET(received, value.link->stream().read_some(
            net::mutable_buffer{value.rx.data() + value.rx_end, value.rx.size() - value.rx_end}));
        if (received.ec || received.value == 0)
            CO2_RETURN((net::io_result<wire::frame_view>{received.ec ? received.ec : net::make_error_code(net::error::eof), {}}));
        value.rx_end += received.value;
    }
}
CO2_END

auto send_settings(connection &value, bool with_preface)
    CO2_BEG((net::task<net::io_result<>>), (value, with_preface),
            block_lease storage; net::io_result<std::size_t> sent; std::size_t prefix = 0;
            wire::encode_result encoded; wire::frame_header header;) {
    storage = value.controls.acquire(value.control_budget);
    if (storage.get() == nullptr) CO2_RETURN((net::io_result<>{std::make_error_code(std::errc::no_buffer_space)}));
    prefix = with_preface ? wire::preface_size : 0;
    if (with_preface) wire::encode_preface({storage.get()->data, prefix});
    encoded = wire::encode_settings(value.options.receive, {storage.get()->data + prefix + 16, 36});
    header.length = static_cast<std::uint32_t>(encoded.written);
    header.head_length = static_cast<std::uint16_t>(encoded.written);
    wire::encode_header(header, {storage.get()->data + prefix, 16});
    CO2_AWAIT_SET(sent, net::write(value.link->stream(),
        net::const_buffer{storage.get()->data, prefix + 16 + encoded.written}));
    CO2_RETURN((net::io_result<>{sent.ec}));
}
CO2_END

auto handshake(connection &value, bool server_side)
    CO2_BEG((net::task<net::io_result<>>), (value, server_side),
            std::array<std::uint8_t, 8> preface{}; net::io_result<std::size_t> received;
            net::io_result<> sent; net::io_result<wire::frame_view> frame;) {
    if (server_side) {
        CO2_AWAIT_SET(received, net::read(value.link->stream(), net::buffer(preface)));
        if (received.ec) CO2_RETURN((net::io_result<>{received.ec}));
        if (wire::decode_preface({preface.data(), preface.size()}).code != wire::error::none)
            CO2_RETURN((net::io_result<>{std::make_error_code(std::errc::protocol_error)}));
    } else {
        CO2_AWAIT_SET(sent, send_settings(value, true));
        if (sent.ec) CO2_RETURN(sent);
    }
    CO2_AWAIT_SET(frame, read_frame(value));
    if (frame.ec) CO2_RETURN((net::io_result<>{frame.ec}));
    if (frame.value.header.type != wire::frame_type::settings)
        CO2_RETURN((net::io_result<>{std::make_error_code(std::errc::protocol_error)}));
    value.peer = wire::decode_settings(frame.value.head).value;
    if (value.peer.max_frame_size < 36)
        CO2_RETURN((net::io_result<>{std::make_error_code(std::errc::protocol_error)}));
    if (server_side) {
        CO2_AWAIT_SET(sent, send_settings(value, false));
        if (sent.ec) CO2_RETURN(sent);
    }
    CO2_RETURN((net::io_result<>{{}}));
}
CO2_END

auto reader_loop(connection &value, std::shared_ptr<connection_observer> owner)
    CO2_BEG(net::task<>, (value, owner), net::io_result<wire::frame_view> frame; unsigned turn = 0;) {
    while (value.state != phase::closed) {
        CO2_AWAIT_SET(frame, read_frame(value));
        if (frame.ec) { value.close(); break; }
        owner->on_frame(frame.value);
        if (++turn == 64) { turn = 0; CO2_AWAIT((yield_awaiter{})); }
    }
    CO2_RETURN();
}
CO2_END

auto writer_loop(connection &value, std::shared_ptr<connection_observer> owner)
    CO2_BEG(net::task<>, (value, owner),
            std::array<block_lease, 16> batch; std::array<net::const_buffer, 16> buffers;
            std::size_t count = 0; std::size_t bytes = 0; block_lease next;
            net::io_result<std::size_t> sent;) {
    while (value.state != phase::closed) {
        if (value.tx.empty()) { owner->on_idle(); if (value.state == phase::closed) break; CO2_AWAIT(value.outbound.wait()); continue; }
        count = 0; bytes = 0;
        while (!value.tx.empty() && count < batch.size() && bytes < 256U * 1024U) {
            next = value.tx.pop();
            // Inspect before publishing method definitions or submitted state.
            // A single frame larger than the batch limit is sent on its own.
            if (count != 0 && next.get()->size > 256U * 1024U - bytes) {
                value.tx.push_front(std::move(next)); break;
            }
            if (next.get()->is_request && !owner->prepare_request(*next.get())) { next.reset(); continue; }
            bytes += next.get()->size;
            buffers[count] = net::const_buffer{next.get()->data, next.get()->size};
            batch[count++] = std::move(next);
        }
        if (count == 0) continue;
        value.writing = true;
#ifdef LRPC_ENABLE_DIAGNOSTICS
        {
            auto &capture = local_queue_capture();
            auto const submitted_at = clock::now();
            if (capture.active) for (std::size_t i = 0; i < count; ++i) {
                auto const &frame = *batch[i].get();
                if (frame.queued_at == clock::time_point{}) continue;
                auto &metric = frame.is_request ? capture.metrics.requests :
                    frame.data[8] == static_cast<std::uint8_t>(wire::frame_type::end) ?
                    capture.metrics.responses : capture.metrics.controls;
                record_queue_wait(metric, static_cast<std::uint64_t>(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(submitted_at - frame.queued_at).count()));
            }
        }
#endif
        CO2_AWAIT_SET(sent, net::write(value.link->stream(), net::const_buffer_span{buffers.data(), count}));
        // net::write finishes all partial writes, or closes the connection on
        // failure below. Never skip a cancelled request's in-flight frame tail.
        for (std::size_t i = 0; i < count; ++i) batch[i].reset();
        value.writing = false;
        if (sent.ec) { value.close(); break; }
        owner->on_idle();
    }
    CO2_RETURN();
}
CO2_END

void start_io(connection &value, std::shared_ptr<connection_observer> owner) {
    auto done = [owner, &value] { --value.io_chains; };
    auto failed = [owner, &value](std::exception_ptr) { --value.io_chains; value.close(); };
    ++value.io_chains;
    try { net::run_async(value.context.get_executor(), done, failed)([owner, &value] { return reader_loop(value, owner); }); }
    catch (...) { --value.io_chains; value.close(); throw; }
    ++value.io_chains;
    try { net::run_async(value.context.get_executor(), done, failed)([owner, &value] { return writer_loop(value, owner); }); }
    catch (...) { --value.io_chains; value.close(); throw; }
}

bool encode_end(block &storage, std::uint32_t id, status_code code, std::size_t body_size,
                wire::settings const &peer, std::size_t head_size, std::size_t body_offset) noexcept {
    if (code != status_code::ok) body_size = 0;
    if (head_size < 2 || head_size > 65535 || body_size > std::numeric_limits<std::uint32_t>::max() - head_size) return false;
    wire::frame_header h{static_cast<std::uint32_t>(body_size + head_size), id, wire::frame_type::end,
                         0, static_cast<std::uint16_t>(head_size), static_cast<std::uint32_t>(code)};
    if (head_size == 2) { storage.data[16] = 0; storage.data[17] = 0; }
    if (body_size != 0 && body_offset != 16 + head_size)
        std::memmove(storage.data + 16 + head_size, storage.data + body_offset, body_size);
    auto const result = wire::encode_header(h, {storage.data, 16}, {peer.max_frame_size, peer.max_message_size});
    storage.size = 16 + head_size + body_size;
    return result.code == wire::error::none;
}

} // namespace detail
} // namespace rpc
