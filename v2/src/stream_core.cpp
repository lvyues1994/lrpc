#include "stream_core.hpp"

#include <algorithm>
#include <cstring>
#include <limits>
#include <new>

namespace rpc {
namespace v2 {
namespace detail {

namespace {
std::uint32_t message_cost(std::size_t const size) noexcept {
    return std::max<std::uint32_t>(1, static_cast<std::uint32_t>(size));
}
} // namespace

stream_core::~stream_core() {
    release_current();
    drop_messages();
}

void stream_core::open(connection &link, std::size_t const outbound_limit) noexcept {
    conn = &link;
    peer_window = send_credit = link.peer().initial_stream_window;
    receive_credit = link.options.receive.initial_stream_window;
    max_inbound = link.options.receive.max_message_size;
    max_outbound = std::min<std::size_t>(link.peer().max_message_size, outbound_limit);
    max_buffered = std::max<std::size_t>(link.options.max_buffered_messages, 1);
}

bool stream_core::push(inbound_message const &message) noexcept {
    if (inbox_count == inbox.size()) {
        if (inbox_count >= max_buffered) return false;
        try {
            std::vector<inbound_message> grown(std::min(std::max<std::size_t>(inbox.size() * 2, 8), max_buffered));
            for (std::size_t i = 0; i != inbox_count; ++i) grown[i] = inbox[(inbox_head + i) % inbox.size()];
            inbox.swap(grown);
            inbox_head = 0;
        } catch (std::bad_alloc const &) {
            return false;
        }
    }
    inbox[(inbox_head + inbox_count) % inbox.size()] = message;
    ++inbox_count;
    return true;
}

bool stream_core::on_message(wire::frame_view const &frame) noexcept {
    auto const flags = frame.header.flags;
    if (remote_half) return false;
    if (ended) return true; // Failed here already; the peer has not heard yet.
    if (flags == wire::end_stream) {
        if (assembling) return false;
        remote_half = true;
        reader.wake();
        return true;
    }
    if (frame.head.size != 0) {
        if (assembling) return false;
        auto const descriptor = wire::decode_message_descriptor(
            frame.head, {std::numeric_limits<std::uint32_t>::max(), std::numeric_limits<std::uint32_t>::max(),
                         conn->features()}).value; // Validated with the frame.
        if (descriptor.encoded_size > max_inbound || descriptor.decoded_size > max_inbound) return false;
        auto const cost = message_cost(std::max(descriptor.encoded_size, descriptor.decoded_size));
        if (flow_controlled() && cost > receive_credit) return false;
        if (receives_one() && received != 0) {
            fail(status_code::resource_exhausted);
            return true;
        }
        std::uint8_t *block = nullptr;
        try {
            if (descriptor.encoded_size != 0) block = shard.memory.allocate(descriptor.encoded_size);
        } catch (std::bad_alloc const &) {
            fail(status_code::resource_exhausted);
            return true;
        }
        if (flow_controlled()) receive_credit -= cost;
        assembly = {block, block != nullptr ? slab::block_size(descriptor.encoded_size) : 0, 0, cost, true};
        assembly_total = descriptor.encoded_size;
        assembling = true;
    } else if (!assembling) {
        return false;
    }
    if (frame.body.size > assembly_total - assembly.size) return false;
    if (frame.body.size != 0) std::memcpy(assembly.block + assembly.size, frame.body.data, frame.body.size);
    assembly.size += frame.body.size;
    if ((flags & wire::more) != 0) return assembly.size < assembly_total;
    if (assembly.size != assembly_total) return false;
    assembling = false;
    if (!push(assembly)) {
        shard.memory.deallocate(assembly.block, assembly.capacity);
        assembly = {};
        fail(status_code::resource_exhausted);
        return true;
    }
    assembly = {};
    ++received;
    reader.wake();
    return true;
}

bool stream_core::on_window(wire::frame_view const &frame) noexcept {
    if (!flow_controlled() || frame.header.aux > peer_window - send_credit) return false;
    send_credit += frame.header.aux;
    if (send_credit >= writer_need) writer.wake();
    return true;
}

status_code stream_core::send(wire::bytes_view const message) noexcept {
    auto &link = *conn;
    std::array<std::uint8_t, wire::message_descriptor_size> descriptor{};
    auto const size = static_cast<std::uint32_t>(message.size);
    if (wire::encode_message_descriptor({size, size, 0}, {descriptor.data(), descriptor.size()}, link.outgoing()).code !=
        wire::error::none)
        return status_code::resource_exhausted;
    if (flow_controlled()) send_credit -= message_cost(size);
    std::size_t const max_frame = link.peer().max_frame_size;
    std::size_t offset = 0;
    bool first = true;
    try {
        do {
            std::size_t const head = first ? descriptor.size() : 0;
            auto const count = std::min(message.size - offset, max_frame - head);
            auto *const frame = link.reserve(wire::header_size + head + count);
            wire::frame_header header{};
            header.length = static_cast<std::uint32_t>(head + count);
            header.stream_id = id;
            header.type = wire::frame_type::message;
            header.flags = offset + count < message.size ? wire::more : std::uint8_t{0};
            header.head_length = static_cast<std::uint16_t>(head);
            if (wire::encode_header(header, {frame, wire::header_size}, link.outgoing()).code != wire::error::none) {
                link.close();
                return status_code::internal;
            }
            if (head != 0) std::memcpy(frame + wire::header_size, descriptor.data(), head);
            if (count != 0) std::memcpy(frame + wire::header_size + head, message.data + offset, count);
            link.commit(wire::header_size + head + count);
            offset += count;
            first = false;
        } while (offset < message.size);
    } catch (std::bad_alloc const &) {
        link.close(); // A message cut short cannot be resumed on the wire.
        return status_code::unavailable;
    }
    ++sent;
    return status_code::ok;
}

status_code stream_core::send_half() noexcept {
    std::uint8_t *frame = nullptr;
    try {
        frame = conn->reserve(wire::header_size);
    } catch (std::bad_alloc const &) {
        conn->close();
        return status_code::unavailable;
    }
    wire::frame_header header{};
    header.stream_id = id;
    header.type = wire::frame_type::message;
    header.flags = wire::end_stream;
    if (wire::encode_header(header, {frame, wire::header_size}, conn->outgoing()).code != wire::error::none) {
        conn->close();
        return status_code::internal;
    }
    conn->commit(wire::header_size);
    local_half = true;
    return status_code::ok;
}

bool stream_core::take(stream_read &out) noexcept {
    release_current();
    if (inbox_count != 0) {
        current = inbox[inbox_head];
        inbox[inbox_head] = {};
        inbox_head = (inbox_head + 1) % inbox.size();
        --inbox_count;
        out = {status_code::ok, false, {current.block, current.size}};
        return true;
    }
    if (ended) {
        out = {result, true, {}};
        return true;
    }
    if (remote_half) {
        out = {status_code::ok, true, {}};
        return true;
    }
    return false;
}

void stream_core::release_current() noexcept {
    if (!current.held) return;
    auto const cost = current.cost;
    shard.memory.deallocate(current.block, current.capacity);
    current = {};
    if (!flow_controlled() || ended || conn == nullptr) return;
    receive_credit += cost;
    conn->send_control(wire::frame_type::window_update, id, cost, 0);
}

void stream_core::drop_messages() noexcept {
    for (; inbox_count != 0; --inbox_count) {
        auto &message = inbox[inbox_head];
        shard.memory.deallocate(message.block, message.capacity);
        message = {};
        inbox_head = (inbox_head + 1) % inbox.size();
    }
    shard.memory.deallocate(assembly.block, assembly.capacity);
    assembly = {};
    assembling = false;
}

} // namespace detail

// ---- operations ----

bool read_operation::await_ready() noexcept {
    if (core_ == nullptr) {
        result_ = {status_code::failed_precondition, true, {}};
        return done_ = true;
    }
    return done_ = core_->take(result_);
}

net::coroutine_handle<> read_operation::await_suspend(net::coroutine_handle<> handle, net::io_env const *env) noexcept {
    if (core_->reader.armed) {
        result_ = {status_code::failed_precondition, false, {}};
        done_ = true;
        return handle;
    }
    core_->reader.arm(handle, env);
    return net::noop_coroutine();
}

stream_read read_operation::await_resume() noexcept {
    if (!done_ && !core_->take(result_)) result_ = {status_code::internal, true, {}};
    return result_;
}

bool write_operation::await_ready() noexcept {
    done_ = true;
    if (core_ == nullptr) return (result_ = status_code::failed_precondition), true;
    auto &s = *core_;
    if (s.ended) return (result_ = s.result == status_code::ok ? status_code::failed_precondition : s.result), true;
    if (s.local_half || (!half_ && s.sends_one() && s.sent != 0)) return (result_ = status_code::failed_precondition), true;
    if (half_) return (result_ = s.send_half()), true;
    auto const cost = std::max<std::uint32_t>(1, static_cast<std::uint32_t>(message_.size));
    if (message_.size > s.max_outbound || (s.flow_controlled() && cost > s.peer_window))
        return (result_ = status_code::resource_exhausted), true;
    if (!s.flow_controlled() || cost <= s.send_credit) return (result_ = s.send(message_)), true;
    s.writer_need = cost;
    return done_ = false;
}

net::coroutine_handle<> write_operation::await_suspend(net::coroutine_handle<> handle, net::io_env const *env) noexcept {
    if (core_->writer.armed) {
        result_ = status_code::failed_precondition;
        done_ = true;
        return handle;
    }
    core_->writer.arm(handle, env);
    return net::noop_coroutine();
}

status_code write_operation::await_resume() noexcept {
    if (done_) return result_;
    auto &s = *core_;
    s.writer_need = 0;
    if (s.ended) return s.result == status_code::ok ? status_code::failed_precondition : s.result;
    return s.send(message_); // Woken with enough credit.
}

} // namespace v2
} // namespace rpc
