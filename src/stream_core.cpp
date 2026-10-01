#include "stream_core.hpp"

#include <rpc/compression.hpp>

#include <algorithm>
#include <cstring>
#include <limits>
#include <new>

namespace rpc {
namespace detail {

namespace {
std::uint32_t message_cost(std::size_t const size) noexcept {
    return std::max<std::uint32_t>(1, static_cast<std::uint32_t>(size));
}
} // namespace

stream_core::~stream_core() {
    assert(!waiting);
    release_current();
    drop_messages();
    release_packed();
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
        if (descriptor.algorithm != 0 &&
            (descriptor.algorithm > 2 ||
             (conn->options.receive.compression & conn->peer().compression & (1U << (descriptor.algorithm - 1U))) == 0))
            return false;
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
        assembly_decoded = descriptor.decoded_size;
        assembly_algorithm = descriptor.algorithm;
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
    if (assembly_algorithm != 0 && !decompress()) return false;
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

// Replaces the assembled encoding with exactly the declared decoded bytes;
// the declared size was bounded before any memory was taken.
bool stream_core::decompress() noexcept {
    auto *const codec = shard.compressor(assembly_algorithm);
    if (codec == nullptr) return false;
    std::uint8_t *decoded = nullptr;
    try {
        if (assembly_decoded != 0) decoded = shard.memory.allocate(assembly_decoded);
    } catch (std::bad_alloc const &) {
        return false;
    }
    auto const result =
        codec->decompress_exact({assembly.block, assembly.size}, {decoded, assembly_decoded});
    shard.memory.deallocate(assembly.block, assembly.capacity);
    assembly.block = decoded;
    assembly.capacity = decoded != nullptr ? slab::block_size(assembly_decoded) : 0;
    assembly.size = assembly_decoded;
    return result.code == wire::error::none && result.written == assembly_decoded;
}

bool stream_core::on_window(wire::frame_view const &frame) noexcept {
    if (!flow_controlled() || frame.header.aux > peer_window - send_credit) return false;
    send_credit += frame.header.aux;
    // On failed the stream may be gone; the reader stops on the closed connection.
    if (outgoing == out_credit && send_credit >= writer_need && start_message() == write_step::done) writer.wake();
    return true;
}

stream_core::write_step stream_core::begin_write(wire::bytes_view const message) noexcept {
    out = message;
    if (flow_controlled() && message_cost(message.size) > send_credit) {
        writer_need = message_cost(message.size);
        outgoing = out_credit;
        return write_step::waiting;
    }
    return start_message();
}

// The whole credit is in hand: compress, take it, and frame what the turn allows.
stream_core::write_step stream_core::start_message() noexcept {
    auto const size = out.size;
    writer_need = 0;
    pack();
    if (wire::encode_message_descriptor(
            {static_cast<std::uint32_t>(out.size), static_cast<std::uint32_t>(size), out_algorithm},
            {out_descriptor.data(), out_descriptor.size()}, conn->outgoing())
            .code != wire::error::none) {
        finish_write(status_code::resource_exhausted);
        return write_step::done;
    }
    if (flow_controlled()) send_credit -= message_cost(size);
    out_offset = 0;
    out_first = true;
    outgoing = out_framing;
    return frame_message();
}

stream_core::write_step stream_core::frame_message() noexcept {
    while (conn->stream_room()) {
        auto const step = emit_fragment();
        if (step != write_step::waiting) return step;
    }
    conn->queue_stream(*this);
    return write_step::waiting;
}

stream_core::write_step stream_core::emit_fragment() noexcept {
    auto &link = *conn;
    std::size_t const head = out_first ? out_descriptor.size() : 0;
    auto const count = std::min({out.size - out_offset, std::size_t{link.peer().max_frame_size} - head,
                                 link.options.stream_fragment_bytes});
    bool const last = out_offset + count == out.size;
    std::uint8_t *frame = nullptr;
    try {
        frame = link.reserve(wire::header_size + head + count);
    } catch (std::bad_alloc const &) {
        link.close(); // A message cut short cannot be resumed on the wire.
        return write_step::failed;
    }
    wire::frame_header header{};
    header.length = static_cast<std::uint32_t>(head + count);
    header.stream_id = id;
    header.type = wire::frame_type::message;
    header.flags = static_cast<std::uint8_t>((last ? 0 : wire::more) |
                                             (out_first && out_algorithm != 0 ? wire::compressed : 0));
    header.head_length = static_cast<std::uint16_t>(head);
    if (wire::encode_header(header, {frame, wire::header_size}, link.outgoing()).code != wire::error::none) {
        link.close();
        return write_step::failed;
    }
    if (head != 0) std::memcpy(frame + wire::header_size, out_descriptor.data(), head);
    if (count != 0) std::memcpy(frame + wire::header_size + head, out.data + out_offset, count);
    link.commit(wire::header_size + head + count);
    out_offset += count;
    out_first = false;
    if (!last) return write_step::waiting;
    ++sent;
    finish_write(status_code::ok);
    return write_step::done;
}

bool stream_core::next_fragment() noexcept {
    auto const step = emit_fragment();
    if (step == write_step::done) writer.wake();
    return step == write_step::waiting;
}

void stream_core::abandon_write() noexcept {
    if (outgoing == out_idle) return;
    assert(!waiting || conn != nullptr);
    if (waiting) conn->unqueue_stream(*this);
    finish_write(result == status_code::ok ? status_code::failed_precondition : result);
}

// Replaces the bytes to send with a compressed copy when compression is
// negotiated and pays off.
void stream_core::pack() noexcept {
    out_algorithm = 0;
    auto const &link = *conn;
    auto const algorithm = link.options.preferred_compression;
    if (algorithm == 0 || out.size < link.options.compression_threshold ||
        (link.features() & wire::message_compression) == 0 || (link.peer().compression & (1U << (algorithm - 1U))) == 0)
        return;
    auto *const codec = shard.compressor(algorithm);
    auto const bound = codec != nullptr ? codec->bound(out.size) : 0;
    if (bound == 0) return;
    try {
        packed = shard.memory.allocate(bound);
        packed_capacity = slab::block_size(bound);
    } catch (std::bad_alloc const &) {
        return; // Send it uncompressed.
    }
    auto const result = codec->compress(out, {packed, bound});
    if (result.code != wire::error::none || result.written >= out.size) {
        release_packed();
        return;
    }
    out = {packed, result.written};
    out_algorithm = algorithm;
}

void stream_core::finish_write(status_code const code) noexcept {
    release_packed();
    out = {};
    outgoing = out_idle;
    writer_need = 0;
    out_result = code;
}

void stream_core::release_packed() noexcept {
    shard.memory.deallocate(packed, packed_capacity);
    packed = nullptr;
    packed_capacity = 0;
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
    if (s.writing() || s.local_half || (!half_ && s.sends_one() && s.sent != 0))
        return (result_ = status_code::failed_precondition), true;
    if (half_) return (result_ = s.send_half()), true;
    auto const cost = std::max<std::uint32_t>(1, static_cast<std::uint32_t>(message_.size));
    if (message_.size > s.max_outbound || (s.flow_controlled() && cost > s.peer_window))
        return (result_ = status_code::resource_exhausted), true;
    switch (s.begin_write(message_)) {
    case detail::stream_core::write_step::done: return (result_ = s.out_result), true;
    case detail::stream_core::write_step::failed: return (result_ = status_code::unavailable), true;
    case detail::stream_core::write_step::waiting: break;
    }
    return done_ = false;
}

net::coroutine_handle<> write_operation::await_suspend(net::coroutine_handle<> handle, net::io_env const *env) noexcept {
    core_->writer.arm(handle, env);
    return net::noop_coroutine();
}

status_code write_operation::await_resume() noexcept { return done_ ? result_ : core_->out_result; }

} // namespace rpc
