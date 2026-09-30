#include "engine.hpp"

#include <net/buffers.hpp>
#include <net/run_async.hpp>
#include <net/task.hpp>

#include <algorithm>
#include <array>
#include <cstring>
#include <limits>
#include <new>
#include <stdexcept>

namespace rpc {
namespace v2 {
namespace detail {
namespace {

constexpr std::size_t tx_chunk_bytes = slab::max_block;
constexpr std::size_t settings_bytes = 64; // Seven varint pairs, generously.

} // namespace

struct connection_io {
    static bool closed(connection const &c) noexcept { return c.state_ == phase::closed; }
    static parse_step parse(connection &c) noexcept { return c.parse(); }
    static bool pause(connection &c) noexcept {
        if (!c.server_side_ || c.tx_bytes_ <= c.options.tx_high_watermark) return false;
        c.reader_paused_ = true;
        return true;
    }
    static event &reader_wake(connection &c) noexcept { return c.reader_wake_; }
    static event &writer_wake(connection &c) noexcept { return c.writer_wake_; }
    static net::any_stream &stream(connection &c) noexcept { return c.link_->stream(); }
    static net::mutable_buffer receive_space(connection &c) noexcept {
        return net::mutable_buffer{c.rx_ + c.rx_end_, c.rx_capacity_ - c.rx_end_};
    }
    static void received(connection &c, std::size_t const bytes) noexcept { c.rx_end_ += bytes; }
    static bool flushed_close(connection const &c) noexcept { return c.close_when_flushed_; }
    static std::size_t gather(connection const &c, net::const_buffer *buffers, std::size_t limit) noexcept {
        return c.gather(buffers, limit);
    }
    static void consume(connection &c, std::size_t const bytes) noexcept { c.consume(bytes); }
    static void exited(connection &c) noexcept { c.io_exited(); }
};

namespace {

auto reader_loop(std::shared_ptr<void> keep, connection *c)
    CO2_BEG(net::task<>, (keep, c), net::io_result<std::size_t> received; std::uint32_t turn = 0;
            parse_step outcome = parse_step::parsed;) {
    for (;;) {
        if (connection_io::closed(*c)) break;
        if (connection_io::pause(*c)) {
            CO2_AWAIT(connection_io::reader_wake(*c).wait());
            continue;
        }
        outcome = connection_io::parse(*c);
        if (outcome == parse_step::parsed) {
            if (++turn >= c->options.frames_per_turn) {
                turn = 0;
                CO2_AWAIT(yield_awaiter{});
            }
            continue;
        }
        if (outcome == parse_step::failed) {
            c->close();
            break;
        }
        turn = 0;
        CO2_AWAIT_SET(received, connection_io::stream(*c).read_some(connection_io::receive_space(*c)));
        if (received.ec || received.value == 0) {
            c->close();
            break;
        }
        connection_io::received(*c, received.value);
    }
    connection_io::exited(*c);
    CO2_RETURN();
}
CO2_END

auto writer_loop(std::shared_ptr<void> keep, connection *c)
    CO2_BEG(net::task<>, (keep, c), net::io_result<std::size_t> sent;
            std::array<net::const_buffer, net::max_iovec> buffers{}; std::size_t count = 0;) {
    for (;;) {
        if (connection_io::closed(*c)) break;
        if (c->queued_bytes() == 0) {
            if (connection_io::flushed_close(*c)) {
                c->close();
                break;
            }
            CO2_AWAIT(connection_io::writer_wake(*c).wait());
            continue;
        }
        count = connection_io::gather(*c, buffers.data(), buffers.size());
        CO2_AWAIT_SET(sent, connection_io::stream(*c).write_some(
                                net::span<net::const_buffer const>{buffers.data(), count}));
        if (sent.ec || sent.value == 0) {
            c->close();
            break;
        }
        connection_io::consume(*c, sent.value);
    }
    connection_io::exited(*c);
    CO2_RETURN();
}
CO2_END

} // namespace

connection::connection(shard_state &owner, connection_options const &config, std::unique_ptr<transport> link,
                       bool const server_side, connection_handler &handler)
    : shard(owner), options(config), link_(std::move(link)), handler_(handler), server_side_(server_side) {
    static stream_ops const handshake_ops{nullptr, nullptr, &connection::on_handshake_deadline, nullptr};
    handshake_timer_.ops = &handshake_ops;
    handshake_timer_.owner = this;
}

connection::~connection() {
    shard.unschedule(handshake_timer_);
    release_rx();
    release_tx();
}

void connection::start(std::shared_ptr<void> keep) {
    assert(state_ == phase::idle);
    if (options.receive_buffer_bytes < wire::header_size + wire::preface_size)
        throw std::invalid_argument{"receive buffer too small"};
    chunks_.resize(4);
    if (!resize_receive(options.receive_buffer_bytes)) throw std::bad_alloc{};
    std::size_t const prefix = server_side_ ? 0 : wire::preface_size;
    auto *const opening = reserve(prefix + wire::header_size + settings_bytes);
    if (!server_side_) wire::encode_preface({opening, prefix});
    auto const encoded = wire::encode_settings(options.receive, {opening + prefix + wire::header_size, settings_bytes});
    if (encoded.code != wire::error::none) throw std::invalid_argument{"invalid SETTINGS"};
    wire::frame_header header{};
    header.type = wire::frame_type::settings;
    header.length = static_cast<std::uint32_t>(encoded.written);
    header.head_length = static_cast<std::uint16_t>(encoded.written);
    if (wire::encode_header(header, {opening + prefix, wire::header_size}).code != wire::error::none)
        throw std::invalid_argument{"invalid SETTINGS"};
    commit(prefix + wire::header_size + encoded.written);
    state_ = phase::handshaking;
    if (options.handshake_timeout.count() > 0) shard.schedule(handshake_timer_, now() + options.handshake_timeout);
    running_ = 1;
    try {
        net::run_async(shard.executor)(reader_loop(keep, this));
    } catch (...) {
        running_ = 0;
        close();
        throw;
    }
    ++running_;
    try {
        net::run_async(shard.executor)(writer_loop(std::move(keep), this));
    } catch (...) {
        --running_;
        close();
        throw;
    }
}

// ---- receive ----

parse_step connection::parse() noexcept {
    if (state_ == phase::handshaking) return handshake();
    auto const available = rx_end_ - rx_begin_;
    auto const decoded = wire::decode_frame(
        {rx_ + rx_begin_, available},
        {options.receive.max_frame_size, std::numeric_limits<std::uint32_t>::max(), features_});
    if (decoded.code == wire::error::none) {
        rx_begin_ += decoded.consumed;
        return dispatch(decoded.value) ? step::parsed : step::failed;
    }
    if (decoded.code == wire::error::need_more) return prepare_receive(available);
    return step::failed;
}

parse_step connection::handshake() noexcept {
    auto const available = rx_end_ - rx_begin_;
    if (server_side_ && !preface_seen_) {
        if (available < wire::preface_size) return prepare_receive(available);
        if (wire::decode_preface({rx_ + rx_begin_, wire::preface_size}).code != wire::error::none) return step::failed;
        rx_begin_ += wire::preface_size;
        preface_seen_ = true;
        return step::parsed;
    }
    auto const decoded = wire::decode_frame(
        {rx_ + rx_begin_, available}, {options.receive.max_frame_size, std::numeric_limits<std::uint32_t>::max(), 0});
    if (decoded.code == wire::error::need_more) return prepare_receive(available);
    if (decoded.code != wire::error::none || decoded.value.header.type != wire::frame_type::settings)
        return step::failed;
    rx_begin_ += decoded.consumed;
    auto const settings = wire::decode_settings(decoded.value.head);
    // The smallest useful frame is an error END plus a little head.
    if (settings.code != wire::error::none || settings.value.max_frame_size < 36) return step::failed;
    peer_ = settings.value;
    features_ = options.receive.features & peer_.features;
    state_ = phase::ready;
    shard.unschedule(handshake_timer_);
    handler_.on_ready();
    return step::parsed;
}

parse_step connection::prepare_receive(std::size_t const available) noexcept {
    if (available == 0) rx_begin_ = rx_end_ = 0;
    std::size_t need = wire::header_size;
    if (available >= wire::header_size) {
        auto const header = wire::decode_header({rx_ + rx_begin_, available},
            {options.receive.max_frame_size, std::numeric_limits<std::uint32_t>::max(), features_});
        if (header.code != wire::error::none) return step::failed;
        need += header.value.length;
    }
    auto const normal = options.receive_buffer_bytes;
    if (need > rx_capacity_) return resize_receive(need) ? step::need_bytes : step::failed;
    if (rx_capacity_ > slab::block_size(normal) && need <= normal && available <= normal)
        return resize_receive(normal) ? step::need_bytes : step::failed;
    if (rx_begin_ != 0 && (rx_begin_ + need > rx_capacity_ || rx_capacity_ - rx_end_ < rx_capacity_ / 4)) {
        std::memmove(rx_, rx_ + rx_begin_, available);
        rx_begin_ = 0;
        rx_end_ = available;
    }
    return step::need_bytes;
}

bool connection::resize_receive(std::size_t const capacity) noexcept {
    std::uint8_t *next = nullptr;
    try {
        next = shard.memory.allocate(capacity);
    } catch (std::bad_alloc const &) {
        return false;
    }
    auto const available = rx_end_ - rx_begin_;
    if (available != 0) std::memcpy(next, rx_ + rx_begin_, available);
    release_rx();
    rx_ = next;
    rx_capacity_ = slab::block_size(capacity);
    rx_begin_ = 0;
    rx_end_ = available;
    return true;
}

bool connection::dispatch(wire::frame_view const &frame) noexcept {
    switch (frame.header.type) {
    case wire::frame_type::ping:
        send_control(wire::frame_type::pong, 0, frame.header.aux, 0);
        return true;
    case wire::frame_type::pong: return true;
    case wire::frame_type::settings: return false;
    default: return handler_.on_frame(frame);
    }
}

void connection::release_rx() noexcept {
    shard.memory.deallocate(rx_, rx_capacity_);
    rx_ = nullptr;
    rx_capacity_ = rx_begin_ = rx_end_ = 0;
}

// ---- send ----

std::uint8_t *connection::reserve(std::size_t const size) {
    if (chunk_count_ != 0) {
        auto &tail = chunk_at(chunk_count_ - 1);
        if (tail.capacity - tail.end >= size) return tail.data + tail.end;
    }
    if (chunk_count_ == chunks_.size()) {
        std::vector<chunk> grown(std::max<std::size_t>(4, chunks_.size() * 2));
        for (std::size_t index = 0; index != chunk_count_; ++index) grown[index] = chunk_at(index);
        chunks_.swap(grown);
        chunk_head_ = 0;
    }
    auto const bytes = std::max(size, tx_chunk_bytes);
    auto *const data = shard.memory.allocate(bytes);
    chunk_at(chunk_count_) = chunk{data, slab::block_size(bytes), 0, 0};
    ++chunk_count_;
    return data;
}

void connection::commit(std::size_t const size) noexcept {
    chunk_at(chunk_count_ - 1).end += size;
    tx_bytes_ += size;
    if (writer_wake_.waiting()) writer_wake_.signal();
}

bool connection::send_control(wire::frame_type const type, std::uint32_t const stream, std::uint32_t const aux,
                              std::uint8_t const flags) noexcept {
    if (state_ == phase::closed) return false;
    std::size_t const head = type == wire::frame_type::end ? 2 : 0;
    std::uint8_t *frame = nullptr;
    try {
        frame = reserve(wire::header_size + head);
    } catch (std::bad_alloc const &) {
        close();
        return false;
    }
    wire::frame_header header{};
    header.type = type;
    header.stream_id = stream;
    header.aux = aux;
    header.flags = flags;
    header.length = static_cast<std::uint32_t>(head);
    header.head_length = static_cast<std::uint16_t>(head);
    if (wire::encode_header(header, {frame, wire::header_size}, outgoing()).code != wire::error::none) {
        close();
        return false;
    }
    if (head != 0) frame[16] = frame[17] = 0;
    commit(wire::header_size + head);
    return true;
}

std::size_t connection::gather(net::const_buffer *const buffers, std::size_t const limit) const noexcept {
    std::size_t count = 0;
    for (std::size_t index = 0; index != chunk_count_ && count != limit; ++index) {
        auto const &c = chunk_at(index);
        if (c.end != c.begin) buffers[count++] = net::const_buffer{c.data + c.begin, c.end - c.begin};
    }
    return count;
}

void connection::consume(std::size_t bytes) noexcept {
    tx_bytes_ -= bytes;
    while (chunk_count_ != 0) {
        auto &front = chunk_at(0);
        auto const take = std::min(bytes, front.end - front.begin);
        front.begin += take;
        bytes -= take;
        if (front.begin != front.end) break;
        shard.memory.deallocate(front.data, front.capacity);
        chunk_head_ = (chunk_head_ + 1) & (chunks_.size() - 1);
        --chunk_count_;
    }
    if (reader_paused_ && tx_bytes_ <= options.tx_low_watermark) {
        reader_paused_ = false;
        reader_wake_.signal();
    }
}

void connection::release_tx() noexcept {
    while (chunk_count_ != 0) {
        auto &front = chunk_at(0);
        shard.memory.deallocate(front.data, front.capacity);
        chunk_head_ = (chunk_head_ + 1) & (chunks_.size() - 1);
        --chunk_count_;
    }
    tx_bytes_ = 0;
}

// ---- lifetime ----

void connection::close() noexcept {
    if (state_ == phase::closed) return;
    state_ = phase::closed;
    shard.unschedule(handshake_timer_);
    if (link_) link_->close();
    writer_wake_.signal();
    reader_wake_.signal();
    handler_.on_closed();
}

void connection::close_when_flushed() noexcept {
    if (state_ == phase::closed) return;
    close_when_flushed_ = true;
    if (tx_bytes_ == 0 && writer_wake_.waiting()) writer_wake_.signal();
}

void connection::io_exited() noexcept {
    assert(running_ != 0);
    if (--running_ != 0) return;
    release_rx();
    release_tx();
    handler_.on_finished();
}

void connection::on_handshake_deadline(stream_state &node) noexcept {
    auto &self = *static_cast<handshake_node &>(node).owner;
    self.handshake_expired_ = true;
    self.close();
}

} // namespace detail
} // namespace v2
} // namespace rpc
