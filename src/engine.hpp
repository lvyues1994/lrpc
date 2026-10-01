#pragma once

#include "core.hpp"

#include <rpc/options.hpp>
#include <rpc/transport.hpp>
#include <rpc/wire.hpp>

#include <cstddef>
#include <cstdint>
#include <memory>

namespace rpc {
namespace detail {

// The role of a connection. Callbacks run on the shard's thread from inside the
// connection; they may close it.
struct connection_handler {
    virtual bool on_frame(wire::frame_view const &frame) noexcept = 0; // False: protocol error.
    virtual void on_ready() noexcept = 0;
    virtual void on_closed() noexcept = 0;   // Once, when the connection starts closing.
    virtual void on_finished() noexcept = 0; // Once, after reader and writer have exited.

protected:
    ~connection_handler() = default;
};

enum class phase : std::uint8_t { idle, handshaking, ready, closed };
enum class parse_step : std::uint8_t { parsed, need_bytes, failed };

// A stream message framed one fragment at a time. While the connection's
// stream budget is spent, sources wait in turn for the writer.
struct fragment_source {
    // Queues the next fragment; true while more remain. May close the connection.
    virtual bool next_fragment() noexcept = 0;
    fragment_source *prev_source = nullptr;
    fragment_source *next_source = nullptr;
    bool waiting = false;

protected:
    ~fragment_source() = default;
};

// Framing, handshake and I/O for one transport. Frames are parsed in place
// from one receive window and sent from a chain of contiguous chunks, so a
// turn's worth of frames leaves in one write.
class connection {
public:
    connection(shard_state &shard, connection_options const &options, std::unique_ptr<transport> link,
               bool server_side, connection_handler &handler);
    ~connection();
    connection(connection const &) = delete;
    connection &operator=(connection const &) = delete;

    // Queues the opening bytes and launches the reader and writer, which hold
    // keep until they exit. Throws std::bad_alloc.
    void start(std::shared_ptr<void> keep);

    // Space for one frame; valid until commit, which must follow without
    // suspending. Throws std::bad_alloc.
    std::uint8_t *reserve(std::size_t size);
    void commit(std::size_t size) noexcept;
    // A frame without body; END carries the minimal empty head. Closes the
    // connection and returns false when out of memory.
    bool send_control(wire::frame_type type, std::uint32_t stream, std::uint32_t aux, std::uint8_t flags) noexcept;

    // A stream may frame a fragment now: none is waiting and the budget is not spent.
    bool stream_room() const noexcept {
        return sources_head_ == nullptr && tx_bytes_ < options.stream_send_budget;
    }
    void queue_stream(fragment_source &source) noexcept;
    void unqueue_stream(fragment_source &source) noexcept; // No-op unless it waits.

    void close() noexcept;
    void close_when_flushed() noexcept;

    phase state() const noexcept { return state_; }
    bool ready() const noexcept { return state_ == phase::ready; }
    std::size_t queued_bytes() const noexcept { return tx_bytes_; }
    wire::settings const &peer() const noexcept { return peer_; }
    wire::limits outgoing() const noexcept { return {peer_.max_frame_size, peer_.max_message_size, features_}; }
    std::uint32_t features() const noexcept { return features_; }
    bool handshake_expired() const noexcept { return handshake_expired_; }
    bool io_running() const noexcept { return running_ != 0; }

    shard_state &shard;
    connection_options const options;

private:
    using step = parse_step;
    struct chunk {
        std::uint8_t *data;
        std::size_t capacity;
        std::size_t begin;
        std::size_t end;
    };

    friend struct connection_io;

    step parse() noexcept;
    step handshake() noexcept;
    step prepare_receive(std::size_t available) noexcept;
    bool resize_receive(std::size_t capacity) noexcept;
    bool dispatch(wire::frame_view const &frame) noexcept;
    std::size_t gather(net::const_buffer *buffers, std::size_t limit) const noexcept;
    void refill() noexcept;
    void consume(std::size_t bytes) noexcept;
    void release_tx() noexcept;
    void release_rx() noexcept;
    void io_exited() noexcept;
    chunk &chunk_at(std::size_t index) noexcept { return chunks_[(chunk_head_ + index) & (chunks_.size() - 1)]; }
    chunk const &chunk_at(std::size_t index) const noexcept {
        return chunks_[(chunk_head_ + index) & (chunks_.size() - 1)];
    }
    static void on_handshake_deadline(stream_state &node) noexcept;

    std::unique_ptr<transport> link_;
    connection_handler &handler_;
    bool const server_side_;
    phase state_ = phase::idle;
    bool preface_seen_ = false;
    bool handshake_expired_ = false;
    bool close_when_flushed_ = false;
    bool reader_paused_ = false;
    std::uint8_t running_ = 0;
    wire::settings peer_{};
    std::uint32_t features_ = 0;

    std::uint8_t *rx_ = nullptr;
    std::size_t rx_capacity_ = 0;
    std::size_t rx_begin_ = 0;
    std::size_t rx_end_ = 0;
    std::size_t turn_bytes_ = 0; // MESSAGE bytes parsed since the reader last yielded.

    std::vector<chunk> chunks_; // Ring; the size is a power of two.
    std::size_t chunk_head_ = 0;
    std::size_t chunk_count_ = 0;
    std::size_t tx_bytes_ = 0;
    fragment_source *sources_head_ = nullptr;
    fragment_source *sources_tail_ = nullptr;

    event writer_wake_;
    event reader_wake_;
    struct handshake_node : stream_state {
        connection *owner = nullptr;
    } handshake_timer_;
};

} // namespace detail
} // namespace rpc
