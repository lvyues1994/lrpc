#pragma once

#include "engine.hpp"

#include <rpc/client.hpp>

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace rpc {
namespace detail {

enum : std::uint8_t { call_idle = 0, call_pending = 1, call_done = 2 };

struct method_slot {
    std::string name;
    // Wire ID on this connection, zero until NEW_METHOD is queued. IDs are
    // handed out in order of first use; streaming peers reject other orders.
    std::uint32_t wire_id = 0;
};

// Chooses the connection for each attempt of a routed call.
struct call_router {
    // A ready connection with room, preferring one other than avoid; null if none.
    virtual client_core *pick(client_core const *avoid) noexcept = 0;
    virtual std::uint32_t max_attempts() const noexcept = 0;

protected:
    ~call_router() = default;
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
    bool has_room() const noexcept { return streams.size() < streams.limit(); }
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
    std::uint32_t next_method = 1;
    bool going_away = false;
    bool handshake_done = false;
    bool closed = false;
    bool endpoint_open = true;
    event ready_event;  // Handshake finished either way.
    event closed_event; // The connection started closing.
    status_code connect_status = status_code::unavailable;
};

// A REQUEST frame sized and checked before space is reserved for it.
struct request_plan {
    method_slot *slot = nullptr;
    wire::request_head head{};
    std::size_t head_size = 0;
    std::size_t payload = 0;
    std::uint32_t wire_id = 0;
    bool intern = false;
};
// A bounded body may be given less room than its size.
status_code plan_request(client_core &core, method_slot &slot, std::uint64_t timeout_us, wire::metadata_list metadata,
                         std::size_t body, bool bounded, request_plan &plan) noexcept;
// Encodes into reserved space and takes the stream ID; the caller commits
// `length` bytes.
status_code write_request(client_core &core, request_plan const &plan, std::uint8_t *frame, std::uint32_t id,
                          request_body const &body, bool end_stream, std::size_t &length) noexcept;
std::uint64_t remaining_us(clock::duration remaining) noexcept;
void keep_trailer(response_trailer &trailer, wire::bytes_view head) noexcept;

net::task<status_code> connect_client(std::shared_ptr<client_core> core, net::ip::tcp::endpoint endpoint);
net::task<status_code> attach_client(std::shared_ptr<client_core> core, std::unique_ptr<transport> link);

} // namespace detail
} // namespace rpc
