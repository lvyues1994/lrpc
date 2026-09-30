#pragma once

#include "engine.hpp"

#include <rpc/v2/client.hpp>

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace rpc {
namespace v2 {
namespace detail {

enum : std::uint8_t { call_idle = 0, call_pending = 1, call_done = 2 };

struct method_slot {
    std::string name;
    bool interned = false; // NEW_METHOD has been queued on this connection.
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
    bool going_away = false;
    bool handshake_done = false;
    bool closed = false;
    bool endpoint_open = true;
    event ready_event;  // Handshake finished either way.
    event closed_event; // The connection started closing.
    status_code connect_status = status_code::unavailable;
};

net::task<status_code> connect_client(std::shared_ptr<client_core> core, net::ip::tcp::endpoint endpoint);
net::task<status_code> attach_client(std::shared_ptr<client_core> core, std::unique_ptr<transport> link);

} // namespace detail
} // namespace v2
} // namespace rpc
