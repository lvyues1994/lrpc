#pragma once

#include <net/any_stream.hpp>
#include <net/tcp.hpp>

#include <memory>

namespace rpc {

// Owns the byte stream of one connection. close() cancels pending I/O; the
// engine keeps its buffers until those operations complete. Tests implement it
// to count writes or fragment reads.
struct transport {
    virtual ~transport() = default;
    virtual net::any_stream &stream() noexcept = 0;
    virtual void close() noexcept = 0;
};

std::unique_ptr<transport> make_tcp_transport(net::tcp_socket socket);

} // namespace rpc
