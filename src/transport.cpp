#include <rpc/transport.hpp>

namespace rpc {
namespace {

struct tcp_transport final : transport {
    explicit tcp_transport(net::tcp_socket socket) : socket_(std::move(socket)), stream_(&socket_) {}
    net::any_stream &stream() noexcept override { return stream_; }
    void close() noexcept override { static_cast<void>(socket_.close()); }

private:
    net::tcp_socket socket_;
    net::any_stream stream_;
};

} // namespace

std::unique_ptr<transport> make_tcp_transport(net::tcp_socket socket) {
    return std::unique_ptr<transport>(new tcp_transport(std::move(socket)));
}

} // namespace rpc
