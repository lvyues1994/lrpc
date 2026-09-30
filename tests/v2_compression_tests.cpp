// Compressed stream messages, v2 with v2 and with v1. Built only with
// LRPC_ENABLE_COMPRESSION.
#include "backend.hpp"
#include "check.hpp"

#include <rpc/compression.hpp>
#include <rpc/stream.hpp>
#include <rpc/v2/client.hpp>
#include <rpc/v2/server.hpp>

#include <net/run_async.hpp>

#include <atomic>
#include <cstring>
#include <iostream>
#include <random>
#include <vector>

namespace {

namespace v2 = rpc::v2;
using rpc::wire::bytes_view;

net::backend_kind selected_backend = net::default_backend_t::kind;

auto chat(v2::server_stream &stream)
    CO2_BEG(net::task<v2::status_code>, (stream), v2::stream_read read; v2::status_code sent;) {
    for (;;) {
        CO2_AWAIT_SET(read, stream.read());
        if (read.code != v2::status_code::ok) CO2_RETURN(read.code);
        if (read.ended) break;
        CO2_AWAIT_SET(sent, stream.write(read.message));
        if (sent != v2::status_code::ok) CO2_RETURN(sent);
    }
    CO2_RETURN(v2::status_code::ok);
}
CO2_END

struct chat_handler final : v2::stream_method_handler {
    net::task<v2::status_code> invoke(v2::server_context &, v2::server_stream &stream) override { return chat(stream); }
};

// Counts bytes the client writes, to see compression on the wire.
struct counting_stream {
    net::tcp_socket *socket;
    std::size_t *written;
    template <class Buffers> auto read_some(Buffers const &buffers) { return socket->read_some(buffers); }
    template <class Buffers> auto write_some(Buffers const &buffers) {
        for (auto it = net::buffer_sequence_begin(buffers); it != net::buffer_sequence_end(buffers); ++it)
            *written += net::const_buffer(*it).size(); // Offered, an upper bound of what was sent.
        return socket->write_some(buffers);
    }
};

struct counting_transport final : v2::transport {
    counting_transport(net::tcp_socket value, std::size_t &written)
        : socket(std::move(value)), counter{&socket, &written}, stream_(&counter) {}
    net::any_stream &stream() noexcept override { return stream_; }
    void close() noexcept override { static_cast<void>(socket.close()); }
    net::tcp_socket socket;
    counting_stream counter;
    net::any_stream stream_;
};

v2::connection_options compressing(std::uint8_t algorithm) {
    v2::connection_options options{};
    options.receive.features |= rpc::wire::message_compression;
    options.receive.compression = rpc::compression_algorithms();
    options.preferred_compression = algorithm;
    return options;
}

std::vector<std::uint8_t> compressible(std::size_t size) {
    std::vector<std::uint8_t> bytes(size);
    for (std::size_t i = 0; i < size; ++i) bytes[i] = static_cast<std::uint8_t>("lrpc v2 "[i % 8]);
    return bytes;
}

std::vector<std::uint8_t> random_bytes(std::size_t size) {
    std::mt19937 generator{7};
    std::vector<std::uint8_t> bytes(size);
    for (auto &byte : bytes) byte = static_cast<std::uint8_t>(generator());
    return bytes;
}

auto exchange(v2::client &client, v2::method_ref method, std::vector<std::uint8_t> const *message)
    CO2_BEG(net::task<>, (client, method, message), v2::open_result opened; v2::status_code wrote;
            v2::stream_read read; v2::call_result result; int i = 0;) {
    CO2_AWAIT_SET(opened, client.open(method, v2::method_kind::bidirectional));
    CHECK(opened.code == v2::status_code::ok);
    for (i = 0; i < 4; ++i) {
        CO2_AWAIT_SET(wrote, opened.stream.write({message->data(), message->size()}));
        CHECK(wrote == v2::status_code::ok);
        CO2_AWAIT_SET(read, opened.stream.read());
        CHECK(read.code == v2::status_code::ok && read.message.size == message->size());
        CHECK(std::memcmp(read.message.data, message->data(), message->size()) == 0);
    }
    CO2_AWAIT_SET(wrote, opened.stream.writes_done());
    CO2_AWAIT_SET(read, opened.stream.read());
    CHECK(read.ended);
    CO2_AWAIT_SET(result, opened.stream.finish());
    CHECK(result.code == v2::status_code::ok);
    CO2_RETURN();
}
CO2_END

struct v2_fixture {
    explicit v2_fixture(std::uint8_t algorithm)
        : server(shard, {{"z/Chat", nullptr, 1U << 20, 0, v2::method_kind::bidirectional, &handler}},
                 server_options(algorithm)),
          client(shard, client_options(algorithm)), endpoint(server.listen({net::ip::address_v4::loopback(), 0})),
          method(client.bind("z/Chat")) {}
    static v2::server_options server_options(std::uint8_t algorithm) {
        v2::server_options options{};
        options.connection = compressing(algorithm);
        return options;
    }
    static v2::client_options client_options(std::uint8_t algorithm) {
        v2::client_options options{};
        options.connection = compressing(algorithm);
        return options;
    }
    net::io_context context{selected_backend, net::single_thread_hint};
    v2::shard shard{context};
    chat_handler handler;
    v2::server server;
    v2::client client;
    net::ip::tcp::endpoint endpoint;
    v2::method_ref method;
    std::size_t written = 0;
};

auto v2_pair(v2_fixture &f)
    CO2_BEG(net::task<>, (f), std::unique_ptr<net::tcp_socket> socket; net::io_result<> connected;
            v2::status_code attached; std::vector<std::uint8_t> message; std::size_t before = 0;) {
    socket.reset(new net::tcp_socket(f.context));
    CO2_AWAIT_SET(connected, socket->connect(f.endpoint));
    CHECK(!connected.ec);
    CO2_AWAIT_SET(attached, f.client.attach(std::unique_ptr<v2::transport>(new counting_transport(std::move(*socket), f.written))));
    CHECK(attached == v2::status_code::ok);
    message = compressible(64U << 10);
    before = f.written;
    CO2_AWAIT(exchange(f.client, f.method, &message));
    CHECK(f.written - before < 4 * message.size() / 8); // Four messages, far smaller on the wire.
    message = random_bytes(64U << 10); // Does not shrink: sent as is.
    before = f.written;
    CO2_AWAIT(exchange(f.client, f.method, &message));
    CHECK(f.written - before >= 4 * message.size());
    CO2_RETURN();
}
CO2_END

// ---- v1 ----

struct v1_chat_service {
    using stream_type = rpc::server_stream<rpc::byte_buffer, rpc::byte_buffer>;
    net::task<rpc::status_code> chat(rpc::server_context &, stream_type &stream) { return run(&stream); }
    static auto run(stream_type *stream)
        CO2_BEG(net::task<rpc::status_code>, (stream), rpc::byte_buffer message; rpc::stream_read_result read;
                rpc::status_code sent;) {
        for (;;) {
            CO2_AWAIT_SET(read, stream->read(message));
            if (read.code != rpc::status_code::ok) CO2_RETURN(read.code);
            if (read.ended) break;
            CO2_AWAIT_SET(sent, stream->write(message));
            if (sent != rpc::status_code::ok) CO2_RETURN(sent);
        }
        CO2_RETURN(rpc::status_code::ok);
    }
    CO2_END
};

auto v2_against_v1(v2::client &client, net::ip::tcp::endpoint endpoint, v2::method_ref method)
    CO2_BEG(net::task<>, (client, endpoint, method), v2::status_code connected; std::vector<std::uint8_t> message;) {
    CO2_AWAIT_SET(connected, client.connect(endpoint));
    CHECK(connected == v2::status_code::ok);
    message = compressible(200U << 10); // Compressed both ways, still fragmented.
    CO2_AWAIT(exchange(client, method, &message));
    CO2_RETURN();
}
CO2_END

template <class F> void drive(net::io_context &context, F factory, std::function<void()> close) {
    std::exception_ptr failure;
    net::run_async(context.get_executor(), close, [&](std::exception_ptr error) {
        failure = error;
        close();
    })(factory);
    context.run();
    if (failure) std::rethrow_exception(failure);
}

} // namespace

int main(int argc, char **argv) {
    try {
        selected_backend = test_backend(argc, argv);
        if (!net::backend_available(selected_backend)) {
            std::cout << "SKIP backend unavailable\n";
            return 77;
        }
        CHECK(rpc::compression_algorithms() == 3);
        for (std::uint8_t algorithm : {std::uint8_t{1}, std::uint8_t{2}}) {
            v2_fixture f{algorithm};
            drive(f.context, [&] { return v2_pair(f); }, [&] {
                f.client.close();
                f.server.close();
            });
        }
        std::cout << "PASS zstd and LZ4 between v2 peers\n";
        for (std::uint8_t algorithm : {std::uint8_t{1}, std::uint8_t{2}}) {
            net::io_context context{selected_backend, net::single_thread_hint};
            v1_chat_service service;
            rpc::server_options options;
            options.connection.receive.features |= rpc::wire::streaming | rpc::wire::message_compression;
            options.connection.receive.initial_stream_window = 1U << 20;
            options.connection.receive.max_message_size = 1U << 20;
            options.connection.receive.compression = rpc::compression_algorithms();
            options.connection.preferred_compression = algorithm;
            auto old_server = rpc::make_server(
                context,
                {rpc::bind_stream_method(rpc::method<rpc::byte_buffer, rpc::byte_buffer>{"z/Chat"},
                                         rpc::method_kind::bidirectional, service, &v1_chat_service::chat, {1U << 20, 2})},
                options);
            auto const endpoint = old_server->listen({net::ip::address_v4::loopback(), 0});
            v2::shard shard{context};
            v2::client_options client_options{};
            client_options.connection = compressing(algorithm);
            client_options.connection.receive.max_frame_size = 64U << 10;
            v2::client client{shard, client_options};
            auto const method = client.bind("z/Chat");
            drive(context, [&] { return v2_against_v1(client, endpoint, method); }, [&] {
                client.close();
                old_server->close();
            });
        }
        std::cout << "PASS zstd and LZ4 between v2 and v1\n";
    } catch (std::exception const &error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
