// Compressed stream messages. Built only with LRPC_ENABLE_COMPRESSION.
#include "backend.hpp"
#include "check.hpp"

#include <rpc/compression.hpp>
#include <rpc/stream.hpp>
#include <rpc/client.hpp>
#include <rpc/server.hpp>

#include <net/run_async.hpp>

#include <atomic>
#include <cstring>
#include <iostream>
#include <random>
#include <vector>

namespace {

using rpc::wire::bytes_view;

net::backend_kind selected_backend = net::default_backend_t::kind;

auto chat(rpc::server_stream &stream)
    CO2_BEG(net::task<rpc::status_code>, (stream), rpc::stream_read read; rpc::status_code sent;) {
    for (;;) {
        CO2_AWAIT_SET(read, stream.read());
        if (read.code != rpc::status_code::ok) CO2_RETURN(read.code);
        if (read.ended) break;
        CO2_AWAIT_SET(sent, stream.write(read.message));
        if (sent != rpc::status_code::ok) CO2_RETURN(sent);
    }
    CO2_RETURN(rpc::status_code::ok);
}
CO2_END

struct chat_handler final : rpc::stream_method_handler {
    net::task<rpc::status_code> invoke(rpc::server_context &, rpc::server_stream &stream) override { return chat(stream); }
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

struct counting_transport final : rpc::transport {
    counting_transport(net::tcp_socket value, std::size_t &written)
        : socket(std::move(value)), counter{&socket, &written}, stream_(&counter) {}
    net::any_stream &stream() noexcept override { return stream_; }
    void close() noexcept override { static_cast<void>(socket.close()); }
    net::tcp_socket socket;
    counting_stream counter;
    net::any_stream stream_;
};

rpc::connection_options compressing(std::uint8_t algorithm) {
    rpc::connection_options options{};
    options.receive.features |= rpc::wire::message_compression;
    options.receive.compression = rpc::compression_algorithms();
    options.preferred_compression = algorithm;
    return options;
}

std::vector<std::uint8_t> compressible(std::size_t size) {
    std::vector<std::uint8_t> bytes(size);
    for (std::size_t i = 0; i < size; ++i) bytes[i] = static_cast<std::uint8_t>("lrpc msg"[i % 8]);
    return bytes;
}

std::vector<std::uint8_t> random_bytes(std::size_t size) {
    std::mt19937 generator{7};
    std::vector<std::uint8_t> bytes(size);
    for (auto &byte : bytes) byte = static_cast<std::uint8_t>(generator());
    return bytes;
}

auto exchange(rpc::client &client, rpc::method_ref method, std::vector<std::uint8_t> const *message)
    CO2_BEG(net::task<>, (client, method, message), rpc::open_result opened; rpc::status_code wrote;
            rpc::stream_read read; rpc::call_result result; int i = 0;) {
    CO2_AWAIT_SET(opened, client.open(method, rpc::method_kind::bidirectional));
    CHECK(opened.code == rpc::status_code::ok);
    for (i = 0; i < 4; ++i) {
        CO2_AWAIT_SET(wrote, opened.stream.write({message->data(), message->size()}));
        CHECK(wrote == rpc::status_code::ok);
        CO2_AWAIT_SET(read, opened.stream.read());
        CHECK(read.code == rpc::status_code::ok && read.message.size == message->size());
        CHECK(std::memcmp(read.message.data, message->data(), message->size()) == 0);
    }
    CO2_AWAIT_SET(wrote, opened.stream.writes_done());
    CO2_AWAIT_SET(read, opened.stream.read());
    CHECK(read.ended);
    CO2_AWAIT_SET(result, opened.stream.finish());
    CHECK(result.code == rpc::status_code::ok);
    CO2_RETURN();
}
CO2_END

// Four letters at random: compresses to about a quarter, still several frames.
std::vector<std::uint8_t> semi_compressible(std::size_t size) {
    std::mt19937 generator{11};
    std::vector<std::uint8_t> bytes(size);
    for (auto &byte : bytes) byte = static_cast<std::uint8_t>('a' + generator() % 4);
    return bytes;
}

struct fixture {
    fixture(std::uint8_t algorithm, std::uint32_t max_frame)
        : server(shard, {{"z/Chat", nullptr, 1U << 20, 0, rpc::method_kind::bidirectional, &handler}},
                 server_options(algorithm, max_frame)),
          client(shard, client_options(algorithm, max_frame)),
          endpoint(server.listen({net::ip::address_v4::loopback(), 0})), method(client.bind("z/Chat")) {}
    static rpc::server_options server_options(std::uint8_t algorithm, std::uint32_t max_frame) {
        rpc::server_options options{};
        options.connection = compressing(algorithm);
        options.connection.receive.max_frame_size = max_frame;
        return options;
    }
    static rpc::client_options client_options(std::uint8_t algorithm, std::uint32_t max_frame) {
        rpc::client_options options{};
        options.connection = compressing(algorithm);
        options.connection.receive.max_frame_size = max_frame;
        return options;
    }
    net::io_context context{selected_backend, net::single_thread_hint};
    rpc::shard shard{context};
    chat_handler handler;
    rpc::server server;
    rpc::client client;
    net::ip::tcp::endpoint endpoint;
    rpc::method_ref method;
    std::size_t written = 0;
};

auto on_the_wire(fixture &f)
    CO2_BEG(net::task<>, (f), std::unique_ptr<net::tcp_socket> socket; net::io_result<> connected;
            rpc::status_code attached; std::vector<std::uint8_t> message; std::size_t before = 0;) {
    socket.reset(new net::tcp_socket(f.context));
    CO2_AWAIT_SET(connected, socket->connect(f.endpoint));
    CHECK(!connected.ec);
    CO2_AWAIT_SET(attached, f.client.attach(std::unique_ptr<rpc::transport>(new counting_transport(std::move(*socket), f.written))));
    CHECK(attached == rpc::status_code::ok);
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

auto fragmented(fixture &f)
    CO2_BEG(net::task<>, (f), rpc::status_code connected; std::vector<std::uint8_t> message;) {
    CO2_AWAIT_SET(connected, f.client.connect(f.endpoint));
    CHECK(connected == rpc::status_code::ok);
    message = semi_compressible(200U << 10); // Compressed both ways, then split across 16 KiB frames.
    CO2_AWAIT(exchange(f.client, f.method, &message));
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
            fixture f{algorithm, rpc::connection_options{}.receive.max_frame_size};
            auto const close = [&] {
                f.client.close();
                f.server.close();
            };
            drive(f.context, [&] { return on_the_wire(f); }, close);
        }
        std::cout << "PASS zstd and LZ4 shrink the wire, incompressible data passes as is\n";
        for (std::uint8_t algorithm : {std::uint8_t{1}, std::uint8_t{2}}) {
            fixture f{algorithm, 16U << 10};
            auto const close = [&] {
                f.client.close();
                f.server.close();
            };
            drive(f.context, [&] { return fragmented(f); }, close);
        }
        std::cout << "PASS compressed messages across frames\n";
    } catch (std::exception const &error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
