// Streams across v1 and v2: each client against the other's server, with
// fragmented messages. v1's streaming client also sends unary calls as
// streams ("extended unary").
#include "backend.hpp"
#include "check.hpp"

#include <rpc/stream.hpp>
#include <rpc/v2/client.hpp>
#include <rpc/v2/server.hpp>

#include <net/run_async.hpp>

#include <array>
#include <cstring>
#include <iostream>
#include <vector>

namespace {

namespace v2 = rpc::v2;
using rpc::wire::bytes_view;

net::backend_kind selected_backend = net::default_backend_t::kind;

// ---- v1 server side ----

auto v1_echo(bytes_view request, rpc::response_writer *response)
    CO2_BEG(net::task<rpc::status_code>, (request, response)) {
    response->assign(request);
    CO2_RETURN(rpc::status_code::ok);
}
CO2_END

struct v1_echo_handler final : rpc::method_handler {
    net::task<rpc::status_code> invoke(rpc::server_context &, bytes_view request, rpc::response_writer &response) override {
        return v1_echo(request, &response);
    }
};

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

// ---- v2 server side ----

auto v2_echo(bytes_view request, v2::response_writer &response)
    CO2_BEG(net::task<v2::status_code>, (request, response)) {
    CO2_RETURN(response.assign(request) ? v2::status_code::ok : v2::status_code::internal);
}
CO2_END

struct v2_echo_handler final : v2::method_handler {
    net::task<v2::status_code> invoke(v2::server_context &, bytes_view request, v2::response_writer &response) override {
        return v2_echo(request, response);
    }
};

auto v2_chat(v2::server_stream &stream)
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

struct v2_chat_handler final : v2::stream_method_handler {
    net::task<v2::status_code> invoke(v2::server_context &, v2::server_stream &stream) override { return v2_chat(stream); }
};

rpc::connection_options v1_connection() {
    rpc::connection_options connection;
    connection.receive.max_frame_size = 1024;
    connection.receive.max_message_size = 65536;
    connection.receive.features |= rpc::wire::streaming;
    connection.receive.initial_stream_window = 65536;
    return connection;
}

v2::connection_options v2_connection() {
    v2::connection_options connection;
    connection.receive.max_frame_size = 1024;
    connection.receive.max_message_size = 65536;
    connection.receive.initial_stream_window = 65536;
    return connection;
}

struct fixture {
    fixture() {
        rpc::server_options old_options;
        old_options.connection = v1_connection();
        std::vector<rpc::method_binding> bindings{{"interop/Echo", 512, &old_echo}};
        bindings.push_back(rpc::bind_stream_method(rpc::method<rpc::byte_buffer, rpc::byte_buffer>{"interop/Chat"},
                                                   rpc::method_kind::bidirectional, old_chat, &v1_chat_service::chat,
                                                   {65536, 2}));
        old_server = rpc::make_server(context, std::move(bindings), old_options);
        rpc::client_options old_client_options;
        old_client_options.connection = v1_connection();
        old_client = rpc::make_client(context, old_client_options);
        chat_handle = old_client->bind(rpc::method_descriptor{"interop/Chat", rpc::method_kind::bidirectional,
                                                               rpc::idempotency::unknown,
                                                               &rpc::codec_for<rpc::byte_buffer>(),
                                                               &rpc::codec_for<rpc::byte_buffer>(), 0});
        v2::server_options new_options;
        new_options.connection = v2_connection();
        new_server.reset(new v2::server(shard, {{"interop/Echo", &new_echo, 512},
                                                {"interop/Chat", nullptr, 65536, 0, v2::method_kind::bidirectional,
                                                 &new_chat}},
                                        new_options));
        v2::client_options new_client_options;
        new_client_options.connection = v2_connection();
        new_client.reset(new v2::client(shard, new_client_options));
    }

    net::io_context context{selected_backend, net::single_thread_hint};
    v2::shard shard{context};
    v1_echo_handler old_echo;
    v1_chat_service old_chat;
    v2_echo_handler new_echo;
    v2_chat_handler new_chat;
    std::unique_ptr<rpc::server> old_server;
    std::unique_ptr<rpc::client> old_client;
    rpc::method_handle chat_handle;
    std::unique_ptr<v2::server> new_server;
    std::unique_ptr<v2::client> new_client;
};

std::vector<std::uint8_t> pattern(std::size_t size, unsigned seed) {
    std::vector<std::uint8_t> bytes(size);
    for (std::size_t i = 0; i < size; ++i) bytes[i] = static_cast<std::uint8_t>(i * 13 + seed);
    return bytes;
}

auto v2_against_v1(fixture &f)
    CO2_BEG(net::task<>, (f), v2::status_code connected; v2::call_result result; v2::open_result opened;
            v2::status_code wrote; v2::stream_read read; v2::method_ref echo; v2::method_ref chat;
            std::vector<std::uint8_t> message; std::array<std::uint8_t, 48> reply{}; unsigned i = 0;) {
    CO2_AWAIT_SET(connected, f.new_client->connect(f.old_server->listen({net::ip::address_v4::loopback(), 0})));
    CHECK(connected == v2::status_code::ok);
    chat = f.new_client->bind("interop/Chat"); // Bound first, used second: wire IDs follow first use.
    echo = f.new_client->bind("interop/Echo");
    message = pattern(48, 1);
    CO2_AWAIT_SET(result, f.new_client->call(echo, {message.data(), message.size()}, {reply.data(), reply.size()}));
    CHECK(result.code == v2::status_code::ok && std::memcmp(reply.data(), message.data(), 48) == 0);
    CO2_AWAIT_SET(opened, f.new_client->open(chat, v2::method_kind::bidirectional));
    CHECK(opened.code == v2::status_code::ok);
    for (i = 0; i < 5; ++i) {
        message = pattern(i == 0 ? 0 : 3000U * i, i); // Fragmented across 1 KiB frames.
        CO2_AWAIT_SET(wrote, opened.stream.write({message.data(), message.size()}));
        CHECK(wrote == v2::status_code::ok);
        CO2_AWAIT_SET(read, opened.stream.read());
        CHECK(read.code == v2::status_code::ok && read.message.size == message.size());
        CHECK(message.empty() || std::memcmp(read.message.data, message.data(), message.size()) == 0);
    }
    CO2_AWAIT_SET(wrote, opened.stream.writes_done());
    CO2_AWAIT_SET(read, opened.stream.read());
    CHECK(read.code == v2::status_code::ok && read.ended);
    CO2_AWAIT_SET(result, opened.stream.finish());
    CHECK(result.code == v2::status_code::ok);
    CO2_RETURN();
}
CO2_END

using old_call = rpc::stream_call<rpc::byte_buffer, rpc::byte_buffer>;

auto v1_against_v2(fixture &f)
    CO2_BEG(net::task<>, (f), rpc::status_code connected; rpc::call_result result; old_call call;
            rpc::stream_read_result read; rpc::byte_buffer request; rpc::byte_buffer reply;
            std::vector<std::uint8_t> message; std::vector<std::uint8_t> output; unsigned i = 0;) {
    CO2_AWAIT_SET(connected, f.old_client->connect(f.new_server->listen({net::ip::address_v4::loopback(), 0})));
    CHECK(connected == rpc::status_code::ok);
    message = pattern(300, 2);
    output.assign(message.size(), 0);
    // v1's streaming client sends this unary call as a stream.
    CO2_AWAIT_SET(result, f.old_client->call("interop/Echo", {message.data(), message.size()}, {output.data(), output.size()}));
    CHECK(result.code == rpc::status_code::ok && result.response_size == message.size() && output == message);
    CO2_AWAIT_SET(call, (rpc::open_stream<rpc::byte_buffer, rpc::byte_buffer>(*f.old_client, f.chat_handle)));
    CHECK(call);
    for (i = 0; i < 5; ++i) {
        message = pattern(i == 0 ? 0 : 2500U * i, i);
        request = rpc::byte_buffer::copy({message.data(), message.size()});
        CO2_AWAIT_SET(connected, call.write(request));
        CHECK(connected == rpc::status_code::ok);
        CO2_AWAIT_SET(read, call.read(reply));
        CHECK(read.code == rpc::status_code::ok && !read.ended && reply.size() == message.size());
        output.assign(message.size(), 0);
        CHECK(reply.copy_to({output.data(), output.size()}) && output == message);
    }
    CO2_AWAIT_SET(connected, call.writes_done());
    CO2_AWAIT_SET(read, call.read(reply));
    CHECK(read.code == rpc::status_code::ok && read.ended);
    CO2_AWAIT_SET(result, call.finish());
    CHECK(result.code == rpc::status_code::ok);
    CO2_RETURN();
}
CO2_END

auto both(fixture &f) CO2_BEG(net::task<>, (f)) {
    CO2_AWAIT(v2_against_v1(f));
    CO2_AWAIT(v1_against_v2(f));
    CO2_RETURN();
}
CO2_END

} // namespace

int main(int argc, char **argv) {
    try {
        selected_backend = test_backend(argc, argv);
        if (!net::backend_available(selected_backend)) {
            std::cout << "SKIP backend unavailable\n";
            return 77;
        }
        fixture f;
        std::exception_ptr failure;
        auto close = [&] {
            f.new_client->close();
            f.old_client->close();
            f.new_server->close();
            f.old_server->close();
        };
        net::run_async(f.context.get_executor(), close, [&](std::exception_ptr error) {
            failure = error;
            close();
        })([&] { return both(f); });
        f.context.run();
        if (failure) std::rethrow_exception(failure);
        std::cout << "PASS v2 streams with a v1 server, v1 streams and extended unary with a v2 server\n";
    } catch (std::exception const &error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
