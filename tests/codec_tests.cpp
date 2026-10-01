// Codec labels: one method name served in several encodings, chosen per
// connection when the method is defined.
#include "backend.hpp"
#include "check.hpp"

#include <rpc/channel.hpp>
#include <rpc/client.hpp>
#include <rpc/server.hpp>

#include <net/run_async.hpp>
#include <net/timeout.hpp>

#include <array>
#include <chrono>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

using rpc::wire::bytes_view;
using std::chrono::milliseconds;

net::backend_kind selected_backend = net::default_backend_t::kind;

auto reply(char tag, rpc::response_writer &response) CO2_BEG(net::task<rpc::status_code>, (tag, response)) {
    CO2_RETURN(response.assign({reinterpret_cast<std::uint8_t const *>(&tag), 1}) ? rpc::status_code::ok
                                                                                     : rpc::status_code::internal);
}
CO2_END

// Answers with its tag, so a call shows which binding served it.
struct tagged final : rpc::method_handler {
    explicit tagged(char value) : tag(value) {}
    net::task<rpc::status_code> invoke(rpc::server_context &, bytes_view, rpc::response_writer &response) override {
        return reply(tag, response);
    }
    char tag;
};

auto chat(char tag, rpc::server_stream &stream)
    CO2_BEG(net::task<rpc::status_code>, (tag, stream), rpc::status_code wrote; rpc::stream_read read;) {
    CO2_AWAIT_SET(wrote, stream.write({reinterpret_cast<std::uint8_t const *>(&tag), 1}));
    if (wrote != rpc::status_code::ok) CO2_RETURN(wrote);
    CO2_AWAIT_SET(read, stream.read());
    CO2_RETURN(read.code);
}
CO2_END

struct tagged_stream final : rpc::stream_method_handler {
    explicit tagged_stream(char value) : tag(value) {}
    net::task<rpc::status_code> invoke(rpc::server_context &, rpc::server_stream &stream) override {
        return chat(tag, stream);
    }
    char tag;
};

rpc::method_binding unary(std::string name, rpc::method_handler &handler, std::string codec) {
    rpc::method_binding binding{std::move(name), &handler, 8};
    binding.codec = std::move(codec);
    return binding;
}

rpc::method_binding stream(std::string name, rpc::stream_method_handler &handler, std::string codec) {
    rpc::method_binding binding{std::move(name), nullptr, 8, 0, rpc::method_kind::bidirectional, &handler};
    binding.codec = std::move(codec);
    return binding;
}

rpc::connection_options without_labels() {
    rpc::connection_options options{};
    options.receive.features &= ~rpc::wire::method_codecs;
    return options;
}

struct fixture {
    explicit fixture(rpc::connection_options server_side = {}, rpc::connection_options client_side = {})
        : shard(context), server(shard, bindings(), server_options(server_side)), client(shard, client_options(client_side)),
          endpoint(server.listen({net::ip::address_v4::loopback(), 0})) {}

    std::vector<rpc::method_binding> bindings() {
        return {unary("svc/Get", a, "a"), unary("svc/Get", b, "b"), unary("svc/Only", a, "a"), unary("svc/Any", u, ""),
                unary("svc/Any", b, "b"), unary("svc/Plain", u, ""), stream("svc/Chat", stream_a, "a"),
                stream("svc/Chat", stream_b, "b")};
    }
    static rpc::server_options server_options(rpc::connection_options const &connection) {
        rpc::server_options options{};
        options.connection = connection;
        return options;
    }
    static rpc::client_options client_options(rpc::connection_options const &connection) {
        rpc::client_options options{};
        options.connection = connection;
        return options;
    }

    template <class F> void run(F factory) {
        std::exception_ptr failure;
        auto close = [&] {
            if (channel) channel->close();
            client.close();
            server.close();
        };
        net::run_async(context.get_executor(), close, [&](std::exception_ptr error) {
            failure = error;
            close();
        })(factory);
        context.run();
        if (failure) std::rethrow_exception(failure);
    }

    net::io_context context{selected_backend, net::single_thread_hint};
    rpc::shard shard;
    tagged a{'A'};
    tagged b{'B'};
    tagged u{'U'};
    tagged_stream stream_a{'a'};
    tagged_stream stream_b{'b'};
    rpc::server server;
    rpc::client client;
    net::ip::tcp::endpoint endpoint;
    std::unique_ptr<rpc::channel> channel;
};

// Calls name with codec and returns the serving tag, or '-' with the result.
auto served(fixture &f, std::string name, std::string codec, rpc::call_result *out)
    CO2_BEG(net::task<char>, (f, name, codec, out), rpc::call_result result; std::array<std::uint8_t, 1> answer{};) {
    CO2_AWAIT_SET(result, f.client.call(f.client.bind(name, codec), {}, {answer.data(), 1}));
    if (out) *out = result;
    CO2_RETURN(result.code == rpc::status_code::ok && result.size == 1 ? static_cast<char>(answer[0]) : '-');
}
CO2_END

auto labelled(fixture &f)
    CO2_BEG(net::task<>, (f), rpc::status_code connected; char tag = 0; rpc::call_result result;
            rpc::open_result opened; rpc::stream_read read; std::array<std::uint8_t, 1> answer{};) {
    CO2_AWAIT_SET(connected, f.client.connect(f.endpoint));
    CHECK(connected == rpc::status_code::ok);
    CO2_AWAIT_SET(tag, served(f, "svc/Get", "a", nullptr));
    CHECK(tag == 'A');
    CO2_AWAIT_SET(tag, served(f, "svc/Get", "b", nullptr));
    CHECK(tag == 'B');
    CO2_AWAIT_SET(tag, served(f, "svc/Get", "", nullptr)); // The first registered.
    CHECK(tag == 'A');
    CO2_AWAIT_SET(tag, served(f, "svc/Get", "c", &result));
    CHECK(tag == '-' && result.code == rpc::status_code::unimplemented && result.not_executed);
    CO2_AWAIT_SET(tag, served(f, "svc/Get", "c", &result)); // Once refused, the method ID stays refused.
    CHECK(result.code == rpc::status_code::unimplemented);
    CO2_AWAIT_SET(tag, served(f, "svc/Only", "b", &result));
    CHECK(result.code == rpc::status_code::unimplemented);
    CO2_AWAIT_SET(tag, served(f, "svc/Only", "", nullptr));
    CHECK(tag == 'A');
    CO2_AWAIT_SET(tag, served(f, "svc/Any", "c", nullptr)); // The unlabelled binding takes any label.
    CHECK(tag == 'U');
    CO2_AWAIT_SET(tag, served(f, "svc/Any", "b", nullptr));
    CHECK(tag == 'B');
    CO2_AWAIT_SET(tag, served(f, "svc/Any", "", nullptr));
    CHECK(tag == 'U');
    CO2_AWAIT_SET(tag, served(f, "svc/Plain", "proto", nullptr));
    CHECK(tag == 'U');
    CO2_AWAIT_SET(opened, f.client.open(f.client.bind("svc/Chat", "b"), rpc::method_kind::bidirectional));
    CHECK(opened.code == rpc::status_code::ok);
    CO2_AWAIT_SET(read, opened.stream.read());
    CHECK(read.code == rpc::status_code::ok && read.message.size == 1 && read.message.data[0] == 'b');
    opened.stream = {};
    f.channel.reset(new rpc::channel(f.shard, std::vector<net::ip::tcp::endpoint>{f.endpoint}));
    CO2_AWAIT_SET(connected, f.channel->wait_ready());
    CHECK(connected == rpc::status_code::ok);
    CO2_AWAIT_SET(result, f.channel->call(f.channel->bind("svc/Get", "b"), {}, {answer.data(), 1}));
    CHECK(result.code == rpc::status_code::ok && answer[0] == 'B');
    CO2_RETURN();
}
CO2_END

// When either side does not take labels, a call goes where an unlabelled one would.
auto unlabelled_peer(fixture &f)
    CO2_BEG(net::task<>, (f), rpc::status_code connected; char tag = 0;) {
    CO2_AWAIT_SET(connected, f.client.connect(f.endpoint));
    CHECK(connected == rpc::status_code::ok);
    CO2_AWAIT_SET(tag, served(f, "svc/Get", "b", nullptr));
    CHECK(tag == 'A');
    CO2_AWAIT_SET(tag, served(f, "svc/Only", "b", nullptr));
    CHECK(tag == 'A');
    CO2_AWAIT_SET(tag, served(f, "svc/Any", "b", nullptr));
    CHECK(tag == 'U');
    CO2_RETURN();
}
CO2_END

void invalid_bindings() {
    net::io_context context{selected_backend, net::single_thread_hint};
    rpc::shard shard{context};
    tagged a{'A'};
    auto refused = [&](std::vector<rpc::method_binding> bindings) {
        try {
            rpc::server server{shard, std::move(bindings)};
        } catch (std::invalid_argument const &) {
            return true;
        }
        return false;
    };
    CHECK(refused({unary("svc/Get", a, "a"), unary("svc/Get", a, "a")}));
    CHECK(refused({unary("svc/Get", a, ""), unary("svc/Get", a, "")}));
    CHECK(refused({unary("svc/Get", a, "two words")}));
    CHECK(refused({unary("svc/Get", a, std::string(65, 'x'))}));
    CHECK(!refused({unary("svc/Get", a, std::string(64, 'x')), unary("svc/Get", a, "")}));
    rpc::client client{shard};
    bool thrown = false;
    try {
        client.bind("svc/Get", "\n");
    } catch (std::invalid_argument const &) {
        thrown = true;
    }
    CHECK(thrown);
    CHECK(client.bind("svc/Get", "a").index != client.bind("svc/Get", "b").index);
    CHECK(client.bind("svc/Get", "a").index == client.bind("svc/Get", "a").index);
    client.close();
    context.run();
}

} // namespace

int main(int argc, char **argv) {
    try {
        selected_backend = test_backend(argc, argv);
        if (!net::backend_available(selected_backend)) {
            std::cout << "SKIP backend unavailable\n";
            return 77;
        }
        { fixture f; f.run([&] { return labelled(f); }); std::cout << "PASS labels choose bindings\n"; }
        { fixture f{without_labels()}; f.run([&] { return unlabelled_peer(f); }); std::cout << "PASS server without labels\n"; }
        { fixture f{{}, without_labels()}; f.run([&] { return unlabelled_peer(f); }); std::cout << "PASS client without labels\n"; }
        invalid_bindings();
        std::cout << "PASS invalid and duplicate labels\n";
    } catch (std::exception const &error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
