#include "backend.hpp"
#include "check.hpp"

#include <rpc/service.hpp>
#include <rpc/typed.hpp>

#include <net/run_async.hpp>
#include <net/timeout.hpp>

#include <array>
#include <chrono>
#include <cstring>
#include <iostream>
#include <string>
#include <vector>

// Four little-endian bytes; a flag makes encode or decode refuse.
struct number {
    std::uint32_t value = 0;
    bool refuse = false;
};

// Text encoded in one pass into an upper bound, as the JSON codec does.
struct text_message {
    std::string text;
};

namespace rpc {
template <> struct codec<number> {
    static std::size_t size(number const &) { return 4; }
    static bool encode(number const &message, wire::mutable_bytes_view out) {
        if (message.refuse || out.size != 4) return false;
        std::memcpy(out.data, &message.value, 4);
        return true;
    }
    static bool decode(wire::bytes_view in, number &message) {
        if (message.refuse || in.size != 4) return false;
        std::memcpy(&message.value, in.data, 4);
        return true;
    }
};
} // namespace rpc

namespace {

using rpc::wire::bytes_view;
using std::chrono::milliseconds;

net::backend_kind selected_backend = net::default_backend_t::kind;

struct bounded_policy {
    template <class Message> static rpc::codec_ops const &operations();
};
template <> rpc::codec_ops const &bounded_policy::operations<text_message>() {
    static rpc::codec_ops const ops{
        [](void const *) -> std::size_t { throw std::logic_error{"size is not used by one-pass codecs"}; },
        [](void const *, rpc::wire::mutable_bytes_view) -> bool { throw std::logic_error{"exact encode is not used"}; },
        [](rpc::wire::bytes_view in, void *message) {
            static_cast<text_message *>(message)->text.assign(reinterpret_cast<char const *>(in.data), in.size);
            return true;
        },
        [](void const *message, rpc::wire::mutable_bytes_view out) -> rpc::wire::encode_result {
            auto const &text = static_cast<text_message const *>(message)->text;
            if (text.size() > out.size) return {rpc::wire::error::output_too_small, 0};
            std::memcpy(out.data, text.data(), text.size());
            return {rpc::wire::error::none, text.size()};
        },
        [](void const *message) { return static_cast<text_message const *>(message)->text.size() + 16; }};
    return ops;
}

rpc::method<number, number> const doubler{"typed/Double"};
rpc::method<text_message, text_message, bounded_policy> const shout{"typed/Shout"};
rpc::method<number, number> const counter{"typed/Count"};

auto double_it(rpc::server_context &context, number const &request, number &response)
    CO2_BEG(net::task<rpc::status_code>, (context, request, response)) {
    if (request.value == 13) {
        CHECK(context.set_trailer({reinterpret_cast<std::uint8_t const *>("unlucky"), 7}));
        CO2_RETURN(rpc::status_code::failed_precondition);
    }
    response.value = request.value * 2;
    response.refuse = request.value == 99; // Encoding the reply fails.
    CO2_RETURN(rpc::status_code::ok);
}
CO2_END

auto shout_it(text_message const &request, text_message &response)
    CO2_BEG(net::task<rpc::status_code>, (request, response)) {
    response.text = request.text + "!";
    CO2_RETURN(rpc::status_code::ok);
}
CO2_END

using count_stream = rpc::typed_server_stream<number, number, rpc::default_codec_policy>;
auto count_up(count_stream &stream)
    CO2_BEG(net::task<rpc::status_code>, (stream), number message; rpc::stream_read read; rpc::status_code wrote;) {
    for (;;) {
        CO2_AWAIT_SET(read, stream.read(message));
        if (read.code != rpc::status_code::ok) CO2_RETURN(read.code);
        if (read.ended) break;
        ++message.value;
        CO2_AWAIT_SET(wrote, stream.write(message));
        if (wrote != rpc::status_code::ok) CO2_RETURN(wrote);
    }
    CO2_RETURN(rpc::status_code::ok);
}
CO2_END

struct service {
    net::task<rpc::status_code> Double(rpc::server_context &context, number const &request, number &response) {
        return double_it(context, request, response);
    }
    net::task<rpc::status_code> Shout(rpc::server_context &, text_message const &request, text_message &response) {
        return shout_it(request, response);
    }
    net::task<rpc::status_code> Count(rpc::server_context &, count_stream &stream) { return count_up(stream); }
};

std::vector<rpc::method_binding> bindings(service &implementation) {
    rpc::method_limits small{};
    small.max_response_bytes = 4;
    return {rpc::bind_method(doubler, implementation, &service::Double, small),
            rpc::bind_method(shout, implementation, &service::Shout),
            rpc::bind_stream_method(counter, rpc::method_kind::bidirectional, implementation, &service::Count)};
}

struct fixture {
    fixture()
        : shard(context), server(shard, bindings(implementation)), client(shard),
          endpoint(server.listen({net::ip::address_v4::loopback(), 0})), call(client, doubler) {}
    template <class F> void run(F factory) {
        std::exception_ptr failure;
        auto close = [&] {
            client.close();
            server.close();
        };
        net::run_async(context.get_executor(), close, [&](std::exception_ptr error) {
            failure = error;
            close();
        })(factory);
        context.run();
        if (failure) std::rethrow_exception(failure);
        CHECK(server.stats().active_calls == 0 && server.stats().response_bytes == 0);
    }
    net::io_context context{selected_backend, net::single_thread_hint};
    rpc::shard shard;
    service implementation;
    rpc::server server;
    rpc::client client;
    net::ip::tcp::endpoint endpoint;
    rpc::bound_method<number, number> call;
};

auto unary(fixture &f)
    CO2_BEG(net::task<>, (f), rpc::status_code connected; rpc::call_result result; number request; number response;
            text_message text; text_message loud; rpc::response_trailer trailer; std::array<std::uint8_t, 64> storage{};) {
    CO2_AWAIT_SET(connected, f.client.connect(f.endpoint));
    CHECK(connected == rpc::status_code::ok);
    request.value = 21;
    CO2_AWAIT_SET(result, f.call(request, response));
    CHECK(result.code == rpc::status_code::ok && response.value == 42 && result.size == 4);

    request.refuse = true; // The client cannot encode: nothing is sent.
    CO2_AWAIT_SET(result, f.call(request, response));
    CHECK(result.code == rpc::status_code::invalid_argument && result.not_executed);
    request.refuse = false;

    request.value = 13; // The handler explains its error through the context.
    trailer.storage = {storage.data(), storage.size()};
    CO2_AWAIT_SET(result, f.call(request, response, nullptr, &trailer));
    CHECK(result.code == rpc::status_code::failed_precondition && trailer.message.size == 7);

    request.value = 99; // The server cannot encode its reply.
    CO2_AWAIT_SET(result, f.call(request, response));
    CHECK(result.code == rpc::status_code::internal);

    request.value = 1;
    response.refuse = true; // The client cannot decode the reply.
    CO2_AWAIT_SET(result, f.call(request, response));
    CHECK(result.code == rpc::status_code::internal);
    response.refuse = false;

    // A request the server cannot decode: send raw bytes of the wrong size.
    CO2_AWAIT_SET(result, f.client.call(f.client.bind(doubler.name), {storage.data(), 3}, {storage.data(), 4}));
    CHECK(result.code == rpc::status_code::invalid_argument);

    text.text = "hello";
    CO2_AWAIT_SET(result, rpc::bind(f.client, shout)(text, loud));
    CHECK(result.code == rpc::status_code::ok && loud.text == "hello!");
    CO2_RETURN();
}
CO2_END

auto streaming(fixture &f)
    CO2_BEG(net::task<>, (f), rpc::status_code connected; rpc::typed_open_result<number, number, rpc::default_codec_policy> opened;
            rpc::status_code wrote; rpc::stream_read read; rpc::call_result result; number message; unsigned i = 0;) {
    CO2_AWAIT_SET(connected, f.client.connect(f.endpoint));
    CHECK(connected == rpc::status_code::ok);
    CO2_AWAIT_SET(opened, (rpc::bound_stream<number, number>{f.client, counter, rpc::method_kind::bidirectional}()));
    CHECK(opened.code == rpc::status_code::ok && opened.stream);
    for (i = 0; i < 5; ++i) {
        message.value = i * 10;
        CO2_AWAIT_SET(wrote, opened.stream.write(message));
        CHECK(wrote == rpc::status_code::ok);
        CO2_AWAIT_SET(read, opened.stream.read(message));
        CHECK(read.code == rpc::status_code::ok && message.value == i * 10 + 1);
    }
    message.refuse = true;
    CO2_AWAIT_SET(wrote, opened.stream.write(message));
    CHECK(wrote == rpc::status_code::invalid_argument); // Refused before anything was queued.
    CO2_AWAIT_SET(wrote, opened.stream.writes_done());
    CO2_AWAIT_SET(read, opened.stream.read(message));
    CHECK(read.ended);
    CO2_AWAIT_SET(result, opened.stream.finish());
    CHECK(result.code == rpc::status_code::ok);
    CO2_RETURN();
}
CO2_END

using users = rpc::service_contract<rpc::default_codec_policy, rpc::unary_method<number, number>>;

auto contract(fixture &f, users const *numbers)
    CO2_BEG(net::task<>, (f, numbers), rpc::status_code connected; rpc::call_result result; number request; number response;) {
    CO2_AWAIT_SET(connected, f.client.connect(f.endpoint));
    CHECK(connected == rpc::status_code::ok);
    request.value = 5;
    CO2_AWAIT_SET(result, rpc::bind_service(f.client, *numbers).call<0>(request, response));
    CHECK(result.code == rpc::status_code::ok && response.value == 10);
    CO2_RETURN();
}
CO2_END

auto through_channel(rpc::channel &channel)
    CO2_BEG(net::task<>, (channel), rpc::status_code ready; rpc::call_result result; number request; number response;) {
    CO2_AWAIT_SET(ready, channel.wait_ready(std::chrono::steady_clock::now() + std::chrono::seconds{5}));
    CHECK(ready == rpc::status_code::ok);
    request.value = 8;
    CO2_AWAIT_SET(result, rpc::bind(channel, doubler)(request, response));
    CHECK(result.code == rpc::status_code::ok && response.value == 16);
    channel.close();
    CO2_RETURN();
}
CO2_END

template <class Function> void rejects(Function function) {
    bool failed = false;
    try {
        function();
    } catch (std::invalid_argument const &) {
        failed = true;
    }
    CHECK(failed);
}

} // namespace

int main(int argc, char **argv) {
    try {
        selected_backend = test_backend(argc, argv);
        if (!net::backend_available(selected_backend)) {
            std::cout << "SKIP backend unavailable\n";
            return 77;
        }
        { fixture f; f.run([&] { return unary(f); }); std::cout << "PASS typed unary, codec failures, trailer\n"; }
        { fixture f; f.run([&] { return streaming(f); }); std::cout << "PASS typed bidirectional stream\n"; }
        {
            using method = rpc::unary_method<number, number>;
            rejects([] { rpc::make_service_contract("", rpc::default_codec_policy{}, method{"a"}); });
            rejects([] { rpc::make_service_contract("s", rpc::default_codec_policy{}, method{"a"}, method{"a"}); });
            rejects([] { rpc::make_service_contract("s", rpc::default_codec_policy{}, method{std::string{"a\0b", 3}}); });
            auto const numbers = rpc::make_service_contract("numbers", rpc::default_codec_policy{}, method{"typed/Double"});
            fixture f;
            f.run([&] { return contract(f, &numbers); });
            service other;
            CHECK(numbers.bindings(other, {{}}, &service::Double).size() == 1);
            using function = net::task<rpc::status_code> (service::*)(rpc::server_context &, number const &, number &);
            rejects([&] { numbers.bindings(other, {{}}, function{}); });
            std::cout << "PASS service contract\n";
        }
        {
            fixture f;
            rpc::channel channel{f.shard, {f.endpoint}};
            f.run([&] { return through_channel(channel); });
            std::cout << "PASS typed calls through a channel\n";
        }
    } catch (std::exception const &error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
