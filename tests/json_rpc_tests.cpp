#include "check.hpp"

#include <rpc/channel.hpp>
#include <rpc/json.hpp>

#include <net/run_async.hpp>

#include <array>
#include <iostream>
#include <string>

struct JsonMessage { std::string text; };
RPC_JSON_FIELDS(JsonMessage, text);
struct JsonReply { std::string text; };
RPC_JSON_FIELDS(JsonReply, text);
struct MemoryLimited { std::string text; };
RPC_JSON_FIELDS(MemoryLimited, text);
namespace rpc {
template <> struct json_options<MemoryLimited> {
    static json_limits limits() noexcept {
        json_limits l;
        l.max_decoded_bytes = 8;
        l.max_string_bytes = 1024;
        l.max_scratch_bytes = 512;
        return l;
    }
};
} // namespace rpc

namespace {

unsigned encodes = 0;
struct measured_policy {
    template <class T> static rpc::codec_ops const &operations() {
        static rpc::codec_ops const ops = [] {
            auto result = rpc::json_codec_policy::operations<T>();
            result.encode_bounded = [](void const *p, rpc::wire::mutable_bytes_view out) {
                if (std::is_same<T, JsonMessage>::value) ++encodes;
                return rpc::json_codec<T>::encode_bounded(*static_cast<T const *>(p), out);
            };
            return result;
        }();
        return ops;
    }
};

using echo_method = rpc::method<JsonMessage, JsonReply, measured_policy>;
echo_method const echo_operation{"json/Echo"};
rpc::json_method<MemoryLimited, JsonReply> const limited_operation{"json/Limited"};

constexpr std::uint32_t frame_limit = 4096;
constexpr std::size_t response_limit = 2048;

struct service {
    unsigned invoked = 0;
    net::task<rpc::status_code> Echo(rpc::server_context &, JsonMessage const &request, JsonReply &reply);
    net::task<rpc::status_code> Limited(rpc::server_context &, MemoryLimited const &, JsonReply &reply);
};

auto echo(service *self, std::string const *text, JsonReply *reply)
    CO2_BEG(net::task<rpc::status_code>, (self, text, reply)) {
    ++self->invoked;
    reply->text = *text;
    CO2_RETURN(rpc::status_code::ok);
}
CO2_END

net::task<rpc::status_code> service::Echo(rpc::server_context &, JsonMessage const &request, JsonReply &reply) {
    return echo(this, &request.text, &reply);
}
net::task<rpc::status_code> service::Limited(rpc::server_context &, MemoryLimited const &request, JsonReply &reply) {
    return echo(this, &request.text, &reply);
}

std::vector<rpc::method_binding> bindings(service &implementation) {
    std::vector<rpc::method_binding> result;
    result.push_back(rpc::bind_method(echo_operation, implementation, &service::Echo, {response_limit, 64}));
    result.push_back(rpc::bind_method(limited_operation, implementation, &service::Limited));
    return result;
}

rpc::connection_options small_frames() {
    rpc::connection_options options;
    options.receive.max_frame_size = frame_limit;
    options.receive.max_message_size = frame_limit;
    return options;
}

auto run(rpc::client *client, rpc::channel *channel, net::ip::tcp::endpoint endpoint, service *implementation,
         rpc::call_target target, rpc::bound_method<JsonMessage, JsonReply, measured_policy> call)
    CO2_BEG(net::task<>, (client, channel, endpoint, implementation, target, call), rpc::status_code ready;
            rpc::call_result result; rpc::call_spec spec; JsonMessage request; JsonReply response;
            MemoryLimited limited; unsigned calls = 0; std::string raw; std::array<std::uint8_t, 64> bytes{};) {
    if (channel) {
        CO2_AWAIT_SET(ready, channel->wait_ready(rpc::clock::now() + std::chrono::seconds{5}));
    } else {
        CO2_AWAIT_SET(ready, client->connect(endpoint));
    }
    CHECK(ready == rpc::status_code::ok);

    // Encoded once, straight into the frame and the reply, though the JSON
    // bounds (escapes counted at their worst) exceed the room in both.
    request.text.assign(1500, 'x');
    encodes = 0;
    CO2_AWAIT_SET(result, call(request, response));
    CHECK(result.code == rpc::status_code::ok && response.text == request.text && encodes == 1);

    // Neither a request beyond the frame nor a reply beyond the method's
    // limit is sent.
    request.text.assign(frame_limit, 'x');
    calls = implementation->invoked;
    CO2_AWAIT_SET(result, call(request, response));
    CHECK(result.code == rpc::status_code::resource_exhausted && result.not_executed);
    CHECK(implementation->invoked == calls);
    request.text.assign(response_limit, 'x');
    CO2_AWAIT_SET(result, call(request, response));
    CHECK(result.code == rpc::status_code::internal && implementation->invoked == calls + 1);

    // A duplicate key is rejected before the handler runs.
    calls = implementation->invoked;
    raw = R"({"text":"a","text":"b"})";
    CO2_AWAIT_SET(result, target.call(target.bind(echo_operation.name), {raw.data(), raw.size()},
                                      {bytes.data(), bytes.size()}, nullptr, nullptr));
    CHECK(result.code == rpc::status_code::invalid_argument && implementation->invoked == calls);

    // Running out of decode memory is not the caller's mistake.
    limited.text = "123456789";
    CO2_AWAIT_SET(result, rpc::bind(target, limited_operation)(limited, response));
    CHECK(result.code == rpc::status_code::resource_exhausted && implementation->invoked == calls);

    // An expired deadline fails before anything is encoded.
    request.text = "late";
    spec.deadline = rpc::clock::now();
    encodes = 0;
    CO2_AWAIT_SET(result, call(request, response, &spec));
    CHECK(result.code == rpc::status_code::deadline_exceeded && encodes == 0);
    if (channel) channel->close();
    CO2_RETURN();
}
CO2_END

void round_trip(bool through_channel) {
    net::io_context context{net::default_backend, net::single_thread_hint};
    rpc::shard shard{context};
    service implementation;
    rpc::server_options server_options;
    server_options.connection = small_frames();
    rpc::server server{shard, bindings(implementation), server_options};
    auto const endpoint = server.listen({net::ip::address_v4::loopback(), 0});
    rpc::client_options client_options;
    client_options.connection = small_frames();
    rpc::client client{shard, client_options};
    rpc::channel_options channel_options;
    channel_options.client = client_options;
    rpc::channel channel{shard, {endpoint}, channel_options};
    std::exception_ptr failure;
    auto close = [&] {
        channel.close();
        client.close();
        server.close();
    };
    net::run_async(context.get_executor(), close, [&](std::exception_ptr error) {
        failure = error;
        close();
    })([&] {
        auto const target = through_channel ? rpc::call_target{channel} : rpc::call_target{client};
        return run(&client, through_channel ? &channel : nullptr, endpoint, &implementation, target,
                   rpc::bind(target, echo_operation));
    });
    context.run();
    if (failure) std::rethrow_exception(failure);
    CHECK(server.stats().active_calls == 0 && server.stats().response_bytes == 0);
}

} // namespace

int main() {
    try {
        round_trip(false);
        round_trip(true);
        std::cout << "PASS JSON calls through a client and a channel, bounds and strict input\n";
    } catch (std::exception const &error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
