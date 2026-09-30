#include <rpc/json.hpp>
#include <rpc/channel.hpp>
#include <net/run_async.hpp>
#include "check.hpp"
#include <array>
#include <iostream>

struct JsonMessage { std::string text; bool retry = false; };
RPC_JSON_FIELDS(JsonMessage, text, retry);
struct JsonReply { std::string text; };
RPC_JSON_FIELDS(JsonReply, text);
namespace {
unsigned encodes = 0;
struct measured_policy {
    template <class T> static rpc::codec_ops const &operations() {
        static rpc::codec_ops const ops = [] {
            auto result = rpc::json_codec_policy::operations<T>();
            result.encode_bounded = [](void const *p, rpc::wire::mutable_bytes_view out) {
                if (std::is_same<T, JsonMessage>::value) ++encodes;
                return rpc::json_codec<T>::encode_bounded(*static_cast<T const *>(p), out);
            }; return result;
        }(); return ops;
    }
};
using operation_type = rpc::method<JsonMessage, JsonReply, measured_policy>;
struct service {
    JsonMessage *original = nullptr; unsigned invoked = 0; unsigned retries = 0;
    std::string first;
    net::task<rpc::status_code> Echo(rpc::server_context &, JsonMessage const &, JsonReply &);
};
auto echo(service *self, JsonMessage const *request, JsonReply *reply)
    CO2_BEG(net::task<rpc::status_code>, (self, request, reply)) {
    ++self->invoked;
    if (request->retry) {
        ++self->retries;
        if (self->retries == 1) {
            self->first = request->text;
            self->original->text = "mutated after first attempt";
            CO2_RETURN(rpc::status_code::unavailable);
        }
        CHECK(request->text == self->first);
    }
    reply->text = request->text; CO2_RETURN(rpc::status_code::ok);
}
CO2_END
net::task<rpc::status_code> service::Echo(rpc::server_context &, JsonMessage const &req, JsonReply &resp) { return echo(this, &req, &resp); }
auto run(rpc::client *client, rpc::channel *channel, net::ip::tcp::endpoint endpoint, service *impl,
         rpc::bound_method<JsonMessage, JsonReply, measured_policy> bound, operation_type operation, bool streaming)
    CO2_BEG(net::task<>, (client, channel, endpoint, impl, bound, operation, streaming),
        rpc::status_code ready; rpc::call_result result; rpc::call_options options;
        JsonMessage request; JsonReply response; unsigned calls = 0; std::string raw; std::array<std::uint8_t, 64> bytes{};) {
    options.timeout = std::chrono::seconds{2};
    if (channel) { CO2_AWAIT_SET(ready, channel->warmup(options)); }
    else { CO2_AWAIT_SET(ready, client->connect(endpoint)); }
    CHECK(ready == rpc::status_code::ok);
    request.text.assign(180, 'x'); encodes = 0;
    CO2_AWAIT_SET(result, bound(request, response, options));
    CHECK(result.code == rpc::status_code::ok && response.text == request.text && encodes == 1);
    if (streaming && !channel) {
        // upper_bound exceeds the entire 1024-byte budget; actual JSON fits
        // with its descriptor, so conservative capacity must leave room for it.
        CHECK(client->stats().active_calls == 0);
    }
    if (streaming) {
        request.text.assign(600, 'x'); encodes = 0;
        CO2_AWAIT_SET(result, bound(request, response, options));
        CHECK(result.code == rpc::status_code::ok && response.text == request.text && encodes == 1);
    }
    calls = impl->invoked; raw = R"({"text":"a","text":"b"})";
    CO2_AWAIT_SET(result, client->call(operation.name, {reinterpret_cast<std::uint8_t const *>(raw.data()), raw.size()}, {bytes.data(), bytes.size()}, options));
    CHECK(result.code == rpc::status_code::invalid_argument && impl->invoked == calls);
    if (channel) {
        impl->original = &request; request.text = "snapshot"; request.retry = true; encodes = 0;
        options.retry.max_attempts = 2; options.retry.initial_backoff = std::chrono::milliseconds{0};
        // The convenience call must consume the same idempotency contract as bind.
        CO2_AWAIT_SET(result, rpc::call(*client, operation, request, response, options));
        CHECK(result.code == rpc::status_code::ok && result.attempts == 2 && response.text == "snapshot" && request.text != response.text && encodes == 1);
        CHECK(channel->metrics().replay_bytes_in_use == 0);
        options.retry.max_attempts = 1; request.retry = false;
        request.text.assign(6000, 'x'); encodes = 0;
        CO2_AWAIT_SET(result, bound(request, response, options)); CHECK(result.code == rpc::status_code::resource_exhausted);
        CHECK(channel->metrics().replay_bytes_in_use == 0);
    }
    options.deadline = rpc::clock::now(); encodes = 0;
    CO2_AWAIT_SET(result, bound(request, response, options));
    CHECK(result.code == rpc::status_code::deadline_exceeded && encodes == 0);
    CO2_RETURN();
}
CO2_END
void round_trip(bool use_channel, bool streaming) {
    net::io_context ctx{net::epoll, net::single_thread_hint}; service impl;
    operation_type operation{"json/Echo", rpc::idempotency::no_side_effects};
    rpc::server_options server_options; server_options.connection.receive.max_frame_size = streaming ? 256 : 512;
    server_options.connection.receive.max_message_size = 4096;
    if (streaming) { server_options.connection.receive.features |= rpc::wire::streaming; server_options.connection.receive.initial_stream_window = 4096; }
    rpc::server_builder builder{ctx, server_options}; builder.add(operation, impl, &service::Echo, {streaming ? 4096U : 256U, 64}); auto server = builder.build();
    auto endpoint = server->listen({net::ip::address_v4::loopback(), 0});
    rpc::client_options client_options; client_options.connection.receive = server_options.connection.receive;
    client_options.request_bytes = 1024;
    std::unique_ptr<rpc::client> client; rpc::channel *channel = nullptr;
    if (use_channel) {
        rpc::channel_options options; options.connection = client_options; options.resolve = rpc::make_static_resolver({endpoint});
        options.max_connections = 1; options.replay_bytes = 16384;
        auto owned = rpc::make_channel(ctx, options); channel = owned.get(); client = std::move(owned);
    } else client = rpc::make_client(ctx, client_options);
    std::exception_ptr error; bool complete = false;
    auto close = [&] { client->close(); server->close(); };
    net::run_async(ctx.get_executor(), [&] { complete = true; close(); }, [&](std::exception_ptr e) { error = e; close(); })
        ([&] { return run(client.get(), channel, endpoint, &impl, rpc::bind(*client, operation), operation, streaming); });
    ctx.run(); if (error) std::rethrow_exception(error); CHECK(complete);
    CHECK(client->stats().active_calls == 0 && client->stats().request_bytes_in_use == 0);
    CHECK(server->stats().active_calls == 0 && server->stats().response_bytes_in_use == 0);
}
}
int main() {
    try { for (bool c : {false, true}) for (bool s : {false, true}) round_trip(c, s); std::cout << "PASS JSON direct/channel, retries, streamed profile and budgets\n"; }
    catch (std::exception const &e) { std::cerr << e.what() << '\n'; return 1; }
}
