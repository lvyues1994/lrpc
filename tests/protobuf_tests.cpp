#include "backend.hpp"
#include "check.hpp"
#include "echo.rpc.hpp"
#include "nested/lite.rpc.hpp"

#include <rpc/channel.hpp>

#include <net/run_async.hpp>
#include <net/timeout.hpp>

#include <array>
#include <climits>
#include <iostream>
#include <memory>
#include <thread>

namespace {
namespace w = rpc::wire;
net::backend_kind selected_backend = net::default_backend_t::kind;

using echo_request = demo::EchoRequest;
using echo_reply = demo::EchoReply;
using item = test::messages::Container::Item;
using mixed_stream = rpc::typed_server_stream<echo_request, echo_reply, rpc::default_codec_policy>;
using mixed_open = rpc::typed_open_result<echo_request, echo_reply, rpc::default_codec_policy>;

void codecs() {
    echo_request request, decoded;
    request.set_payload(std::string("a\0b", 3));
    request.set_marker(0);
    request.add_numbers(7);
    request.add_numbers(-4);
    auto const size = rpc::codec<echo_request>::size(request);
    std::vector<std::uint8_t> encoded(size);
    CHECK(!rpc::codec<echo_request>::encode(request, {encoded.data(), size - 1}));
    CHECK(rpc::codec<echo_request>::encode(request, {encoded.data(), size}));
    CHECK(rpc::codec<echo_request>::decode({encoded.data(), size}, decoded));
    CHECK(decoded.payload() == request.payload() && decoded.has_marker() && decoded.numbers(1) == -4);
    CHECK(!rpc::codec<echo_request>::decode({encoded.data(), size - 1}, decoded));
    std::uint8_t const malformed[] = {0x0a, 0xff};
    CHECK(!rpc::codec<echo_request>::decode({malformed, sizeof(malformed)}, decoded));
    CHECK(!rpc::codec<echo_request>::decode({encoded.data(), static_cast<std::size_t>(INT_MAX) + 1}, decoded));
    CHECK(!rpc::codec<echo_request>::decode({nullptr, 1}, decoded));
    CHECK(!rpc::codec<echo_request>::encode(request, {encoded.data(), static_cast<std::size_t>(INT_MAX) + 1}));
    // Unknown field 100 (varint) must survive parsing without rejection.
    encoded.insert(encoded.end(), {0xa0, 0x06, 1});
    CHECK(rpc::codec<echo_request>::decode({encoded.data(), encoded.size()}, decoded));
    echo_reply empty;
    CHECK(rpc::codec<echo_reply>::size(empty) == 0);
    CHECK(rpc::codec<echo_reply>::encode(empty, {}));
    CHECK(rpc::codec<echo_reply>::decode({}, empty));
    item required;
    rpc::codec<item>::size(required);
    CHECK(!rpc::codec<item>::encode(required, {}));
    CHECK(!rpc::codec<item>::decode({}, required));
    required.set_value("nested");
    encoded.resize(rpc::codec<item>::size(required));
    CHECK(rpc::codec<item>::encode(required, {encoded.data(), encoded.size()}));
    rpc::call_arena arena;
    auto *on_arena = google::protobuf::Arena::CreateMessage<echo_reply>(&arena.get());
    CHECK(on_arena->GetArena() == &arena.get());
    CHECK(arena.get().SpaceAllocated() == 2048);
}

struct echo final : demo::EchoService {
    net::task<rpc::status_code> RoundTrip(rpc::server_context &, echo_request const &, echo_reply &) override;
    net::task<rpc::status_code> Required(rpc::server_context &, item const &, item &) override;
    net::task<rpc::status_code> delete_(rpc::server_context &context, echo_request const &request,
                                        echo_reply &reply) override {
        return RoundTrip(context, request, reply);
    }
    unsigned entered = 0, exited = 0;
    bool wait = false, bad_response = false, throws = false;
    rpc::status_code status = rpc::status_code::ok;
};

auto round_trip(echo *self, rpc::server_context *context, echo_request const *request, echo_reply *response)
    CO2_BEG(net::task<rpc::status_code>, (self, context, request, response), net::io_result<> waited;) {
    ++self->entered;
    CHECK(request->GetArena() != nullptr && request->GetArena() == response->GetArena());
    if (self->throws) {
        ++self->exited;
        throw std::runtime_error{"typed handler failure"};
    }
    if (context->metadata.count) {
        auto const entry = w::decode_metadata_entry(context->metadata.entries);
        CHECK(entry.code == w::error::none);
        CHECK(context->set_trailer({}, {&entry.value, 1}));
    }
    if (self->wait) CO2_AWAIT_SET(waited, net::delay(std::chrono::milliseconds{30}));
    // Still valid when the server object is gone: the call owns both.
    CHECK(request->GetArena() == response->GetArena());
    response->set_payload(request->payload());
    ++self->exited;
    CO2_RETURN(waited.ec ? rpc::status_code::cancelled : self->status);
}
CO2_END

net::task<rpc::status_code> echo::RoundTrip(rpc::server_context &context, echo_request const &request,
                                            echo_reply &response) {
    return round_trip(this, &context, &request, &response);
}

auto required_reply(echo *self, item const *request, item *response)
    CO2_BEG(net::task<rpc::status_code>, (self, request, response)) {
    ++self->entered;
    CHECK(request->GetArena() != nullptr && request->GetArena() == response->GetArena());
    if (!self->bad_response) response->set_value(request->value());
    ++self->exited;
    CO2_RETURN(rpc::status_code::ok);
}
CO2_END

net::task<rpc::status_code> echo::Required(rpc::server_context &, item const &request, item &response) {
    return required_reply(this, &request, &response);
}

auto lite_reply(demo::lite::Message const *request, demo::lite::Message *response)
    CO2_BEG(net::task<rpc::status_code>, (request, response)) {
    CHECK(request->GetArena() != nullptr && request->GetArena() == response->GetArena());
    response->set_payload(request->payload());
    CO2_RETURN(rpc::status_code::ok);
}
CO2_END

struct lite_echo final : demo::lite::LiteService {
    net::task<rpc::status_code> Echo(rpc::server_context &, demo::lite::Message const &request,
                                     demo::lite::Message &response) override {
        return lite_reply(&request, &response);
    }
};

auto unary(echo_request const *request, echo_reply *reply)
    CO2_BEG(net::task<rpc::status_code>, (request, reply)) {
    reply->set_payload(request->payload());
    CO2_RETURN(rpc::status_code::ok);
}
CO2_END

auto upload(mixed_stream *stream)
    CO2_BEG(net::task<rpc::status_code>, (stream), echo_request request; echo_reply response; rpc::stream_read read;
            rpc::status_code sent;) {
    for (;;) {
        CO2_AWAIT_SET(read, stream->read(request));
        if (read.code != rpc::status_code::ok) CO2_RETURN(read.code);
        if (read.ended) break;
        response.mutable_payload()->append(request.payload());
    }
    CO2_AWAIT_SET(sent, stream->write(response));
    CO2_RETURN(sent);
}
CO2_END

auto download(mixed_stream *stream)
    CO2_BEG(net::task<rpc::status_code>, (stream), echo_request request; echo_reply response; rpc::stream_read read;
            rpc::status_code sent;) {
    CO2_AWAIT_SET(read, stream->read(request));
    if (read.code != rpc::status_code::ok || read.ended) CO2_RETURN(rpc::status_code::invalid_argument);
    response.set_payload(request.payload());
    CO2_AWAIT_SET(read, stream->read(request));
    if (!read.ended) CO2_RETURN(rpc::status_code::invalid_argument);
    CO2_AWAIT_SET(sent, stream->write(response));
    if (sent != rpc::status_code::ok) CO2_RETURN(sent);
    CO2_AWAIT_SET(sent, stream->write(response));
    CO2_RETURN(sent);
}
CO2_END

auto chat(mixed_stream *stream)
    CO2_BEG(net::task<rpc::status_code>, (stream), echo_request request; echo_reply response; rpc::stream_read read;
            rpc::status_code sent;) {
    for (;;) {
        CO2_AWAIT_SET(read, stream->read(request));
        if (read.code != rpc::status_code::ok) CO2_RETURN(read.code);
        if (read.ended) break;
        response.set_payload(request.payload());
        CO2_AWAIT_SET(sent, stream->write(response));
        if (sent != rpc::status_code::ok) CO2_RETURN(sent);
    }
    CO2_RETURN(rpc::status_code::ok);
}
CO2_END

struct mixed final : demo::MixedService {
    net::task<rpc::status_code> Unary(rpc::server_context &, echo_request const &request, echo_reply &reply) override {
        return unary(&request, &reply);
    }
    net::task<rpc::status_code> Upload(rpc::server_context &, mixed_stream &stream) override { return upload(&stream); }
    net::task<rpc::status_code> Download(rpc::server_context &, mixed_stream &stream) override {
        return download(&stream);
    }
    net::task<rpc::status_code> Chat(rpc::server_context &context, mixed_stream &stream) override {
        CHECK(context.metadata.count == 1);
        w::metadata_entry entry{{reinterpret_cast<std::uint8_t const *>("stream"), 6},
                                {reinterpret_cast<std::uint8_t const *>("ok"), 2}};
        CHECK(stream.set_trailer({}, {&entry, 1}));
        return chat(&stream);
    }
};

auto malformed_reply(rpc::response_writer *response) CO2_BEG(net::task<rpc::status_code>, (response)) {
    std::uint8_t const bytes[] = {0x0a, 0xff};
    CO2_RETURN(response->assign({bytes, sizeof(bytes)}) ? rpc::status_code::ok : rpc::status_code::internal);
}
CO2_END

struct malformed final : rpc::method_handler {
    net::task<rpc::status_code> invoke(rpc::server_context &, w::bytes_view, rpc::response_writer &response) override {
        return malformed_reply(&response);
    }
};

std::vector<rpc::method_binding> bindings(echo &service, lite_echo &lite, mixed &streams, malformed &raw) {
    demo::EchoLimits limits;
    limits.RoundTrip = limits.Required = limits.delete_ = {256, 64};
    demo::MixedLimits mixed_limits;
    mixed_limits.Unary = mixed_limits.Upload = mixed_limits.Download = mixed_limits.Chat = {16384, 64};
    auto result = demo::Echo_bindings(service, limits);
    for (auto &binding : demo::lite::Lite_bindings(lite, {{256, 2}})) result.push_back(std::move(binding));
    for (auto &binding : demo::Mixed_bindings(streams, mixed_limits)) result.push_back(std::move(binding));
    result.push_back({"test/Malformed", &raw, 16});
    return result;
}

struct fixture {
    fixture()
        : shard(context), server(new rpc::server{shard, bindings(service, lite, streams, raw)}), client(shard),
          endpoint(server->listen({net::ip::address_v4::loopback(), 0})), channel(shard, {endpoint}), stub(client),
          lite_stub(client), mixed_stub(channel) {}
    template <class Factory> void run(Factory factory) {
        std::exception_ptr error;
        auto close = [&] {
            channel.close();
            client.close();
            if (server) server->close();
        };
        net::run_async(context.get_executor(), close, [&](std::exception_ptr value) {
            error = value;
            close();
        })(factory);
        context.run();
        if (error) std::rethrow_exception(error);
        CHECK(service.entered == service.exited);
        if (server) CHECK(server->stats().active_calls == 0 && server->stats().response_bytes == 0);
    }
    net::io_context context{selected_backend, net::single_thread_hint};
    rpc::shard shard;
    echo service{};
    lite_echo lite{};
    mixed streams{};
    malformed raw{};
    std::unique_ptr<rpc::server> server;
    rpc::client client;
    net::ip::tcp::endpoint endpoint;
    rpc::channel channel;
    demo::EchoStub stub;
    demo::lite::LiteStub lite_stub;
    demo::MixedStub mixed_stub;
};

auto integration(fixture *f)
    CO2_BEG(net::task<>, (f), rpc::status_code connected; rpc::call_result result; echo_request request;
            echo_reply response; google::protobuf::Arena caller_arena; echo_reply *arena_reply = nullptr;
            item required; item required_response; demo::lite::Message lite_request; demo::lite::Message lite_response;
            std::array<std::uint8_t, 64> storage{}; std::array<std::uint8_t, 16> raw{}; std::uint8_t key = 1;
            std::uint8_t value = 2; w::metadata_entry entry{}; rpc::call_spec spec; rpc::response_trailer trailer;
            unsigned before = 0;) {
    CO2_AWAIT_SET(connected, f->client.connect(f->endpoint));
    CHECK(connected == rpc::status_code::ok);
    request.set_payload(std::string("hello\0arena", 11));
    entry = {{&key, 1}, {&value, 1}};
    spec.metadata = {&entry, 1};
    trailer.storage = {storage.data(), storage.size()};
    f->service.wait = true;
    CO2_AWAIT_SET(result, f->stub.RoundTrip(request, response, &spec, &trailer));
    CHECK(result.code == rpc::status_code::ok && response.payload() == request.payload());
    CHECK(trailer.metadata.count == 1 && w::decode_metadata_entry(trailer.metadata.entries).value.value.data[0] == 2);
    f->service.wait = false;

    // The reply decodes into the caller's own message, on the caller's Arena.
    arena_reply = google::protobuf::Arena::CreateMessage<echo_reply>(&caller_arena);
    CO2_AWAIT_SET(result, f->stub.delete_(request, *arena_reply));
    CHECK(result.code == rpc::status_code::ok && arena_reply->GetArena() == &caller_arena &&
          arena_reply->payload() == request.payload());
    lite_request.set_payload("lite");
    CO2_AWAIT_SET(result, f->lite_stub.Echo(lite_request, lite_response));
    CHECK(result.code == rpc::status_code::ok && lite_response.payload() == "lite");

    // proto2 required fields: neither side sends an incomplete message.
    required.set_value("nested");
    CO2_AWAIT_SET(result, f->stub.Required(required, required_response));
    CHECK(result.code == rpc::status_code::ok && required_response.value() == "nested");
    before = f->service.entered;
    required.clear_value();
    CO2_AWAIT_SET(result, f->stub.Required(required, required_response));
    CHECK(result.code == rpc::status_code::invalid_argument && result.not_executed && f->service.entered == before);
    CO2_AWAIT_SET(result, f->client.call(f->client.bind("demo.Echo/Required"), {}, {raw.data(), raw.size()}));
    CHECK(result.code == rpc::status_code::invalid_argument && f->service.entered == before);
    raw[0] = 0x0a;
    raw[1] = 0xff;
    CO2_AWAIT_SET(result, f->client.call(f->client.bind("demo.Echo/RoundTrip"), {raw.data(), 2}, {}));
    CHECK(result.code == rpc::status_code::invalid_argument && f->service.entered == before);
    required.set_value("valid");
    f->service.bad_response = true;
    CO2_AWAIT_SET(result, f->stub.Required(required, required_response));
    CHECK(result.code == rpc::status_code::internal);

    // An error leaves the reply untouched but still carries the trailer.
    f->service.status = rpc::status_code::permission_denied;
    response.set_payload("unchanged");
    trailer.metadata = {};
    CO2_AWAIT_SET(result, f->stub.RoundTrip(request, response, &spec, &trailer));
    CHECK(result.code == rpc::status_code::permission_denied && response.payload() == "unchanged" &&
          trailer.metadata.count == 1);
    f->service.status = rpc::status_code::ok;
    request.set_payload(std::string(300, 'x')); // The reply exceeds its 256-byte limit.
    CO2_AWAIT_SET(result, f->stub.RoundTrip(request, response));
    CHECK(result.code == rpc::status_code::internal);
    request.clear_payload();
    CO2_AWAIT_SET(result, f->stub.RoundTrip(request, response));
    CHECK(result.code == rpc::status_code::ok && response.payload().empty());
    f->service.throws = true;
    CO2_AWAIT_SET(result, f->stub.RoundTrip(request, response));
    CHECK(result.code == rpc::status_code::internal);
    f->service.throws = false;
    CO2_AWAIT_SET(result, rpc::bind(f->client, rpc::method<echo_request, echo_reply>{"test/Malformed"})(request, response));
    CHECK(result.code == rpc::status_code::internal); // The reply does not decode.
    CO2_RETURN();
}
CO2_END

auto round_trip_call(demo::EchoStub const *stub, echo_request const *request, echo_reply *response)
    CO2_BEG(net::task<rpc::call_result>, (stub, request, response), rpc::call_result result;) {
    CO2_AWAIT_SET(result, stub->RoundTrip(*request, *response));
    CO2_RETURN(result);
}
CO2_END

auto cancellation(fixture *f)
    CO2_BEG(net::task<>, (f), rpc::status_code connected; rpc::call_result result; echo_request request;
            echo_reply response; rpc::call_spec spec; net::stop_source stop; bool done = false;
            std::exception_ptr failure;) {
    CO2_AWAIT_SET(connected, f->client.connect(f->endpoint));
    CHECK(connected == rpc::status_code::ok);
    request.set_payload("alive after suspension");
    f->service.wait = true;
    spec.timeout = std::chrono::milliseconds{3};
    CO2_AWAIT_SET(result, f->stub.RoundTrip(request, response, &spec));
    CHECK(result.code == rpc::status_code::deadline_exceeded);
    net::run_async(f->context.get_executor(), stop.get_token(), nullptr,
                   [&](rpc::call_result value) {
                       result = value;
                       done = true;
                   },
                   [&](std::exception_ptr error) {
                       failure = error;
                       done = true;
                   })([&] { return round_trip_call(&f->stub, &request, &response); });
    CO2_AWAIT(net::delay(std::chrono::milliseconds{2}));
    {
        std::thread requester{[&] { stop.request_stop(); }};
        requester.join();
    }
    while (!done) CO2_AWAIT(net::delay(std::chrono::milliseconds{1}));
    if (failure) std::rethrow_exception(failure);
    CHECK(result.code == rpc::status_code::cancelled);
    while (f->service.exited != f->service.entered) CO2_AWAIT(net::delay(std::chrono::milliseconds{1}));
    CO2_RETURN();
}
CO2_END

// The handler outlives the server object: the call keeps its adapter, its
// messages and its connection state.
auto late_handler(fixture *f)
    CO2_BEG(net::task<>, (f), rpc::status_code connected; rpc::call_result result; echo_request request;
            echo_reply response; bool done = false; std::exception_ptr failure;) {
    CO2_AWAIT_SET(connected, f->client.connect(f->endpoint));
    CHECK(connected == rpc::status_code::ok);
    request.set_payload("Arena survives the server");
    f->service.wait = true;
    net::run_async(f->context.get_executor(),
                   [&](rpc::call_result value) {
                       result = value;
                       done = true;
                   },
                   [&](std::exception_ptr error) {
                       failure = error;
                       done = true;
                   })([&] { return round_trip_call(&f->stub, &request, &response); });
    while (f->service.entered == 0) CO2_AWAIT(net::delay(std::chrono::milliseconds{1}));
    f->server.reset();
    while (!done) CO2_AWAIT(net::delay(std::chrono::milliseconds{1}));
    if (failure) std::rethrow_exception(failure);
    CHECK(result.code != rpc::status_code::ok);
    while (f->service.exited == 0) CO2_AWAIT(net::delay(std::chrono::milliseconds{1}));
    CO2_RETURN();
}
CO2_END

// Every call shape of a generated service, through a channel.
auto streaming(fixture *f)
    CO2_BEG(net::task<>, (f), rpc::status_code ready; echo_request request; echo_reply response; rpc::call_result result;
            mixed_open call; rpc::status_code sent; rpc::stream_read read; rpc::call_spec spec;
            std::array<w::metadata_entry, 1> metadata{}; std::array<std::uint8_t, 64> storage{};
            rpc::response_trailer trailer;) {
    CO2_AWAIT_SET(ready, f->channel.wait_ready(rpc::clock::now() + std::chrono::seconds{5}));
    CHECK(ready == rpc::status_code::ok);
    request.set_payload(std::string(4096, 'p'));
    spec.timeout = std::chrono::seconds{3};
    metadata[0] = {{reinterpret_cast<std::uint8_t const *>("caller"), 6},
                   {reinterpret_cast<std::uint8_t const *>("stream"), 6}};
    spec.metadata = {metadata.data(), metadata.size()};
    trailer.storage = {storage.data(), storage.size()};
    CO2_AWAIT_SET(result, f->mixed_stub.Unary(request, response, &spec));
    CHECK(result.code == rpc::status_code::ok && response.payload() == request.payload());

    CO2_AWAIT_SET(call, f->mixed_stub.Upload(&spec));
    CHECK(call.code == rpc::status_code::ok && call.stream);
    CO2_AWAIT_SET(sent, call.stream.write(request));
    CHECK(sent == rpc::status_code::ok);
    CO2_AWAIT_SET(sent, call.stream.write(request));
    CHECK(sent == rpc::status_code::ok);
    CO2_AWAIT_SET(sent, call.stream.writes_done());
    CHECK(sent == rpc::status_code::ok);
    CO2_AWAIT_SET(read, call.stream.read(response));
    CHECK(read.code == rpc::status_code::ok && !read.ended && response.payload().size() == 8192);
    CO2_AWAIT_SET(result, call.stream.finish());
    CHECK(result.code == rpc::status_code::ok);

    CO2_AWAIT_SET(call, f->mixed_stub.Download(&spec));
    CHECK(call.code == rpc::status_code::ok && call.stream);
    CO2_AWAIT_SET(sent, call.stream.write(request));
    CHECK(sent == rpc::status_code::ok);
    CO2_AWAIT_SET(sent, call.stream.writes_done());
    CHECK(sent == rpc::status_code::ok);
    CO2_AWAIT_SET(read, call.stream.read(response));
    CHECK(read.code == rpc::status_code::ok && !read.ended && response.payload() == request.payload());
    CO2_AWAIT_SET(read, call.stream.read(response));
    CHECK(read.code == rpc::status_code::ok && !read.ended);
    CO2_AWAIT_SET(read, call.stream.read(response));
    CHECK(read.code == rpc::status_code::ok && read.ended);
    CO2_AWAIT_SET(result, call.stream.finish());
    CHECK(result.code == rpc::status_code::ok);

    CO2_AWAIT_SET(call, f->mixed_stub.Chat(&spec));
    CHECK(call.code == rpc::status_code::ok && call.stream);
    CO2_AWAIT_SET(sent, call.stream.write(request));
    CHECK(sent == rpc::status_code::ok);
    CO2_AWAIT_SET(read, call.stream.read(response));
    CHECK(read.code == rpc::status_code::ok && response.payload() == request.payload());
    CO2_AWAIT_SET(sent, call.stream.writes_done());
    CHECK(sent == rpc::status_code::ok);
    CO2_AWAIT_SET(read, call.stream.read(response));
    CHECK(read.code == rpc::status_code::ok && read.ended);
    CO2_AWAIT_SET(result, call.stream.finish(&trailer));
    CHECK(result.code == rpc::status_code::ok && trailer.metadata.count == 1);
    CHECK(w::decode_metadata_entry(trailer.metadata.entries).value.key.size == 6);

    CO2_AWAIT_SET(call, f->mixed_stub.Chat(&spec));
    CHECK(call.code == rpc::status_code::ok && call.stream);
    call.stream.cancel();
    CO2_AWAIT_SET(result, call.stream.finish());
    CHECK(result.code == rpc::status_code::cancelled);
    f->channel.close();
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
        codecs();
        { fixture f; f.run([&] { return integration(&f); }); }
        { fixture f; f.run([&] { return cancellation(&f); }); }
        { fixture f; f.run([&] { return late_handler(&f); }); }
        { fixture f; f.run([&] { return streaming(&f); }); }
        std::cout << "PASS protobuf codecs, generated stubs and streams, Arena and cancellation\n";
    } catch (std::exception const &error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
