#include "check.hpp"
#include "backend.hpp"
#include "client_fixture.hpp"
#include "echo.rpc.hpp"
#include "nested/lite.rpc.hpp"

#include <net/this_coro.hpp>
#include <net/timeout.hpp>

#include <array>
#include <iostream>
#include <thread>

rpc::service_descriptor const *descriptor_from_other_translation_unit();

namespace {
namespace w = rpc::wire;
net::backend_kind selected_backend = net::default_backend_t::kind;

void codecs() {
    demo::EchoRequest request, decoded;
    request.set_payload(std::string("a\0b", 3)); request.set_marker(0);
    request.add_numbers(7); request.add_numbers(-4);
    auto const size = rpc::codec<demo::EchoRequest>::size(request);
    std::vector<std::uint8_t> encoded(size);
    CHECK(!rpc::codec<demo::EchoRequest>::encode(request, {encoded.data(), size - 1}));
    CHECK(rpc::codec<demo::EchoRequest>::encode(request, {encoded.data(), size}));
    CHECK(rpc::codec<demo::EchoRequest>::decode({encoded.data(), size}, decoded));
    CHECK(decoded.payload() == request.payload() && decoded.has_marker() && decoded.numbers(1) == -4);
    CHECK(!rpc::codec<demo::EchoRequest>::decode({encoded.data(), size - 1}, decoded));
    std::uint8_t const malformed[] = {0x0a, 0xff};
    CHECK(!rpc::codec<demo::EchoRequest>::decode({malformed, sizeof(malformed)}, decoded));
    CHECK(!rpc::codec<demo::EchoRequest>::decode({encoded.data(), static_cast<std::size_t>(INT_MAX) + 1}, decoded));
    CHECK(!rpc::codec<demo::EchoRequest>::decode({nullptr, 1}, decoded));
    CHECK(!rpc::codec<demo::EchoRequest>::encode(request, {encoded.data(), static_cast<std::size_t>(INT_MAX) + 1}));
    // Unknown field 100 (varint) must survive parsing without rejection.
    encoded.insert(encoded.end(), {0xa0, 0x06, 1});
    CHECK(rpc::codec<demo::EchoRequest>::decode({encoded.data(), encoded.size()}, decoded));
    demo::EchoReply empty;
    CHECK(rpc::codec<demo::EchoReply>::size(empty) == 0);
    CHECK(rpc::codec<demo::EchoReply>::encode(empty, {}));
    CHECK(rpc::codec<demo::EchoReply>::decode({}, empty));
    test::messages::Container::Item required;
    rpc::codec<test::messages::Container::Item>::size(required);
    CHECK(!rpc::codec<test::messages::Container::Item>::encode(required, {}));
    CHECK(!rpc::codec<test::messages::Container::Item>::decode({}, required));
    required.set_value("nested");
    encoded.resize(rpc::codec<test::messages::Container::Item>::size(required));
    CHECK(rpc::codec<test::messages::Container::Item>::encode(required, {encoded.data(), encoded.size()}));
    rpc::call_arena arena;
    auto *on_arena = google::protobuf::Arena::CreateMessage<demo::EchoReply>(&arena.get());
    CHECK(on_arena->GetArena() == &arena.get());
    CHECK(arena.get().SpaceAllocated() == 2048);
    auto const &descriptor = demo::Echo_service_descriptor();
    CHECK(&descriptor == descriptor_from_other_translation_unit() && descriptor.size == 3);
    CHECK(std::string{descriptor.methods[0].name} == "demo.Echo/RoundTrip");
    CHECK(descriptor.methods[0].semantics == rpc::idempotency::idempotent);
    CHECK(demo::Empty_service_descriptor().size == 0);
}

struct echo final : demo::EchoService {
    net::task<rpc::status_code> RoundTrip(rpc::server_context &, demo::EchoRequest const &, demo::EchoReply &) override;
    net::task<rpc::status_code> Required(rpc::server_context &, test::messages::Container::Item const &, test::messages::Container::Item &) override;
    net::task<rpc::status_code> delete_(rpc::server_context &ctx, demo::EchoRequest const &req, demo::EchoReply &reply) override {
        return RoundTrip(ctx, req, reply);
    }
    unsigned entered = 0, exited = 0;
    bool wait = false, bad_response = false;
    bool gated = false, throws = false;
    rpc::detail::event gate{};
    rpc::status_code status = rpc::status_code::ok;
};

auto round_trip(echo *self, rpc::server_context *ctx, demo::EchoRequest const *request, demo::EchoReply *response)
    CO2_BEG(net::task<rpc::status_code>, (self, ctx, request, response), net::io_result<> waited;) {
    ++self->entered;
    CHECK(request->GetArena() != nullptr && request->GetArena() == response->GetArena());
    if (self->throws) { ++self->exited; throw std::runtime_error{"typed handler failure"}; }
    if (ctx->metadata.count) {
        auto item = w::decode_metadata_entry(ctx->metadata.entries); CHECK(item.code == w::error::none);
        CHECK(ctx->response_metadata.assign({&item.value, 1}));
    }
    if (self->wait) CO2_AWAIT_SET(waited, net::delay(std::chrono::milliseconds{30}));
    if (self->gated) CO2_AWAIT(self->gate.wait());
    CHECK(request->GetArena() == response->GetArena());
    response->set_payload(request->payload()); ++self->exited;
    CO2_RETURN(waited.ec ? rpc::status_code::cancelled : self->status);
}
CO2_END
net::task<rpc::status_code> echo::RoundTrip(rpc::server_context &ctx, demo::EchoRequest const &request, demo::EchoReply &response) {
    return round_trip(this, &ctx, &request, &response);
}
auto required_reply(echo *self, test::messages::Container::Item const *request, test::messages::Container::Item *response)
    CO2_BEG(net::task<rpc::status_code>, (self, request, response)) {
    ++self->entered;
    CHECK(request->GetArena() != nullptr && request->GetArena() == response->GetArena());
    if (!self->bad_response) response->set_value(request->value());
    ++self->exited; CO2_RETURN(rpc::status_code::ok);
}
CO2_END
net::task<rpc::status_code> echo::Required(rpc::server_context &, test::messages::Container::Item const &req, test::messages::Container::Item &resp) {
    return required_reply(this, &req, &resp);
}

struct lite_echo final : demo::lite::LiteService {
    net::task<rpc::status_code> Echo(rpc::server_context &, demo::lite::Message const &, demo::lite::Message &) override;
};
auto lite_reply(demo::lite::Message const *request, demo::lite::Message *response)
    CO2_BEG(net::task<rpc::status_code>, (request, response)) {
    CHECK(request->GetArena() != nullptr && request->GetArena() == response->GetArena());
    response->set_payload(request->payload()); CO2_RETURN(rpc::status_code::ok);
}
CO2_END
net::task<rpc::status_code> lite_echo::Echo(rpc::server_context &, demo::lite::Message const &req, demo::lite::Message &resp) {
    return lite_reply(&req, &resp);
}

std::unique_ptr<rpc::server> build(net::io_context &context, echo &service, lite_echo &lite, std::size_t cache_entries) {
    rpc::server_builder builder{context};
    demo::EchoLimits limits{}; limits.RoundTrip = {256, 64}; limits.Required = {256, 64}; limits.delete_ = {256, 64};
    limits.RoundTrip.message_cache_entries = limits.Required.message_cache_entries = limits.delete_.message_cache_entries = cache_entries;
    demo::add_Echo_service(builder, service, limits);
    demo::lite::add_Lite_service(builder, lite, {{256, 2, cache_entries}});
    return builder.build();
}

struct fixture {
    explicit fixture(std::size_t cache_entries = 0) : server(build(context, service, lite, cache_entries)), client(rpc::make_client(context)),
                endpoint(server->listen({net::ip::address_v4::loopback(), 0})), stub(*client), lite_stub(*client) {}
    template <class Factory> void run(Factory factory) {
        std::exception_ptr error;
        auto close = [&] { client->close(); if (server) server->close(); };
        net::run_async(context.get_executor(), close, [&](std::exception_ptr value) { error = value; close(); })(factory);
        context.run();
        if (error) std::rethrow_exception(error);
        CHECK(service.entered == service.exited);
        if (server) {
            CHECK(server->stats().connections == 0 && server->stats().active_calls == 0);
            CHECK(server->stats().response_bytes_in_use == 0 && server->stats().request_bytes_in_use == 0);
        }
        CHECK(client->stats().active_calls == 0 && client->stats().request_bytes_in_use == 0);
    }
    net::io_context context{selected_backend, net::single_thread_hint};
    echo service{};
    lite_echo lite{};
    std::unique_ptr<rpc::server> server;
    std::unique_ptr<rpc::client> client;
    net::ip::tcp::endpoint endpoint;
    demo::EchoStub stub;
    demo::lite::LiteStub lite_stub;
};

auto integration(fixture *f)
    CO2_BEG(net::task<>, (f), rpc::status_code connected; rpc::call_result result;
        demo::EchoRequest request; demo::EchoReply response; google::protobuf::Arena caller_arena;
        demo::EchoReply *arena_reply = nullptr; test::messages::Container::Item required, required_response;
        demo::lite::Message lite_request, lite_response; std::array<std::uint8_t, 64> meta{};
        std::array<std::uint8_t, 16> raw{}; std::uint8_t key = 1, value = 2; w::metadata_entry entry{};
        rpc::call_options options{}; unsigned before = 0;) {
    CO2_AWAIT_SET(connected, f->client->connect(f->endpoint)); CHECK(connected == rpc::status_code::ok);
    request.set_payload(std::string("hello\0arena", 11));
    entry = {{&key, 1}, {&value, 1}}; options.metadata = {&entry, 1}; options.response_metadata = {meta.data(), meta.size()};
    f->service.wait = true;
    CO2_AWAIT_SET(result, f->stub.RoundTrip(request, response, options));
    CHECK(result.code == rpc::status_code::ok && response.payload() == request.payload());
    CHECK(result.response_metadata.count == 1 && w::decode_metadata_entry(result.response_metadata.entries).value.value.data[0] == 2);
    f->service.wait = false;
    arena_reply = google::protobuf::Arena::CreateMessage<demo::EchoReply>(&caller_arena);
    CO2_AWAIT_SET(result, f->stub.delete_(request, *arena_reply));
    CHECK(result.code == rpc::status_code::ok && arena_reply->GetArena() == &caller_arena && arena_reply->payload() == request.payload());
    lite_request.set_payload("lite"); CO2_AWAIT_SET(result, f->lite_stub.Echo(lite_request, lite_response));
    CHECK(result.code == rpc::status_code::ok && lite_response.payload() == "lite");
    required.set_value("nested"); CO2_AWAIT_SET(result, f->stub.Required(required, required_response));
    CHECK(result.code == rpc::status_code::ok && required_response.value() == "nested");
    before = f->service.entered; required.clear_value();
    CO2_AWAIT_SET(result, f->stub.Required(required, required_response));
    CHECK(result.code == rpc::status_code::invalid_argument && f->service.entered == before);
    CO2_AWAIT_SET(result, f->client->call("demo.Echo/Required", {}, {raw.data(), raw.size()}));
    CHECK(result.code == rpc::status_code::invalid_argument && f->service.entered == before);
    raw[0] = 0x0a; raw[1] = 0xff;
    CO2_AWAIT_SET(result, f->client->call("demo.Echo/RoundTrip", {raw.data(), 2}, {}));
    CHECK(result.code == rpc::status_code::invalid_argument && f->service.entered == before);
    f->service.status = rpc::status_code::permission_denied; response.set_payload("unchanged");
    CO2_AWAIT_SET(result, f->stub.RoundTrip(request, response, options));
    CHECK(result.code == rpc::status_code::permission_denied && response.payload() == "unchanged" && result.response_metadata.count == 1);
    f->service.status = rpc::status_code::ok; request.set_payload(std::string(300, 'x'));
    CO2_AWAIT_SET(result, f->stub.RoundTrip(request, response)); CHECK(result.code == rpc::status_code::internal);
    required.set_value("valid"); f->service.bad_response = true;
    CO2_AWAIT_SET(result, f->stub.Required(required, required_response)); CHECK(result.code == rpc::status_code::internal);
    request.clear_payload(); CO2_AWAIT_SET(result, f->stub.RoundTrip(request, response));
    CHECK(result.code == rpc::status_code::ok && response.payload().empty());
    f->service.throws = true;
    CO2_AWAIT_SET(result, f->stub.RoundTrip(request, response)); CHECK(result.code == rpc::status_code::internal);
    CO2_RETURN();
}
CO2_END

auto late_handler(fixture *f)
    CO2_BEG(net::task<>, (f), rpc::status_code connected; rpc::call_result result;
        demo::EchoRequest request; demo::EchoReply response; net::stop_source stop;
        bool done = false; std::exception_ptr failure;) {
    CO2_AWAIT_SET(connected, f->client->connect(f->endpoint)); CHECK(connected == rpc::status_code::ok);
    request.set_payload("Arena survives client cancellation"); f->service.gated = true;
    net::run_async(f->context.get_executor(), stop.get_token(), nullptr,
        [&](rpc::call_result value) { result = value; done = true; },
        [&](std::exception_ptr error) { failure = error; done = true; })([&] { return f->stub.RoundTrip(request, response); });
    while (f->service.entered == 0) CO2_AWAIT(net::delay(std::chrono::milliseconds{1}));
    stop.request_stop();
    while (!done) CO2_AWAIT(net::delay(std::chrono::milliseconds{1}));
    if (failure) std::rethrow_exception(failure);
    CHECK(result.code == rpc::status_code::cancelled && f->service.exited == 0);
    CHECK(f->server->stats().active_calls == 1 && f->server->stats().response_bytes_in_use != 0);
    request.Clear(); f->server.reset(); // Pending handler must retain its adapter and Arena.
    f->service.gate.signal();
    while (f->service.exited == 0) CO2_AWAIT(net::delay(std::chrono::milliseconds{1}));
    CO2_RETURN();
}
CO2_END

auto cancellation(fixture *f)
    CO2_BEG(net::task<>, (f), rpc::status_code connected; rpc::call_result result;
        demo::EchoRequest request; demo::EchoReply response; rpc::call_options options{};
        net::stop_source stop; bool done = false; std::exception_ptr failure;) {
    CO2_AWAIT_SET(connected, f->client->connect(f->endpoint)); CHECK(connected == rpc::status_code::ok);
    request.set_payload("alive after suspension"); f->service.wait = true;
    options.timeout = std::chrono::milliseconds{3};
    CO2_AWAIT_SET(result, f->stub.RoundTrip(request, response, options)); CHECK(result.code == rpc::status_code::deadline_exceeded);
    net::run_async(f->context.get_executor(), stop.get_token(), nullptr,
        [&](rpc::call_result value) { result = value; done = true; },
        [&](std::exception_ptr error) { failure = error; done = true; })([&] { return f->stub.RoundTrip(request, response); });
    CO2_AWAIT(net::delay(std::chrono::milliseconds{2}));
    { std::thread requester{[&] { stop.request_stop(); }}; requester.join(); }
    while (!done) CO2_AWAIT(net::delay(std::chrono::milliseconds{1}));
    if (failure) std::rethrow_exception(failure);
    CHECK(result.code == rpc::status_code::cancelled);
    CO2_RETURN();
}
CO2_END

void bad_response() {
    test_client::fixture f; demo::EchoRequest request; demo::EchoReply response;
    rpc::call_result result; bool done = false; std::exception_ptr error;
    net::run_async(f.context.get_executor(), [&](rpc::call_result value) { result = value; done = true; },
        [&](std::exception_ptr value) { error = value; done = true; })([&] {
        return rpc::call(*f.client, rpc::method<demo::EchoRequest, demo::EchoReply>{"Echo"}, request, response);
    });
    f.context.poll(); f.pipe->feed(test_client::reply(1, 0xff)); f.context.poll();
    if (error) std::rethrow_exception(error);
    CHECK(done && result.code == rpc::status_code::data_loss && f.client->ready()); f.zero();
}
} // namespace

int main(int argc, char **argv) {
    try {
        selected_backend = test_backend(argc, argv); codecs(); bad_response();
        { fixture f; f.run([&] { return integration(&f); }); }
        { fixture f; f.run([&] { return cancellation(&f); }); }
        { fixture f; f.run([&] { return late_handler(&f); }); }
        { fixture f{2}; f.run([&] { return integration(&f); }); }
        { fixture f{2}; f.run([&] { return cancellation(&f); }); }
        { fixture f{2}; f.run([&] { return late_handler(&f); }); }
        std::cout << "PASS protobuf codecs, generated stubs, Arena and cancellation\n";
    } catch (std::system_error const &error) {
        if (error.code() == std::errc::operation_not_supported || error.code() == std::errc::permission_denied) return 77;
        std::cerr << error.what() << '\n'; return 1;
    } catch (std::exception const &error) { std::cerr << error.what() << '\n'; return 1; }
}
