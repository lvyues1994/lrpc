#include "client_fixture.hpp"
#include <rpc/typed.hpp>
#include <net/timeout.hpp>

#include <iostream>
#include <functional>
#include <thread>

static std::function<void()> decode_hook;

struct byte_message {
    std::uint8_t value = 42;
    rpc::client *channel = nullptr;
    unsigned close_at = 0; // size, encode or decode.
    bool allocation_failure = false;
};

namespace rpc {
template <> struct codec<byte_message> {
    static std::size_t size(byte_message const &value) {
        if (value.close_at == 1) value.channel->close();
        if (value.allocation_failure) throw std::bad_alloc{};
        return 1;
    }
    static bool encode(byte_message const &value, wire::mutable_bytes_view out) {
        if (value.close_at == 2) value.channel->close();
        if (out.size != 1) return false;
        out.data[0] = value.value; return true;
    }
    static bool decode(wire::bytes_view in, byte_message &value) {
        if (decode_hook) decode_hook();
        if (value.close_at == 3) value.channel->close();
        if (in.size != 1) return false;
        value.value = in.data[0]; return true;
    }
};
} // namespace rpc

namespace {
void client_codec(unsigned close_at, bool allocation_failure = false) {
    test_client::fixture f; byte_message request, response;
    request.channel = response.channel = f.client.get(); request.close_at = response.close_at = close_at;
    request.allocation_failure = allocation_failure;
    unsigned completed = 0; rpc::call_result result; std::exception_ptr error;
    net::run_async(f.context.get_executor(), [&](rpc::call_result value) { result = value; ++completed; },
        [&](std::exception_ptr value) { error = value; })([&] {
        return rpc::call(*f.client, rpc::method<byte_message, byte_message>{"Echo"}, request, response);
    });
    f.context.poll();
    if (close_at == 0 && !allocation_failure) {
        auto frame = rpc::wire::decode_frame({f.pipe->output.data(), f.pipe->output.size()});
        CHECK(frame.code == rpc::wire::error::none && frame.value.body.size == 1 && frame.value.body.data[0] == 42);
    }
    if (!completed) { f.pipe->feed(test_client::reply(1, 73)); f.context.poll(); }
    if (error) std::rethrow_exception(error);
    CHECK(completed == 1);
    if (allocation_failure) CHECK(result.code == rpc::status_code::resource_exhausted);
    else if (close_at == 1 || close_at == 2) CHECK(result.code == rpc::status_code::unavailable);
    else CHECK(result.code == rpc::status_code::ok && response.value == 73 && result.response_size == 1);
    f.zero(); CHECK(completed == 1);
}

struct service {
    bool entered = false;
    net::task<rpc::status_code> echo(rpc::server_context &, byte_message const &, byte_message &);
};
auto echo_task(byte_message const *request, byte_message *response)
    CO2_BEG(net::task<rpc::status_code>, (request, response)) {
    CO2_AWAIT(net::delay(std::chrono::milliseconds{1}));
    response->value = static_cast<std::uint8_t>(request->value + 1); CO2_RETURN(rpc::status_code::ok);
}
CO2_END
net::task<rpc::status_code> service::echo(rpc::server_context &, byte_message const &request, byte_message &response) {
    entered = true;
    return echo_task(&request, &response);
}

void typed_adapter(rpc::status_code expected = rpc::status_code::ok) {
    service impl;
    auto binding = rpc::bind_method<byte_message, byte_message, service, rpc::status_code>(rpc::method<byte_message, byte_message>{"custom/Echo"}, impl, &service::echo, {1, 2});
    auto copy = binding; binding = {};
    CHECK(copy.handler == copy.owned_handler.get());
    net::io_context context{net::default_backend, net::single_thread_hint};
    rpc::server_context call; std::uint8_t input = 41, output = 0;
    net::stop_source stop;
    call.stop_token = stop.get_token();
    if (expected == rpc::status_code::cancelled) decode_hook = [&] { stop.request_stop(); };
    if (expected == rpc::status_code::deadline_exceeded) {
        call.deadline = rpc::clock::now() + std::chrono::milliseconds{1};
        decode_hook = [&] { std::this_thread::sleep_until(call.deadline); };
    }
    rpc::response_writer writer{{&output, 1}}; bool completed = false; std::exception_ptr error;
    net::run_async(context.get_executor(), [&](rpc::status_code code) {
        CHECK(code == expected); completed = true;
    }, [&](std::exception_ptr value) { error = value; })([&] { return copy.handler->invoke(call, {&input, 1}, writer); });
    context.run(); decode_hook = {}; if (error) std::rethrow_exception(error);
    CHECK(completed);
    if (expected == rpc::status_code::ok) CHECK(impl.entered && output == 42 && writer.size() == 1);
    else CHECK(!impl.entered && output == 0 && writer.size() == 0);
}

void bound_methods() {
    rpc::client_options options{}; options.max_registered_methods = 2;
    test_client::fixture f{options}, other;
    auto const *ops = &rpc::codec_for<byte_message>();
    rpc::method_descriptor descriptor{"Echo", rpc::method_kind::unary, rpc::idempotency::unknown, ops, ops};
    auto echo = f.client->bind(descriptor);
    CHECK(echo && f.client->bind(descriptor));
    rpc::codec_ops different = *ops;
    auto conflict = descriptor; conflict.request_codec = &different;
    rpc::method_descriptor batch[] = {{"New", rpc::method_kind::unary, rpc::idempotency::unknown, ops, ops}, conflict};
    bool rejected = false;
    try { f.client->bind({"S", batch, 2}); } catch (std::invalid_argument const &) { rejected = true; }
    CHECK(rejected);
    auto another = descriptor; another.name = "Another";
    CHECK(f.client->bind(another)); // Failed batch did not consume its slot.
    auto foreign = other.client->bind(descriptor);
    byte_message request, response; rpc::call_result result; unsigned completed = 0;
    auto start = [&](rpc::method_handle handle, bool string_call = false) {
        net::run_async(f.context.get_executor(), [&](rpc::call_result value) { result = std::move(value); ++completed; },
            [](std::exception_ptr error) { std::rethrow_exception(error); })([&] {
            return string_call ? rpc::call(*f.client, rpc::method<byte_message, byte_message>{"Echo"}, request, response)
                               : rpc::call(*f.client, handle, request, response);
        });
        f.context.poll();
    };
    start(echo); f.pipe->feed(test_client::reply(1, 73)); f.context.poll();
    CHECK(completed == 1 && result.code == rpc::status_code::ok && response.value == 73);
    f.pipe->output.clear(); start(echo, true);
    auto frame = rpc::wire::decode_frame({f.pipe->output.data(), f.pipe->output.size()});
    CHECK(frame.code == rpc::wire::error::none && !(frame.value.header.flags & rpc::wire::new_method));
    CHECK(frame.value.header.aux == 1);
    f.pipe->feed(test_client::reply(2)); f.context.poll();
    start(echo); f.pipe->feed(test_client::reply(3)); f.context.poll();
    CHECK(completed == 3 && result.code == rpc::status_code::ok);
    start(foreign); CHECK(completed == 4 && result.code == rpc::status_code::invalid_argument);
    start({}); CHECK(completed == 5 && result.code == rpc::status_code::invalid_argument);
    f.zero(); other.zero();
}

struct status_service {
    rpc::status result{rpc::status_code::permission_denied, "denied after suspension"};
    net::task<rpc::status> echo(rpc::server_context &, byte_message const &, byte_message &);
};
auto status_reply(status_service *self, byte_message const *request, byte_message *response)
    CO2_BEG(net::task<rpc::status>, (self, request, response)) {
    CO2_AWAIT(net::delay(std::chrono::milliseconds{1}));
    response->value = request->value;
    CO2_RETURN(self->result);
}
CO2_END
net::task<rpc::status> status_service::echo(rpc::server_context &, byte_message const &req, byte_message &resp) {
    return status_reply(this, &req, &resp);
}
void owning_status() {
    net::io_context context{net::default_backend, net::single_thread_hint};
    status_service impl;
    auto binding = rpc::bind_method(rpc::method<byte_message, byte_message>{"Status"}, impl, &status_service::echo, {1, 64});
    std::uint8_t input = 42, output = 0; std::array<std::uint8_t, 64> head{};
    for (auto code : {rpc::status_code::permission_denied, rpc::status_code::ok}) {
        impl.result = {code, code == rpc::status_code::ok ? "" : "denied after suspension"};
        rpc::server_context call; call.response_metadata = rpc::metadata_writer{{head.data(), head.size()}};
        rpc::response_writer writer{{&output, 1}}; bool completed = false;
        net::run_async(context.get_executor(), [&](rpc::status_code value) { CHECK(value == code); completed = true; },
            [](std::exception_ptr error) { std::rethrow_exception(error); })
            ([&] { return binding.handler->invoke(call, {&input, 1}, writer); });
        context.run(); CHECK(completed);
        auto decoded = rpc::wire::decode_end_head({head.data(), call.response_metadata.size()});
        CHECK(decoded.code == rpc::wire::error::none && decoded.value.message.size == impl.result.message.size());
        CHECK(writer.size() == (code == rpc::status_code::ok ? 1U : 0U));
    }
    rpc::server_builder builder{context}; builder.add(std::vector<rpc::method_binding>{binding});
    auto server = builder.build(); CHECK(server->stats().active_calls == 0 && context.run() == 0);
    bool rejected = false;
    try { builder.build(); } catch (std::logic_error const &) { rejected = true; }
    CHECK(rejected); server->close(); context.run();
}
} // namespace

int main() {
    try {
        for (unsigned at = 0; at < 4; ++at) client_codec(at);
        client_codec(0, true); typed_adapter();
        typed_adapter(rpc::status_code::cancelled);
        typed_adapter(rpc::status_code::deadline_exceeded);
        bound_methods(); owning_status();
        std::cout << "PASS generic codec, owned adapter, reentrant close and allocation failure\n";
    } catch (std::exception const &error) { std::cerr << error.what() << '\n'; return 1; }
}
