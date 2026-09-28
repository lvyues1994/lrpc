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
    auto binding = rpc::bind_method(rpc::method<byte_message, byte_message>{"custom/Echo"}, impl, &service::echo, {1, 2});
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
} // namespace

int main() {
    try {
        for (unsigned at = 0; at < 4; ++at) client_codec(at);
        client_codec(0, true); typed_adapter();
        typed_adapter(rpc::status_code::cancelled);
        typed_adapter(rpc::status_code::deadline_exceeded);
        std::cout << "PASS generic codec, owned adapter, reentrant close and allocation failure\n";
    } catch (std::exception const &error) { std::cerr << error.what() << '\n'; return 1; }
}
