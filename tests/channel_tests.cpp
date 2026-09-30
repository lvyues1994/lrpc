#include "check.hpp"
#include <rpc/channel.hpp>
#include <net/run_async.hpp>
#include <net/timeout.hpp>
#include <net/test/run_blocking.hpp>
#include <array>
#include <iostream>
#include <sstream>

struct test_message {
    std::uint8_t value = 0;
    unsigned *encodes = nullptr;
};
namespace rpc {
template <> struct codec<test_message> {
    static std::size_t size(test_message const &) { return 1; }
    static bool encode(test_message const &m, wire::mutable_bytes_view out) {
        if (out.size != 1) return false;
        if (m.encodes) ++*m.encodes;
        out.data[0] = m.value; return true;
    }
    static bool decode(wire::bytes_view in, test_message &m) {
        if (in.size != 1) return false;
        m.value = in.data[0]; return true;
    }
};
} // namespace rpc
namespace {
using namespace rpc;
struct hook final : interceptor {
    unsigned logical_before = 0, logical_after = 0, attempt_before = 0, attempt_after = 0;
    status before(call_info const &info) override { if (info.attempt) ++attempt_before; else ++logical_before; return {}; }
    void after(call_info const &info, call_result const &) override { if (info.attempt) ++attempt_after; else ++logical_after; }
};
struct echo_handler final : method_handler {
    std::array<unsigned, 4> invoked{};
    net::task<status_code> invoke(server_context &context, wire::bytes_view request, response_writer &response) override;
};
auto echo(echo_handler *handler, server_context *context, wire::bytes_view request, response_writer *response)
    CO2_BEG(net::task<status_code>, (handler, context, request, response), std::uint8_t mode = 0; unsigned attempt = 0;) {
    if (request.size != 1 || request.data[0] > 3) CO2_RETURN(status_code::invalid_argument);
    mode = request.data[0]; attempt = ++handler->invoked[mode];
    if (mode == 3) CO2_AWAIT(net::delay(std::chrono::milliseconds{20}));
    if (mode == 2 || (mode == 1 && attempt == 1)) {
        context->response_metadata.assign_message({reinterpret_cast<std::uint8_t const *>("try later"), 9});
        CO2_RETURN(status_code::unavailable);
    }
    response->assign(request);
    CO2_RETURN(status_code::ok);
}
CO2_END
net::task<status_code> echo_handler::invoke(server_context &context, wire::bytes_view request, response_writer &response) {
    return echo(this, &context, request, &response);
}

struct queued_result {
    unsigned done = 0;
    call_result value{};
    std::uint8_t response = 0;
    std::uint8_t request = 3;
};
struct test_run {
    queued_result first{}, second{};
    bool retry_done = false;
    call_result retry_result{};
    test_message retry_message{2, nullptr}, retry_reply{};
};
void start_queued(net::io_context &ctx, channel *c, queued_result *out, call_options options) {
    net::run_async(ctx.get_executor(), [out](call_result r) { out->value = std::move(r); ++out->done; },
        [](std::exception_ptr e) { std::rethrow_exception(e); })([c, out, options] {
        return c->call("Echo", {&out->request, 1}, {&out->response, 1}, options);
    });
}
void start_retry(net::io_context &ctx, channel *c, method_handle method, test_run *out, call_options options) {
    net::run_async(ctx.get_executor(), [out](call_result r) { out->retry_result = std::move(r); out->retry_done = true; },
        [](std::exception_ptr e) { std::rethrow_exception(e); })([c, method, out, options] {
        return rpc::call(*c, method, out->retry_message, out->retry_reply, options);
    });
}
auto exercise(net::io_context &ctx, channel *c, server *s, echo_handler *handler, hook *hooks,
              method_handle unknown, method_handle idempotent, test_run *run)
    CO2_BEG(net::task<>, (ctx, c, s, handler, hooks, unknown, idempotent, run),
            status_code warmed; call_options options; call_result result; std::uint8_t request = 0; std::uint8_t response = 255;
            test_message message; test_message reply; unsigned encodes = 0;
            call_options queued; unsigned after_shutdown = 0;) {
    options.timeout = std::chrono::milliseconds{500};
    CO2_AWAIT_SET(warmed, c->warmup(options)); CHECK(warmed == status_code::ok);
    CO2_AWAIT_SET(result, c->call("Echo", {&request, 1}, {&response, 1}, options));
    CHECK(result.code == status_code::ok && response == request && !result.not_executed);
    options.retry.max_attempts = 2;
    CO2_AWAIT_SET(result, c->call("Echo", {&request, 1}, {nullptr, 1}, options));
    CHECK(result.code == status_code::invalid_argument);
    options.response_metadata = {nullptr, 8};
    CO2_AWAIT_SET(result, c->call("Echo", {&request, 1}, {&response, 1}, options));
    CHECK(result.code == status_code::invalid_argument); options.response_metadata = {};
    message.value = 1; message.encodes = &encodes;
    CO2_AWAIT_SET(result, rpc::call(*c, unknown, message, reply, options));
    CHECK(result.code == status_code::unavailable && result.attempts == 1 && !result.not_executed && encodes == 1);
    handler->invoked[1] = 0; encodes = 0;
    CO2_AWAIT_SET(result, rpc::call(*c, idempotent, message, reply, options));
    CHECK(result.code == status_code::ok && result.attempts == 2 && reply.value == 1 && encodes == 1);
    // Peer advertises one stream while client advertises 64: the second call waits.
    queued.timeout = std::chrono::milliseconds{500}; queued.wait_for_capacity = true;
    request = 3;
    start_queued(ctx, c, &run->first, queued);
    start_queued(ctx, c, &run->second, queued);
    CO2_AWAIT(net::delay(std::chrono::milliseconds{2}));
    CHECK(c->metrics().waiting_calls == 1);
    while (run->first.done == 0 || run->second.done == 0) CO2_AWAIT(net::delay(std::chrono::milliseconds{1}));
    CHECK(run->first.value.code == status_code::ok && run->second.value.code == status_code::ok);
    CHECK(c->metrics().waiting_calls == 0 && c->metrics().waiting_bytes == 0);
    // Closing a long retry backoff must finish its hooks before shutdown returns.
    options.retry.initial_backoff = std::chrono::milliseconds{1000}; options.retry.max_backoff = std::chrono::milliseconds{1000};
    options.timeout = std::chrono::seconds{5}; message.value = 2;
    start_retry(ctx, c, idempotent, run, options);
    CO2_AWAIT(net::delay(std::chrono::milliseconds{5}));
    CO2_AWAIT(c->shutdown(std::chrono::milliseconds{0}));
    CHECK(run->retry_done && run->retry_result.code == status_code::cancelled && run->retry_result.message.empty());
    after_shutdown = hooks->logical_after;
    CO2_AWAIT_SET(result, c->call("Echo", {&request, 1}, {&response, 1}, {}));
    CHECK(result.code == status_code::unavailable && hooks->logical_after == after_shutdown);
    CO2_AWAIT(net::delay(std::chrono::milliseconds{5})); CHECK(hooks->logical_after == after_shutdown);
    CHECK(c->quiescent() && c->metrics().replay_bytes_in_use == 0);
    CHECK(hooks->logical_before == hooks->logical_after && hooks->attempt_before == hooks->attempt_after);
    CO2_AWAIT(s->shutdown(std::chrono::milliseconds{50}));
    CO2_RETURN();
}
CO2_END

void channel_cases() {
    net::io_context ctx{net::epoll, net::single_thread_hint}; echo_handler handler; hook hooks;
    server_options server_config{}; server_config.connection.receive.max_concurrent_streams = 1;
    auto s = make_server(ctx, {{"Echo", 1, &handler, 64}, {"Unknown", 1, &handler, 64}, {"Idem", 1, &handler, 64}}, server_config);
    auto endpoint = s->listen({net::ip::make_address("127.0.0.1"), 0});
    channel_options config{}; config.resolve = make_static_resolver({endpoint}); config.max_connections = 1;
    config.max_waiting_calls = 2; config.max_waiting_bytes = 1024; config.interceptors = {&hooks};
    auto c = make_channel(ctx, config);
    auto const *ops = &codec_for<test_message>();
    auto unknown = c->bind({"Unknown", method_kind::unary, idempotency::unknown, ops, ops});
    auto idempotent = c->bind({"Idem", method_kind::unary, idempotency::idempotent, ops, ops});
    std::exception_ptr error; bool completed = false; test_run run;
    net::run_async(ctx.get_executor(), [&] { completed = true; }, [&](std::exception_ptr e) { error = e; c->close(); s->close(); })
        ([&] { return exercise(ctx, c.get(), s.get(), &handler, &hooks, unknown, idempotent, &run); });
    ctx.run(); if (error) std::rethrow_exception(error); CHECK(completed);
    std::ostringstream out; export_metrics(out, c->metrics()); CHECK(out.str().find("\"retries\":") != std::string::npos);
}
auto grace_call(net::io_context &ctx, channel *c, test_run *run)
    CO2_BEG(net::task<>, (ctx, c, run), call_options options; status_code warmed;) {
    options.timeout = std::chrono::milliseconds{500}; options.wait_for_ready = true;
    CO2_AWAIT_SET(warmed, c->warmup(options)); CHECK(warmed == status_code::ok);
    start_queued(ctx, c, &run->first, options);
    CO2_AWAIT(net::delay(std::chrono::milliseconds{5}));
    CO2_AWAIT(c->shutdown(std::chrono::milliseconds{100}));
    CHECK(run->first.done == 1 && run->first.value.code == status_code::ok && c->quiescent());
    CO2_RETURN();
}
CO2_END
void grace_case() {
    net::io_context ctx; echo_handler handler; test_run run;
    auto s = make_server(ctx, {{"Echo", 1, &handler}});
    channel_options options; options.max_connections = 1;
    options.resolve = make_static_resolver({s->listen({net::ip::make_address("127.0.0.1"), 0})});
    auto c = make_channel(ctx, options); std::exception_ptr error;
    net::run_async(ctx.get_executor(), [&] { s->close(); }, [&](std::exception_ptr e) { error = e; c->close(); s->close(); })
        ([&] { return grace_call(ctx, c.get(), &run); });
    ctx.run(); if (error) std::rethrow_exception(error);
}
struct ordered_hook final : interceptor {
    ordered_hook(unsigned id, std::vector<unsigned> &events) : id(id), events(events) {}
    status before(call_info const &info) override {
        events.push_back(id);
        if (refuse && (attempt_only ? info.attempt != 0 : info.attempt == 0)) return {status_code::permission_denied};
        return {};
    }
    void after(call_info const &, call_result const &) override { events.push_back(id + 10); }
    unsigned id; std::vector<unsigned> &events;
    bool refuse = false, attempt_only = false;
};
void hook_prefix() {
    net::io_context ctx; std::vector<unsigned> events;
    ordered_hook first{1, events}, second{2, events}, third{3, events};
    second.refuse = true;
    channel_options options; options.resolve = make_static_resolver({{net::ip::make_address("127.0.0.1"), 1}});
    options.interceptors = {&first, &second, &third};
    auto c = make_channel(ctx, options); std::uint8_t request = 0, response = 0;
    auto result = net::test::run_blocking(ctx, c->call("Echo", {&request, 1}, {&response, 1}));
    CHECK(result.code == status_code::permission_denied);
    CHECK((events == std::vector<unsigned>{1, 2, 12, 11}));
    c->close(); ctx.run();
}
} // namespace
int main() {
    try { channel_cases(); grace_case(); hook_prefix(); std::cout << "channel tests passed\n"; }
    catch (std::exception const &e) { std::cerr << e.what() << '\n'; return 1; }
}
