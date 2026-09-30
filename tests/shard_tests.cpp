#include "check.hpp"
#include <rpc/runtime.hpp>
#include <net/test/run_blocking.hpp>
#include <net/timeout.hpp>
#include <thread>
#include <future>
#include <iostream>

namespace {
using namespace rpc;
struct echo_handler final : method_handler {
    unsigned invoked = 0;
    std::thread::id thread{};
    net::task<status_code> invoke(server_context &, wire::bytes_view request, response_writer &response) override;
};
auto echo(echo_handler *handler, wire::bytes_view request, response_writer *response)
    CO2_BEG(net::task<status_code>, (handler, request, response)) {
    if (handler->invoked != 0) CHECK(handler->thread == std::this_thread::get_id());
    ++handler->invoked; handler->thread = std::this_thread::get_id();
    response->assign(request); CO2_RETURN(status_code::ok);
}
CO2_END
net::task<status_code> echo_handler::invoke(server_context &, wire::bytes_view request, response_writer &response) {
    return echo(this, request, &response);
}
auto local_call(runtime *rt, client *c)
    CO2_BEG(net::task<>, (rt, c), std::uint8_t request = 42; std::uint8_t response = 0;
            call_options options; call_result result;) {
    options.timeout = std::chrono::milliseconds{500}; options.wait_for_ready = true;
    CHECK(call_sync(*rt, *c, "Echo", {&request, 1}, {&response, 1}, options).code == status_code::failed_precondition);
    CO2_AWAIT_SET(result, c->call("Echo", {&request, 1}, {&response, 1}, options));
    CHECK(result.code == status_code::ok && response == request);
    CO2_RETURN();
}
CO2_END

void shards(net::backend_kind backend, std::size_t count) {
    auto rt = make_runtime({count, backend, 1024, {}});
    server_options config{}; config.connection.receive.max_frame_size = 1024;
    config.connection.receive.max_message_size = 1024; config.connection.receive.max_concurrent_streams = 4;
    config.max_connections = 4; config.max_active_calls = 4;
    config.request_bytes = config.response_bytes = 16U * 1024U; config.control_bytes = 4096;
    config.request_bytes_per_connection = config.response_bytes_per_connection = 8192;
    config.control_bytes_per_connection = 2048;
    std::vector<std::unique_ptr<echo_handler>> handlers;
    std::vector<std::vector<method_binding>> registries;
    for (std::size_t i = 0; i < count; ++i) {
        handlers.push_back(std::make_unique<echo_handler>());
        registries.push_back({{"Echo", 1, handlers.back().get()}});
    }
    auto s = make_server(*rt, std::move(registries), config);
    auto endpoint = s->listen({net::ip::make_address("127.0.0.1"), 0});
    channel_options client_config{}; client_config.resolve = make_static_resolver({endpoint});
    client_config.connection.connection = config.connection;
    client_config.connection.request_bytes = 16U * 1024U; client_config.connection.control_bytes = 2048;
    client_config.max_connections = 1; client_config.max_retired_connections = 1;
    client_config.max_waiting_calls = 16; client_config.max_waiting_bytes = 16384;
    auto c = make_channel(*rt, client_config);
    rt->start(); net::io_context caller;
    call_options options{}; options.timeout = std::chrono::seconds{2};
    CHECK(net::test::run_blocking(caller, c->warmup(options)) == status_code::ok);
    CHECK(c->ready());
    std::vector<std::thread> callers;
    std::vector<std::exception_ptr> errors(count);
    for (std::size_t i = 0; i < count; ++i) callers.emplace_back([&, i] {
        try {
            std::uint8_t input = static_cast<std::uint8_t>(i + 1), output = 0;
            auto queued = options; queued.wait_for_capacity = true; queued.wait_for_ready = true;
            for (unsigned n = 0; n < 16; ++n) {
                auto result = call_sync(*rt, *c, "Echo", {&input, 1}, {&output, 1}, queued);
                CHECK(result.code == status_code::ok && input == output);
            }
        } catch (...) { errors[i] = std::current_exception(); }
    });
    for (auto &thread : callers) thread.join();
    for (auto error : errors) if (error) std::rethrow_exception(error);
    auto completion = std::make_shared<std::promise<void>>(); auto future = completion->get_future();
    CHECK(rt->post(0, [&rt, &c, completion] {
        net::run_async(rt->context(0).get_executor(), [completion] { completion->set_value(); },
            [completion](std::exception_ptr e) { completion->set_exception(e); })([&] { return local_call(rt.get(), c.get()); });
    }));
    future.get();
    auto metrics = c->metrics(); CHECK(metrics.calls == count * 16 + 1 && metrics.completed[0] == metrics.calls);
    net::test::run_blocking(caller, c->shutdown(std::chrono::milliseconds{100}));
    net::test::run_blocking(caller, s->shutdown(std::chrono::milliseconds{100}));
    rt->shutdown(); CHECK(!rt->accepting());
    unsigned total = 0;
    for (auto const &handler : handlers) { CHECK(handler->invoked != 0); CHECK(handler->thread != std::this_thread::get_id()); total += handler->invoked; }
    CHECK(total == count * 16 + 1);
}
} // namespace
int main(int argc, char **argv) {
    auto backend = net::backend_kind::epoll;
    if (argc > 1) {
        std::string name{argv[1]};
        if (name == "poll") backend = net::backend_kind::poll;
        else if (name == "select") backend = net::backend_kind::select;
        else if (name == "io_uring") backend = net::backend_kind::io_uring;
    }
    if (!net::backend_available(backend)) return 77;
    try { shards(backend, 2); shards(backend, 8); std::cout << "shard tests passed\n"; }
    catch (std::exception const &e) { std::cerr << e.what() << '\n'; return 1; }
}
