#include "check.hpp"
#include "backend.hpp"
#include "echo.rpc.hpp"
#include <rpc/runtime.hpp>
#include <net/run_async.hpp>
#include <net/test/run_blocking.hpp>
#include <net/timeout.hpp>
#include <iostream>

namespace {
using namespace rpc;
using server_call = server_stream<demo::EchoRequest, demo::EchoReply>;
using client_call = stream_call<demo::EchoRequest, demo::EchoReply>;
struct service final : demo::MixedService {
    net::task<status_code> Unary(server_context &, demo::EchoRequest const &a, demo::EchoReply &b) override { return unary(&a, &b); }
    static auto unary(demo::EchoRequest const *a, demo::EchoReply *b)
        CO2_BEG(net::task<status_code>, (a, b)) { b->set_payload(a->payload()); CO2_RETURN(status_code::ok); } CO2_END
    net::task<status_code> Upload(server_context &, server_call &s) override { return upload(&s); }
    static auto upload(server_call *s)
        CO2_BEG(net::task<status_code>, (s), demo::EchoRequest request; demo::EchoReply response; stream_read_result read; status_code sent;) {
        for (;;) {
            CO2_AWAIT_SET(read, s->read(request)); if (read.code != status_code::ok) CO2_RETURN(read.code);
            if (read.ended) break;
            response.mutable_payload()->append(request.payload());
        }
        CO2_AWAIT_SET(sent, s->write(response)); CO2_RETURN(sent);
    }
    CO2_END
    net::task<status_code> Download(server_context &, server_call &s) override { return download(&s); }
    static auto download(server_call *s)
        CO2_BEG(net::task<status_code>, (s), demo::EchoRequest request; demo::EchoReply response; stream_read_result read; status_code sent;) {
        CO2_AWAIT_SET(read, s->read(request)); if (read.code != status_code::ok || read.ended) CO2_RETURN(status_code::invalid_argument);
        response.set_payload(request.payload()); CO2_AWAIT_SET(read, s->read(request));
        if (!read.ended) CO2_RETURN(status_code::invalid_argument);
        CO2_AWAIT_SET(sent, s->write(response)); if (sent != status_code::ok) CO2_RETURN(sent);
        CO2_AWAIT_SET(sent, s->write(response)); CO2_RETURN(sent);
    }
    CO2_END
    net::task<status_code> Chat(server_context &context, server_call &s) override {
        CHECK(context.metadata.count == 1);
        wire::metadata_entry entry{{reinterpret_cast<std::uint8_t const *>("stream"), 6}, {reinterpret_cast<std::uint8_t const *>("ok"), 2}};
        CHECK(context.response_metadata.assign({&entry, 1})); return chat(&s);
    }
    static auto chat(server_call *s)
        CO2_BEG(net::task<status_code>, (s), demo::EchoRequest request; demo::EchoReply response; stream_read_result read; status_code sent;) {
        for (;;) {
            CO2_AWAIT_SET(read, s->read(request)); if (read.code != status_code::ok) CO2_RETURN(read.code);
            if (read.ended) break;
            response.set_payload(request.payload());
            CO2_AWAIT_SET(sent, s->write(response)); if (sent != status_code::ok) CO2_RETURN(sent);
        }
        CO2_RETURN(status_code::ok);
    }
    CO2_END
};
auto exercise(demo::MixedStub *stub)
    CO2_BEG(net::task<>, (stub), demo::EchoRequest request; demo::EchoReply response; call_result result;
            client_call call; status_code sent; stream_read_result read; call_options options;
            std::array<wire::metadata_entry, 1> metadata{}; std::array<std::uint8_t, 64> response_metadata{};) {
    request.set_payload(std::string(4096, 'p')); options.timeout = std::chrono::seconds{3}; options.wait_for_ready = true;
    metadata[0] = {{reinterpret_cast<std::uint8_t const *>("caller"), 6}, {reinterpret_cast<std::uint8_t const *>("stream"), 6}};
    options.metadata = {metadata.data(), metadata.size()}; options.response_metadata = {response_metadata.data(), response_metadata.size()};
    CO2_AWAIT_SET(result, stub->Unary(request, response, options)); if (result.code != status_code::ok) throw std::runtime_error{"unary status=" + std::to_string(static_cast<unsigned>(result.code))}; CHECK(response.payload() == request.payload());
    CO2_AWAIT_SET(call, stub->Upload(options)); CHECK(call);
    CO2_AWAIT_SET(sent, call.write(request)); CHECK(sent == status_code::ok);
    CO2_AWAIT_SET(sent, call.write(request)); CHECK(sent == status_code::ok);
    CO2_AWAIT_SET(sent, call.writes_done()); CHECK(sent == status_code::ok);
    CO2_AWAIT_SET(read, call.read(response)); CHECK(read.code == status_code::ok && !read.ended && response.payload().size() == 8192);
    CO2_AWAIT_SET(result, call.finish()); CHECK(result.code == status_code::ok);
    CO2_AWAIT_SET(call, stub->Download(options)); CHECK(call);
    CO2_AWAIT_SET(sent, call.write(request)); CHECK(sent == status_code::ok);
    CO2_AWAIT_SET(sent, call.writes_done()); CHECK(sent == status_code::ok);
    CO2_AWAIT_SET(read, call.read(response)); CHECK(read.code == status_code::ok && !read.ended && response.payload() == request.payload());
    CO2_AWAIT_SET(read, call.read(response)); CHECK(read.code == status_code::ok && !read.ended);
    CO2_AWAIT_SET(read, call.read(response)); CHECK(read.code == status_code::ok && read.ended);
    CO2_AWAIT_SET(result, call.finish()); CHECK(result.code == status_code::ok);
    CO2_AWAIT_SET(call, stub->Chat(options)); CHECK(call);
    CO2_AWAIT_SET(sent, call.write(request)); CHECK(sent == status_code::ok);
    CO2_AWAIT_SET(read, call.read(response)); CHECK(read.code == status_code::ok && response.payload() == request.payload());
    CO2_AWAIT_SET(sent, call.writes_done()); CHECK(sent == status_code::ok);
    CO2_AWAIT_SET(read, call.read(response)); CHECK(read.code == status_code::ok && read.ended);
    CO2_AWAIT_SET(result, call.finish()); CHECK(result.code == status_code::ok && result.response_metadata.count == 1);
    CHECK(wire::decode_metadata_entry(result.response_metadata.entries).value.key.size == 6);
    CO2_AWAIT_SET(call, stub->Chat(options)); CHECK(call);
    call.cancel(); CO2_AWAIT_SET(result, call.finish()); CHECK(result.code == status_code::cancelled);
    CO2_RETURN();
}
CO2_END
}
int main(int argc, char **argv) {
    auto backend = test_backend(argc, argv); if (!net::backend_available(backend)) return 77;
    service implementation; demo::MixedLimits limits;
    limits.Unary = limits.Upload = limits.Download = limits.Chat = {16384, 2};
    limits.Chat.max_response_head_bytes = 128;
    connection_options connection; connection.receive.features |= wire::streaming; connection.receive.initial_stream_window = 16384;
    connection.receive.max_frame_size = 256; connection.receive.max_message_size = 16384;
    auto rt = make_runtime({2, backend}); server_options config; config.connection = connection;
    std::vector<std::vector<method_binding>> methods(2);
    for (auto &shard : methods) shard = demo::Mixed_bindings(implementation, limits);
    auto server = make_server(*rt, std::move(methods), config);
    auto endpoint = server->listen({net::ip::address_v4::loopback(), 0});
    channel_options options; options.connection.connection = connection; options.resolve = make_static_resolver({endpoint});
    options.max_waiting_calls = 8; options.max_waiting_bytes = 65536;
    auto channel = make_channel(*rt, options); demo::MixedStub stub{*channel}; rt->start();
    net::io_context caller{net::default_backend, net::single_thread_hint}; std::exception_ptr error;
    net::run_async(caller.get_executor(), [] {}, [&](std::exception_ptr e) { error = e; })([&] { return exercise(&stub); }); caller.run();
    net::test::run_blocking(caller, channel->shutdown(std::chrono::milliseconds{20}));
    net::test::run_blocking(caller, server->shutdown(std::chrono::milliseconds{20})); rt->shutdown();
    if (error) std::rethrow_exception(error);
    CHECK(channel->metrics().resources.active_calls == 0);
    std::cout << "PASS generated mixed service, typed streaming and cross-shard calls\n";
}
