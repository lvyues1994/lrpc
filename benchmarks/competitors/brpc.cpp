#include "common.hpp"
#include "echo.pb.h"
#include <brpc/channel.h>
#include <brpc/closure_guard.h>
#include <brpc/server.h>
#include <bthread/bthread.h>
#include <butil/logging.h>

namespace cb = comparison_bench;
namespace {
struct echo_service final : comparison::EchoService {
    void Echo(google::protobuf::RpcController *, comparison::Payload const *request,
              comparison::Payload *response, google::protobuf::Closure *done) override {
        brpc::ClosureGuard guard{done};
        response->set_payload(request->payload());
    }
};
void serve() {
    echo_service service;
    brpc::Server server;
    if (server.AddService(&service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0)
        throw std::runtime_error{"brpc AddService failed"};
    brpc::ServerOptions options;
    options.num_threads = 4;
    if (server.Start("127.0.0.1:0", &options) != 0)
        throw std::runtime_error{"brpc server startup failed"};
    cb::ready(server.listen_address().port); cb::await_shutdown();
    server.Stop(0); server.Join();
}
struct lane {
    cb::options const &config;
    cb::phase &state;
    comparison::EchoService_Stub &stub;
    std::atomic<bool> &failed;
};
void *run_lane(void *argument) {
    auto &lane = *static_cast<struct lane *>(argument);
    try {
        comparison::Payload request, reply;
        brpc::Controller controller;
        request.set_payload(cb::payload(lane.config));
        for (;;) {
            auto const index = lane.state.next.fetch_add(1, std::memory_order_relaxed);
            if (index >= lane.state.samples.size()) break;
            cb::sequence(*request.mutable_payload(), index);
            controller.Reset(); reply.Clear();
            auto &sample = lane.state.samples[index];
            sample.started_ns = cb::now_ns(lane.state);
            lane.stub.Echo(&controller, &request, &reply, nullptr);
            sample.completed_ns = cb::now_ns(lane.state);
            sample.status = not controller.Failed() and request.payload() == reply.payload() ? 0 : 1;
        }
    } catch (...) { lane.failed.store(true); }
    return nullptr;
}
std::int64_t run_phase(cb::options const &config, cb::phase &state, comparison::EchoService_Stub &stub) {
    std::atomic<bool> failed{false};
    std::vector<bthread_t> threads(config.inflight);
    lane context{config, state, stub, failed};
    state.epoch = cb::clock::now();
    std::size_t started = 0;
    for (auto &thread : threads) {
        if (bthread_start_background(&thread, nullptr, run_lane, &context) != 0) break;
        ++started;
    }
    for (std::size_t i = 0; i < started; ++i) bthread_join(threads[i], nullptr);
    if (started != threads.size() or failed.load()) throw std::runtime_error{"bthread lane failed"};
    return cb::now_ns(state);
}
void client(cb::options const &config) {
    brpc::ChannelOptions options;
    options.protocol = "baidu_std";
    options.connection_type = "single";
    options.timeout_ms = -1; options.connect_timeout_ms = -1;
    options.max_retry = 0; options.backup_request_ms = -1;
    brpc::Channel channel;
    if (channel.Init("127.0.0.1", config.port, &options) != 0)
        throw std::runtime_error{"brpc channel init failed"};
    comparison::EchoService_Stub stub{&channel};
    cb::phase warm; warm.samples.resize(config.warmup);
    run_phase(config, warm, stub);
    if (std::any_of(warm.samples.begin(), warm.samples.end(), [](auto const &s) { return s.status != 0; }))
        throw std::runtime_error{"warmup failed"};
    cb::phase measured; measured.samples.resize(config.iterations);
    rusage before{}, after{}; getrusage(RUSAGE_SELF, &before);
    auto const elapsed = run_phase(config, measured, stub);
    getrusage(RUSAGE_SELF, &after);
    cb::finish(config, measured, elapsed, cb::cpu_seconds(after) - cb::cpu_seconds(before), "brpc");
}
} // namespace
int main(int argc, char **argv) {
    try {
        auto const config = cb::parse(argc, argv);
        if (config.server) cb::block_shutdown_signals();
        logging::SetMinLogLevel(logging::BLOG_FATAL);
        if (bthread_setconcurrency(4) != 0) throw std::runtime_error{"cannot set bthread concurrency"};
        if (config.server) serve(); else client(config);
        return 0;
    } catch (std::exception const &error) { std::cerr << error.what() << '\n'; return 1; }
}
