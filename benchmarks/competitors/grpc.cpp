#include "common.hpp"
#include "echo.grpc.pb.h"
#include <grpcpp/grpcpp.h>
#include <thread>
#include <mutex>

namespace cb = comparison_bench;
namespace {
struct server_slot {
    bool finishing = false;
    std::unique_ptr<grpc::ServerContext> context;
    std::unique_ptr<grpc::ServerAsyncResponseWriter<comparison::Payload>> writer;
    comparison::Payload request;
    comparison::Payload reply;
};
void accept(server_slot &slot, comparison::EchoService::AsyncService &service,
            grpc::ServerCompletionQueue &queue) {
    slot.finishing = false;
    slot.request.Clear(); slot.reply.Clear();
    slot.writer.reset();
    slot.context = std::make_unique<grpc::ServerContext>();
    slot.writer = std::make_unique<grpc::ServerAsyncResponseWriter<comparison::Payload>>(slot.context.get());
    service.RequestEcho(slot.context.get(), &slot.request, slot.writer.get(), &queue, &queue, &slot);
}
void serve() {
    comparison::EchoService::AsyncService service;
    grpc::ServerBuilder builder;
    int port = 0;
    builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port);
    builder.RegisterService(&service);
    auto queue = builder.AddCompletionQueue();
    auto server = builder.BuildAndStart();
    if (not server) throw std::runtime_error{"gRPC server startup failed"};
    std::vector<server_slot> slots(128);
    for (auto &slot : slots) accept(slot, service, *queue);
    std::mutex lifecycle;
    bool stopping = false;
    auto poller = std::thread{[&] {
        void *tag = nullptr; bool ok = false;
        while (queue->Next(&tag, &ok)) {
            auto &slot = *static_cast<server_slot *>(tag);
            if (slot.finishing) {
                std::lock_guard<std::mutex> guard{lifecycle};
                if (not stopping) accept(slot, service, *queue);
            } else if (ok) {
                slot.reply.set_payload(slot.request.payload());
                slot.finishing = true;
                slot.writer->Finish(slot.reply, grpc::Status::OK, &slot);
            }
        }
    }};
    cb::ready(port); cb::await_shutdown();
    { std::lock_guard<std::mutex> guard{lifecycle}; stopping = true; }
    server->Shutdown(std::chrono::system_clock::now() + std::chrono::seconds{5});
    queue->Shutdown(); poller.join();
}
struct client_slot {
    std::size_t index = 0;
    comparison::Payload request;
    comparison::Payload reply;
    grpc::Status status;
    std::unique_ptr<grpc::ClientContext> context;
    std::unique_ptr<grpc::ClientAsyncResponseReader<comparison::Payload>> reader;
};
void issue(client_slot &slot, std::size_t index, cb::phase &phase,
           comparison::EchoService::Stub &stub, grpc::CompletionQueue &queue) {
    slot.index = index;
    cb::sequence(*slot.request.mutable_payload(), index);
    slot.reader.reset(); slot.reply.Clear();
    slot.context = std::make_unique<grpc::ClientContext>();
    phase.samples[index].started_ns = cb::now_ns(phase);
    slot.reader = stub.AsyncEcho(slot.context.get(), slot.request, &queue);
    slot.reader->Finish(&slot.reply, &slot.status, &slot);
}
std::int64_t run_phase(cb::options const &config, cb::phase &phase,
                       comparison::EchoService::Stub &stub, grpc::CompletionQueue &queue) {
    std::vector<client_slot> slots(config.inflight);
    for (auto &slot : slots) slot.request.set_payload(cb::payload(config));
    phase.epoch = cb::clock::now();
    std::size_t issued = 0;
    for (auto &slot : slots) if (issued < phase.samples.size()) issue(slot, issued++, phase, stub, queue);
    for (std::size_t completed = 0; completed < phase.samples.size(); ++completed) {
        void *tag = nullptr; bool ok = false;
        if (not queue.Next(&tag, &ok)) throw std::runtime_error{"premature CQ shutdown"};
        auto &slot = *static_cast<client_slot *>(tag);
        auto &sample = phase.samples[slot.index];
        sample.completed_ns = cb::now_ns(phase);
        sample.status = ok and slot.status.ok() and slot.reply.payload() == slot.request.payload() ? 0 : 1;
        if (issued < phase.samples.size()) issue(slot, issued++, phase, stub, queue);
    }
    return cb::now_ns(phase);
}
void client(cb::options const &config) {
    grpc::ChannelArguments arguments;
    arguments.SetInt(GRPC_ARG_ENABLE_RETRIES, 0);
    auto channel = grpc::CreateCustomChannel("127.0.0.1:" + std::to_string(config.port),
        grpc::InsecureChannelCredentials(), arguments);
    if (not channel->WaitForConnected(std::chrono::system_clock::now() + std::chrono::seconds{10}))
        throw std::runtime_error{"gRPC connect failed"};
    auto stub = comparison::EchoService::NewStub(channel);
    grpc::CompletionQueue queue;
    cb::phase warm; warm.samples.resize(config.warmup);
    run_phase(config, warm, *stub, queue);
    if (std::any_of(warm.samples.begin(), warm.samples.end(), [](auto const &s) { return s.status != 0; }))
        throw std::runtime_error{"warmup failed"};
    cb::phase measured; measured.samples.resize(config.iterations);
    rusage before{}, after{}; getrusage(RUSAGE_SELF, &before);
    auto const elapsed = run_phase(config, measured, *stub, queue);
    getrusage(RUSAGE_SELF, &after);
    queue.Shutdown(); void *tag = nullptr; bool ok = false;
    while (queue.Next(&tag, &ok)) {}
    cb::finish(config, measured, elapsed, cb::cpu_seconds(after) - cb::cpu_seconds(before), "grpc");
}
} // namespace
int main(int argc, char **argv) {
    try {
        auto const config = cb::parse(argc, argv);
        if (config.server) cb::block_shutdown_signals();
        if (config.server) serve(); else client(config);
        return 0;
    } catch (std::exception const &error) { std::cerr << error.what() << '\n'; return 1; }
}
