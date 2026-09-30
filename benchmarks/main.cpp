#include "bench.hpp"

#include <algorithm>
#include <cmath>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <stdexcept>
#include <sys/resource.h>

namespace bench {
namespace {

double ns(rpc::clock::duration duration) { return std::chrono::duration<double, std::nano>(duration).count(); }

void distribution(char const *name, std::vector<double> values) {
    std::cout << ",\"" << name << "\":";
    if (values.empty()) { std::cout << "null"; return; }
    std::sort(values.begin(), values.end());
    auto percentile = [&](double fraction) {
        auto const index = static_cast<std::size_t>(std::ceil(fraction * static_cast<double>(values.size()))) - 1;
        return values[index] / 1000.0;
    };
    std::cout << "{\"p50_us\":" << percentile(0.50) << ",\"p99_us\":" << percentile(0.99)
              << ",\"max_us\":" << values.back() / 1000.0 << '}';
}

options parse(int argc, char **argv) {
    options config{};
    for (int i = 1; i < argc; ++i) {
        std::string const key = argv[i];
        if (i + 1 == argc) throw std::invalid_argument{"missing option value: " + key};
        std::string const value = argv[++i];
        if (key == "--transport") { config.transport = value; continue; }
        if (key == "--codec") { config.codec = value; continue; }
        if (key == "--rpc-entry") { config.rpc_entry = value; continue; }
        if (key == "--samples") { config.samples = value; continue; }
        if (key == "--allocation-trace") { config.allocation_trace = value; continue; }
        if (key == "--role") { config.role = value; continue; }
        if (key == "--frame-allocator") {
            if (value != "system" && value != "recycling") throw std::invalid_argument{"unknown frame allocator"};
            config.recycle_frames = value == "recycling"; continue;
        }
        if (key == "--backend") {
            if (value == "epoll") config.backend = net::backend_kind::epoll;
            else if (value == "poll") config.backend = net::backend_kind::poll;
            else if (value == "select") config.backend = net::backend_kind::select;
            else if (value == "io_uring") config.backend = net::backend_kind::io_uring;
            else throw std::invalid_argument{"unknown backend"};
            continue;
        }
        if (value.empty() || value.find_first_not_of("0123456789") != std::string::npos)
            throw std::invalid_argument{"nonnegative integer required: " + key};
        auto const number = std::stoull(value);
        if (key == "--bytes") config.bytes = static_cast<std::size_t>(number);
        else if (key == "--runtime-cpu0") config.runtime_cpu0 = static_cast<unsigned>(number);
        else if (key == "--runtime-cpu1") config.runtime_cpu1 = static_cast<unsigned>(number);
        else if (key == "--arena-cache") config.arena_cache = static_cast<std::size_t>(number);
        else if (key == "--inflight") config.inflight = static_cast<std::size_t>(number);
        else if (key == "--iterations") config.iterations = static_cast<std::size_t>(number);
        else if (key == "--warmup") config.warmup = static_cast<std::size_t>(number);
        else if (key == "--rpc-streams") config.rpc_streams = static_cast<std::size_t>(number);
        else if (key == "--rate") config.rate = number;
        else if (key == "--burst") config.burst = static_cast<std::size_t>(number);
        else if (key == "--receive-buffer-bytes") config.receive_buffer_bytes = static_cast<std::size_t>(number);
        else if (key == "--deadline-us") config.deadline_us = number;
        else if (key == "--max-schedule-lag-us") config.max_schedule_lag_us = number;
        else if (key == "--port" && number <= 65535) config.port = static_cast<std::uint16_t>(number);
        else throw std::invalid_argument{"unknown option: " + key};
    }
    if ((config.transport != "net" && config.transport != "rpc" && config.transport != "v2") ||
        config.arena_cache > 65536 || (config.arena_cache && config.codec != "protobuf") ||
        (config.rpc_entry != "client" && config.rpc_entry != "channel" && config.rpc_entry != "runtime") ||
        (config.rpc_entry != "client" && (config.transport != "rpc" || config.role != "both" || config.recycle_frames)) ||
        (config.codec != "bytes" && config.codec != "protobuf") ||
        (config.transport != "rpc" && config.codec != "bytes") || (config.bytes != 64 && config.bytes != 4096) ||
        config.inflight == 0 || config.inflight > 1024 || config.iterations == 0 || config.iterations > 10000000 ||
        config.warmup == 0 || config.warmup > 1000000 || config.burst == 0 || config.burst > 1024 ||
        config.receive_buffer_bytes < 8208 || config.receive_buffer_bytes > 16U * 1024U * 1024U + 16 ||
        (config.transport == "net" && config.receive_buffer_bytes != 65536) ||
        config.rpc_streams == 0 || config.rpc_streams > 1024 || config.rate > 1000000000 ||
        config.deadline_us > 30000000 || config.max_schedule_lag_us > 30000000 || (config.transport == "net" && config.deadline_us != 0) ||
        (config.rate == 0 && config.burst != 1)) throw std::invalid_argument{"invalid benchmark configuration"};
    if (!net::backend_available(config.backend)) throw std::runtime_error{"backend unavailable"};
    if ((config.role != "both" && config.role != "server" && config.role != "client") ||
        (config.role == "client" && config.port == 0) || (config.role != "client" && config.port != 0))
        throw std::invalid_argument{"invalid role/port"};
    if (config.role == "server" && !config.allocation_trace.empty())
        throw std::invalid_argument{"allocation tracing is recorded by the measurement process only"};
#ifndef LRPC_BENCH_COUNT_ALLOCATIONS
    if (!config.allocation_trace.empty()) throw std::invalid_argument{"allocation trace requires bench-alloc or diagnose build"};
#endif
#ifndef LRPC_BENCH_PROTOBUF
    if (config.codec == "protobuf") throw std::invalid_argument{"protobuf benchmark requires LRPC_BUILD_PROTOBUF and LRPC_BUILD_CODEGEN"};
#endif
    return config;
}

#ifdef LRPC_ENABLE_DIAGNOSTICS
void queue_distribution(char const *name, rpc::detail::queue_distribution const &values) {
    std::cout << '"' << name << "\":{\"count\":" << values.count
        << ",\"total_ns\":" << values.total_ns << ",\"max_ns\":" << values.maximum_ns << ",\"buckets\":[";
    for (std::size_t i = 0; i < values.buckets.size(); ++i) { if (i) std::cout << ','; std::cout << values.buckets[i]; }
    std::cout << "]}";
}
#endif

} // namespace

cpu_times cpu_now() {
    rusage usage{};
    if (::getrusage(RUSAGE_SELF, &usage) != 0) throw std::runtime_error{"getrusage failed"};
    auto to_ns = [](timeval const &time) { return static_cast<double>(time.tv_sec) * 1e9 + static_cast<double>(time.tv_usec) * 1e3; };
    return {to_ns(usage.ru_utime), to_ns(usage.ru_stime)};
}

void report(options const &config, measurements const &result) {
#ifdef LRPC_BENCH_COUNT_ALLOCATIONS
    constexpr bool counting = true;
#else
    constexpr bool counting = false;
#endif
#ifdef LRPC_ENABLE_DIAGNOSTICS
    constexpr bool queues = true;
#else
    constexpr bool queues = false;
#endif
    constexpr bool diagnostic = counting || queues || LRPC_BENCH_SANITIZED;
    std::vector<double> success, rejected, timed_out, failed, schedule_lag, total_latency, offer_lag;
    std::array<std::size_t, 17> statuses{};
    std::size_t drops = 0;
    std::ofstream samples;
    if (!config.samples.empty()) {
        samples.open(config.samples);
        if (!samples) throw std::runtime_error{"cannot open sample file"};
        samples << "id,scheduled_ns,offered_ns,started_ns,completed_ns,status,generator_drop\n";
    }
    auto relative = [&](rpc::clock::time_point time) {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(time - result.epoch).count();
    };
    for (std::size_t i = 0; i < result.samples.size(); ++i) {
        auto const &entry = result.samples[i];
        offer_lag.push_back(ns(entry.offered - entry.scheduled));
        if (entry.generator_drop) ++drops;
        else {
            auto const code = static_cast<std::size_t>(entry.code);
            ++statuses.at(code);
            auto &latencies = entry.code == rpc::status_code::ok ? success :
                entry.code == rpc::status_code::resource_exhausted ? rejected :
                entry.code == rpc::status_code::deadline_exceeded ? timed_out : failed;
            latencies.push_back(ns(entry.completed - entry.started));
            schedule_lag.push_back(ns(entry.started - entry.scheduled));
            if (entry.code == rpc::status_code::ok) total_latency.push_back(ns(entry.completed - entry.scheduled));
        }
        if (samples.is_open()) samples << i << ',' << relative(entry.scheduled) << ',' << relative(entry.offered) << ','
            << (entry.generator_drop ? -1 : relative(entry.started)) << ',' << relative(entry.completed) << ','
            << (entry.generator_drop ? -1 : static_cast<int>(entry.code)) << ',' << entry.generator_drop << '\n';
    }
    if (samples.is_open()) { samples.flush(); if (!samples) throw std::runtime_error{"sample write failed"}; }
    auto const seconds = ns(result.finished - result.epoch) / 1e9;
    auto const offering_seconds = ns(result.last_offer - result.epoch) / 1e9;
    auto first_started = result.finished;
    auto last_started = result.epoch;
    for (auto const &entry : result.samples) if (!entry.generator_drop) {
        first_started = std::min(first_started, entry.started); last_started = std::max(last_started, entry.started);
    }
    auto const actual_start_span = ns(last_started - first_started) / 1e9;
    auto const load_faithful = drops == 0 && !schedule_lag.empty() &&
        *std::max_element(schedule_lag.begin(), schedule_lag.end()) <= static_cast<double>(config.max_schedule_lag_us) * 1000;
    auto const count = static_cast<double>(config.iterations);
    std::cout << std::fixed << std::setprecision(3)
        << "{\"transport\":\"" << config.transport << "\",\"codec\":\"" << config.codec
        << "\",\"mode\":\"" << (config.rate ? "open" : "closed")
        << "\",\"backend\":\"" << net::to_string(config.backend) << "\",\"build\":\"" << LRPC_BENCH_BUILD_TYPE
        << "\",\"raw_path\":\"" << (config.transport != "net" ? "none" : config.inflight == 1 && config.rate == 0 ? "direct" : "fifo")
        << "\",\"topology\":\"" << (config.rpc_entry == "runtime" ? "fixed_shard_loopback" : config.role == "client" ? "separate_process_loopback" : "same_thread_loopback")
        << "\",\"cpu_scope\":\"" << (config.role == "client" ? "client_process" : "both_endpoints")
        << "\",\"rpc_entry\":\"" << config.rpc_entry << "\",\"connections\":" << (config.rpc_entry == "runtime" ? 2 : 1)
        << ",\"threads\":" << (config.rpc_entry == "runtime" ? 3 : config.role == "client" ? 2 : 1) << ",\"bytes\":" << config.bytes
        << ",\"inflight_limit\":" << config.inflight << ",\"max_inflight\":" << result.maximum_inflight
        << ",\"rpc_streams\":" << config.rpc_streams << ",\"warmup\":" << config.warmup
        << ",\"offered\":" << config.iterations << ",\"started\":" << config.iterations - drops
        << ",\"success\":" << success.size() << ",\"rejected\":" << rejected.size() << ",\"timeout\":" << timed_out.size()
        << ",\"other_errors\":" << failed.size() << ",\"generator_drop\":" << drops
        << ",\"target_rate\":" << config.rate << ",\"burst\":" << config.burst << ",\"deadline_us\":" << config.deadline_us
        << ",\"receive_buffer_bytes\":" << config.receive_buffer_bytes
        << ",\"frame_allocator\":\"" << (config.recycle_frames ? "recycling" : "system") << '"'
        << ",\"arena_cache_entries\":" << config.arena_cache
        << ",\"max_schedule_lag_us\":" << config.max_schedule_lag_us
        << ",\"diagnostic_only\":" << (diagnostic ? "true" : "false")
        << ",\"allocation_counting\":" << (counting ? "true" : "false")
        << ",\"allocation_stacks\":" << (!config.allocation_trace.empty() ? "true" : "false")
        << ",\"queue_instrumentation\":" << (queues ? "true" : "false")
        << ",\"sanitizers\":" << (LRPC_BENCH_SANITIZED ? "true" : "false")
        << ",\"load_faithful\":" << (config.rate == 0 ? "null" : load_faithful ? "true" : "false")
        << ",\"eligible_for_zero_error_p99\":" << (!diagnostic && (config.rate == 0 || load_faithful) && success.size() == config.iterations ? "true" : "false")
        << ",\"elapsed_ms\":" << seconds * 1000 << ",\"offering_ms\":" << offering_seconds * 1000
        << ",\"last_started_ms\":" << (drops == config.iterations ? 0 : ns(last_started - result.epoch) / 1e6)
        << ",\"actual_start_span_rate\":" << (config.iterations - drops > 1 && actual_start_span > 0 ?
            static_cast<double>(config.iterations - drops - 1) / actual_start_span : 0)
        << ",\"drain_ms\":" << ns(result.finished - result.last_offer) / 1e6
        << ",\"success_per_second\":" << static_cast<double>(success.size()) / seconds
        << ",\"started_per_second\":" << static_cast<double>(config.iterations - drops) / seconds
        << ",\"offered_per_second\":" << count / seconds
        << ",\"reject_fraction_of_started\":" << (drops == config.iterations ? 0.0 : static_cast<double>(rejected.size()) / static_cast<double>(config.iterations - drops))
        << ",\"user_ns_per_offered\":" << result.cpu.user_ns / count
        << ",\"system_ns_per_offered\":" << result.cpu.system_ns / count
        << ",\"new_calls_per_offered\":";
#ifdef LRPC_BENCH_COUNT_ALLOCATIONS
    std::cout << static_cast<double>(result.new_calls) / count;
#else
    std::cout << "null";
#endif
    std::cout << ",\"rpc_queue_latency\":";
#ifdef LRPC_ENABLE_DIAGNOSTICS
    if (config.transport != "rpc") std::cout << "null";
    else {
        std::cout << "{\"scope\":\"" << (config.role == "client" ? "client_process" : "both_endpoints")
            << "\",\"population\":\"submitted_frames_only\",\"unit\":\"ns\",\"bucket_upper_bound\":\"2^index\",\"boundary\":\"enqueue_to_first_write_submission\",";
        queue_distribution("requests", result.queues.requests); std::cout << ',';
        if (config.role == "client") std::cout << "\"responses\":null";
        else queue_distribution("responses", result.queues.responses);
        std::cout << ','; queue_distribution("controls", result.queues.controls); std::cout << '}';
    }
#else
    std::cout << "null";
#endif
    std::cout << ",\"response_validation\":true,\"status_counts\":[";
    for (std::size_t i = 0; i < statuses.size(); ++i) { if (i) std::cout << ','; std::cout << statuses[i]; }
    std::cout << ']';
    distribution("success_api", std::move(success)); distribution("rejected_api", std::move(rejected));
    distribution("timeout_api", std::move(timed_out)); distribution("other_error_api", std::move(failed));
    distribution("schedule_lag", std::move(schedule_lag)); distribution("success_scheduled", std::move(total_latency));
    distribution("offer_lag", std::move(offer_lag));
    std::cout << "}\n";
}

} // namespace bench

int main(int argc, char **argv) {
    try {
        auto const config = bench::parse(argc, argv);
        if (config.role == "server") bench::serve(config);
        else {
            bench::report(config, bench::run(config));
#ifdef LRPC_BENCH_COUNT_ALLOCATIONS
            if (!config.allocation_trace.empty()) bench::write_allocation_trace(config.allocation_trace);
#endif
        }
    }
    catch (std::exception const &error) { std::cerr << error.what() << '\n'; return 1; }
}
