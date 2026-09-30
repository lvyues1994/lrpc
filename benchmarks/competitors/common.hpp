#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>
#include <pthread.h>
#include <sys/resource.h>
#include <unistd.h>

namespace comparison_bench {
using clock = std::chrono::steady_clock;
struct options {
    bool server = false;
    int port = 0;
    std::size_t bytes = 64;
    std::size_t inflight = 1;
    std::size_t iterations = 100000;
    std::size_t warmup = 10000;
    std::string samples;
};
struct sample {
    std::int64_t started_ns = 0;
    std::int64_t completed_ns = 0;
    int status = -1;
};
struct phase {
    clock::time_point epoch = clock::now();
    std::vector<sample> samples;
    std::atomic<std::size_t> next{0};
};

inline options parse(int argc, char **argv) {
    options result;
    for (int i = 1; i < argc; i += 2) {
        if (i + 1 == argc) throw std::invalid_argument{"missing option value"};
        auto const key = std::string_view{argv[i]};
        auto const value = std::string{argv[i + 1]};
        if (key == "--role") {
            if (value != "server" and value != "client") throw std::invalid_argument{"invalid role"};
            result.server = value == "server";
        } else if (key == "--port") result.port = std::stoi(value);
        else if (key == "--bytes") result.bytes = std::stoull(value);
        else if (key == "--inflight") result.inflight = std::stoull(value);
        else if (key == "--iterations") result.iterations = std::stoull(value);
        else if (key == "--warmup") result.warmup = std::stoull(value);
        else if (key == "--samples") result.samples = value;
        else throw std::invalid_argument{"unknown option: " + std::string{key}};
    }
    if ((result.bytes != 64 and result.bytes != 4096) or result.inflight == 0 or
        result.inflight > 64 or result.iterations == 0 or result.warmup == 0 or
        result.port < 0 or result.port > 65535 or
        (not result.server and (result.port == 0 or result.samples.empty())))
        throw std::invalid_argument{"invalid benchmark configuration"};
    return result;
}

inline std::size_t body_bytes(options const &config) {
    return config.bytes == 64 ? 62 : 4093;
}
inline std::int64_t now_ns(phase const &state) {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(clock::now() - state.epoch).count();
}
inline std::string payload(options const &config) {
    return std::string(body_bytes(config), 'x');
}
inline void sequence(std::string &body, std::size_t index) {
    auto const value = static_cast<std::uint64_t>(index);
    std::memcpy(body.data(), &value, sizeof(value));
}
inline void block_shutdown_signals() {
    sigset_t signals;
    sigemptyset(&signals); sigaddset(&signals, SIGINT); sigaddset(&signals, SIGTERM);
    if (pthread_sigmask(SIG_BLOCK, &signals, nullptr) != 0)
        throw std::runtime_error{"cannot block shutdown signals"};
}
inline void await_shutdown() {
    sigset_t signals;
    sigemptyset(&signals); sigaddset(&signals, SIGINT); sigaddset(&signals, SIGTERM);
    int received = 0;
    if (sigwait(&signals, &received) != 0) throw std::runtime_error{"sigwait failed"};
}
inline void ready(int port) {
    if (port <= 0) throw std::runtime_error{"invalid listening port"};
    std::cout << "{\"ready\":true,\"port\":" << port << ",\"pid\":" << ::getpid()
              << "}\n" << std::flush;
}
inline double cpu_seconds(rusage const &usage) {
    return static_cast<double>(usage.ru_utime.tv_sec + usage.ru_stime.tv_sec) +
           static_cast<double>(usage.ru_utime.tv_usec + usage.ru_stime.tv_usec) / 1e6;
}
inline void finish(options const &config, phase const &state, std::int64_t elapsed,
                   double cpu, char const *framework) {
    std::ofstream output{config.samples};
    output << "sequence,started_ns,completed_ns,status\n";
    std::vector<std::int64_t> latencies;
    std::size_t errors = 0;
    for (std::size_t i = 0; i < state.samples.size(); ++i) {
        auto const &item = state.samples[i];
        output << i << ',' << item.started_ns << ',' << item.completed_ns << ',' << item.status << '\n';
        if (item.status != 0 or item.started_ns <= 0 or item.completed_ns <= item.started_ns or
            item.completed_ns > elapsed) ++errors;
        else latencies.push_back(item.completed_ns - item.started_ns);
    }
    output.close();
    if (not output or errors != 0 or latencies.size() != config.iterations)
        throw std::runtime_error{"failed requests or incomplete sample output"};
    std::sort(latencies.begin(), latencies.end());
    auto const rank = (99 * latencies.size() + 99) / 100 - 1;
    std::cout << "{\"framework\":\"" << framework << "\",\"bytes\":" << config.bytes
              << ",\"body_bytes\":" << body_bytes(config) << ",\"inflight\":" << config.inflight
              << ",\"iterations\":" << config.iterations << ",\"warmup\":" << config.warmup
              << ",\"errors\":" << errors << ",\"elapsed_ns\":" << elapsed
              << ",\"p99_ns\":" << latencies[rank] << ",\"client_cpu_seconds\":" << cpu
              << "}\n";
}
} // namespace comparison_bench
