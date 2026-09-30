#include <rpc/json.hpp>
#include <algorithm>
#include <chrono>
#include <iostream>

struct Payload { std::uint64_t id = 7; std::string text; std::vector<int> flags{1, 2, 3}; };
RPC_JSON_FIELDS(Payload, id, text, flags);
namespace {
using timer = std::chrono::steady_clock;
void benchmark(std::string const &mode, std::size_t text_bytes, unsigned iterations) {
    Payload request, reply; request.text.assign(text_bytes, 'x');
    std::vector<std::uint8_t> buffer(rpc::json_codec<Payload>::upper_bound(request));
    auto const encoded = rpc::json_codec<Payload>::encode_bounded(request, {buffer.data(), buffer.size()});
    if (encoded.code != rpc::wire::error::none) throw std::runtime_error{"initial encode failed"};
    auto const operation = [&] {
        if (mode == "bounded_encode") {
            auto result = rpc::json_codec<Payload>::encode_bounded(request, {buffer.data(), buffer.size()});
            if (result.code != rpc::wire::error::none || result.written != encoded.written) throw std::runtime_error{"bounded encode failed"};
        } else if (mode == "size_then_encode") {
            auto size = rpc::json_codec<Payload>::size(request);
            if (!rpc::json_codec<Payload>::encode(request, {buffer.data(), size})) throw std::runtime_error{"exact encode failed"};
        } else if (!rpc::json_codec<Payload>::decode({buffer.data(), encoded.written}, reply) || reply.text != request.text)
            throw std::runtime_error{"decode failed"};
    };
    for (unsigned i = 0; i < 1000; ++i) operation();
    std::vector<std::uint64_t> samples; samples.reserve(iterations); std::uint64_t total = 0;
    for (unsigned i = 0; i < iterations; ++i) {
        auto begin = timer::now(); operation();
        auto const ns = static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(timer::now() - begin).count());
        total += ns; samples.push_back(ns);
    }
    std::sort(samples.begin(), samples.end());
    std::cout << "{\"mode\":\"" << mode << "\",\"text_bytes\":" << text_bytes << ",\"wire_bytes\":" << encoded.written
              << ",\"iterations\":" << iterations << ",\"p50_ns\":" << samples[iterations / 2]
              << ",\"p99_ns\":" << samples[(static_cast<std::size_t>(iterations) * 99) / 100]
              << ",\"mean_ns\":" << total / iterations << "}\n";
}
}
int main(int argc, char **argv) {
    try {
        auto const requested = argc > 1 ? std::stoul(argv[1]) : 100000UL;
        if (!requested || requested > 10000000) throw std::invalid_argument{"iterations must be in 1..10000000"};
        auto const iterations = static_cast<unsigned>(requested);
        for (auto bytes : {64U, 256U, 1024U}) for (auto const *mode : {"bounded_encode", "size_then_encode", "decode"}) benchmark(mode, bytes, iterations);
    } catch (std::exception const &e) { std::cerr << e.what() << '\n'; return 1; }
}
