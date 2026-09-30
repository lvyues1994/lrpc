#include "check.hpp"
#include <rpc/byte_buffer.hpp>
#include <rpc/compression.hpp>
#include <thread>
#include <cstring>
#include <iostream>

namespace {
void buffers() {
    auto budget = std::make_shared<rpc::buffer_budget>(4096);
    auto bytes = rpc::byte_buffer::copy({reinterpret_cast<std::uint8_t const *>("abcdef"), 6}, budget);
    auto slice = bytes.slice(2, 1);
    auto moved = std::move(bytes); CHECK(bytes.size() == 0 && bytes.segment_count() == 0 && bytes.copy_to({})); moved.clear();
    CHECK(slice.size() == 1 && slice.retained_capacity() == 6 && budget->used() == 6);
    auto joined = rpc::byte_buffer::join({slice, slice});
    CHECK(joined.size() == 2 && joined.retained_capacity() == 6 && joined.segment_count() == 2);
    std::uint8_t output[2]{}; CHECK(joined.copy_to({output, 2}) && output[0] == 'c' && output[1] == 'c');
    auto const &ops = rpc::codec_for<rpc::byte_buffer>(); CHECK(ops.owned);
    rpc::byte_buffer decoded; CHECK(ops.owned->decode(joined, &decoded));
    slice.clear(); joined.clear(); CHECK(budget->used() == 6);
    std::thread releaser([value = std::move(decoded)]() mutable { value.clear(); }); releaser.join(); CHECK(budget->used() == 0);
    std::vector<std::uint8_t> external; external.reserve(1024); external.push_back(42);
    auto adopted = rpc::byte_buffer::adopt(std::move(external), budget);
    CHECK(adopted.size() == 1 && adopted.retained_capacity() >= 1024 && budget->used() == adopted.retained_capacity());
    bool refused = false; try { rpc::buffer_builder too_large(4096, budget); } catch (std::bad_alloc const &) { refused = true; }
    CHECK(refused); adopted.clear(); CHECK(budget->used() == 0);
    auto assigned = rpc::byte_buffer::copy({reinterpret_cast<std::uint8_t const *>("move"), 4}, budget);
    adopted = std::move(assigned); CHECK(assigned.size() == 0 && adopted.size() == 4); adopted.clear();
    auto parent = std::make_shared<rpc::buffer_budget>(8);
    auto child = std::make_shared<rpc::buffer_budget>(4, parent);
    rpc::buffer_builder bounded(4, child); CHECK(child->used() == 4 && parent->used() == 4);
    refused = false; try { rpc::buffer_builder overflow(1, child); } catch (std::bad_alloc const &) { refused = true; }
    CHECK(refused && parent->used() == 4);
}
void compression() {
    std::vector<std::uint8_t> input(65536, 42), decoded(input.size());
    for (auto algorithm : {rpc::compression_algorithm::zstd, rpc::compression_algorithm::lz4}) {
        auto compressor = rpc::make_message_compressor(algorithm);
        if (!compressor) continue;
        CHECK(compressor->workspace_bytes() != 0);
        std::vector<std::uint8_t> encoded(compressor->bound(input.size()));
        auto zipped = compressor->compress({input.data(), input.size()}, {encoded.data(), encoded.size()});
        CHECK(zipped.code == rpc::wire::error::none && zipped.written < input.size());
        auto restored = compressor->decompress_exact({encoded.data(), zipped.written}, {decoded.data(), decoded.size()});
        CHECK(restored.code == rpc::wire::error::none && decoded == input);
        CHECK(compressor->decompress_exact({encoded.data(), zipped.written}, {decoded.data(), decoded.size() - 1}).code != rpc::wire::error::none);
        CHECK(compressor->decompress_exact({encoded.data(), zipped.written - 1}, {decoded.data(), decoded.size()}).code != rpc::wire::error::none);
        encoded[zipped.written] = 0;
        CHECK(compressor->decompress_exact({encoded.data(), zipped.written + 1}, {decoded.data(), decoded.size()}).code != rpc::wire::error::none);
        zipped = compressor->compress({}, {encoded.data(), encoded.size()});
        CHECK(zipped.code == rpc::wire::error::none);
        CHECK(compressor->decompress_exact({encoded.data(), zipped.written}, {}).code == rpc::wire::error::none);
    }
}
}
int main() {
    try { buffers(); compression(); std::cout << "message tests passed\n"; }
    catch (std::exception const &e) { std::cerr << e.what() << '\n'; return 1; }
}
