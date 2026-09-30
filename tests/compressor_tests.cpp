#include "check.hpp"

#include <rpc/compression.hpp>

#include <iostream>
#include <vector>

namespace {

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

} // namespace

int main() {
    try {
        compression();
        std::cout << "compressor tests passed\n";
    } catch (std::exception const &e) {
        std::cerr << e.what() << '\n';
        return 1;
    }
}
