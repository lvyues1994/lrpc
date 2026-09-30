#pragma once
#include <rpc/codec.hpp>
#include <atomic>
#include <memory>
#include <vector>
#include <utility>

namespace rpc {
// Counts physical payload capacity, including bytes pinned by slices. Shared
// budget/storage may outlive sockets and be released from another thread.
class buffer_budget {
public:
    explicit buffer_budget(std::size_t limit, std::shared_ptr<buffer_budget> parent = {}) noexcept : limit_(limit), parent_(std::move(parent)) {}
    std::size_t limit() const noexcept { return limit_; }
    std::size_t used() const noexcept { return used_.load(std::memory_order_relaxed); }
    bool acquire(std::size_t bytes) noexcept;
    void release(std::size_t bytes) noexcept;
private:
    std::size_t limit_;
    std::atomic<std::size_t> used_{0};
    std::shared_ptr<buffer_budget> parent_{};
};
namespace detail { struct buffer_storage; }
class buffer_builder;
class byte_buffer {
public:
    byte_buffer() noexcept = default;
    byte_buffer(byte_buffer const &) = default;
    byte_buffer &operator=(byte_buffer const &other) { if (this != &other) { byte_buffer copy{other}; *this = std::move(copy); } return *this; }
    byte_buffer(byte_buffer &&other) noexcept : lifetimes_(std::move(other.lifetimes_)), segments_(std::move(other.segments_)), size_(std::exchange(other.size_, 0)) { other.segments_.clear(); }
    byte_buffer &operator=(byte_buffer &&other) noexcept {
        if (this != &other) { clear(); lifetimes_ = std::move(other.lifetimes_); segments_ = std::move(other.segments_); size_ = std::exchange(other.size_, 0); other.segments_.clear(); }
        return *this;
    }
    static byte_buffer copy(wire::bytes_view bytes, std::shared_ptr<buffer_budget> budget = {});
    // Transfers vector ownership and charges capacity, not size. Existing
    // mutable views must no longer be used after transfer.
    static byte_buffer adopt(std::vector<std::uint8_t> bytes, std::shared_ptr<buffer_budget> budget = {});
    static byte_buffer join(std::vector<byte_buffer> const &parts, std::size_t max_segments = 64);
    std::size_t size() const noexcept { return size_; }
    std::size_t segment_count() const noexcept { return segments_.size(); }
    std::size_t retained_capacity() const noexcept;
    bool charged_to(buffer_budget const *budget) const noexcept;
    wire::bytes_view segment(std::size_t index) const;
    byte_buffer slice(std::size_t offset, std::size_t length) const;
    bool copy_to(wire::mutable_bytes_view output) const noexcept;
    // Retains a completion receipt even for an empty message. Slices and joins
    // inherit it; final release occurs after this view releases its storage.
    void retain(std::shared_ptr<void> lifetime) { lifetimes_.push_back(std::move(lifetime)); }
    void clear() noexcept { segments_.clear(); lifetimes_.clear(); size_ = 0; }
private:
    friend class buffer_builder;
    struct segment_view {
        std::shared_ptr<detail::buffer_storage const> storage;
        std::size_t offset = 0, length = 0;
    };
    std::vector<std::shared_ptr<void>> lifetimes_{};
    std::vector<segment_view> segments_{};
    std::size_t size_ = 0;
};
// Mutable only before finish(); no mutable alias is exported with byte_buffer.
class buffer_builder {
public:
    explicit buffer_builder(std::size_t size, std::shared_ptr<buffer_budget> budget = {});
    buffer_builder(buffer_builder &&) noexcept = default;
    buffer_builder &operator=(buffer_builder &&) noexcept = default;
    buffer_builder(buffer_builder const &) = delete;
    buffer_builder &operator=(buffer_builder const &) = delete;
    wire::mutable_bytes_view buffer() noexcept;
    byte_buffer finish();
    byte_buffer finish(std::size_t size);
private:
    std::shared_ptr<detail::buffer_storage> storage_{};
};

template <> struct codec<byte_buffer> {
    static std::size_t size(byte_buffer const &value) noexcept { return value.size(); }
    static bool encode(byte_buffer const &value, wire::mutable_bytes_view output) noexcept { return value.copy_to(output); }
    static bool decode(wire::bytes_view input, byte_buffer &value) { value = byte_buffer::copy(input); return true; }
};
namespace detail {
template <> struct owned_codec_for<byte_buffer> {
    static owned_codec_ops const *get() {
        static owned_codec_ops const operations{
            [](void const *value, byte_buffer &out) { out = *static_cast<byte_buffer const *>(value); return true; },
            [](byte_buffer const &in, void *value) { *static_cast<byte_buffer *>(value) = in; return true; }};
        return &operations;
    }
};
}
} // namespace rpc
