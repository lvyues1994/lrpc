#include <rpc/byte_buffer.hpp>
#include <algorithm>
#include <cstring>
#include <limits>
#include <stdexcept>

namespace rpc {
namespace detail {
struct buffer_storage {
    explicit buffer_storage(std::size_t capacity, std::shared_ptr<buffer_budget> budget)
        : capacity(capacity), budget(std::move(budget)) {
        if (this->budget && !this->budget->acquire(capacity)) throw std::bad_alloc{};
        charged = true;
    }
    ~buffer_storage() {
        bytes.reset(); std::vector<std::uint8_t>{}.swap(adopted);
        if (charged && budget) budget->release(capacity);
    }
    std::unique_ptr<std::uint8_t[]> bytes{};
    std::vector<std::uint8_t> adopted{};
    std::uint8_t *data = nullptr;
    std::size_t capacity = 0, size = 0;
    std::shared_ptr<buffer_budget> budget{};
    bool charged = false;
};
}
bool buffer_budget::acquire(std::size_t bytes) noexcept {
    if (parent_ && !parent_->acquire(bytes)) return false;
    auto used = used_.load(std::memory_order_relaxed);
    do { if (bytes > limit_ - used) { if (parent_) parent_->release(bytes); return false; } }
    while (!used_.compare_exchange_weak(used, used + bytes, std::memory_order_relaxed));
    return true;
}
void buffer_budget::release(std::size_t bytes) noexcept { used_.fetch_sub(bytes, std::memory_order_relaxed); if (parent_) parent_->release(bytes); }
buffer_builder::buffer_builder(std::size_t size, std::shared_ptr<buffer_budget> budget)
    : storage_(std::make_shared<detail::buffer_storage>(size, std::move(budget))) {
    if (size != 0) storage_->bytes.reset(new std::uint8_t[size]);
    storage_->data = storage_->bytes.get(); storage_->size = size;
}
wire::mutable_bytes_view buffer_builder::buffer() noexcept { return storage_ ? wire::mutable_bytes_view{storage_->data, storage_->size} : wire::mutable_bytes_view{}; }
byte_buffer buffer_builder::finish() {
    return finish(storage_ ? storage_->size : 0);
}
byte_buffer buffer_builder::finish(std::size_t size) {
    byte_buffer result;
    if (storage_) {
        if (size > storage_->size) throw std::out_of_range{"buffer commit"};
        result.size_ = size;
        if (size != 0) result.segments_.push_back({storage_, 0, size});
        storage_.reset();
    }
    return result;
}
byte_buffer byte_buffer::copy(wire::bytes_view bytes, std::shared_ptr<buffer_budget> budget) {
    if (bytes.size && !bytes.data) throw std::invalid_argument{"null buffer input"};
    buffer_builder result(bytes.size, std::move(budget));
    if (bytes.size) std::memcpy(result.buffer().data, bytes.data, bytes.size);
    return result.finish();
}
byte_buffer byte_buffer::adopt(std::vector<std::uint8_t> bytes, std::shared_ptr<buffer_budget> budget) {
    byte_buffer result;
    if (bytes.empty()) return result; // Do not pin unused external capacity.
    auto storage = std::make_shared<detail::buffer_storage>(bytes.capacity(), std::move(budget));
    storage->adopted = std::move(bytes); storage->data = storage->adopted.data(); storage->size = storage->adopted.size();
    result.size_ = storage->size; result.segments_.push_back({std::move(storage), 0, result.size_}); return result;
}
wire::bytes_view byte_buffer::segment(std::size_t index) const {
    auto const &value = segments_.at(index); return {value.storage->data + value.offset, value.length};
}
std::size_t byte_buffer::retained_capacity() const noexcept {
    std::size_t result = 0;
    for (std::size_t i = 0; i < segments_.size(); ++i) {
        bool seen = false;
        for (std::size_t j = 0; j < i; ++j) if (segments_[j].storage == segments_[i].storage) { seen = true; break; }
        if (!seen) result += segments_[i].storage->capacity;
    }
    return result;
}
bool byte_buffer::charged_to(buffer_budget const *budget) const noexcept {
    for (auto const &segment : segments_) if (segment.storage->budget.get() != budget) return false;
    return true;
}
byte_buffer byte_buffer::join(std::vector<byte_buffer> const &parts, std::size_t max_segments) {
    byte_buffer result;
    for (auto const &part : parts) {
        if (part.segment_count() > max_segments - result.segment_count() || part.size() > std::numeric_limits<std::size_t>::max() - result.size_)
            throw std::invalid_argument{"buffer segment/length limit"};
        result.segments_.insert(result.segments_.end(), part.segments_.begin(), part.segments_.end()); result.size_ += part.size();
        if (part.lifetimes_.size() > max_segments - result.lifetimes_.size()) throw std::invalid_argument{"buffer receipt limit"};
        result.lifetimes_.insert(result.lifetimes_.end(), part.lifetimes_.begin(), part.lifetimes_.end());
    }
    return result;
}
byte_buffer byte_buffer::slice(std::size_t offset, std::size_t length) const {
    if (offset > size_ || length > size_ - offset) throw std::out_of_range{"buffer slice"};
    byte_buffer result; result.size_ = length; result.lifetimes_ = lifetimes_;
    for (auto const &part : segments_) {
        if (offset >= part.length) { offset -= part.length; continue; }
        auto count = std::min(length, part.length - offset);
        if (count) result.segments_.push_back({part.storage, part.offset + offset, count});
        length -= count; offset = 0; if (length == 0) break;
    }
    return result;
}
bool byte_buffer::copy_to(wire::mutable_bytes_view output) const noexcept {
    if (output.size != size_ || (output.size && !output.data)) return false;
    std::size_t offset = 0;
    for (auto const &part : segments_) {
        // Encoding requires disjoint storage, like all other codecs.
        std::memcpy(output.data + offset, part.storage->data + part.offset, part.length); offset += part.length;
    }
    return true;
}
} // namespace rpc
