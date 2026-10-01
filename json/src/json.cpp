#include <rpc/json.hpp>
#include <rapidjson/reader.h>
#include <rapidjson/writer.h>
#include <rapidjson/memorystream.h>
#include <array>

namespace rpc { namespace json_detail {
namespace {
constexpr std::size_t maximum_depth = 64;

bool valid_limits(json_limits const &limits) noexcept {
    return limits.max_depth != 0 && limits.max_depth <= maximum_depth && limits.max_scratch_bytes >= 512;
}

// Reader and Writer each own one internal Stack. Throw on exhaustion: upstream
// Stack does not check a null Realloc result. Count old/new blocks during growth.
class scratch_allocator {
public:
    static constexpr bool kNeedFree = false;
    explicit scratch_allocator(std::size_t limit = 256U * 1024U) noexcept : limit_(limit) {}
    void *Malloc(std::size_t size) { return Realloc(nullptr, 0, size); }
    void *Realloc(void *original, std::size_t original_size, std::size_t size) {
        if (original != current_ || original_size != capacity_) throw std::bad_alloc{};
        if (size == 0) { heap_.reset(); current_ = nullptr; capacity_ = 0; return nullptr; }
        if (size <= inline_.size()) {
            if (heap_) std::memcpy(inline_.data(), original, std::min(original_size, size));
            heap_.reset(); current_ = inline_.data(); capacity_ = size; return current_;
        }
        auto const old_heap = heap_ ? capacity_ : 0;
        if (inline_.size() > limit_ || old_heap > limit_ - inline_.size() || size > limit_ - inline_.size() - old_heap)
            throw std::bad_alloc{};
        auto next = std::make_unique<std::uint8_t[]>(size);
        if (original_size) std::memcpy(next.get(), original, std::min(original_size, size));
        heap_ = std::move(next); current_ = heap_.get(); capacity_ = size; return current_;
    }
    static void Free(void *) noexcept {}
private:
    alignas(std::max_align_t) std::array<std::uint8_t, 512> inline_{};
    std::unique_ptr<std::uint8_t[]> heap_{};
    void *current_ = nullptr;
    std::size_t capacity_ = 0;
    std::size_t limit_;
};
static_assert(!scratch_allocator::kNeedFree, "scratch storage is released by its owner");

class byte_output {
public:
    using Ch = char;
    byte_output(wire::mutable_bytes_view bytes, bool count) noexcept : bytes_(bytes), count_(count) {}
    void Put(char value) noexcept {
        if (overflow_) return;
        if (size_ == std::numeric_limits<std::size_t>::max() || (!count_ && size_ == bytes_.size)) { overflow_ = true; return; }
        if (!count_) bytes_.data[size_] = static_cast<std::uint8_t>(value);
        ++size_;
    }
    void Flush() noexcept {}
    std::size_t size() const noexcept { return size_; }
    bool overflowed() const noexcept { return overflow_; }
private:
    wire::mutable_bytes_view bytes_;
    std::size_t size_ = 0;
    bool count_;
    bool overflow_ = false;
};

class writer_output final : public output {
public:
    using writer_type = rapidjson::Writer<byte_output, rapidjson::UTF8<>, rapidjson::UTF8<>,
        scratch_allocator, rapidjson::kWriteValidateEncodingFlag>;
    writer_output(byte_output &bytes, scratch_allocator &scratch) : bytes_(bytes), writer_(bytes, &scratch, 8) {}
    bool value(atom const &a) override {
        bool okay = false;
        switch (a.kind) {
        case atom_kind::null: okay = writer_.Null(); break;
        case atom_kind::boolean: okay = writer_.Bool(a.boolean); break;
        case atom_kind::signed_integer: okay = writer_.Int64(a.signed_integer); break;
        case atom_kind::unsigned_integer: okay = writer_.Uint64(a.unsigned_integer); break;
        case atom_kind::number: okay = writer_.Double(a.number); break;
        case atom_kind::string:
            if (a.size > std::numeric_limits<rapidjson::SizeType>::max()) return false;
            okay = writer_.String(a.text, static_cast<rapidjson::SizeType>(a.size)); break;
        }
        return okay && !bytes_.overflowed();
    }
    bool start_object() override { return writer_.StartObject() && !bytes_.overflowed(); }
    bool key(char const *name, std::size_t size) override {
        return size <= std::numeric_limits<rapidjson::SizeType>::max() &&
            writer_.Key(name, static_cast<rapidjson::SizeType>(size)) && !bytes_.overflowed();
    }
    bool end_object() override { return writer_.EndObject() && !bytes_.overflowed(); }
    bool start_array() override { return writer_.StartArray() && !bytes_.overflowed(); }
    bool end_array() override { return writer_.EndArray() && !bytes_.overflowed(); }
    bool complete() const noexcept { return writer_.IsComplete(); }
private:
    byte_output &bytes_;
    writer_type writer_;
};

struct parse_frame {
    node value{};
    node pending{};
    node_kind kind = node_kind::object;
    bool has_pending = false;
    std::uint64_t seen = 0;
    std::size_t count = 0;
    std::vector<std::string> unknown_keys{};
};

class reader_handler {
public:
    reader_handler(node root, json_limits limits) : root_(root), context_{limits, 0} {}
    bool Null() { return scalar({}); }
    bool Bool(bool value) { atom a; a.kind = atom_kind::boolean; a.boolean = value; return scalar(a); }
    bool Int(int value) { return Int64(value); }
    bool Uint(unsigned value) { return Uint64(value); }
    bool Int64(std::int64_t value) { atom a; a.kind = atom_kind::signed_integer; a.signed_integer = value; return scalar(a); }
    bool Uint64(std::uint64_t value) { atom a; a.kind = atom_kind::unsigned_integer; a.unsigned_integer = value; return scalar(a); }
    bool Double(double value) { atom a; a.kind = atom_kind::number; a.number = value; return scalar(a); }
    bool RawNumber(char const *, rapidjson::SizeType, bool) { return false; }
    bool String(char const *text, rapidjson::SizeType size, bool) {
        if (size > context_.limits.max_string_bytes) return false;
        atom a; a.kind = atom_kind::string; a.text = text; a.size = size; return scalar(a);
    }
    bool StartObject() { return start(node_kind::object); }
    bool StartArray() { return start(node_kind::array); }
    bool Key(char const *text, rapidjson::SizeType size, bool) {
        if (depth_ == 0 || size > context_.limits.max_string_bytes) return false;
        auto &frame = frames_[depth_ - 1];
        if (frame.kind != node_kind::object || frame.has_pending || frame.count >= context_.limits.max_object_fields) return false;
        auto const *operations = frame.value.operations;
        if (operations && operations->entry) { // A map: every key is new, or the object is rejected.
            frame.pending = operations->entry(context_, frame.value.target, text, size);
            if (!frame.pending.operations) return false;
            ++frame.count; frame.has_pending = true; return true;
        }
        std::size_t index = std::numeric_limits<std::size_t>::max();
        frame.pending = operations ? operations->field(frame.value.target, text, size, index) : node{};
        if (frame.pending.operations) {
            auto const bit = std::uint64_t{1} << index;
            if (frame.seen & bit) return false;
            frame.seen |= bit;
        } else {
            for (auto const &key : frame.unknown_keys) if (key.size() == size && std::memcmp(key.data(), text, size) == 0) return false;
            context_.charge(sizeof(std::string) + size);
            frame.unknown_keys.emplace_back(text, size);
        }
        ++frame.count; frame.has_pending = true; return true;
    }
    bool EndObject(rapidjson::SizeType count) { return end(node_kind::object, count); }
    bool EndArray(rapidjson::SizeType count) { return end(node_kind::array, count); }
    bool complete() const noexcept { return claimed_ && depth_ == 0; }
private:
    bool take(node &value) {
        if (depth_ == 0) {
            if (claimed_) return false;
            claimed_ = true; value = root_; return true;
        }
        auto &frame = frames_[depth_ - 1];
        if (frame.kind == node_kind::object) {
            if (!frame.has_pending) return false;
            value = frame.pending; frame.pending = {}; frame.has_pending = false; return true;
        }
        if (frame.count >= context_.limits.max_array_elements) return false;
        ++frame.count;
        value = frame.value.operations ? frame.value.operations->element(context_, frame.value.target) : node{};
        return !frame.value.operations || value.operations;
    }
    bool scalar(atom const &a) {
        node value;
        if (!take(value)) return false;
        return !value.operations || value.operations->value(context_, value.target, a);
    }
    bool start(node_kind kind) {
        if (depth_ >= context_.limits.max_depth) return false;
        node value;
        if (!take(value) || (value.operations && value.operations->kind != kind)) return false;
        if (value.operations) value.operations->reset(context_, value.target);
        auto &frame = frames_[depth_++];
        frame.value = value; frame.pending = {}; frame.kind = kind; frame.has_pending = false;
        frame.seen = 0; frame.count = 0; frame.unknown_keys.clear(); return true;
    }
    bool end(node_kind kind, std::size_t count) {
        if (depth_ == 0) return false;
        auto &frame = frames_[depth_ - 1];
        if (frame.kind != kind || frame.has_pending || frame.count != count) return false;
        auto const required = frame.value.operations ? frame.value.operations->required() : std::uint64_t{0};
        if ((frame.seen & required) != required) return false;
        frame.unknown_keys.clear(); --depth_; return true;
    }
    node root_;
    read_context context_;
    std::array<parse_frame, maximum_depth> frames_{};
    std::size_t depth_ = 0;
    bool claimed_ = false;
};
} // namespace

wire::encode_result write_message(void const *value, write_function write, wire::mutable_bytes_view bytes,
                                  json_limits limits, bool count) {
    if (!valid_limits(limits) || (!count && bytes.size && !bytes.data)) return {wire::error::invalid_argument, 0};
    scratch_allocator scratch{limits.max_scratch_bytes};
    byte_output stream{bytes, count}; writer_output writer{stream, scratch};
    write_context context{writer, limits, 0};
    auto const okay = write(context, value);
    if (stream.overflowed()) return {wire::error::output_too_small, 0};
    if (!okay || !writer.complete()) return {wire::error::invalid_argument, 0};
    return {wire::error::none, stream.size()};
}

bool read_message(wire::bytes_view bytes, node value, json_limits limits) {
    if (!valid_limits(limits) || (bytes.size && !bytes.data)) return false;
    char const empty = 0;
    auto const *data = bytes.data ? reinterpret_cast<char const *>(bytes.data) : &empty;
    rapidjson::MemoryStream stream{data, bytes.size};
    scratch_allocator scratch{limits.max_scratch_bytes};
    rapidjson::GenericReader<rapidjson::UTF8<>, rapidjson::UTF8<>, scratch_allocator> reader{&scratch, 256};
    reader_handler handler{value, limits};
    auto const result = reader.Parse<rapidjson::kParseValidateEncodingFlag | rapidjson::kParseFullPrecisionFlag |
        rapidjson::kParseIterativeFlag>(stream, handler);
    return result && handler.complete() && stream.Tell() == bytes.size;
}
} } // namespace rpc::json_detail
