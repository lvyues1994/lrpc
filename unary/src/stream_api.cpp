#include <rpc/stream.hpp>

namespace rpc {
namespace {
auto unsupported_open() CO2_BEG(net::task<stream_open_result>, ()) { CO2_RETURN((stream_open_result{status_code::unimplemented})); } CO2_END
}
net::task<stream_open_result> client::open_stream(std::string, method_kind, call_options) { return unsupported_open(); }
net::task<stream_open_result> client::open_stream(method_handle, call_options) { return unsupported_open(); }
namespace detail {
auto failed_stream_read(status_code code) CO2_BEG(net::task<stream_read_result>, (code)) { CO2_RETURN((stream_read_result{code})); } CO2_END
auto failed_stream_operation(status_code code) CO2_BEG(net::task<status_code>, (code)) { CO2_RETURN(code); } CO2_END
auto failed_stream_finish(status_code code) CO2_BEG(net::task<call_result>, (code)) { CO2_RETURN((call_result{code})); } CO2_END
auto read_typed_stream(std::shared_ptr<byte_stream> stream, decoded_response message)
    CO2_BEG(net::task<stream_read_result>, (stream, message), stream_read_result result; byte_buffer bytes; std::unique_ptr<buffer_builder> scratch;) {
    if (!message.message || !message.operations || !message.operations->decode) CO2_RETURN((stream_read_result{status_code::invalid_argument}));
    CO2_AWAIT_SET(result, stream->read(bytes));
    if (result.code != status_code::ok || result.ended) CO2_RETURN(std::move(result));
    try {
        bool decoded = false;
        if (message.operations->owned && message.operations->owned->decode) decoded = message.operations->owned->decode(bytes, message.message);
        else if (bytes.segment_count() <= 1) decoded = message.operations->decode(bytes.size() ? bytes.segment(0) : wire::bytes_view{}, message.message);
        else {
            scratch = std::make_unique<buffer_builder>(bytes.size()); bytes.copy_to(scratch->buffer());
            decoded = message.operations->decode({scratch->buffer().data, scratch->buffer().size}, message.message);
        }
        if (!decoded) result.code = status_code::data_loss;
    } catch (std::bad_alloc const &) { result.code = status_code::resource_exhausted; }
    catch (...) { result.code = status_code::data_loss; }
    if (result.code != status_code::ok) stream->cancel();
    CO2_RETURN(std::move(result));
}
CO2_END
}
} // namespace rpc
