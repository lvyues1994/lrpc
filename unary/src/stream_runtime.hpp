#pragma once
#include <rpc/stream.hpp>
namespace rpc { namespace detail {
std::unique_ptr<client> make_stream_client(net::io_context &, client_options);
std::unique_ptr<server> make_stream_server(net::io_context &, std::vector<method_binding>, server_options);
void validate_stream_options(connection_options const &);
} }
