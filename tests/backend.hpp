#pragma once
#include <net/backend.hpp>
#include <stdexcept>
#include <string>

inline net::backend_kind test_backend(int argc, char **argv) {
    if (argc == 1) return net::default_backend_t::kind;
    if (argc == 2) for (auto kind : {net::backend_kind::epoll, net::backend_kind::poll,
                                    net::backend_kind::select, net::backend_kind::io_uring})
        if (argv[1] == std::string{net::to_string(kind)}) return kind;
    throw std::invalid_argument{"expected optional backend: epoll|poll|select|io_uring"};
}
