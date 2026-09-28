#pragma once

#include <stdexcept>
#include <string>

// Unlike assert(), test checks remain active in Release builds.
#define CHECK(expression) do { \
    if (!(expression)) throw std::runtime_error( \
        std::string{__FILE__} + ":" + std::to_string(__LINE__) + ": " #expression); \
} while (false)
