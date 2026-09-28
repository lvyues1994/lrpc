#include "check.hpp"

#include <net/io_context.hpp>
#include <net/run_async.hpp>
#include <net/task.hpp>

#include <exception>
#include <iostream>

namespace {

auto answer() CO2_BEG(net::task<int>, ()) { CO2_RETURN(42); }
CO2_END

auto chain() CO2_BEG(net::task<int>, (), int value{};) {
    CO2_AWAIT_SET(value, answer());
    CO2_RETURN(value);
}
CO2_END

} // namespace

int main() {
    try {
        net::io_context context{net::default_backend, net::single_thread_hint};
        int result = 0;
        int completions = 0;
        std::exception_ptr failure;
        net::run_async(context.get_executor(), [&](int value) {
            result = value;
            ++completions;
        }, [&](std::exception_ptr error) { failure = error; })([] { return chain(); });
        CHECK(completions == 0);
        CHECK(context.run() > 0);
        if (failure) std::rethrow_exception(failure);
        CHECK(completions == 1);
        CHECK(result == 42);
        CHECK(context.run() == 0);
        std::cout << "net/co2 integration passed\n";
    } catch (std::exception const &error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
