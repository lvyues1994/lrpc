#include <array>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <iostream>
#include <stdexcept>
#include <string>
#include <system_error>
#include <vector>
#include <cerrno>
#include <fcntl.h>
#include <sched.h>
#include <sys/resource.h>
#include <time.h>
#include <unistd.h>

namespace {
std::int64_t wall_now() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
}

struct observation {
    std::int64_t begin = 0, wall = 0, cpu = 0;
    unsigned long long runtime = 0, runqueue = 0, slices = 0;
    rusage usage{};
};

class scheduler_counters {
public:
    scheduler_counters() : fd_(::open("/proc/self/schedstat", O_RDONLY | O_CLOEXEC)) {
        if (fd_ == -1) throw std::system_error{errno, std::generic_category(), "open schedstat"};
    }
    ~scheduler_counters() { ::close(fd_); }
    scheduler_counters(scheduler_counters const &) = delete;
    scheduler_counters &operator=(scheduler_counters const &) = delete;
    observation sample() const {
        observation value{};
        value.begin = wall_now();
        timespec cpu{};
        if (::clock_gettime(CLOCK_THREAD_CPUTIME_ID, &cpu) != 0 || ::getrusage(RUSAGE_THREAD, &value.usage) != 0)
            throw std::system_error{errno, std::generic_category(), "thread counters"};
        value.cpu = cpu.tv_sec * 1000000000LL + cpu.tv_nsec;
        std::array<char, 160> data{};
        auto const size = ::pread(fd_, data.data(), data.size() - 1, 0);
        if (size <= 0 || std::sscanf(data.data(), "%llu %llu %llu", &value.runtime, &value.runqueue, &value.slices) != 3)
            throw std::runtime_error{"invalid schedstat"};
        value.wall = wall_now();
        return value;
    }
private:
    int fd_;
};

struct interval { observation before{}, after{}; };
}

int main(int argc, char **argv) {
    try {
        if (argc > 2) throw std::invalid_argument{"usage: scheduler_probe [seconds]"};
        std::size_t parsed = 0;
        auto const seconds = argc == 2 ? std::stoi(argv[1], &parsed) : 10;
        if (argc == 2 && parsed != std::string{argv[1]}.size()) throw std::invalid_argument{"invalid seconds"};
        if (seconds < 1 || seconds > 60) throw std::invalid_argument{"seconds outside 1..60"};
        scheduler_counters counters;
        std::vector<interval> gaps(4096);
        std::size_t found = 0, lost = 0;
        // Fault in instrumentation code and buffers before observing stalls.
        for (unsigned i = 0; i < 1000; ++i) counters.sample();
        auto previous = counters.sample();
        auto const start = previous.wall;
        auto const end = start + seconds * 1000000000LL;
        auto const initial = previous;
        std::int64_t now = start;
        unsigned turn = 0;
        while (now < end) {
            now = wall_now();
            if (++turn != 256) continue;
            turn = 0;
            auto const current = counters.sample();
            if (current.wall - previous.wall > 40000) {
                if (found < gaps.size()) gaps[found++] = {previous, current};
                else ++lost;
            }
            previous = current;
        }
        auto const final = counters.sample();
        std::cout << "at_ns,wall_ns,cpu_ns,sched_runtime_ns,runqueue_ns,slices,voluntary,involuntary,minor_faults,major_faults,before_sample_ns,after_sample_ns\n";
        auto print = [&](observation const &a, observation const &b) {
            std::cout << b.wall - start << ',' << b.wall - a.wall << ',' << b.cpu - a.cpu << ','
                      << b.runtime - a.runtime << ',' << b.runqueue - a.runqueue << ',' << b.slices - a.slices << ','
                      << b.usage.ru_nvcsw - a.usage.ru_nvcsw << ',' << b.usage.ru_nivcsw - a.usage.ru_nivcsw << ','
                      << b.usage.ru_minflt - a.usage.ru_minflt << ',' << b.usage.ru_majflt - a.usage.ru_majflt << ','
                      << a.wall - a.begin << ',' << b.wall - b.begin << '\n';
        };
        for (std::size_t i = 0; i < found; ++i) print(gaps[i].before, gaps[i].after);
        std::cerr << "cpu=" << ::sched_getcpu() << " seconds=" << seconds << " start_ns=" << start << " gaps=" << found << " lost=" << lost
                  << " voluntary=" << final.usage.ru_nvcsw - initial.usage.ru_nvcsw
                  << " involuntary=" << final.usage.ru_nivcsw - initial.usage.ru_nivcsw
                  << " runqueue_ns=" << final.runqueue - initial.runqueue << '\n';
    } catch (std::exception const &error) { std::cerr << error.what() << '\n'; return 1; }
}
