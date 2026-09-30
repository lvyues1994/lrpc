#include <rpc/channel.hpp>
#include <net/resolver.hpp>
#include <net/error.hpp>
#include <ostream>
#include <stdexcept>

namespace rpc {
namespace {
auto static_resolve(std::vector<net::ip::tcp::endpoint> endpoints)
    CO2_BEG(net::task<resolution>, (endpoints)) {
    CO2_RETURN((resolution{{}, std::move(endpoints)}));
}
CO2_END
struct static_resolver final : resolver {
    explicit static_resolver(std::vector<net::ip::tcp::endpoint> endpoints) : endpoints_(std::move(endpoints)) {}
    net::task<resolution> resolve(net::io_context &) override { return static_resolve(endpoints_); }
private:
    std::vector<net::ip::tcp::endpoint> endpoints_;
};
auto dns_resolve(net::io_context &owner, std::string host, std::string service)
    CO2_BEG(net::task<resolution>, (owner, host, service),
            std::unique_ptr<net::ip::tcp::resolver> lookup;
            net::io_result<net::ip::tcp::resolver::results_type> addresses; resolution result;) {
    lookup = std::make_unique<net::ip::tcp::resolver>(owner);
    CO2_AWAIT_SET(addresses, lookup->resolve(host, service));
    if (addresses.ec) {
        result.result.code = addresses.ec == net::cond::canceled ? status_code::cancelled : status_code::unavailable;
    } else {
        for (auto const &entry : addresses.value) result.endpoints.push_back(entry.endpoint);
        if (result.endpoints.empty()) result.result.code = status_code::unavailable;
    }
    CO2_RETURN(std::move(result));
}
CO2_END
struct dns_resolver final : resolver {
    dns_resolver(std::string host, std::string service) : host_(std::move(host)), service_(std::move(service)) {}
    net::task<resolution> resolve(net::io_context &owner) override { return dns_resolve(owner, host_, service_); }
private:
    std::string host_, service_;
};
struct trace_interceptor final : interceptor {
    explicit trace_interceptor(trace_sink &sink) : sink(sink) {}
    status before(call_info const &info) override { sink.record({info, false, status_code::ok, 0}); return {}; }
    void after(call_info const &info, call_result const &result) override {
        auto const elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(clock::now() - info.began).count();
        sink.record({info, true, result.code, static_cast<std::uint64_t>(std::max<std::int64_t>(0, elapsed))});
    }
    trace_sink &sink;
};
} // namespace
std::unique_ptr<interceptor> make_trace_interceptor(trace_sink &sink) { return std::make_unique<trace_interceptor>(sink); }
std::shared_ptr<resolver> make_static_resolver(std::vector<net::ip::tcp::endpoint> endpoints) {
    if (endpoints.empty()) throw std::invalid_argument{"empty resolver endpoints"};
    return std::make_shared<static_resolver>(std::move(endpoints));
}
std::shared_ptr<resolver> make_dns_resolver(std::string host, std::string service) {
    if (host.empty() || service.empty()) throw std::invalid_argument{"empty DNS name/service"};
    return std::make_shared<dns_resolver>(std::move(host), std::move(service));
}
void export_metrics(std::ostream &out, metrics_snapshot const &s) {
    out << "{\"calls\":" << s.calls << ",\"attempts\":" << s.attempts << ",\"retries\":" << s.retries
        << ",\"queued\":" << s.queued << ",\"rejected\":" << s.rejected << ",\"reconnects\":" << s.reconnects
        << ",\"queue_nanoseconds\":" << s.queue_nanoseconds << ",\"call_nanoseconds\":" << s.call_nanoseconds
        << ",\"waiting_calls\":" << s.waiting_calls << ",\"waiting_bytes\":" << s.waiting_bytes
        << ",\"live_sessions\":" << s.live_sessions << ",\"replay_bytes_in_use\":" << s.replay_bytes_in_use
        << ",\"resources\":{\"connections\":" << s.resources.connections << ",\"active_calls\":" << s.resources.active_calls
        << ",\"request_bytes_in_use\":" << s.resources.request_bytes_in_use << ",\"response_bytes_in_use\":" << s.resources.response_bytes_in_use
        << ",\"control_bytes_in_use\":" << s.resources.control_bytes_in_use << ",\"storage_bytes\":" << s.resources.storage_bytes
        << ",\"queued_calls\":" << s.resources.queued_calls << ",\"queued_bytes\":" << s.resources.queued_bytes
        << "},\"completed\":[";
    for (std::size_t i = 0; i < s.completed.size(); ++i) { if (i != 0) out << ','; out << s.completed[i]; }
    out << "]}";
}
} // namespace rpc
