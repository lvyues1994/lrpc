#include "client_fixture.hpp"
#include <iostream>
#include <thread>

namespace {
using namespace test_client;

void partial_cancel() {
    call a, b; fixture f;
    f.pipe->block_writes = true; f.start(a); f.context.poll();
    CHECK(f.pipe->writer && a.completions == 0);
    f.pipe->finish_write(3); f.context.poll();
    auto retained = f.client->stats().request_bytes_in_use;
    a.stop.request_stop(); f.context.poll();
    CHECK(a.completions == 1 && a.result.code == rpc::status_code::cancelled);
    CHECK(retained != 0 && f.client->stats().request_bytes_in_use == retained);
    a.input = 9; a.output = 99;
    f.start(b); f.context.poll(); f.flush();
    std::size_t offset = 0; unsigned requests = 0, cancels = 0, position = 0; std::uint32_t second = 0;
    while (offset < f.pipe->output.size()) {
        auto frame = wire::decode_frame({f.pipe->output.data() + offset, f.pipe->output.size() - offset});
        CHECK(frame.code == wire::error::none);
        CHECK(position < 3);
        CHECK(frame.value.header.type == (position == 1 ? wire::frame_type::cancel : wire::frame_type::request));
        CHECK(frame.value.header.stream_id == (position == 2 ? 2U : 1U));
        ++position;
        if (frame.value.header.type == wire::frame_type::request) {
            ++requests;
            CHECK(frame.value.body.size == 1 && frame.value.body.data[0] == 42);
            if (requests == 1) CHECK(frame.value.header.flags & wire::new_method);
            else { CHECK(!(frame.value.header.flags & wire::new_method)); second = frame.value.header.stream_id; }
        } else { CHECK(frame.value.header.type == wire::frame_type::cancel && frame.value.header.stream_id == 1); ++cancels; }
        offset += frame.consumed;
    }
    CHECK(requests == 2 && cancels == 1 && second == 2);
    CHECK(f.client->stats().request_bytes_in_use == 0);
    f.pipe->feed(reply(second)); f.context.poll();
    CHECK(b.completions == 1 && b.result.code == rpc::status_code::ok && b.output == 42);
    f.pipe->feed(reply(1, 7)); f.context.poll();
    CHECK(a.completions == 1 && a.output == 99); f.zero();
}

void completion_orders() {
    for (unsigned order = 0; order < 5; ++order) {
        call a; fixture f;
        auto deadline = rpc::clock::now() + std::chrono::milliseconds{100};
        f.start(a, "Echo", {order >= 3 ? deadline : rpc::clock::time_point::max(), {}}); f.context.poll();
        CHECK(a.completions == 0 && f.client->stats().active_calls == 1);
        auto expected = rpc::status_code::ok;
        if (order == 0) {
            f.pipe->feed(reply(1));
            while (f.client->stats().active_calls != 0) CHECK(f.context.poll_one() != 0);
            CHECK(a.completions == 0); a.stop.request_stop(); f.client->close();
        } else if (order == 1) {
            f.pipe->feed(reply(1)); a.stop.request_stop(); expected = rpc::status_code::cancelled;
        } else if (order == 2) {
            f.client->close(); a.stop.request_stop(); expected = rpc::status_code::unavailable;
        } else {
            std::this_thread::sleep_until(deadline);
            if (order == 4) a.stop.request_stop();
            f.pipe->feed(reply(1));
            expected = order == 4 ? rpc::status_code::cancelled : rpc::status_code::deadline_exceeded;
        }
        f.context.poll();
        CHECK(a.completions == 1 && !a.failure && a.result.code == expected);
        CHECK(a.output == (expected == rpc::status_code::ok ? 42 : 0));
        a.output = 99; f.pipe->feed(reply(1, 11)); f.context.poll();
        CHECK(a.completions == 1 && a.output == 99); f.zero();
    }
}

void io_failures() {
    for (bool writing : {false, true}) {
        call a; fixture f; f.pipe->block_writes = writing;
        f.start(a); f.context.poll();
        if (writing) f.pipe->finish_write(0, std::make_error_code(std::errc::broken_pipe));
        else f.pipe->abort(false);
        f.context.poll();
        CHECK(a.completions == 1 && a.result.code == rpc::status_code::unavailable);
        CHECK(!f.client->ready()); f.zero();
    }
}
}
int main() {
    try { partial_cancel(); completion_orders(); io_failures(); std::cout << "PASS client partial-write/cancel, completion orders, I/O failure drain\n"; }
    catch (std::exception const &error) { std::cerr << error.what() << '\n'; return 1; }
}
