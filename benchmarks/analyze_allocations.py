"""Symbolize bounded new/new[] traces from a non-PIE diagnose binary."""
import argparse
from collections import defaultdict
import hashlib
import json
from pathlib import Path
import subprocess


def category(frames):
    # Classify the immediate allocating stack frame, including its inlined
    # callees. Deeper frames are retained as evidence, not used to double count.
    first = next((frame for frame in frames if "benchmarks/allocations.cpp:" not in frame), "")
    if "io_awaitable_promise_base" in first:
        if "bench::" in first and "perform(" in first:
            return "driver_task_frame"
        if "bench::" in first and "echo(" in first:
            return "handler_task_frame"
        if "read_frame(" in first:
            return "rpc_read_frame"
        if "call_task(" in first:
            return "rpc_client_task_frame"
        if "invoke_handler(" in first:
            return "rpc_invoke_task_frame"
        if "wait_deadline(" in first:
            return "rpc_deadline_task_frame"
        return "other_task_frame"
    if "net/run_async.hpp:" in first:
        return "driver_completion_state" if "bench::" in first else "rpc_completion_state"
    if "create_timer(" in first:
        return "net_timer_object"
    if "stop_source::stop_source" in first:
        return "stop_state"
    if "_Hash_node<" in first:
        if "client_call" in first:
            return "client_stream_table_node"
        if "server_call" in first:
            return "server_stream_table_node"
    if "_Sp_counted_ptr_inplace<rpc::detail::client_call" in first:
        return "client_call_object"
    if "_Sp_counted_ptr_inplace<rpc::detail::server_call" in first:
        return "server_call_object"
    return "unclassified"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--trace", type=Path, required=True)
    parser.add_argument("--summary", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    trace = json.loads(args.trace.read_text())
    summary = json.loads(args.summary.read_text())
    if trace["lost"]:
        raise ValueError("trace table overflowed; cannot claim complete attribution")
    decoded = {}
    for site in trace["sites"]:
        frames = []
        for address in site["return_addresses"]:
            if address not in decoded:
                decoded[address] = subprocess.check_output(
                    ["addr2line", "-C", "-f", "-i", "-e", str(args.binary), hex(int(address, 16) - 1)], text=True)
            frames.append(decoded[address])
        site["frames"] = frames
        site["category"] = category(frames)
    totals = defaultdict(lambda: {"calls": 0, "requested_bytes": 0})
    for site in trace["sites"]:
        totals[site["category"]]["calls"] += site["calls"]
        totals[site["category"]]["requested_bytes"] += site["bytes"]
    count = sum(value["calls"] for value in totals.values())
    expected = summary["new_calls_per_offered"] * summary["offered"]
    if abs(count - expected) > .00051 * summary["offered"]:
        raise ValueError("trace does not cover the counted measurement interval")
    for value in totals.values():
        value["calls_per_offered"] = value["calls"] / summary["offered"]
        value["requested_bytes_per_offered"] = value["requested_bytes"] / summary["offered"]
    result = {"binary_sha256": hashlib.sha256(args.binary.read_bytes()).hexdigest(), "scope": summary["cpu_scope"],
              "offered": summary["offered"], "new_calls": count, "lost": trace["lost"], "categories": dict(totals),
              "sites": trace["sites"]}
    args.out.write_text(json.dumps(result, indent=2) + "\n")
    for key, value in sorted(totals.items()):
        print(f"{key}: {value['calls_per_offered']:.4f} calls/offer, {value['requested_bytes_per_offered']:.1f} requested bytes/offer")


if __name__ == "__main__":
    main()
