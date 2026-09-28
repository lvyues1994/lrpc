"""Run sequential paired trials; preserve request samples and verify their statistics."""

import argparse
import csv
import gzip
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import shutil
import statistics
import subprocess
import selectors
import signal
import time


def command(args):
    return subprocess.run(args, check=True, text=True, capture_output=True, timeout=60).stdout.strip()


def read_ready(server, timeout=10):
    deadline = time.monotonic() + timeout
    data = b""
    descriptor = server.stdout.fileno()
    os.set_blocking(descriptor, False)
    try:
        with selectors.DefaultSelector() as poller:
            poller.register(descriptor, selectors.EVENT_READ)
            while b"\n" not in data:
                remaining = deadline - time.monotonic()
                if remaining <= 0 or not poller.select(remaining):
                    raise RuntimeError("server readiness timeout")
                chunk = os.read(descriptor, 4096)
                if not chunk:
                    raise RuntimeError("server exited before readiness")
                data += chunk
                if len(data) > 16384:
                    raise RuntimeError("oversized server readiness")
        return json.loads(data.split(b"\n", 1)[0])
    finally:
        os.set_blocking(descriptor, True)


def measured_command(invocation, config, args):
    if args.topology == "same-thread":
        return json.loads(command(invocation))
    server_command = ["taskset", "-c", str(args.server_cpu), str(args.binary.resolve()), "--role", "server"]
    for key, value in config.items():
        server_command.extend(["--" + key, str(value)])
    server = subprocess.Popen(server_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        ready = read_ready(server)
        assert ready["ready"] and ready["pid"] == server.pid and 0 < ready["port"] <= 65535
        affinity = sorted(os.sched_getaffinity(server.pid))
        assert affinity == [args.server_cpu]
        client_command = invocation + ["--role", "client", "--port", str(ready["port"])]
        result = json.loads(command(client_command))
        result.update(command=client_command, server_command=server_command, server_affinity=affinity, server_ready=ready)
    finally:
        if server.poll() is None:
            server.send_signal(signal.SIGTERM)
        try:
            stdout, stderr = server.communicate(timeout=10)
        except subprocess.TimeoutExpired:
            server.kill(); server.communicate()
            raise RuntimeError("server failed to drain; discard trial")
        if server.returncode != 0:
            raise RuntimeError(f"server exited {server.returncode}: {stderr}")
    return result


def environment(binary):
    root = Path(__file__).resolve().parent.parent
    files = [*root.glob("unary/**/*.cpp"), *root.glob("unary/**/*.hpp"), *root.glob("wire/**/*.cpp"),
             *root.glob("wire/**/*.hpp"), *root.glob("benchmarks/*.cpp"), *root.glob("benchmarks/*.hpp"),
             *root.glob("benchmarks/*.py"), root / "CMakeLists.txt", root / "CMakePresets.json"]
    result = {"uname": platform.uname()._asdict(), "affinity": sorted(os.sched_getaffinity(0)),
              "lscpu": json.loads(command(["lscpu", "-J"])), "binary": str(binary.resolve()),
              "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
              "sources_sha256": {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(files)}}
    cpu = result["affinity"][0]
    for name, path in {"governor": f"/sys/devices/system/cpu/cpu{cpu}/cpufreq/scaling_governor",
                       "energy_policy": f"/sys/devices/system/cpu/cpu{cpu}/cpufreq/energy_performance_preference",
                       "no_turbo": "/sys/devices/system/cpu/intel_pstate/no_turbo"}.items():
        if Path(path).exists():
            result[name] = Path(path).read_text().strip()
    cache_path = binary.resolve().parent.parent / "CMakeCache.txt"
    cache = {}
    if cache_path.exists():
        for line in cache_path.read_text().splitlines():
            if not line.startswith(("#", "//")) and ":" in line and "=" in line:
                key, value = line.split("=", 1)
                cache[key.split(":", 1)[0]] = value
    for name, key in (("net", "LRPC_NET_SOURCE_DIR"), ("co2", "NET_CO2_DIR")):
        directory = cache.get(key)
        if directory and (Path(directory) / ".git").exists():
            result[name + "_commit"] = command(["git", "-C", directory, "rev-parse", "HEAD"])
    if cache.get("CMAKE_CXX_COMPILER"):
        result["compiler"] = command([cache["CMAKE_CXX_COMPILER"], "--version"]).splitlines()[0]
    result["build_flags"] = {k: cache.get(k) for k in ("CMAKE_BUILD_TYPE", "CMAKE_CXX_FLAGS", "CMAKE_CXX_FLAGS_RELEASE",
                                                     "LRPC_ENABLE_SANITIZERS", "LRPC_BENCH_COUNT_ALLOCATIONS", "LRPC_ENABLE_DIAGNOSTICS")}
    return result


def validate(summary, path, config):
    assert summary["receive_buffer_bytes"] == config["receive-buffer-bytes"]
    groups = {k: [] for k in ("success_api", "rejected_api", "timeout_api", "other_error_api",
                              "schedule_lag", "success_scheduled", "offer_lag")}
    counts = [0] * 17
    dropped = 0
    with path.open() as stream:
        rows = list(csv.DictReader(stream))
    assert len(rows) == summary["offered"]
    for i, row in enumerate(rows):
        row = {k: int(v) for k, v in row.items()}
        assert row["id"] == i
        scheduled, offered, started, completed = [row[k] for k in ("scheduled_ns", "offered_ns", "started_ns", "completed_ns")]
        assert 0 <= scheduled <= offered <= completed
        if summary["target_rate"]:
            assert scheduled == (i // summary["burst"] * summary["burst"] * 1_000_000_000 // summary["target_rate"])
        groups["offer_lag"].append(offered - scheduled)
        if row["generator_drop"]:
            assert started == -1 and row["status"] == -1
            dropped += 1
            continue
        assert offered <= started <= completed
        code = row["status"]
        counts[code] += 1
        category = {0: "success_api", 8: "rejected_api", 4: "timeout_api"}.get(code, "other_error_api")
        groups[category].append(completed - started)
        groups["schedule_lag"].append(started - scheduled)
        if code == 0:
            groups["success_scheduled"].append(completed - scheduled)
    assert counts == summary["status_counts"] and dropped == summary["generator_drop"]
    assert sum(counts) + dropped == summary["offered"]
    assert summary["started"] == sum(counts)
    assert summary["success"] == counts[0] and summary["rejected"] == counts[8] and summary["timeout"] == counts[4]
    assert summary["other_errors"] == sum(counts) - counts[0] - counts[8] - counts[4]
    faithful = dropped == 0 and bool(groups["schedule_lag"]) and max(groups["schedule_lag"]) <= summary["max_schedule_lag_us"] * 1000
    diagnostic = summary["allocation_counting"] or summary["queue_instrumentation"] or summary["sanitizers"]
    assert summary["diagnostic_only"] == diagnostic
    assert summary["load_faithful"] == (faithful if summary["target_rate"] else None)
    assert summary["eligible_for_zero_error_p99"] == (not diagnostic and
           (summary["target_rate"] == 0 or faithful) and counts[0] == summary["offered"])
    for name, values in groups.items():
        if not values:
            assert summary[name] is None
            continue
        values.sort()
        expected = {"p50_us": values[math.ceil(len(values) * .5) - 1] / 1000,
                    "p99_us": values[math.ceil(len(values) * .99) - 1] / 1000, "max_us": values[-1] / 1000}
        for field, value in expected.items():
            assert abs(value - summary[name][field]) <= .00051, (name, field, value, summary[name][field])


def cases(suite):
    if suite == "smoke":
        configs = [(64, n, 0, 1, 64, 0) for n in (1, 8, 64)] + [(4096, 64, 0, 1, 64, 0),
                   (64, 8, 1_000_000, 16, 64, 0), (64, 64, 100_000, 16, 1, 0), (64, 8, 0, 1, 64, 1)]
    elif suite == "core":
        configs = [(b, 1, 0, 1, 64, 0) for b in (64, 4096)]
        configs += [(b, 128, 300_000, 1, 64, 0) for b in (64, 4096)]
    elif suite == "open-core":
        configs = [(b, 128, 300_000, 1, 64, 0) for b in (64, 4096)]
    elif suite == "open":
        configs = [(b, 128, rate, 1, 64, 0) for b in (64, 4096) for rate in (20_000, 100_000, 300_000, 1_000_000)]
        configs += [(b, 128, 100_000, 16, 64, 0) for b in (64, 4096)]
    else:
        configs = [(b, n, 0, 1, 64, 0) for b in (64, 4096) for n in (1, 8, 64)]
        if suite == "cache":
            configs += [(64, n, 0, 1, 64, 50_000) for n in (1, 8, 64)]
    for b, n, rate, burst, streams, deadline in configs:
        for transport in ("net", "rpc"):
            if deadline and transport == "net":
                continue
            for allocator in (("system", "recycling") if suite == "cache" else ("system",)):
                yield {"transport": transport, "bytes": b, "inflight": n, "rate": rate, "burst": burst,
                       "rpc-streams": streams, "deadline-us": deadline, "frame-allocator": allocator}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--suite", choices=("smoke", "closed", "open", "cache", "core", "open-core"), default="closed")
    parser.add_argument("--rpc-receive-buffer-bytes", type=int, nargs="+", default=[65536],
                        help="RPC read windows to compare; raw net always uses 65536 bytes")
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument("--iterations", type=int, default=100_000)
    parser.add_argument("--warmup", type=int, default=5000)
    parser.add_argument("--cpu", type=int, default=2)
    parser.add_argument("--topology", choices=("same-thread", "separate-process"), default="same-thread")
    parser.add_argument("--server-cpu", type=int, default=4)
    args = parser.parse_args()
    if args.rounds < 1:
        parser.error("rounds must be positive")
    if any(size < 8208 or size > 16 * 1024 * 1024 + 16 for size in args.rpc_receive_buffer_bytes):
        parser.error("RPC receive windows must be between 8208 and 16777232 bytes")
    if len(set(args.rpc_receive_buffer_bytes)) != len(args.rpc_receive_buffer_bytes):
        parser.error("duplicate RPC receive windows")
    allowed = os.sched_getaffinity(0)
    if args.cpu not in allowed or (args.topology == "separate-process" and args.server_cpu not in allowed):
        parser.error("requested CPU is not in the initial affinity mask")
    os.sched_setaffinity(0, {args.cpu})
    args.out.mkdir(parents=True, exist_ok=False)
    env = environment(args.binary)
    env.update(topology=args.topology, client_cpu=args.cpu, server_cpu=args.server_cpu if args.topology == "separate-process" else None,
               rpc_receive_buffer_bytes=args.rpc_receive_buffer_bytes)
    (args.out / "environment.json").write_text(json.dumps(env, indent=2) + "\n")
    configurations = [dict(config, **{"receive-buffer-bytes": size}) for config in cases(args.suite)
                      for size in (args.rpc_receive_buffer_bytes if config["transport"] == "rpc" else [65536])]
    summaries = []
    with (args.out / "summary.jsonl").open("w") as output:
        for round_id in range(args.rounds):
            # Reverse the order on alternate rounds; keep related configurations adjacent.
            order = list(range(len(configurations)))
            if round_id % 2:
                order.reverse()
            for index in order:
                config = configurations[index]
                path = args.out / f"case-{index:02d}-round-{round_id + 1}.csv"
                invocation = [str(args.binary.resolve()), "--iterations", str(args.iterations), "--warmup", str(args.warmup),
                              "--samples", str(path)]
                for key, value in config.items():
                    invocation.extend(["--" + key, str(value)])
                summary = measured_command(invocation, config, args)
                validate(summary, path, config)
                summary.update(case=index, round=round_id + 1)
                summary.setdefault("command", invocation)
                output.write(json.dumps(summary) + "\n"); output.flush()
                summaries.append(summary)
                with path.open("rb") as source, gzip.open(path.with_suffix(".csv.gz"), "wb") as target:
                    shutil.copyfileobj(source, target)
                path.unlink()
                print(f"round {round_id + 1}/{args.rounds} case {index + 1}/{len(configurations)} "
                      f"{config['transport']} {config['bytes']}B n={config['inflight']} "
                      f"ok={summary['success']} reject={summary['rejected']} timeout={summary['timeout']} "
                      f"drop={summary['generator_drop']}", flush=True)
    lines = ["# Local benchmark results", "", "Per-request CSV statistics independently verified. See environment.json and summary.jsonl.",
             "Medians include diagnostic/invalid rounds; check Eligible rounds before making load claims.", "",
             "| Case | Transport | Bytes | Inflight | Rate | Burst | Deadline µs | Frames | Receive bytes | Success p50 µs median | Success p99 µs median [min,max] | Success/s median | Eligible rounds |",
             "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |"]
    for index, config in enumerate(configurations):
        selected = [s for s in summaries if s["case"] == index]
        p50 = [s["success_api"]["p50_us"] for s in selected if s["success_api"]]
        p99 = [s["success_api"]["p99_us"] for s in selected if s["success_api"]]
        lines.append(f"| {index} | {config['transport']} | {config['bytes']} | {config['inflight']} | {config['rate']} | "
                     f"{config['burst']} | {config['deadline-us']} | {config['frame-allocator']} | {config['receive-buffer-bytes']} | "
                     f"{statistics.median(p50) if p50 else '—'} | "
                     f"{f'{statistics.median(p99):.3f} [{min(p99):.3f},{max(p99):.3f}]' if p99 else '—'} | "
                     f"{statistics.median(s['success_per_second'] for s in selected):.0f} | "
                     f"{sum(s['eligible_for_zero_error_p99'] for s in selected)}/{args.rounds} |")
    (args.out / "summary.md").write_text("\n".join(lines) + "\n")


if __name__ == "__main__":
    main()
