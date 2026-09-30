"""Compare real unary APIs sequentially; validate every recorded request."""

import argparse
import csv
from datetime import datetime, timezone
import gzip
import hashlib
import json
import math
import os
from pathlib import Path
import shutil
import signal
import statistics
import subprocess
import time

import run_suite as harness


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def check(condition, message):
    if not condition:
        raise RuntimeError(message)


def snapshot(pid, cpu, binary):
    root = Path(f"/proc/{pid}")
    try:
        if (root / "exe").resolve() != binary.resolve():
            return None  # taskset has not exec'ed the actual benchmark yet.
        masks = []
        for entry in (root / "task").iterdir():
            try:
                status = (entry / "status").read_text()
                masks.append(next(line.split(":", 1)[1].strip() for line in status.splitlines()
                                  if line.startswith("Cpus_allowed_list:")))
            except FileNotFoundError:
                pass
        check(all(mask == str(cpu) for mask in masks), f"unexpected thread affinity: {masks}")
        return len(masks)
    except FileNotFoundError:
        return None


def tcp_connections(port):
    pairs = set()
    def endpoint(value):
        address, port_hex = value.split(":")
        if address.startswith("0000000000000000FFFF0000"):
            address = address[-8:]  # IPv4-mapped IPv6 and IPv4 name the same socket pair.
        return address, int(port_hex, 16)
    for table in ("/proc/net/tcp", "/proc/net/tcp6"):
        for line in Path(table).read_text().splitlines()[1:]:
            fields = line.split()
            local, remote, state = fields[1:4]
            if state != "01":
                continue
            local, remote = endpoint(local), endpoint(remote)
            if local[1] == port or remote[1] == port:
                pairs.add(tuple(sorted((local, remote))))
    return len(pairs)


def validate(summary, path, framework, config, args):
    if framework == "lrpc":
        harness.validate(summary, path, {"receive-buffer-bytes": 65536})
        check(summary["eligible_for_zero_error_p99"], "lrpc round is ineligible")
        check(summary["codec"] == "protobuf" and summary["target_rate"] == 0, "wrong lrpc codec/load")
        check(summary["bytes"] == config["bytes"] and summary["inflight_limit"] == config["inflight"], "wrong lrpc case")
        check(summary["warmup"] == args.warmup, "wrong lrpc warmup")
        check(summary["offered"] == args.iterations, "wrong lrpc iterations")
        elapsed_ns = round(summary["elapsed_ms"] * 1e6)
        cpu = (summary["user_ns_per_offered"] + summary["system_ns_per_offered"]) * args.iterations / 1e9
        claimed_p99 = summary["success_api"]["p99_us"] * 1000
    else:
        for name in ("bytes", "inflight"):
            check(summary[name] == config[name], f"wrong {name}")
        check(summary["framework"] == framework and summary["iterations"] == args.iterations and
              summary["warmup"] == args.warmup and summary["errors"] == 0, "wrong native summary")
        check(summary["body_bytes"] == (62 if config["bytes"] == 64 else 4093), "wrong business size")
        elapsed_ns = summary["elapsed_ns"]
        cpu = summary["client_cpu_seconds"]
        claimed_p99 = summary["p99_ns"]
    latencies, events = [], []
    with path.open() as stream:
        rows = list(csv.DictReader(stream))
    check(len(rows) == args.iterations, "incomplete CSV")
    for index, row in enumerate(rows):
        check(int(row["id" if framework == "lrpc" else "sequence"]) == index, "wrong sequence")
        started, completed = int(row["started_ns"]), int(row["completed_ns"])
        # lrpc elapsed_ms is printed at 0.001 ms precision.
        check(0 < started < completed <= elapsed_ns + 501, "invalid request timestamps")
        check(int(row["status"]) == 0, "unsuccessful request")
        latencies.append(completed - started)
        events.extend(((started, 1), (completed, -1)))
    active, maximum = 0, 0
    for _, change in sorted(events):
        active += change
        check(active >= 0, "negative in-flight count")
        maximum = max(maximum, active)
    check(active == 0 and maximum <= config["inflight"], "in-flight bound exceeded")
    latencies.sort()
    quantile = lambda p: latencies[math.ceil(len(latencies) * p) - 1] / 1000
    check(abs(quantile(.99) * 1000 - claimed_p99) <= .51, "p99 does not match CSV")
    return {"p50_us": quantile(.50), "p99_us": quantile(.99), "p999_us": quantile(.999),
            "max_us": latencies[-1] / 1000, "success_per_second": args.iterations * 1e9 / elapsed_ns,
            "client_cpu_ns_per_call": cpu * 1e9 / args.iterations, "maximum_api_inflight": maximum,
            "successful": len(latencies), "errors": 0, "elapsed_ns": elapsed_ns}


def run(framework, binary, config, args, directory):
    prefix = directory / framework
    csv_path = prefix.with_suffix(".csv")
    common = ["--bytes", str(config["bytes"]), "--inflight", str(config["inflight"])]
    extra = ["--transport", "rpc", "--codec", "protobuf", "--backend", "epoll",
             "--receive-buffer-bytes", "65536", "--rpc-streams", "64"] if framework == "lrpc" else []
    server_command = ["taskset", "-c", str(args.server_cpu), str(binary), "--role", "server"] + common + extra
    server_err = prefix.with_suffix(".server.stderr")
    client_out, client_err = prefix.with_suffix(".stdout"), prefix.with_suffix(".stderr")
    observations = {"client_threads": [], "server_threads": [], "established_tcp_connections": []}
    with server_err.open("w") as errors:
        server = subprocess.Popen(server_command, stdout=subprocess.PIPE, stderr=errors, text=True)
        process = None
        ready = None
        try:
            ready = harness.read_ready(server, timeout=15)
            check(ready["ready"] and ready["pid"] == server.pid, "invalid server readiness")
            check(os.sched_getaffinity(server.pid) == {args.server_cpu}, "wrong server affinity")
            client_command = ["taskset", "-c", str(args.cpu), str(binary), "--role", "client",
                              "--port", str(ready["port"]), "--iterations", str(args.iterations),
                              "--warmup", str(args.warmup), "--samples", str(csv_path)] + common + extra
            with client_out.open("w") as stdout, client_err.open("w") as stderr:
                process = subprocess.Popen(client_command, stdout=stdout, stderr=stderr, text=True)
                deadline = time.monotonic() + 120
                while process.poll() is None:
                    if time.monotonic() > deadline:
                        process.kill(); process.wait()
                        raise RuntimeError("client watchdog expired")
                    for role, pid, cpu in (("client", process.pid, args.cpu), ("server", server.pid, args.server_cpu)):
                        count = snapshot(pid, cpu, binary)
                        if count is not None:
                            observations[role + "_threads"].append(count)
                    observations["established_tcp_connections"].append(tcp_connections(ready["port"]))
                    time.sleep(.025)
                check(process.returncode == 0, f"{framework} client failed: {client_err.read_text()}")
            summary = json.loads(client_out.read_text())
            normalized = validate(summary, csv_path, framework, config, args)
            check(max(observations["established_tcp_connections"], default=0) == 1, "single TCP not observed")
            summary.update(normalized=normalized, framework=framework, config=config,
                           observations=observations, server_ready=ready,
                           command=client_command, server_command=server_command)
        finally:
            prefix.with_suffix(".observations.json").write_text(json.dumps(observations) + "\n")
            if process is not None and process.poll() is None:
                process.kill(); process.wait()
            if server.poll() is None:
                server.send_signal(signal.SIGTERM)
            try:
                remaining, _ = server.communicate(timeout=15)
            except subprocess.TimeoutExpired:
                server.kill(); server.communicate()
                raise RuntimeError("server failed to drain")
            check(server.returncode == 0, f"server exit {server.returncode}: {server_err.read_text()}")
            prefix.with_suffix(".server.stdout").write_text(json.dumps(ready) + "\n" + remaining)
    with csv_path.open("rb") as source, gzip.open(csv_path.with_suffix(".csv.gz"), "wb") as destination:
        shutil.copyfileobj(source, destination)
    csv_path.unlink()
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--lrpc", type=Path, default=Path("build/protobuf-release/benchmarks/unary_bench"))
    parser.add_argument("--competitors", type=Path, default=Path("build/competitors"))
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument("--iterations", type=int, default=100000)
    parser.add_argument("--warmup", type=int, default=10000)
    parser.add_argument("--cpu", type=int, default=2)
    parser.add_argument("--server-cpu", type=int, default=4)
    parser.add_argument("--control-cpu", type=int, default=8)
    args = parser.parse_args()
    check(min(args.rounds, args.iterations, args.warmup) > 0, "counts must be positive")
    allowed = os.sched_getaffinity(0)
    cpus = (args.cpu, args.server_cpu, args.control_cpu)
    check(len(set(cpus)) == 3 and all(cpu in allowed for cpu in cpus), "invalid CPU configuration")
    cores = [Path(f"/sys/devices/system/cpu/cpu{cpu}/topology/core_id").read_text().strip() for cpu in cpus]
    check(len(set(cores)) == 3, "CPUs share a physical core")
    os.sched_setaffinity(0, {args.control_cpu})
    args.out = args.out.resolve(); args.out.mkdir(parents=True, exist_ok=False)
    binaries = {"lrpc": args.lrpc.resolve(), **{name: (args.competitors / (name + "-adapter") / "comparison_bench").resolve()
                for name in ("grpc", "brpc", "coro_rpc")}}
    environment = harness.environment(args.lrpc)
    environment.update(started_utc=datetime.now(timezone.utc).isoformat(), client_cpu=args.cpu,
                       server_cpu=args.server_cpu, control_cpu=args.control_cpu, cpu_core_ids=cores,
                       topology="separate-process", iterations=args.iterations, warmup=args.warmup, rounds=args.rounds,
                       binaries={name: {"path": str(path), "sha256": sha256(path)} for name, path in binaries.items()})
    environment["cpu_policies"] = {}
    for cpu in cpus:
        policy = {}
        for name in ("scaling_governor", "energy_performance_preference", "scaling_min_freq", "scaling_max_freq"):
            path = Path(f"/sys/devices/system/cpu/cpu{cpu}/cpufreq/{name}")
            if path.exists():
                policy[name] = path.read_text().strip()
        environment["cpu_policies"][str(cpu)] = policy
    names = list(binaries)
    environment["framework_order_by_round"] = [names[r % len(names):] + names[:r % len(names)]
                                                for r in range(args.rounds)]
    (args.out / "environment.json").write_text(json.dumps(environment, indent=2) + "\n")
    cases = [{"bytes": size, "inflight": count} for size in (64, 4096) for count in (1, 8, 64)]
    summaries = []
    with (args.out / "summary.jsonl").open("w") as output:
        for round_id in range(args.rounds):
            order = cases if round_id % 2 == 0 else list(reversed(cases))
            names = environment["framework_order_by_round"][round_id]
            for case in order:
                directory = args.out / f"round-{round_id + 1}-{case['bytes']}B-n{case['inflight']}"
                directory.mkdir()
                for name in names:
                    summary = run(name, binaries[name], case, args, directory)
                    summary["round"] = round_id + 1
                    output.write(json.dumps(summary) + "\n"); output.flush()
                    summaries.append(summary)
                    result = summary["normalized"]
                    print(f"round {round_id + 1}/{args.rounds} {name} {case['bytes']}B n={case['inflight']} "
                          f"p99={result['p99_us']:.3f}us rate={result['success_per_second']:.0f}/s", flush=True)
    aggregate = []
    for case in cases:
        for name in binaries:
            selected = [s["normalized"] for s in summaries if s["framework"] == name and s["config"] == case]
            record = dict(framework=name, **case, rounds=len(selected))
            for metric in ("p50_us", "p99_us", "p999_us", "max_us", "success_per_second", "client_cpu_ns_per_call"):
                values = [s[metric] for s in selected]
                record[metric] = {"median": statistics.median(values), "min": min(values), "max": max(values)}
            aggregate.append(record)
    result = {"environment": environment, "summaries": summaries, "aggregate": aggregate,
              "finished_utc": datetime.now(timezone.utc).isoformat()}
    (args.out / "results.json").write_text(json.dumps(result, indent=2) + "\n")


if __name__ == "__main__":
    main()
