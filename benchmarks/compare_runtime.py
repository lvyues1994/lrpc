"""Pair frozen and current RPC binaries; validate and retain every request sample."""

import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path
import shutil
import statistics
import time

from run_suite import environment, measured_command, validate


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def configurations(suite, inflights):
    for codec in ("bytes", "protobuf"):
        for size in (64, 4096):
            for inflight in (inflights if suite == "closed" else (128,)):
                for deadline in (0, 50000):
                    yield {"transport": "rpc", "codec": codec, "bytes": size, "inflight": inflight,
                           "deadline-us": deadline, "rate": 0 if suite == "closed" else 300000,
                           "burst": 1, "backend": "epoll", "rpc-streams": 64,
                           "frame-allocator": "system", "receive-buffer-bytes": 65536}


def check_configuration(result, config, args):
    aliases = {"inflight": "inflight_limit", "rpc-streams": "rpc_streams", "rate": "target_rate"}
    for key, value in config.items():
        assert result[aliases.get(key, key.replace("-", "_"))] == value, (key, result)
    assert result["offered"] == args.iterations and result["warmup"] == args.warmup


def summarize(configs, results, rounds):
    lines = ["# Runtime comparison", "", "All rounds retained; p99 values are medians [minimum, maximum].",
             "Allocation-instrumented latency is diagnostic only. CSV statistics independently verified.", "",
             "| Codec | Bytes | Inflight | Deadline µs | Before p99 µs | After p99 µs | Paired p99 change % | Before/after success/s | Before/after new/call | Eligible pairs |",
             "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |"]
    for index, config in enumerate(configs):
        versions = {name: [r for r in results if r["case"] == index and r["version"] == name]
                    for name in ("before", "after")}
        p99 = {}
        for name, values in versions.items():
            numbers = [r["success_api"]["p99_us"] for r in values if r["success_api"]]
            p99[name] = f"{statistics.median(numbers):.3f} [{min(numbers):.3f},{max(numbers):.3f}]" if numbers else "—"
        paired = [(b, a) for b in versions["before"] for a in versions["after"] if b["round"] == a["round"]
                  and b["eligible_for_zero_error_p99"] and a["eligible_for_zero_error_p99"]]
        changes = [(a["success_api"]["p99_us"] / b["success_api"]["p99_us"] - 1) * 100 for b, a in paired]
        change = f"{statistics.median(changes):+.1f}" if changes else "—"
        throughput = [statistics.median(r["success_per_second"] for r in versions[name]) for name in versions]
        allocations = [[r["new_calls_per_offered"] for r in versions[name] if r["new_calls_per_offered"] is not None]
                       for name in versions]
        allocation = "/".join(f"{statistics.median(v):.3f}" if v else "—" for v in allocations)
        lines.append(f"| {config['codec']} | {config['bytes']} | {config['inflight']} | {config['deadline-us']} | "
                     f"{p99['before']} | {p99['after']} | {change} | {throughput[0]:.0f}/{throughput[1]:.0f} | "
                     f"{allocation} | {len(paired)}/{rounds} |")
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--before", type=Path, required=True)
    parser.add_argument("--after", type=Path, required=True)
    parser.add_argument("--baseline-source", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--suite", choices=("closed", "open-core"), default="closed")
    parser.add_argument("--inflight", type=int, nargs="+", default=[1, 8, 64], choices=(1, 8, 64))
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--iterations", type=int, default=20000)
    parser.add_argument("--warmup", type=int, default=2000)
    parser.add_argument("--cpu", type=int, default=2)
    parser.add_argument("--server-cpu", type=int, default=4)
    parser.add_argument("--topology", choices=("same-thread", "separate-process"), default="same-thread")
    args = parser.parse_args()
    if min(args.rounds, args.iterations, args.warmup) <= 0:
        parser.error("rounds, iterations and warmup must be positive")
    allowed = os.sched_getaffinity(0)
    if args.cpu not in allowed or (args.topology == "separate-process" and args.server_cpu not in allowed):
        parser.error("CPU outside current affinity mask")
    os.sched_setaffinity(0, {args.cpu})
    args.out.mkdir(parents=True, exist_ok=False)
    env = environment(args.after)
    # A frozen binary is not built from the current source tree. Preserve its
    # separate library snapshot, common harness and original build configuration.
    frozen_files = [args.baseline_source / "implementation.tar", args.baseline_source / "harness.tar",
                    *sorted((args.baseline_source / "config").glob("*"))]
    env.update(started_unix=time.time(), topology=args.topology, suite=args.suite, client_cpu=args.cpu,
               server_cpu=args.server_cpu if args.topology == "separate-process" else None,
               before_binary=str(args.before.resolve()), before_binary_sha256=digest(args.before),
               before_source={str(p.resolve()): digest(p) for p in frozen_files},
               before_note="Old library archive plus the common protobuf benchmark harness; config copied before rebuilding.")
    (args.out / "environment.json").write_text(json.dumps(env, indent=2) + "\n")
    configs = list(configurations(args.suite, args.inflight))
    results = []
    with (args.out / "summary.jsonl").open("w") as output:
        for round_id in range(args.rounds):
            order = list(range(len(configs)))
            if round_id % 2:
                order.reverse()
            for index in order:
                for version in (("before", "after") if round_id % 2 == 0 else ("after", "before")):
                    args.binary = getattr(args, version)
                    config = configs[index]
                    sample = args.out / f"{version}-case-{index:02}-round-{round_id + 1}.csv"
                    invocation = [str(args.binary.resolve()), "--iterations", str(args.iterations),
                                  "--warmup", str(args.warmup), "--samples", str(sample)]
                    for key, value in config.items():
                        invocation.extend(["--" + key, str(value)])
                    result = measured_command(invocation, config, args)
                    check_configuration(result, config, args)
                    validate(result, sample, config)
                    result.update(version=version, case=index, round=round_id + 1)
                    result.setdefault("command", invocation)
                    output.write(json.dumps(result) + "\n"); output.flush()
                    results.append(result)
                    with sample.open("rb") as source, gzip.open(sample.with_suffix(".csv.gz"), "wb") as target:
                        shutil.copyfileobj(source, target)
                    sample.unlink()
                    print(f"round {round_id + 1}/{args.rounds} case {index + 1}/{len(configs)} {version} "
                          f"ok={result['success']} reject={result['rejected']} timeout={result['timeout']} "
                          f"drop={result['generator_drop']}", flush=True)
    (args.out / "summary.md").write_text(summarize(configs, results, args.rounds))


if __name__ == "__main__":
    main()
