"""Paired unary regression, frame/Arena caches and channel/runtime entry costs."""
import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path
import statistics
import subprocess

from run_suite import environment, validate


def variants():
    for codec, size, inflight in [("bytes", 64, 1), ("bytes", 64, 64),
                                  ("protobuf", 64, 1), ("protobuf", 64, 8),
                                  ("protobuf", 4096, 1), ("protobuf", 4096, 8)]:
        yield "regression", codec, size, inflight, [("before", {}), ("after", {})]
    for size in (64, 4096):
        for inflight in (1, 8):
            yield "frame", "bytes", size, inflight, [("system", {}), ("recycling", {"frame-allocator": "recycling"})]
            yield "arena", "protobuf", size, inflight, [("per_call", {}), ("cache64", {"arena-cache": 64})]
    for inflight in (1, 8):
        yield "entry", "bytes", 64, inflight, [(v, {"rpc-entry": v}) for v in ("client", "channel", "runtime")]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--before", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument("--iterations", type=int, default=30000)
    parser.add_argument("--warmup", type=int, default=3000)
    parser.add_argument("--cpu", type=int, default=2)
    parser.add_argument("--worker-cpus", type=int, nargs=2, default=[4, 6])
    args = parser.parse_args()
    allowed = os.sched_getaffinity(0)
    if not {args.cpu, *args.worker_cpus} <= allowed or min(args.rounds, args.iterations, args.warmup) <= 0:
        parser.error("invalid CPU or sample count")
    args.out.mkdir(parents=True, exist_ok=False)
    env = environment(args.binary)
    env.update(client_cpu=args.cpu, worker_cpus=args.worker_cpus,
               before_sha256=hashlib.sha256(args.before.read_bytes()).hexdigest(),
               before_note="Frozen steps 1–4 binary; its intermediate source snapshot was not archived.")
    (args.out / "environment.json").write_text(json.dumps(env, indent=2) + "\n")
    os.sched_setaffinity(0, {args.cpu})
    results = []
    cases = list(variants())
    with (args.out / "summary.jsonl").open("w") as output:
        for round_id in range(args.rounds):
            for index in (range(len(cases)) if round_id % 2 == 0 else reversed(range(len(cases)))):
                experiment, codec, size, inflight, versions = cases[index]
                for name, extra in (versions if round_id % 2 == 0 else reversed(versions)):
                    binary = args.before if name == "before" else args.binary
                    config = {"transport": "rpc", "codec": codec, "bytes": size, "inflight": inflight,
                              "receive-buffer-bytes": 65536, "backend": "epoll", **extra}
                    if name == "runtime":
                        config.update(zip(("runtime-cpu0", "runtime-cpu1"), args.worker_cpus))
                    sample = args.out / f"case-{index}-{name}-{round_id}.csv"
                    command = [str(binary.resolve()), "--iterations", str(args.iterations),
                               "--warmup", str(args.warmup), "--samples", str(sample)]
                    for key, value in config.items():
                        command.extend(["--" + key, str(value)])
                    trial = subprocess.run(command, capture_output=True, text=True, timeout=40)
                    if trial.returncode:
                        output.write(json.dumps({"case": index, "variant": name, "round": round_id,
                                                 "command": command, "exit_code": trial.returncode,
                                                 "stderr": trial.stderr, "stdout": trial.stdout}) + "\n"); output.flush()
                        raise RuntimeError(trial.stderr)
                    result = json.loads(trial.stdout)
                    result.update(case=index, experiment=experiment, variant=name, round=round_id, command=command)
                    try:
                        validate(result, sample, config)
                    except AssertionError as error:
                        result["validation_error"] = str(error)
                        output.write(json.dumps(result) + "\n"); output.flush()
                        raise
                    result["csv_validated"] = True
                    results.append(result)
                    output.write(json.dumps(result) + "\n"); output.flush()
                    with gzip.open(str(sample) + ".gz", "wb") as compressed:
                        compressed.write(sample.read_bytes())
                    sample.unlink()
                print(f"round {round_id + 1}/{args.rounds} {experiment} {codec} {size}B n={inflight}", flush=True)
    lines = ["# Steps 5–11 performance experiments", "", f"{args.rounds} paired rounds; every request CSV independently validated.", "",
             "Client and channel use one caller/server executor and one connection. Runtime uses two worker CPUs, two client/server shards and two connections; this measures that complete topology, not facade-only cost or scaling.", "",
             "| Experiment | Codec | Bytes | Inflight | Variant | p99 µs median [min, max] | Success/s median | Eligible rounds |",
             "| --- | --- | --- | --- | --- | --- | --- | --- |"]
    for index, (experiment, codec, size, inflight, versions) in enumerate(cases):
        for name, _ in versions:
            selected = [r for r in results if r["case"] == index and r["variant"] == name]
            eligible = [r for r in selected if r["eligible_for_zero_error_p99"]]
            p99 = [r["success_api"]["p99_us"] for r in eligible]
            rate = statistics.median(r["success_per_second"] for r in selected)
            latency = f"{statistics.median(p99):.3f} [{min(p99):.3f}, {max(p99):.3f}]" if p99 else "—"
            lines.append(f"| {experiment} | {codec} | {size} | {inflight} | {name} | {latency} | {rate:.0f} | {len(eligible)}/{args.rounds} |")
    (args.out / "report.md").write_text("\n".join(lines) + "\n")
    (args.out / "results.json").write_text(json.dumps({"environment": env, "rounds": results}, indent=2) + "\n")


if __name__ == "__main__":
    main()
