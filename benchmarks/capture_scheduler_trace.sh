#!/usr/bin/env bash
# Run as the ordinary user. Only perf collection/decoding needs sudo.
# CPU 2 tracepoints around a 10-second probe; perf also records mapping/task metadata.
# No sysctl, affinity of other tasks, or IRQ changes.
set -euo pipefail

if (( EUID == 0 )); then
    echo 'Run this script as your ordinary user; it requests sudo only for perf.' >&2
    exit 2
fi
if (( $# > 1 )) || { (( $# == 1 )) && [[ $1 != --dry-run ]]; }; then
    echo 'Usage: bash benchmarks/capture_scheduler_trace.sh [--dry-run]' >&2
    exit 2
fi

rpc_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)
caller=$(id -un)
caller_ids=$(id -u):$(id -g)
probe="$rpc_root/build/bench/benchmarks/scheduler_probe"
out="$rpc_root/build/scheduler-trace.XXXXXX"
dry_run=${1:-}

for program in cmake perf taskset sudo runuser timeout mktemp; do
    command -v "$program" >/dev/null
done

if [[ $dry_run != --dry-run ]]; then
    cmake --build "$rpc_root/build/bench" --target scheduler_probe
    umask 077
    out=$(mktemp -d -- "$out")
fi

record=(taskset -c 10 sudo -n timeout --signal=INT --kill-after=3s 15s
        perf record -a -C 2 --clockid mono -m 1024 --synth no --no-buildid --no-buildid-cache
        -e sched:sched_switch
        -e irq:irq_handler_entry -e irq:irq_handler_exit
        -e irq:softirq_entry -e irq:softirq_exit
        -e workqueue:workqueue_execute_start -e workqueue:workqueue_execute_end
        -o "$out/perf.data" -- runuser -u "$caller" -- taskset -c 2 "$probe" 10)
decode=(sudo -n perf script -i "$out/perf.data" --ns --show-lost-events
        -F trace:time,cpu,event,trace)

if [[ $dry_run == --dry-run ]]; then
    printf 'Build: '; printf '%q ' cmake --build "$rpc_root/build/bench" --target scheduler_probe; printf '\n'
    printf 'Capture: '; printf '%q ' "${record[@]}"; printf '\n'
    printf 'Decode: '; printf '%q ' "${decode[@]}"; printf '\n'
    printf 'Output directory: %s (created uniquely when run)\n' "$out"
    exit 0
fi

printf 'Recording CPU 2 tracepoints around a 10-second probe; files remain local in %s\n' "$out"
printf 'Raw perf.data also includes kernel/module mappings and task sideband metadata from other CPUs.\n'
sudo -v
"${record[@]}" >"$out/probe.csv" 2>"$out/record.log"
"${decode[@]}" >"$out/events.txt" 2>"$out/decode.log"
sudo -n chown --no-dereference -- "$caller_ids" "$out/perf.data"
printf 'Capture complete: %s\n' "$out"
