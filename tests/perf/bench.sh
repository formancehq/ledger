#!/usr/bin/env bash
set -euo pipefail

# go run starts a separate executable; record its PID before replacing this wrapper.
if [[ "${1:-}" == "exec" ]]; then
    shift
    echo "$$" > "$BENCH_RUNTIME_DIR/ledger.pid"
    exec "$@"
fi

cd "$(dirname "${BASH_SOURCE[0]}")/../.."
mkdir -p build/bench
# A startup failure must not expose a previous run's measurements or server log.
rm -f build/bench/summary.json
: > build/bench/ledger.log
for tool in go k6 python3; do
    command -v "$tool" >/dev/null || { echo "Missing $tool; run inside nix develop." >&2; exit 1; }
done

export BENCH_DISK_THRESHOLD="${BENCH_DISK_THRESHOLD:-0.99}"
# Check the shared local volume and refuse ports occupied by another service.
python3 - <<'PY'
import os
import socket

try:
    threshold = float(os.environ['BENCH_DISK_THRESHOLD'])
except ValueError:
    raise SystemExit('BENCH_DISK_THRESHOLD must be a number between 0 and 1, exclusive')
if not 0 < threshold < 1:
    raise SystemExit('BENCH_DISK_THRESHOLD must be a number between 0 and 1, exclusive')
volume = os.statvfs('build/bench')
used = 1 - volume.f_bavail / volume.f_blocks
free_gib = volume.f_bavail * volume.f_frsize / (1024 ** 3)
if used >= threshold:
    raise SystemExit(f'Cannot start benchmark: disk usage {used:.1%} exceeds its {threshold:.1%} limit '
                     f'({free_gib:.1f} GiB free). Free space on this volume before retrying.')
print(f'Local disk: {used:.1%} used, {free_gib:.1f} GiB free; benchmark limit {threshold:.1%}')

sockets = [socket.socket() for _ in range(3)]
for listener, port in zip(sockets, (17777, 18888, 19000)):
    try:
        # macOS can allow a wildcard bind beside an existing loopback listener.
        with socket.socket() as probe:
            probe.settimeout(0.2)
            if probe.connect_ex(("127.0.0.1", port)) == 0:
                raise OSError("another service is already listening")
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("0.0.0.0", port))
        listener.listen()
    except OSError as error:
        raise SystemExit(f"Benchmark port {port} is unavailable: {error}")
PY

export BENCH_RUNTIME_DIR
BENCH_RUNTIME_DIR=$(mktemp -d "$PWD/build/bench/run.XXXXXX")
go_pid=""
k6_pid=""

cleanup() {
    local status=$? ledger_pid=""
    trap - EXIT
    if [[ -n "$k6_pid" ]]; then
        kill -TERM "$k6_pid" 2>/dev/null || true
        kill -KILL "$k6_pid" 2>/dev/null || true
        wait "$k6_pid" 2>/dev/null || true
    fi
    if [[ -f "$BENCH_RUNTIME_DIR/ledger.pid" ]]; then
        ledger_pid=$(cat "$BENCH_RUNTIME_DIR/ledger.pid")
        kill -TERM "$ledger_pid" 2>/dev/null || true
        for ((attempt = 0; attempt < 50; attempt++)); do
            kill -0 "$ledger_pid" 2>/dev/null || break
            sleep 0.1
        done
        kill -KILL "$ledger_pid" 2>/dev/null || true
    fi
    if [[ -n "$go_pid" ]]; then
        kill -TERM "$go_pid" 2>/dev/null || true
        wait "$go_pid" 2>/dev/null || true
    fi
    rm -rf "$BENCH_RUNTIME_DIR"
    if (( status != 0 && status != 99 )); then
        # A threshold failure is a completed measurement; interrupted/startup runs are not.
        rm -f build/bench/summary.json
    fi
    if (( status != 0 )); then
        echo "Benchmark failed. Ledger log: build/bench/ledger.log" >&2
        tail -n 15 build/bench/ledger.log >&2
    fi
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

export HTTP_ADDR=http://127.0.0.1:19000 LEDGER_NAME="bench-$$"
export BULK_SIZE=50 BULK_ATOMIC=true USE_NUMSCRIPT=true
export BENCH_DURATION="${BENCH_DURATION:-60}"
export BENCH_MIN_TPS="${BENCH_MIN_TPS:-100000}"
export BENCH_VUS="${BENCH_VUS:-100}"
# Keep dotenv or caller k6 shortcuts from replacing the benchmark scenario.
unset K6_STAGES K6_DURATION K6_VUS K6_ITERATIONS

run_k6() {
    k6 run --quiet --no-thresholds=false --linger=false \
        --summary-trend-stats='avg,p(95)' tests/perf/scripts/local_bench.js &
    k6_pid=$!
    wait "$k6_pid"
    k6_pid=""
}

echo "Starting Ledger with fresh storage; log: build/bench/ledger.log"
go run -exec "bash tests/perf/bench.sh exec" . run \
    --bootstrap --node-id 1 --cluster-id "$LEDGER_NAME" \
    --bind-addr 127.0.0.1:17777 --grpc-port 18888 --http-port 19000 \
    --wal-dir "$BENCH_RUNTIME_DIR/wal" --data-dir "$BENCH_RUNTIME_DIR/data" \
    --health-wal-threshold "$BENCH_DISK_THRESHOLD" --health-data-threshold "$BENCH_DISK_THRESHOLD" \
    --auth-enabled=false > build/bench/ledger.log 2>&1 &
go_pid=$!

echo "Warming up for 5 seconds"
BENCH_WARMUP=true run_k6
kill -0 "$go_pid" 2>/dev/null || { echo "Ledger exited unexpectedly." >&2; exit 1; }
echo "Measuring ${BENCH_DURATION}s; minimum ${BENCH_MIN_TPS} successful transactions/s"
BENCH_WARMUP=false run_k6
echo "Benchmark: PASS"
