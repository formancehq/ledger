#!/usr/bin/env bash
# Schemathesis API testing runner for Ledger V3.
#
# Builds and starts a single-node server (auth disabled by default),
# waits for readiness, runs Schemathesis tests, then tears down.
#
# Usage: bash tests/schemathesis/run.sh
# Env vars: HTTP_PORT, GRPC_PORT, RAFT_PORT, MAX_EXAMPLES, SCHEMATHESIS_WORKERS,
#   SCHEMATHESIS_SHRINK, REPO_ROOT, SCHEMATHESIS_OPENAPI_PATH
#   SCHEMATHESIS_WORKERS=N runs the endpoint suite across N concurrent workers
#   (default 1). Keep at 1 for the reproducible gate: >1 breaks the
#   `derandomize` determinism (see test_api.py). The suite is fast at 1 worker.
#   SCHEMATHESIS_SHRINK=1 re-enables Hypothesis shrinking (minimal failing
#   examples) for local debugging. Off by default — see test_api.py --shrink.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="${REPO_ROOT:-$(cd "$SCRIPT_DIR/../.." && pwd)}"
SCHEMATHESIS_OPENAPI_PATH="${SCHEMATHESIS_OPENAPI_PATH:-$REPO_ROOT/openapi.yml}"

HTTP_PORT=${HTTP_PORT:-9099}
GRPC_PORT=${GRPC_PORT:-8899}
RAFT_PORT=${RAFT_PORT:-7779}
MAX_EXAMPLES=${MAX_EXAMPLES:-50}
SCHEMATHESIS_WORKERS=${SCHEMATHESIS_WORKERS:-1}
HISTORICAL_BALANCES_AVAILABLE=false
if grep -q '^message ConfigureHistoricalBalancesRequest' "$REPO_ROOT/misc/proto/bucket.proto" 2>/dev/null; then
    HISTORICAL_BALANCES_AVAILABLE=true
fi

TMPDIR=$(mktemp -d)
# On exit, preserve the server log as an uploadable diagnostic BEFORE removing
# TMPDIR, so a failing run still ships server-side context. The filename matches
# the CI artifact glob (/tmp/schemathesis-*.txt).
# Every command is guarded with `|| true`: `set -e` is active inside the trap, so
# an unguarded non-zero status would abort the remaining cleanup. `wait` returns
# the SIGTERM status (143) of the server we just killed, and `kill` fails if the
# server already exited — either would otherwise skip the log copy (most valuable
# exactly when the server crashed) and corrupt the script's real exit status.
trap 'kill "${SERVER_PID:-}" 2>/dev/null || true; wait "${SERVER_PID:-}" 2>/dev/null || true; cp "$TMPDIR/server.log" /tmp/schemathesis-server.txt 2>/dev/null || true; rm -rf "$TMPDIR" || true' EXIT

echo "==> Building server..."
cd "$REPO_ROOT"
go build -o "$TMPDIR/ledger-server" .
if [[ "$HISTORICAL_BALANCES_AVAILABLE" == true ]]; then
    go build -o "$TMPDIR/ledgerctl" ./cmd/ledgerctl
fi

echo "==> Starting server (single-node, bootstrap, no auth)..."
# The temporary store shares the developer/agent host filesystem. Keep API
# conformance independent from that host's production disk-pressure threshold.
"$TMPDIR/ledger-server" run \
    --node-id 1 --cluster-id schemathesis-test --bootstrap \
    --bind-addr "127.0.0.1:$RAFT_PORT" \
    --wal-dir "$TMPDIR/wal" --data-dir "$TMPDIR/data" \
    --health-wal-threshold 1 --health-data-threshold 1 \
    --http-port "$HTTP_PORT" --grpc-port "$GRPC_PORT" \
    > "$TMPDIR/server.log" 2>&1 &
SERVER_PID=$!

echo "==> Waiting for server readiness..."
for i in $(seq 1 30); do
    if curl -sf "http://localhost:$HTTP_PORT/readyz" > /dev/null 2>&1; then
        echo "    Server ready."
        break
    fi
    if [ "$i" -eq 30 ]; then
        echo "ERROR: Server did not become ready within 30s" >&2
        echo "Server log:" >&2
        cat "$TMPDIR/server.log" >&2
        exit 1
    fi
    sleep 1
done

# Seed deterministic rows. Without this the list/detail operations return empty
# arrays, and `response_schema_conformance` validates nothing: an empty [] can
# satisfy ANY items schema, so the whole gate is vacuous on those routes. The
# ledger name matches the coercion in test_api.py, which rewrites any invalid
# ledgerName path parameter to "test-ledger".
echo "==> Seeding fixture data..."
API="http://localhost:$HTTP_PORT/v3"

seed() {
    local desc="$1"
    shift
    local code
    code=$(curl -s -o /dev/null -w '%{http_code}' "$@")
    case "$code" in
        2*) echo "    $desc -> $code" ;;
        *)
            echo "ERROR: seed step '$desc' returned $code" >&2
            echo "Server log:" >&2
            cat "$TMPDIR/server.log" >&2
            exit 1
            ;;
    esac
}

seed "create ledger" -X POST "$API/test-ledger"

# Sacrificial ledger for DELETE /v3/{ledgerName}. That operation is itself
# fuzzed, and test_api.py's before_call hook routes it here instead of the
# fixture ledger above — see the hook for why. It needs no data; existence is
# enough for the fuzzer's first delete attempt to get a 2xx.
seed "create delete-sacrifice ledger" -X POST "$API/delete-me-ledger"

seed "transaction 1" -X POST "$API/test-ledger/transactions" \
    -H 'Content-Type: application/json' \
    -d '{"postings":[{"source":"world","destination":"alice","amount":12345,"asset":"USD/2"}],"metadata":{"kind":"seed"},"reference":"seed-ref-1"}'

seed "transaction 2" -X POST "$API/test-ledger/transactions" \
    -H 'Content-Type: application/json' \
    -d '{"postings":[{"source":"world","destination":"bob","amount":500,"asset":"USD/2"}],"reference":"seed-ref-2"}'

# Populates reverted / revertedAt / revertedByTransactionId / revertsTransactionId,
# so the reversion half of TransactionResponse is exercised too. Sourced from
# "world" rather than "alice" for transaction 2 above: alice's balance must
# still hold the full posted amount for the revert (which reverses it exactly)
# to succeed, and a prior alice->bob spend leaves her short.
seed "revert transaction 1" -X POST "$API/test-ledger/transactions/1/revert"

if [[ "$HISTORICAL_BALANCES_AVAILABLE" == true ]]; then
    echo "==> Enabling historical balances for fixture ledger..."
    if ! "$TMPDIR/ledgerctl" \
        --server "localhost:$GRPC_PORT" --insecure \
        ledgers historical-balances enable test-ledger --timeout 10s; then
        echo "ERROR: failed to enable historical balances" >&2
        echo "Server log:" >&2
        cat "$TMPDIR/server.log" >&2
        exit 1
    fi

    echo "==> Waiting for historical balance readiness..."
    for i in $(seq 1 100); do
        code=$(curl -s -o "$TMPDIR/history-readiness.json" -w '%{http_code}' \
            "$API/test-ledger/volumes?at=2100-01-01T00%3A00%3A00Z")
        case "$code" in
            200)
                echo "    Historical balances ready."
                break
                ;;
            503)
                if [[ "$i" -eq 100 ]]; then
                    echo "ERROR: historical balances did not become ready" >&2
                    cat "$TMPDIR/history-readiness.json" >&2
                    cat "$TMPDIR/server.log" >&2
                    exit 1
                fi
                sleep 0.1
                ;;
            *)
                echo "ERROR: historical readiness probe returned $code" >&2
                cat "$TMPDIR/history-readiness.json" >&2
                cat "$TMPDIR/server.log" >&2
                exit 1
                ;;
        esac
    done
fi

# Set up Python venv and install deps if needed
VENV_DIR="$SCRIPT_DIR/.venv"
if [ ! -d "$VENV_DIR" ]; then
    echo "==> Creating Python venv..."
    python3 -m venv "$VENV_DIR"
fi
# shellcheck disable=SC1091
source "$VENV_DIR/bin/activate"
# Install unconditionally: an `import schemathesis` guard would skip install when
# a pre-existing .venv already has *some* version, silently bypassing the exact
# pins in requirements.txt (defeating the reproducibility they exist for). pip is
# a fast no-op when the pinned versions are already satisfied.
echo "==> Installing/verifying pinned Schemathesis dependencies..."
pip3 install -q -r "$SCRIPT_DIR/requirements.txt"

echo "==> Running Schemathesis tests..."
echo ""
SHRINK_FLAG=""
if [ -n "${SCHEMATHESIS_SHRINK:-}" ] && [ "${SCHEMATHESIS_SHRINK}" != "0" ]; then
    SHRINK_FLAG="--shrink"
fi
# Tee the full run (stdout+stderr) to an uploadable report. `set -o pipefail`
# (see `set` above) makes the pipeline inherit test_api.py's non-zero exit, so a
# conformity failure still fails the job. Filename matches the CI artifact glob.
SCHEMATHESIS_OPENAPI_PATH="$SCHEMATHESIS_OPENAPI_PATH" python3 "$SCRIPT_DIR/test_api.py" \
    --base-url "http://localhost:$HTTP_PORT" \
    --max-examples "$MAX_EXAMPLES" \
    --workers "$SCHEMATHESIS_WORKERS" \
    $SHRINK_FLAG 2>&1 | tee /tmp/schemathesis-report.txt
