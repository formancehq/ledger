#!/usr/bin/env bash
# Schemathesis API testing runner for Ledger V3.
#
# Builds and starts a single-node server (auth disabled by default),
# waits for readiness, runs Schemathesis tests, then tears down.
#
# Usage: bash tests/schemathesis/run.sh
# Env vars: HTTP_PORT, GRPC_PORT, RAFT_PORT, MAX_EXAMPLES, SCHEMATHESIS_WORKERS,
#   SCHEMATHESIS_SHRINK
#   SCHEMATHESIS_WORKERS=N runs the endpoint suite across N concurrent workers
#   (default 1). Keep at 1 for the reproducible gate: >1 breaks the
#   `derandomize` determinism (see test_api.py). The suite is fast at 1 worker.
#   SCHEMATHESIS_SHRINK=1 re-enables Hypothesis shrinking (minimal failing
#   examples) for local debugging. Off by default — see test_api.py --shrink.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

HTTP_PORT=${HTTP_PORT:-9099}
GRPC_PORT=${GRPC_PORT:-8899}
RAFT_PORT=${RAFT_PORT:-7779}
MAX_EXAMPLES=${MAX_EXAMPLES:-50}
SCHEMATHESIS_WORKERS=${SCHEMATHESIS_WORKERS:-1}

TMPDIR=$(mktemp -d)
# On exit, preserve the server log as an uploadable diagnostic BEFORE removing
# TMPDIR, so a failing run still ships server-side context. The filename matches
# the CI artifact glob (/tmp/schemathesis-*.txt).
trap 'kill "$SERVER_PID" 2>/dev/null; cp "$TMPDIR/server.log" /tmp/schemathesis-server.txt 2>/dev/null || true; rm -rf "$TMPDIR"' EXIT

echo "==> Building server..."
cd "$REPO_ROOT"
go build -o "$TMPDIR/ledger-server" .

echo "==> Starting server (single-node, bootstrap, no auth)..."
"$TMPDIR/ledger-server" run \
    --node-id 1 --cluster-id schemathesis-test --bootstrap \
    --bind-addr "127.0.0.1:$RAFT_PORT" \
    --wal-dir "$TMPDIR/wal" --data-dir "$TMPDIR/data" \
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

# Set up Python venv and install deps if needed
VENV_DIR="$SCRIPT_DIR/.venv"
if [ ! -d "$VENV_DIR" ]; then
    echo "==> Creating Python venv..."
    python3 -m venv "$VENV_DIR"
fi
# shellcheck disable=SC1091
source "$VENV_DIR/bin/activate"
if ! python3 -c "import schemathesis" 2>/dev/null; then
    echo "==> Installing Schemathesis..."
    pip3 install -q -r "$SCRIPT_DIR/requirements.txt"
fi

echo "==> Running Schemathesis tests..."
echo ""
SHRINK_FLAG=""
if [ -n "${SCHEMATHESIS_SHRINK:-}" ] && [ "${SCHEMATHESIS_SHRINK}" != "0" ]; then
    SHRINK_FLAG="--shrink"
fi
# Tee the full run (stdout+stderr) to an uploadable report. `set -o pipefail`
# (see `set` above) makes the pipeline inherit test_api.py's non-zero exit, so a
# conformity failure still fails the job. Filename matches the CI artifact glob.
python3 "$SCRIPT_DIR/test_api.py" \
    --base-url "http://localhost:$HTTP_PORT" \
    --max-examples "$MAX_EXAMPLES" \
    --workers "$SCHEMATHESIS_WORKERS" \
    $SHRINK_FLAG 2>&1 | tee /tmp/schemathesis-report.txt
