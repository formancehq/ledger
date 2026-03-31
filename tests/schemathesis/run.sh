#!/usr/bin/env bash
# Schemathesis API testing runner for Ledger V3.
#
# Builds and starts a single-node server (auth disabled by default),
# waits for readiness, runs Schemathesis tests, then tears down.
#
# Usage: bash tests/schemathesis/run.sh
# Env vars: HTTP_PORT, GRPC_PORT, RAFT_PORT, MAX_EXAMPLES
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

HTTP_PORT=${HTTP_PORT:-9099}
GRPC_PORT=${GRPC_PORT:-8899}
RAFT_PORT=${RAFT_PORT:-7779}
MAX_EXAMPLES=${MAX_EXAMPLES:-50}

TMPDIR=$(mktemp -d)
trap 'kill "$SERVER_PID" 2>/dev/null; rm -rf "$TMPDIR"' EXIT

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
python3 "$SCRIPT_DIR/test_api.py" \
    --base-url "http://localhost:$HTTP_PORT" \
    --max-examples "$MAX_EXAMPLES"
