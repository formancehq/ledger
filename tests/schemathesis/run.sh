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
# Every command is guarded with `|| true`: `set -e` is active inside the trap, so
# an unguarded non-zero status would abort the remaining cleanup. `wait` returns
# the SIGTERM status (143) of the server we just killed, and `kill` fails if the
# server already exited — either would otherwise skip the log copy (most valuable
# exactly when the server crashed) and corrupt the script's real exit status.
trap 'kill "${SERVER_PID:-}" 2>/dev/null || true; wait "${SERVER_PID:-}" 2>/dev/null || true; cp "$TMPDIR/server.log" /tmp/schemathesis-server.txt 2>/dev/null || true; rm -rf "$TMPDIR" || true' EXIT

echo "==> Building server and ledgerctl..."
cd "$REPO_ROOT"
go build -o "$TMPDIR/ledger-server" .
# ledgerctl is needed for the sink and signing-key fixtures: neither has an HTTP
# write route (internal/adapter/http/handler.go exposes GET /_/events-sinks and
# GET /_/signing-keys only), so the only way to populate them is the gRPC API the
# CLI speaks.
go build -o "$TMPDIR/ledgerctl" ./cmd/ledgerctl

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

# gRPC-only seed step. Same contract as seed() above: a failure aborts loudly
# with both the CLI output and the server log, so a fixture that silently stopped
# being creatable cannot degrade the gate to a vacuous one.
ctl() {
    local desc="$1"
    shift
    if "$TMPDIR/ledgerctl" --server "127.0.0.1:$GRPC_PORT" --insecure "$@" \
        > "$TMPDIR/ledgerctl.out" 2>&1; then
        echo "    $desc -> ok"
    else
        echo "ERROR: seed step '$desc' failed" >&2
        echo "ledgerctl output:" >&2
        cat "$TMPDIR/ledgerctl.out" >&2
        echo "Server log:" >&2
        cat "$TMPDIR/server.log" >&2
        exit 1
    fi
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

# --- Index / signing-key / sink fixtures (EN-1791) --------------------------
# The nine index, signing-key and events-sink routes had a bare `type: object`
# 200 schema until EN-1791 typed them. Typing them is only load-bearing if
# `response_schema_conformance` sees a populated body: an empty `[]` satisfies
# any `items:` schema, and an unseeded detail route 404s and is validated
# against the 404 schema instead. Without the fixtures below those nine schemas
# are guarded by a check that cannot fail.
#
# A *metadata* index is seeded deliberately rather than a builtin one. The
# original EN-1791 defect was that `MetadataIndexID.target` disappeared from the
# JSON because TARGET_TYPE_ACCOUNT is enum 0 and protojson omits zero values,
# which made metadata indexes unidentifiable over HTTP. A builtin index
# (`tx_builtin:…`) leaves exactly that arm unexercised.
#
# processCreateIndex rejects a metadata index whose schema field is not declared
# (validateIndexTarget -> ErrMetadataFieldNotInSchema), so the field type has to
# be set first.
seed "declare account metadata field color" -X PUT "$API/test-ledger/metadata-schema/account/color" \
    -H 'Content-Type: application/json' \
    -d '{"type":"string"}'

seed "create metadata index" -X POST "$API/test-ledger/indexes" \
    -H 'Content-Type: application/json' \
    -d '{"id":"metadata:TARGET_TYPE_ACCOUNT:color"}'

# Sacrificial index for DELETE /v3/{ledgerName}/indexes/{canonicalId}. That
# operation is itself fuzzed, and test_api.py's before_call hook coerces
# {canonicalId} so the per-index GET routes reach 200 — aimed at the fixture
# index above, the fuzzer would drop it on its first example and every later
# index request would 404, undoing the coverage this block exists to add while
# the suite still reported green. Same shape as the delete-me-ledger sacrifice;
# see the hook for the full argument.
seed "declare transaction metadata field sacrifice" -X PUT "$API/test-ledger/metadata-schema/transaction/sacrifice" \
    -H 'Content-Type: application/json' \
    -d '{"type":"string"}'

seed "create delete-sacrifice index" -X POST "$API/test-ledger/indexes" \
    -H 'Content-Type: application/json' \
    -d '{"id":"metadata:TARGET_TYPE_TRANSACTION:sacrifice"}'

# Sacrificial metadata-schema field for
# DELETE /v3/{ledgerName}/metadata-schema/{targetType}/{key}. That operation is
# fuzzed too, and it is destructive at one remove: removing a schema field
# cascade-drops the index attached to it (processRemoveMetadataFieldType), so a
# delete aimed at ("account", "color") would take the fixture index above with
# it. test_api.py's before_call coerces {key} here; declared on all three
# targetType values because the parameter is an enum the fuzzer enumerates in
# full, and a sacrifice that is only declared for one of them leaves the other
# two 404-ing instead of exercising the route.
for target in account transaction ledger; do
    seed "declare $target metadata-schema sacrifice" \
        -X PUT "$API/test-ledger/metadata-schema/$target/schema-sacrifice" \
        -H 'Content-Type: application/json' \
        -d '{"type":"string"}'
done

# Wait for the replica to finish building the fixture index. This is a
# determinism requirement, not an optimisation: `IndexEntry.currentVersion` is
# per-replica and starts at 0, and the routes gated on it (notably
# GET /v3/{ledgerName}/indexes/{canonicalId}/inspect, which answers 503
# INDEX_BUILDING while CurrentVersion == 0) would otherwise validate against a
# different response schema depending on whether the backfill happened to land
# before the fuzzer reached them — a race the `derandomize` setting cannot cover.
# Poll for the condition, no bare sleep.
echo "==> Waiting for the fixture index to be built..."
for i in $(seq 1 30); do
    if curl -sf "$API/test-ledger/indexes/metadata:TARGET_TYPE_ACCOUNT:color/status" \
        | python3 -c 'import json,sys; sys.exit(0 if json.load(sys.stdin)["data"]["currentVersion"] > 0 else 1)'; then
        echo "    Index built."
        break
    fi
    if [ "$i" -eq 30 ]; then
        echo "ERROR: fixture index still unbuilt after 30s" >&2
        echo "Server log:" >&2
        cat "$TMPDIR/server.log" >&2
        exit 1
    fi
    sleep 1
done

# Documented gap: GET /v3/_/indexes/{canonicalId} and its /status sibling look
# the entry up under an EMPTY ledger, and no production path writes a registry
# entry with an empty ledger — processCreateIndex requires a loaded ledger, and
# the audit index lives in its own readstore keyspace, not in SubAttrIndex (see
# the comment on handleListBucketIndexes). Both routes therefore 404 whatever we
# seed, and GET /v3/_/indexes?scope=bucket returns [] for the same reason. Their
# 200 schemas stay unvalidated until bucket-scoped indexes actually exist; there
# is no fixture that would change that, only a fake one.

# Populates ListSigningKeysResponse. The key is public by definition and no
# private half exists or is needed — register-key takes only the public 32
# bytes. Registering a key does NOT switch on signature enforcement: that is
# SetSigningConfig(require_signatures), left at its default, so the unsigned
# HTTP writes the rest of the suite performs keep working.
ctl "register signing key" signing register-key --key-id schemathesis-key \
    --public-key 0000000000000000000000000000000000000000000000000000000000000001

# Populates GetEventsSinksResponse, the largest of the new schemas (fifteen
# component schemas hang off it). One sink per SinkConfig variant so every arm
# is validated, plus both Databricks credential kinds since `authMethod`
# switches on them.
#
# Only the HTTP sink is constructible in the default light build; Kafka, NATS,
# ClickHouse and Databricks need their build tags. An unavailable type is inert
# rather than harmful — Manager.reconcile logs one "Failed to create sink" line
# and returns nil, so no emitter, no retry loop, no publish attempt — while the
# stored configuration is still served by GET /v3/_/events-sinks, which is the
# only thing the response schema covers.
#
# Every credential is the obviously-fake literal `not-a-real-secret`, asserted
# absent from the response below.
ctl "add http sink" events add-sink --name sth-http \
    --http-endpoint "http://127.0.0.1:$HTTP_PORT/readyz" \
    --http-secret not-a-real-secret

ctl "add kafka sink" events add-sink --name sth-kafka \
    --kafka-brokers broker-a:9092,broker-b:9092 --kafka-topic ledger-events \
    --kafka-tls --kafka-sasl-mechanism SCRAM-SHA-256 \
    --kafka-sasl-username sth-user --kafka-sasl-password not-a-real-secret

ctl "add clickhouse sink" events add-sink --name sth-clickhouse \
    --clickhouse-dsn 'clickhouse://sth-user:not-a-real-secret@127.0.0.1:9000/sth' \
    --clickhouse-table ledger_events

# Non-default format/batching/event-types on one sink so those fields are not
# all validated at their zero value.
ctl "add nats sink" events add-sink --name sth-nats \
    --nats-url nats://127.0.0.1:4222 --nats-topic ledger.events \
    --event-types COMMITTED_TRANSACTION,REVERTED_TRANSACTION \
    --format protobuf --batch-size 128 --batch-delay-ms 50

ctl "add databricks sink (PAT)" events add-sink --name sth-databricks-pat \
    --databricks-host adb-000000.example.invalid \
    --databricks-http-path /sql/1.0/warehouses/sth \
    --databricks-token not-a-real-secret \
    --databricks-catalog main --databricks-schema default

ctl "add databricks sink (OAuth M2M)" events add-sink --name sth-databricks-oauth \
    --databricks-host adb-000001.example.invalid \
    --databricks-http-path /sql/1.0/warehouses/sth2 \
    --databricks-client-id sth-client --databricks-client-secret not-a-real-secret \
    --databricks-catalog main --databricks-schema default

# SinkStatus.error / the SinkError schema under it are only populated once a
# publish attempt has failed, so the HTTP sink points at the server's own
# GET-only /readyz: every POST gets a 405. The failure is bounded by design —
# sink_failure_state.go backs off exponentially to a 60s cap and re-proposes an
# identical message at most every 5min — so a permanently broken sink neither
# spins nor floods Raft. Poll for the recorded error instead of sleeping, the
# same bounded-condition pattern as the /readyz wait above.
echo "==> Waiting for the HTTP sink to record a failure..."
for i in $(seq 1 30); do
    if curl -sf "$API/_/events-sinks" | grep -q '"error"'; then
        echo "    Sink error recorded."
        break
    fi
    if [ "$i" -eq 30 ]; then
        echo "ERROR: no sink error recorded within 30s — SinkError stays unvalidated" >&2
        echo "Server log:" >&2
        cat "$TMPDIR/server.log" >&2
        exit 1
    fi
    sleep 1
done

# Regression assertion for the point of EN-1791: the five secret-bearing sink
# fields (Kafka saslPassword, HTTP secret, ClickHouse dsn, Databricks token,
# OAuth clientSecret) have no DTO field, so no credential can reach the wire.
# Schema conformance cannot catch a leak — an extra property is allowed by every
# schema above — so assert it directly.
echo "==> Asserting no seeded sink credential reaches the wire..."
SINKS_BODY=$(curl -sf "$API/_/events-sinks")
case "$SINKS_BODY" in
    *not-a-real-secret*)
        echo "ERROR: a seeded sink credential leaked into GET /v3/_/events-sinks" >&2
        echo "$SINKS_BODY" >&2
        exit 1
        ;;
    *)
        echo "    No credential in the response."
        ;;
esac

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
#
# The status is captured rather than allowed to abort the script: the fixtures
# must be re-checked below on the success path, and on the failure path the
# original exit code still has to be the one this script returns.
set +e
python3 "$SCRIPT_DIR/test_api.py" \
    --base-url "http://localhost:$HTTP_PORT" \
    --max-examples "$MAX_EXAMPLES" \
    --workers "$SCHEMATHESIS_WORKERS" \
    $SHRINK_FLAG 2>&1 | tee /tmp/schemathesis-report.txt
FUZZ_STATUS=$?
set -e

# Non-vacuity check. Everything above verifies the fixtures BEFORE the fuzz run;
# nothing verified they were still there afterwards, and test_api.py exits on
# has_failures/has_errors alone - it never asserts that any route was observed
# returning 200. So the whole gate rested on before_call's coercion being
# complete, and when it was not (twice on this branch: see the comments in
# test_api.py) a fuzzed DELETE destroyed a fixture, every later request
# validated against an error schema, and the suite still reported green.
#
# Re-assert the fixture index positively and fail hard. This is the one check
# that cannot pass vacuously: it demands a 200 with the ACCOUNT target present,
# which is exactly the field the original EN-1791 defect dropped.
echo "==> Re-asserting the fixture index survived the fuzz run..."
FIXTURE_AFTER=$(curl -s -w '\n%{http_code}' \
    "$API/test-ledger/indexes/metadata:TARGET_TYPE_ACCOUNT:color")
FIXTURE_CODE=$(printf '%s' "$FIXTURE_AFTER" | tail -n1)
FIXTURE_BODY=$(printf '%s' "$FIXTURE_AFTER" | sed '$d')

if [ "$FIXTURE_CODE" != "200" ]; then
    echo "ERROR: fixture index is gone after the fuzz run (HTTP $FIXTURE_CODE)." >&2
    echo "       A fuzzed destructive route de-seeded the suite, so every later" >&2
    echo "       request validated against an error schema and the conformance" >&2
    echo "       checks above are vacuous. Extend before_call in test_api.py to" >&2
    echo "       coerce that route away from the fixtures." >&2
    echo "Body: $FIXTURE_BODY" >&2
    exit 1
fi

if ! printf '%s' "$FIXTURE_BODY" | grep -q '"TARGET_TYPE_ACCOUNT"'; then
    echo "ERROR: fixture index responded 200 but without its TARGET_TYPE_ACCOUNT" >&2
    echo "       target - the exact field the EN-1791 defect dropped." >&2
    echo "Body: $FIXTURE_BODY" >&2
    exit 1
fi

echo "    Fixture index intact."

exit "$FUZZ_STATUS"
