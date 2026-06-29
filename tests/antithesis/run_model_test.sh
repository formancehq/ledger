#!/usr/bin/env bash
#
# run_model_test.sh -- build and run the singleton_driver_model model-checker
# against a local ledger, then report any findings to stdout.
#
# Topology is selected with --nodes N (default 1):
#   * 1 node  -- a single bootstrapped node. Fast inner-loop check; no fault
#                injection, so transient gRPC errors are rare.
#   * N nodes -- an N-node Raft cluster (bootstrap node 1, join the rest) with a
#                rolling restart: one node at a time is killed and rejoined,
#                keeping quorum, while the driver round-robins across all nodes
#                and retries transients forever. This exercises leadership
#                changes / recovery -- a lost-committed-state or double-apply
#                bug surfaces as a model finding. Each killed node stays down
#                DEAD_TIME seconds with a low snapshot threshold, so it rejoins
#                via a snapshot install (not log replay) -- exercising snapshot
#                transfer + follower restore.
#
# A "finding" is a failed antithesis assertion (a hit Unreachable, or an
# Always/Sometimes whose condition was false), a server/driver panic, or (N>1)
# the cluster failing to recover to N voters after a restart.
#
# Usage:
#   ./run_model_test.sh [--nodes N | --cluster] [DURATION_SECONDS]
#   NODES=3 ./run_model_test.sh 300
#
# Environment:
#   REPO              path to the ledger repo checkout (default: the repo root, two levels up from this script)
#   NODES             node count (default 1); --nodes / --cluster override it
#   GRPC_BASE         base gRPC port; raft = +100, http = +200 per band (default: random)
#   RESTART_INTERVAL  seconds to soak between restarts, N>1 only; 0 disables restarts (default: 15)
#   RECOVER_TIMEOUT   seconds to wait for N-voter recovery after a restart, N>1 only (default: 90)
#   DEAD_TIME             seconds a killed node stays down, N>1 only (default: 30)
#   COMPACTION_MARGIN     raft log entries between snapshots; low forces snapshot recovery (default: 200)
#   MAINTENANCE_INTERVAL  background WAL snapshot + checkpoint cadence (default: 10s)
#   MODEL_LEDGERS     logical ledgers (default: driver default)
#   MODEL_WORKERS     concurrent workers (default: driver default)
#   MODEL_DEBUG       set to enable driver debug logging
#   MODEL_FAIL_FAST   stop on the first finding (default on); 0/off runs the full duration; a substring stops only on a matching one
#   KEEP_WORKDIR      if set, don't delete the temp work dir on exit (for debugging)

set -uo pipefail

# --- Arguments ------------------------------------------------------------
NODES="${NODES:-1}"
DURATION=""
while [ $# -gt 0 ]; do
	case "$1" in
		--cluster)  NODES=3; shift ;;
		--nodes)    NODES="$2"; shift 2 ;;
		--nodes=*)  NODES="${1#*=}"; shift ;;
		-h|--help)  sed -n '2,40p' "$0"; exit 0 ;;
		*)          DURATION="$1"; shift ;;
	esac
done

case "$NODES" in
	''|*[!0-9]*) echo "ERROR: --nodes must be a positive integer (got '$NODES')" >&2; exit 2 ;;
esac
[ "$NODES" -ge 1 ] || { echo "ERROR: --nodes must be >= 1" >&2; exit 2; }

# Single-node iterates fast; a cluster needs longer to see restart cycles.
if [ -z "$DURATION" ]; then
	if [ "$NODES" -gt 1 ]; then DURATION=120; else DURATION=30; fi
fi

RESTART_INTERVAL="${RESTART_INTERVAL:-15}"
RECOVER_TIMEOUT="${RECOVER_TIMEOUT:-90}"
# A killed node stays down DEAD_TIME seconds (N>1). Combined with a low
# COMPACTION_MARGIN and a short MAINTENANCE_INTERVAL, the leader snapshots and
# compacts its Raft log past the dead node's last index during the outage, so
# the node must rejoin via a snapshot install -- exercising the snapshot fetcher
# / follower restore path -- instead of plain log replay. COMPACTION_MARGIN 200
# matches the Antithesis runs.
DEAD_TIME="${DEAD_TIME:-30}"
COMPACTION_MARGIN="${COMPACTION_MARGIN:-200}"
MAINTENANCE_INTERVAL="${MAINTENANCE_INTERVAL:-10s}"
CLUSTER_ID="model-test-cluster"

# Fail-fast (on by default): stop the run the moment a finding appears instead
# of running out the clock. Set to 0/off to run the full duration, or to a
# substring to stop only on a finding whose assertion message matches.
MODEL_FAIL_FAST="${MODEL_FAIL_FAST-1}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# This script lives at tests/antithesis/ inside the repo; default REPO to the
# repo root (two levels up). Override REPO=... to point at another checkout.
REPO="${REPO:-$(cd "$SCRIPT_DIR/../.." && pwd)}"

# Randomized port bands per run: a leaked node/driver from a killed shell can
# never reach a new run's cluster (fixed ports would let an orphan corrupt a
# later run with concurrent traffic). grpc = base+i, raft = base+100+i,
# http = base+200+i.
GRPC_BASE="${GRPC_BASE:-$(( 20000 + RANDOM % 10000 ))}"
RAFT_BASE=$(( GRPC_BASE + 100 ))
HTTP_BASE=$(( GRPC_BASE + 200 ))

WORKDIR="$(mktemp -d /tmp/model-test.XXXXXX)"
DRIVER_LOG="$WORKDIR/driver.log"
ASSERTIONS="$WORKDIR/assertions.json"
SERVER_BIN="$WORKDIR/ledger-server"
DRIVER_BIN="$WORKDIR/model-driver"
LEDGERCTL_BIN="$WORKDIR/ledgerctl"
DRIVER_PID=""
RECOVERY_FAILED=0
DRIVER_EXITED_EARLY=0

GRPC_PORTS=() RAFT_PORTS=() HTTP_PORTS=() NODE_DIRS=() SERVER_LOGS=() SERVER_PIDS=()
for i in $(seq 0 $(( NODES - 1 ))); do
	GRPC_PORTS+=( $(( GRPC_BASE + i )) )
	RAFT_PORTS+=( $(( RAFT_BASE + i )) )
	HTTP_PORTS+=( $(( HTTP_BASE + i )) )
	NODE_DIRS+=( "$WORKDIR/n$i" )
	SERVER_LOGS+=( "$WORKDIR/server-$i.log" )
	SERVER_PIDS+=( "" )
done

log() { echo "[run_model_test] $*"; }

cleanup() {
	# SIGTERM then SIGKILL so a driver/server that ignores or is slow to handle
	# the term signal cannot survive as an orphan polluting later runs.
	[ -n "$DRIVER_PID" ] && kill "$DRIVER_PID" 2>/dev/null
	for pid in "${SERVER_PIDS[@]}"; do [ -n "$pid" ] && kill "$pid" 2>/dev/null; done
	sleep 1
	[ -n "$DRIVER_PID" ] && kill -9 "$DRIVER_PID" 2>/dev/null
	for pid in "${SERVER_PIDS[@]}"; do [ -n "$pid" ] && kill -9 "$pid" 2>/dev/null; done
	wait 2>/dev/null
	if [ -z "${KEEP_WORKDIR:-}" ]; then rm -rf "$WORKDIR"; else log "work dir kept at $WORKDIR"; fi
}
trap cleanup EXIT INT TERM

if [ ! -d "$REPO" ]; then
	echo "ERROR: REPO not found at $REPO (set REPO=...)" >&2
	exit 2
fi

# --- Launch one node ------------------------------------------------------
# mode: bootstrap (new cluster) | join (learner via node 1's Raft transport,
# which serves peer discovery) | rejoin (restart from persisted state -- no
# --bootstrap, no --join).
start_node() {
	local i="$1" mode="$2"
	local flags=()
	case "$mode" in
		bootstrap) flags+=( --bootstrap ) ;;
		join)      flags+=( --join "127.0.0.1:${RAFT_PORTS[0]}" ) ;;
		rejoin)    ;;
	esac
	"$SERVER_BIN" run \
		--node-id "$(( i + 1 ))" \
		--cluster-id "$CLUSTER_ID" \
		--bind-addr "127.0.0.1:${RAFT_PORTS[$i]}" \
		--advertise-addr "127.0.0.1:${RAFT_PORTS[$i]}" \
		--grpc-port "${GRPC_PORTS[$i]}" \
		--http-port "${HTTP_PORTS[$i]}" \
		--wal-dir "${NODE_DIRS[$i]}/wal" \
		--data-dir "${NODE_DIRS[$i]}/data" \
		--raft-compaction-margin "$COMPACTION_MARGIN" \
		--maintenance-interval "$MAINTENANCE_INTERVAL" \
		${flags[@]+"${flags[@]}"} \
		>> "${SERVER_LOGS[$i]}" 2>&1 &
	SERVER_PIDS[$i]=$!
}

# Wait (bounded) for a node to log leadership; fail if it dies first.
wait_leader() {
	local i="$1"
	for _ in $(seq 1 60); do
		if grep -qE "Became leader|became leader at term" "${SERVER_LOGS[$i]}" 2>/dev/null; then return 0; fi
		if ! kill -0 "${SERVER_PIDS[$i]}" 2>/dev/null; then
			echo "ERROR: node $(( i + 1 )) exited during startup:" >&2
			tail -20 "${SERVER_LOGS[$i]}" >&2
			return 1
		fi
		sleep 1
	done
	echo "ERROR: node $(( i + 1 )) did not acquire leadership within 60s:" >&2
	tail -20 "${SERVER_LOGS[$i]}" >&2
	return 1
}

# --- Cluster health (N>1) -------------------------------------------------
# Echoes "<leaderID> <voterCount>" from the first live node (node-id routing
# sends GetClusterState to the leader, so any live node yields the full set).
cluster_health() {
	local i out parsed
	for i in $(seq 0 $(( NODES - 1 ))); do
		[ -n "${SERVER_PIDS[$i]}" ] && kill -0 "${SERVER_PIDS[$i]}" 2>/dev/null || continue
		out="$("$LEDGERCTL_BIN" cluster status --insecure \
			--server "127.0.0.1:${GRPC_PORTS[$i]}" --json --timeout 3s 2>/dev/null)" || continue
		[ -n "$out" ] || continue
		if command -v jq >/dev/null 2>&1; then
			parsed="$(printf '%s' "$out" | jq -r '"\(.leader // 0) \([.nodes[]? | select(.suffrage=="Voter")] | length)"' 2>/dev/null)"
		else
			local leader voters
			leader="$(printf '%s' "$out" | grep -oE '"leader": [0-9]+' | head -1 | grep -oE '[0-9]+$')"
			voters="$(printf '%s' "$out" | grep -c '"suffrage": "Voter"')"
			parsed="${leader:-0} ${voters:-0}"
		fi
		if [ -n "$parsed" ] && [ "$parsed" != "0 0" ]; then printf '%s\n' "$parsed"; return 0; fi
	done
	printf '0 0\n'
}

# Wait until there is a leader and all NODES are voters, bounded by $1 seconds.
wait_healthy() {
	local timeout="${1:-$RECOVER_TIMEOUT}" deadline leader voters
	deadline=$(( $(date +%s) + timeout ))
	while [ "$(date +%s)" -lt "$deadline" ]; do
		read -r leader voters < <(cluster_health)
		if [ "${leader:-0}" != "0" ] && [ "${voters:-0}" -eq "$NODES" ]; then return 0; fi
		sleep 1
	done
	return 1
}

# True (0) when MODEL_FAIL_FAST is set and a finding (condition:false + hit:true,
# optionally matching the MODEL_FAIL_FAST substring) is present in the output.
check_fail_fast() {
	case "$MODEL_FAIL_FAST" in ''|0|off|false|no) return 1 ;; esac
	[ -s "$ASSERTIONS" ] || return 1
	local ff
	ff="$(grep -E '"condition":[[:space:]]*false' "$ASSERTIONS" 2>/dev/null | grep -E '"hit":[[:space:]]*true')"
	[ -n "$ff" ] || return 1
	if [ "$MODEL_FAIL_FAST" != "1" ]; then
		ff="$(printf '%s\n' "$ff" | grep -F "$MODEL_FAIL_FAST")"
	fi
	[ -n "$ff" ]
}

# ---------------------------------------------------------------------------
# Build (server + driver; ledgerctl only when a cluster needs health checks).
# ---------------------------------------------------------------------------
build_cmd="go build -o '$SERVER_BIN' . && "
if [ "$NODES" -gt 1 ]; then
	build_cmd="${build_cmd}go build -o '$LEDGERCTL_BIN' ./cmd/ledgerctl && "
fi
build_cmd="${build_cmd}cd tests/antithesis/workload && go build -o '$DRIVER_BIN' ./bin/cmds/model/singleton_driver_model"

# Build inside the nix dev shell for a reproducible toolchain — unless we are
# already in one (CI runs this as `nix develop --command just test-model`), in
# which case nesting another `nix develop` is redundant.
if [ -n "${IN_NIX_SHELL:-}" ]; then
	log "building (already in nix shell)..."
	build_runner=( bash -c "$build_cmd" )
else
	log "building (via nix develop)..."
	build_runner=( nix develop --command bash -c "$build_cmd" )
fi
if ! ( cd "$REPO" && "${build_runner[@]}" ) > "$WORKDIR/build.log" 2>&1; then
	echo "ERROR: build failed:" >&2
	cat "$WORKDIR/build.log" >&2
	exit 2
fi
log "build ok"

# ---------------------------------------------------------------------------
# Start the cluster: bootstrap node 1, then join the rest (if any).
# ---------------------------------------------------------------------------
log "bootstrapping node 1 (grpc :${GRPC_PORTS[0]})..."
start_node 0 bootstrap
log "waiting for node 1 leadership..."
wait_leader 0 || exit 2

if [ "$NODES" -gt 1 ]; then
	for i in $(seq 1 $(( NODES - 1 ))); do
		log "joining node $(( i + 1 )) (grpc :${GRPC_PORTS[$i]})..."
		start_node "$i" join
	done
	log "waiting for all $NODES nodes to become voters..."
	if ! wait_healthy 60; then
		echo "ERROR: cluster did not reach $NODES voters within 60s:" >&2
		for i in $(seq 0 $(( NODES - 1 ))); do tail -10 "${SERVER_LOGS[$i]}" >&2; done
		exit 2
	fi
	read -r leader voters < <(cluster_health)
	log "cluster ready: leader=$leader voters=$voters"
else
	log "server ready"
fi

# ---------------------------------------------------------------------------
# Run the driver against all node(s).
# ---------------------------------------------------------------------------
ADDR_LIST="127.0.0.1:${GRPC_PORTS[0]}"
for i in $(seq 1 $(( NODES - 1 ))); do ADDR_LIST="$ADDR_LIST,127.0.0.1:${GRPC_PORTS[$i]}"; done

log "running driver for ${DURATION}s against $ADDR_LIST ..."
# MODEL_MAX_SECONDS makes the driver self-terminate even if this script never
# gets to signal it (defence-in-depth against orphaned drivers). A small buffer
# over DURATION lets the script-driven stop win in the normal case.
LEDGER_GRPC_ADDR="$ADDR_LIST" \
ANTITHESIS_SDK_LOCAL_OUTPUT="$ASSERTIONS" \
MODEL_DEBUG="${MODEL_DEBUG:-}" \
MODEL_LEDGERS="${MODEL_LEDGERS:-}" \
MODEL_WORKERS="${MODEL_WORKERS:-}" \
MODEL_MAX_SECONDS="$(( DURATION + 15 ))" \
	"$DRIVER_BIN" > "$DRIVER_LOG" 2>&1 &
DRIVER_PID=$!

# Monitor loop. For N>1, roll a restart every RESTART_INTERVAL (kill one node,
# rejoin, wait for full recovery before touching the next) -- quorum (N-1 of N)
# is preserved throughout. MODEL_FAIL_FAST stops the moment a finding appears.
deadline=$(( $(date +%s) + DURATION ))
restart_idx=0
cycle=0
next_restart=$(( $(date +%s) + RESTART_INTERVAL ))
while [ "$(date +%s)" -lt "$deadline" ]; do
	if ! kill -0 "$DRIVER_PID" 2>/dev/null; then log "driver exited early"; DRIVER_EXITED_EARLY=1; break; fi
	if check_fail_fast; then log "fail-fast: model finding detected, stopping early"; break; fi

	if [ "$NODES" -gt 1 ] && [ "$RESTART_INTERVAL" -gt 0 ] && [ "$(date +%s)" -ge "$next_restart" ]; then
		i=$(( restart_idx % NODES )); restart_idx=$(( restart_idx + 1 )); cycle=$(( cycle + 1 ))
		read -r leader voters < <(cluster_health)
		role="follower"; [ "$leader" = "$(( i + 1 ))" ] && role="LEADER"
		log "cycle $cycle: killing node $(( i + 1 )) ($role); leader=$leader voters=$voters"
		kill -9 "${SERVER_PIDS[$i]}" 2>/dev/null
		wait "${SERVER_PIDS[$i]}" 2>/dev/null
		SERVER_PIDS[$i]=""
		# Stay down long enough for the leader to compact its log past this
		# node's last index (the driver keeps writing on the surviving quorum),
		# forcing a snapshot install on rejoin rather than log replay.
		log "cycle $cycle: node $(( i + 1 )) down for ${DEAD_TIME}s"
		sleep "$DEAD_TIME"
		log "cycle $cycle: restarting node $(( i + 1 ))"
		start_node "$i" rejoin
		if wait_healthy "$RECOVER_TIMEOUT"; then
			read -r leader voters < <(cluster_health)
			log "cycle $cycle: recovered — leader=$leader voters=$voters"
		else
			log "cycle $cycle: CLUSTER DID NOT RECOVER to $NODES voters within ${RECOVER_TIMEOUT}s"
			RECOVERY_FAILED=1
			break
		fi
		next_restart=$(( $(date +%s) + RESTART_INTERVAL ))
		continue
	fi
	sleep 1
done

log "stopping driver..."
kill "$DRIVER_PID" 2>/dev/null
for _ in $(seq 1 5); do kill -0 "$DRIVER_PID" 2>/dev/null || break; sleep 1; done
kill -9 "$DRIVER_PID" 2>/dev/null
wait "$DRIVER_PID" 2>/dev/null
DRIVER_PID=""

# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------
echo
echo "================= model test report ================="
log "topology: $NODES node(s)"
[ "$NODES" -gt 1 ] && log "restart cycles completed: $cycle"

findings=0

# 1. Failed assertions (condition:false AND hit:true) from the driver model. We
# deliberately do NOT exclude "Sometimes" here, unlike a platform run. On the
# antithesis platform an individual Sometimes:false is expected noise (it fails
# only if never true across the whole campaign, and fault injection can
# legitimately make a single attempt fail). singleton_driver_model's only
# Sometimes assertions are the "success-or-transient" idiom from the
# CreateLedger/GetLedger helpers (cond = `err == nil || IsTransient(err)`); with
# transients retried to a definitive outcome that condition must hold on every
# call, so a Sometimes:false here is a genuine non-transient failure worth
# surfacing. (This would over-report a pure reachability Sometimes such as
# `Sometimes(commitIndex > 0)`; this script only runs singleton_driver_model,
# which has none of those.)
if [ -s "$ASSERTIONS" ]; then
	if command -v jq >/dev/null 2>&1; then
		failed="$(jq -c 'select(.antithesis_assert.hit == true and .antithesis_assert.condition == false) | .antithesis_assert | {display_type, message, details}' "$ASSERTIONS" 2>/dev/null)"
	else
		failed="$(grep -E '"hit":true' "$ASSERTIONS" 2>/dev/null | grep -E '"condition":false')"
	fi
	if [ -n "$failed" ]; then
		echo "MODEL FINDINGS (failed assertions):"
		echo "$failed"
		findings=$((findings + $(printf '%s\n' "$failed" | grep -c .)))
	else
		echo "model findings: none"
	fi
else
	echo "WARNING: no assertion output produced at $ASSERTIONS"
	echo "         (driver may not have started -- check driver log below)"
fi

# 2. Driver panic / crash.
if grep -qE "panic:|fatal error:" "$DRIVER_LOG" 2>/dev/null; then
	echo
	echo "DRIVER CRASH:"
	grep -nE "panic:|fatal error:" "$DRIVER_LOG" | head -5
	sed -n '1,40p' "$DRIVER_LOG"
	findings=$((findings + 1))
fi

# 3. Server panic / crash on any node.
for i in $(seq 0 $(( NODES - 1 ))); do
	if grep -qE "panic:|fatal error:|Panicked:" "${SERVER_LOGS[$i]}" 2>/dev/null; then
		echo
		echo "SERVER CRASH (node $(( i + 1 ))):"
		grep -nE "panic:|fatal error:|Panicked:" "${SERVER_LOGS[$i]}" | head -5
		findings=$((findings + 1))
	fi
done

# 4. Cluster failed to recover after a restart (N>1).
if [ "$RECOVERY_FAILED" -ne 0 ]; then
	echo
	echo "CLUSTER RECOVERY FAILURE: did not return to $NODES voters after a restart"
	findings=$((findings + 1))
fi

# 5. Driver exited before the deadline with nothing above explaining it. The
# driver self-terminates only at MODEL_MAX_SECONDS (> DURATION), so an exit
# during the run is abnormal -- typically a setup/connection error that logged
# and returned (NewClient / setupLedgers), leaving no assertion or panic. The
# model did not run for the requested duration, so this is not a pass.
if [ "$DRIVER_EXITED_EARLY" -ne 0 ] && [ "$findings" -eq 0 ]; then
	echo
	echo "DRIVER EXITED EARLY: the model ran for less than the requested ${DURATION}s"
	echo "  (no assertion or crash recorded -- likely a setup/connection error; driver log:)"
	tail -20 "$DRIVER_LOG" 2>/dev/null
	findings=$((findings + 1))
fi

echo "-----------------------------------------------------"
if [ "$findings" -eq 0 ]; then
	echo "RESULT: PASS (no findings)"
	echo "====================================================="
	exit 0
fi
echo "RESULT: FAIL ($findings finding(s))"
echo "  driver log:  $DRIVER_LOG"
echo "  server logs: ${SERVER_LOGS[*]}"
echo "  assertions:  $ASSERTIONS"
echo "  (re-run with KEEP_WORKDIR=1 to preserve these)"
echo "====================================================="
exit 1
