#!/usr/bin/env bash
#
# restore-orchestrator.sh -- services the model driver's restore rendezvous in
# the Antithesis k8s environment, as a sidecar in the workload pod.
#
# The driver (singleton_driver_model) quiesces itself, writes MODEL_RESTORE_REQ
# on the shared volume, and waits for MODEL_RESTORE_RESP. One cycle here is a
# full disaster-recovery pass through the operator:
#
#   1. incremental backup at the quiesce point (exec ledgerctl in a ledger pod)
#   2. tear the cluster down: delete the Cluster CR and every PVC
#   3. re-create the Cluster in restore mode (1 replica), exec
#      `ledgerctl restore download` + `restore finalize` (RebuildDelta runs
#      inside bootstrap-from-backup on the restored store)
#   4. flip the Cluster back to normal mode: pod 0 self-bootstraps from the
#      restored store (RESTORED marker), the other replicas join fresh and
#      snapshot-install the restored state
#   5. answer ok/err; the driver resumes and its ordinary model checks validate
#      the rebuilt state
#
# The declarative parts (pod restarts, mode switches, membership) converge via
# the operator no matter what faults hit the ledger pods; the imperative execs
# are retried here. This sidecar itself is exempt from fault injection. On any
# failure the normal-mode Cluster spec is re-applied so the driver never
# resumes against a cluster stuck in restore mode.
#
# An initial full backup is taken once at startup (mid-traffic is fine: the
# restore replays the per-cycle incremental delta, cut at the quiesce point, on
# top of whatever checkpoint the full backup carried).
set -uo pipefail

NS="${NAMESPACE:-default}"
CLUSTER="${CLUSTER_NAME:-ledger}"
REPLICAS="${CLUSTER_REPLICAS:-3}"
REQ="${MODEL_RESTORE_REQ:?MODEL_RESTORE_REQ must be set}"
RESP="${MODEL_RESTORE_RESP:?MODEL_RESTORE_RESP must be set}"

STS="ledger-$CLUSTER"
POD_PREFIX="ledger-$CLUSTER"
NORMAL_SPEC=/tmp/cluster-normal.json
RESTORE_SPEC=/tmp/cluster-restore.json

S3_ARGS=(
	--s3-bucket backups --s3-endpoint "http://minio.$NS.svc:9000"
	--s3-region us-east-1 --s3-access-key-id minioadmin --s3-secret-access-key minioadmin
)
BACKUP_ARGS=(--driver s3 "${S3_ARGS[@]}")

log() { echo "[restore-orchestrator] $*"; }

k() { kubectl -n "$NS" "$@"; }

# exec_ledgerctl POD ARGS... -- runs ledgerctl inside a ledger pod with the
# pod-local server address and cluster credentials (the operator-e2e idiom).
exec_ledgerctl() {
	local pod="$1"; shift
	k exec "$pod" -c ledger -- /bin/sh -c \
		"./ledgerctl $* --server 127.0.0.1:8888 --insecure --auth-token \"\$CLUSTER_SECRET\" --timeout 120s"
}

# retry N CMD... -- runs CMD up to N times, 5s apart.
retry() {
	local n="$1" i=1; shift
	while true; do
		"$@" && return 0
		[ "$i" -ge "$n" ] && return 1
		i=$(( i + 1 ))
		sleep 5
	done
}

# backup_exec KIND -- runs a backup command against any live ledger pod (the
# RPC needs a reachable server; retrying across pods covers a dead one).
backup_exec() {
	local kind="$1" i
	for i in $(seq 0 $(( REPLICAS - 1 ))); do
		if exec_ledgerctl "$POD_PREFIX-$i" "$kind" "${BACKUP_ARGS[*]}"; then return 0; fi
		log "$kind via $POD_PREFIX-$i failed; trying next pod"
	done
	return 1
}

wait_ready_replicas() {
	local want="$1" deadline=$(( $(date +%s) + ${2:-600} ))
	while [ "$(date +%s)" -lt "$deadline" ]; do
		[ "$(k get statefulset "$STS" -o jsonpath='{.status.readyReplicas}' 2>/dev/null)" = "$want" ] && return 0
		sleep 5
	done
	return 1
}

wait_pod_running() {
	local pod="$1" deadline=$(( $(date +%s) + ${2:-300} ))
	while [ "$(date +%s)" -lt "$deadline" ]; do
		[ "$(k get pod "$pod" -o jsonpath='{.status.phase}' 2>/dev/null)" = "Running" ] && return 0
		sleep 5
	done
	return 1
}

# capture_specs -- snapshots the live Cluster CR as the normal-mode spec and
# derives the restore-mode variant. Runs once; the CR is deleted and re-created
# from these on every cycle.
capture_specs() {
	k get cluster "$CLUSTER" -o json \
		| jq 'del(.status, .metadata.resourceVersion, .metadata.uid,
		          .metadata.generation, .metadata.creationTimestamp,
		          .metadata.managedFields)' > "$NORMAL_SPEC" || return 1
	jq '.spec.restore = true | .spec.replicas = 1' "$NORMAL_SPEC" > "$RESTORE_SPEC"
}

teardown_cluster() {
	k delete cluster "$CLUSTER" --wait=true --timeout=120s || return 1
	local i
	for i in $(seq 0 $(( REPLICAS - 1 ))); do
		k wait --for=delete "pod/$POD_PREFIX-$i" --timeout=120s 2>/dev/null || true
	done
	for i in $(seq 0 $(( REPLICAS - 1 ))); do
		k delete pvc "wal-$POD_PREFIX-$i" "data-$POD_PREFIX-$i" "cold-cache-$POD_PREFIX-$i" \
			--ignore-not-found --wait=true --timeout=120s || return 1
	done
}

do_one_restore() {
	retry 3 backup_exec "store incremental-backup" || { log "incremental backup failed"; return 1; }

	teardown_cluster || { log "teardown failed"; return 1; }

	k apply -f "$RESTORE_SPEC" || { log "applying restore-mode cluster failed"; return 1; }
	wait_pod_running "$POD_PREFIX-0" || { log "restore-mode pod never ran"; return 1; }

	retry 5 exec_ledgerctl "$POD_PREFIX-0" "restore download" "${S3_ARGS[*]}" \
		|| { log "restore download failed"; return 1; }
	retry 5 exec_ledgerctl "$POD_PREFIX-0" "restore finalize --yes" \
		|| { log "restore finalize failed"; return 1; }

	k apply -f "$NORMAL_SPEC" || { log "applying normal-mode cluster failed"; return 1; }
	# The restore-mode pod does not roll on its own after the spec flip
	# (operator e2e does the same bounce).
	sleep 5
	k delete pod "$POD_PREFIX-0" --force --grace-period=0 2>/dev/null || true
	wait_ready_replicas "$REPLICAS" || { log "cluster did not return to $REPLICAS ready replicas"; return 1; }
	log "cluster back up on the restored (RebuildDelta) store"
}

log "waiting for $STS to reach $REPLICAS ready replicas"
wait_ready_replicas "$REPLICAS" 1800 || log "WARNING: cluster not ready at startup; continuing"

retry 10 capture_specs || { log "FATAL: cannot capture cluster spec"; exit 1; }

log "taking initial full backup"
retry 5 backup_exec "store backup" || log "WARNING: initial full backup failed; first cycle will retry"

log "armed; watching $REQ"
while true; do
	if [ ! -f "$REQ" ]; then sleep 1; continue; fi
	rm -f "$REQ" "$RESP"
	log "restore cycle requested"
	if do_one_restore; then
		printf 'ok\n' > "$RESP"
	else
		# Converge back to normal mode so the driver never resumes against a
		# cluster parked in restore mode; the driver logs the err and continues.
		k apply -f "$NORMAL_SPEC" 2>/dev/null || true
		printf 'err\n' > "$RESP"
	fi
done
