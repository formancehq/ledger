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
# Bounded by coreutils timeout rather than ledgerctl's --timeout: not every
# subcommand defines that flag (restore download does not), and a uniform
# client-side bound also covers a hung exec stream.
exec_ledgerctl() {
	local pod="$1"; shift
	k exec "$pod" -c ledger -- /bin/sh -c \
		"timeout 300 ./ledgerctl $* --server 127.0.0.1:8888 --insecure --auth-token \"\$CLUSTER_SECRET\""
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
# RPC needs a reachable server; retrying across pods covers a dead one). The
# command's --json output is left in $BACKUP_OUT for callers that inspect it.
BACKUP_OUT=/tmp/last-backup.json
backup_exec() {
	local kind="$1" i
	for i in $(seq 0 $(( REPLICAS - 1 ))); do
		if exec_ledgerctl "$POD_PREFIX-$i" "$kind" "${BACKUP_ARGS[*]}" --json \
			> "$BACKUP_OUT" 2> "$BACKUP_OUT.err"; then
			return 0
		fi
		log "$kind via $POD_PREFIX-$i failed; trying next pod: $(tail -c 300 "$BACKUP_OUT.err" 2>/dev/null)"
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

# teardown_cluster is idempotent and succeeds only once the CR, pods, and
# volume claims are verifiably gone — callers retry it until then. A lingering
# claim would hand the re-created pod its old store back, silently voiding the
# RebuildDelta coverage the cycle claims.
teardown_cluster() {
	k delete cluster "$CLUSTER" --ignore-not-found --wait=true --timeout=120s || return 1
	local i
	for i in $(seq 0 $(( REPLICAS - 1 ))); do
		k wait --for=delete "pod/$POD_PREFIX-$i" --timeout=120s 2>/dev/null || true
	done
	for i in $(seq 0 $(( REPLICAS - 1 ))); do
		k delete pvc "wal-$POD_PREFIX-$i" "data-$POD_PREFIX-$i" "cold-cache-$POD_PREFIX-$i" \
			--ignore-not-found --wait=true --timeout=120s || return 1
	done
	for i in $(seq 0 $(( REPLICAS - 1 ))); do
		[ -z "$(k get pvc "wal-$POD_PREFIX-$i" "data-$POD_PREFIX-$i" "cold-cache-$POD_PREFIX-$i" \
			--ignore-not-found -o name 2>/dev/null)" ] || return 1
	done
}

# The exec's response can be lost mid-step (network partition, bounded client)
# while the server-side operation completed, so the download/finalize retries
# must recognise the FailedPrecondition a repeat then hits as prior success:
# a repeated download reports "already downloaded"; a repeated finalize
# reports "no backup downloaded" because finalize consumed the staging — and
# the download step has already proven the staging existed.
download_step() {
	local out
	out=$(exec_ledgerctl "$POD_PREFIX-0" "restore download" "${S3_ARGS[*]}" 2>&1) && return 0
	case "$out" in *"already downloaded"*) return 0 ;; esac
	log "download attempt failed: $(printf '%s' "$out" | tail -c 200)"
	return 1
}

finalize_step() {
	local out
	out=$(exec_ledgerctl "$POD_PREFIX-0" "restore finalize --yes" 2>&1) && return 0
	case "$out" in *"no backup downloaded"*) return 0 ;; esac
	log "finalize attempt failed: $(printf '%s' "$out" | tail -c 200)"
	return 1
}

# restore_from_backup -- one attempt at the post-teardown choreography:
# restore-mode cluster, download + finalize, flip back to the full cluster.
restore_from_backup() {
	k apply -f "$RESTORE_SPEC" || { log "applying restore-mode cluster failed"; return 1; }
	wait_pod_running "$POD_PREFIX-0" || { log "restore-mode pod never ran"; return 1; }

	retry 5 download_step || { log "restore download failed"; return 1; }
	retry 5 finalize_step || { log "restore finalize failed"; return 1; }

	k apply -f "$NORMAL_SPEC" || { log "applying normal-mode cluster failed"; return 1; }
	# The restore-mode pod does not roll on its own after the spec flip, so it
	# is bounced here. Graceful delete only: it keeps the pod object (and the
	# StatefulSet from creating the replacement) until the old container has
	# actually exited — a force delete drops the object immediately while the
	# kubelet is still killing the container, and the replacement then boots
	# into the old process's live Pebble lock ("resource temporarily
	# unavailable", exit 1) and gets flagged as an unexpected container exit.
	sleep 5
	k delete pod "$POD_PREFIX-0" --wait=true --timeout=120s 2>/dev/null || true
	wait_ready_replicas "$REPLICAS" || { log "cluster did not return to $REPLICAS ready replicas"; return 1; }
}

# do_one_restore has two phases with different failure semantics. Before the
# teardown a failure leaves the running cluster untouched, so the cycle just
# reports err and the driver carries on. From the teardown onward the driver's
# committed state exists ONLY in the backup — releasing the driver against a
# fresh empty cluster would make every model check diverge — so the restore is
# retried until it lands, however long that takes (the run's duration bounds
# it; the driver's lease expiry just parks it on transient retries meanwhile).
do_one_restore() {
	# Incrementals build on the initial full backup; if that never succeeded
	# (or a cycle-time check finds it missing), (re)take it here — the driver
	# is quiesced, so a cycle-time full backup is as valid a base as any.
	if [ "$FULL_BACKUP_OK" != 1 ]; then
		retry 3 backup_exec "store backup" || { log "full backup (retry) failed"; return 1; }
		FULL_BACKUP_OK=1
	fi

	retry 3 backup_exec "store incremental-backup" || { log "incremental backup failed"; return 1; }

	# RebuildDelta only runs when the manifest carries exports; a cycle whose
	# incremental exported nothing (e.g. taken right after a cycle-time full
	# backup) would restore without exercising the replay. Fail it rather than
	# record hollow coverage — the local runner has the same guard.
	local exports
	exports=$(jq -r '(.segmentsUploaded // 0) + (.logEntriesExported // 0)' "$BACKUP_OUT" 2>/dev/null)
	case "$exports" in
	""|0|null)
		log "incremental produced no exports (RebuildDelta would be a no-op); failing cycle"
		return 1
		;;
	esac

	# Point of no return: the quiesce-point backup above is verified, so the
	# data survives whatever happens to the cluster from here. The restore is
	# only entered on a verified-complete teardown — booting the restore-mode
	# pod with any old storage left would serve stale data as "restored".
	local attempt=1
	until teardown_cluster; do
		attempt=$(( attempt + 1 ))
		log "teardown incomplete; retrying until the old store is verifiably gone (attempt $attempt)"
		sleep 10
	done

	attempt=1
	until restore_from_backup; do
		attempt=$(( attempt + 1 ))
		log "restore attempt failed; the data now lives only in the backup — retrying (attempt $attempt)"
		# Clear whatever half-state the attempt left so the next one starts clean.
		until teardown_cluster; do sleep 10; done
		sleep 10
	done
	log "cluster back up on the restored (RebuildDelta) store"
}

log "waiting for $STS to reach $REPLICAS ready replicas"
wait_ready_replicas "$REPLICAS" 1800 || log "WARNING: cluster not ready at startup; continuing"

retry 10 capture_specs || { log "FATAL: cannot capture cluster spec"; exit 1; }

FULL_BACKUP_OK=0
log "taking initial full backup"
if retry 5 backup_exec "store backup"; then
	FULL_BACKUP_OK=1
else
	log "WARNING: initial full backup failed; the next cycle retakes it before its incremental"
fi

log "armed; watching $REQ"
while true; do
	# Atomic claim: the driver withdraws an expired request with a rename of
	# its own, so exactly one side wins — a request the driver has withdrawn
	# (it is no longer quiesced) can never start a cycle here.
	if ! mv "$REQ" "$REQ.claimed" 2>/dev/null; then sleep 1; continue; fi
	rm -f "$REQ.claimed" "$RESP"
	log "restore cycle requested"
	if do_one_restore; then
		printf 'ok\n' > "$RESP"
	else
		# Only pre-teardown steps report err (backup / exports guard), and
		# those leave the running cluster untouched — the driver just logs
		# the err and continues against it. Post-teardown failures never
		# reach here: do_one_restore retries the restore until it lands.
		printf 'err\n' > "$RESP"
	fi
done
