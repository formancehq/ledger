#!/usr/bin/env bash
# Invoked by Chainsaw in its own namespace. Each Pod has a stable DNS name and
# dedicated PVCs; deleting a Pod never deletes the rc.1-created data.
set -euo pipefail

namespace="$1"
: "${UPGRADE_KUBECONFIG:?}" "${UPGRADE_KUBE_CONTEXT:?}" "${UPGRADE_SOURCE_IMAGE:?}" "${UPGRADE_TARGET_IMAGE:?}"
phase_seconds="${UPGRADE_PHASE_SECONDS:-15}"
recover_seconds="${UPGRADE_RECOVER_SECONDS:-120}"
case "$phase_seconds:$recover_seconds" in *[!0-9:]*|:*|*:) echo "invalid upgrade timeout" >&2; exit 2 ;; esac
[ "$phase_seconds" -gt 0 ] && [ "$recover_seconds" -gt 0 ] || exit 2

k() { kubectl --kubeconfig "$UPGRADE_KUBECONFIG" --context "$UPGRADE_KUBE_CONTEXT" -n "$namespace" "$@"; }
pod_name() { printf 'ledger-upgrade-%s' "$1"; }
cli() {
	local i="$1"; shift
	k exec "$(pod_name "$i")" -c ledger -- /app/ledgerctl "$@" \
		--insecure --server 127.0.0.1:8888
}
status() {
	local id="$1" i output
	for i in 0 1 2; do
		output="$(cli "$i" cluster status --node-id "$id" --json --timeout 3s 2>/dev/null)" || continue
		jq -e '.raftStatus != null' <<<"$output" >/dev/null 2>&1 || continue
		printf '%s\n' "$output"
		return 0
	done
	return 1
}
cluster_health() {
	local output
	output="$(status 0)" || { echo '0 0'; return; }
	jq -r '"\(.leader // "0") \([.nodes[]? | select(.suffrage == "Voter")] | length)"' <<<"$output"
}
wait_consistent() {
	local maintenance="$1" deadline leader voters horizon state i ready
	deadline=$(( $(date +%s) + recover_seconds ))
	while [ "$(date +%s)" -lt "$deadline" ]; do
		read -r leader voters < <(cluster_health)
		if [ "$leader" != 0 ] && [ "$voters" -eq 3 ]; then
			state="$(status "$leader")" || { sleep 1; continue; }
			horizon="$(jq -r '.raftStatus.commit // 0' <<<"$state")"
			if [ "$horizon" != 0 ]; then
				ready=1
				for i in 1 2 3; do
					state="$(status "$i")" || { ready=0; break; }
					jq -e --argjson leader "$leader" --argjson node "$i" --argjson horizon "$horizon" --argjson maintenance "$maintenance" \
						'(.leader | tonumber) == $leader and (.localNode | tonumber) == $node and (.raftStatus.state == "Leader" or .raftStatus.state == "Follower") and (.raftStatus.lastPersistedIndex | tonumber) >= $horizon and ((.syncProgress.status // "normal") == "normal") and ((.maintenanceMode // false) == $maintenance)' \
						<<<"$state" >/dev/null || { ready=0; break; }
					k wait --for=condition=Ready "pod/$(pod_name "$((i-1))")" --timeout=3s >/dev/null 2>&1 || { ready=0; break; }
				done
				if [ "$ready" -eq 1 ]; then echo "all voters durable past commit $horizon; leader=$leader maintenance=$maintenance"; return 0; fi
			fi
		fi
		sleep 1
	done
	echo "cluster did not converge with maintenance=$maintenance" >&2
	return 1
}

create_pod() {
	local i="$1" image="$2" mode="$3" name id
	name="$(pod_name "$i")"; id="$((i+1))"
	jq -n --arg name "$name" --arg image "$image" --arg id "$id" --arg mode "$mode" --arg namespace "$namespace" '
	  ["run", "--node-id", $id, "--cluster-id", "chainsaw-rc1-upgrade", "--bind-addr", "0.0.0.0:7777",
	   "--advertise-addr", ($name + ".ledger-upgrade." + $namespace + ".svc.cluster.local:7777"),
	   "--grpc-port", "8888", "--http-port", "8080", "--wal-dir", "/wal", "--data-dir", "/data",
	   "--health-wal-threshold", "0.95", "--health-data-threshold", "0.95"] as $base |
	  {apiVersion:"v1",kind:"Pod",metadata:{name:$name,labels:{app:"ledger-upgrade"}},
	   spec:{hostname:$name,subdomain:"ledger-upgrade",restartPolicy:"Never",
	     containers:[{name:"ledger",image:$image,imagePullPolicy:"Never",command:["/app/ledger-server"],
	       args:($base + (if $mode == "bootstrap" then ["--bootstrap"] elif $mode == "join" then
	         ["--join", ("ledger-upgrade-0.ledger-upgrade." + $namespace + ".svc.cluster.local:7777")] else [] end)),
	       ports:[{name:"raft",containerPort:7777},{name:"grpc",containerPort:8888},{name:"http",containerPort:8080}],
	       readinessProbe:{httpGet:{path:"/readyz",port:8080},periodSeconds:2,failureThreshold:30},
	       volumeMounts:[{name:"wal",mountPath:"/wal"},{name:"data",mountPath:"/data"}]}],
	     volumes:[{name:"wal",persistentVolumeClaim:{claimName:($name + "-wal")}},
	              {name:"data",persistentVolumeClaim:{claimName:($name + "-data")}}]}}' | k apply -f -
	k wait --for=condition=Ready "pod/$name" --timeout=180s
	[ "$(k get pod "$name" -o jsonpath='{.spec.containers[0].image}')" = "$image" ] || return 1
}
replace_pod() {
	local i="$1" name old_uid new_uid
	name="$(pod_name "$i")"
	old_uid="$(k get pod "$name" -o jsonpath='{.metadata.uid}')"
	echo "replacing $name with target image"
	k delete pod "$name" --wait=true --timeout=120s
	create_pod "$i" "$UPGRADE_TARGET_IMAGE" rejoin
	new_uid="$(k get pod "$name" -o jsonpath='{.metadata.uid}')"
	[ -n "$new_uid" ] && [ "$new_uid" != "$old_uid" ] || { echo "pod UID unchanged: $name" >&2; return 1; }
	wait_consistent true
}
account() { cli "$1" accounts get bank --ledger "$ledger" --json | jq -cS '{address,volumes,metadata}'; }
check_data() {
	local expected="$1" i actual
	for i in 0 1 2; do
		actual="$(account "$i")" || return 1
		[ "$actual" = "$expected" ] || { echo "account mismatch on node $((i+1))" >&2; return 1; }
	done
}
check_maintenance() {
	local label="$1" deadline i output
	deadline=$(( $(date +%s) + phase_seconds ))
	echo "testing maintenance window: $label"
	while [ "$(date +%s)" -lt "$deadline" ]; do
		wait_consistent true
		for i in 0 1 2; do
			if output="$(cli "$i" ledgers create "blocked-$label-$i" 2>&1)"; then
				echo "business write succeeded on node $((i+1))" >&2; return 1
			fi
			grep -Eiq 'MAINTENANCE_MODE|maintenance mode' <<<"$output" || { echo "unexpected write error: $output" >&2; return 1; }
		done
		check_data "$before"
	done
}

on_exit() {
	local result="$?" i
	if [ "$result" -ne 0 ]; then
		for i in 0 1 2; do
			k logs "$(pod_name "$i")" -c ledger >"${UPGRADE_EVIDENCE_DIR:-/tmp}/pod-$i.log" 2>&1 || true
		done
	fi
}
trap on_exit EXIT

k apply -f - <<'EOF'
apiVersion: v1
kind: Service
metadata:
  name: ledger-upgrade
spec:
  clusterIP: None
  publishNotReadyAddresses: true
  selector:
    app: ledger-upgrade
  ports:
    - {name: raft, port: 7777}
    - {name: grpc, port: 8888}
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata: {name: ledger-upgrade-0-wal}
spec: {accessModes: [ReadWriteOnce], resources: {requests: {storage: 1Gi}}}
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata: {name: ledger-upgrade-0-data}
spec: {accessModes: [ReadWriteOnce], resources: {requests: {storage: 1Gi}}}
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata: {name: ledger-upgrade-1-wal}
spec: {accessModes: [ReadWriteOnce], resources: {requests: {storage: 1Gi}}}
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata: {name: ledger-upgrade-1-data}
spec: {accessModes: [ReadWriteOnce], resources: {requests: {storage: 1Gi}}}
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata: {name: ledger-upgrade-2-wal}
spec: {accessModes: [ReadWriteOnce], resources: {requests: {storage: 1Gi}}}
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata: {name: ledger-upgrade-2-data}
spec: {accessModes: [ReadWriteOnce], resources: {requests: {storage: 1Gi}}}
EOF

create_pod 0 "$UPGRADE_SOURCE_IMAGE" bootstrap
create_pod 1 "$UPGRADE_SOURCE_IMAGE" join
create_pod 2 "$UPGRADE_SOURCE_IMAGE" join
wait_consistent false
read -r original_leader voters < <(cluster_health)
ledger="upgrade-proof-$(date +%s)"
cli 0 ledgers create "$ledger" --idempotency-key "$ledger-create"
cli 0 transactions create --ledger "$ledger" --posting world,bank,100,USD --reference before-upgrade
wait_consistent false
before="$(account 0)"
jq -e '.address == "bank" and any(.volumes[]; .asset == "USD" and .volumes.balance == "100")' <<<"$before" >/dev/null
check_data "$before"

cli "$((original_leader-1))" cluster maintenance enable
wait_consistent true
check_maintenance baseline
for i in 0 1 2; do
	[ "$((i+1))" -eq "$original_leader" ] && continue
	read -r current_leader _ < <(cluster_health)
	[ "$current_leader" = "$original_leader" ] || { echo "unexpected leader change" >&2; exit 1; }
	replace_pod "$i"
	check_maintenance "follower-$((i+1))"
	transferee="$((i+1))"
done
cli "$((original_leader-1))" cluster transfer-leader "$transferee"
wait_consistent true
read -r current_leader _ < <(cluster_health)
[ "$current_leader" = "$transferee" ] || { echo "leadership transfer failed" >&2; exit 1; }
check_maintenance transfer
replace_pod "$((original_leader-1))"
wait_consistent true
cli "$((transferee-1))" cluster maintenance disable
wait_consistent false
check_data "$before"
cli "$((transferee-1))" transactions create --ledger "$ledger" --posting world,bank,50,USD --reference after-upgrade
wait_consistent false
after="$(account 0)"
jq -e '.address == "bank" and any(.volumes[]; .asset == "USD" and .volumes.balance == "150")' <<<"$after" >/dev/null
check_data "$after"
for i in 0 1 2; do
	echo "checking audit and derived store on node $((i+1))"
	cli "$i" store check
done
wait_consistent false
echo "PASS: rc.1 data, mixed maintenance, leadership transfer, and post-upgrade data verified"
