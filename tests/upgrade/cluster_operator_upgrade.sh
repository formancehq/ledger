#!/usr/bin/env bash
# Chainsaw owns the namespace. The operator owns the StatefulSet and PVCs.
set -euo pipefail

namespace="$1"
: "${UPGRADE_KUBECONFIG:?}" "${UPGRADE_KUBE_CONTEXT:?}" \
  "${UPGRADE_SOURCE_REPOSITORY:?}" "${UPGRADE_SOURCE_TAG:?}" \
  "${UPGRADE_TARGET_REPOSITORY:?}" "${UPGRADE_TARGET_TAG:?}" \
  "${UPGRADE_TARGET_IMAGE_ID:?}"
source_image="$UPGRADE_SOURCE_REPOSITORY:$UPGRADE_SOURCE_TAG"
target_image="$UPGRADE_TARGET_REPOSITORY:$UPGRADE_TARGET_TAG"
cluster=upgrade-cluster
statefulset=ledger-upgrade-cluster

k() { kubectl --kubeconfig "$UPGRADE_KUBECONFIG" --context "$UPGRADE_KUBE_CONTEXT" -n "$namespace" "$@"; }
pod() { printf '%s-%s' "$statefulset" "$1"; }
cli() {
  local ordinal="$1"; shift
  k exec "$(pod "$ordinal")" -c ledger -- /bin/sh -c \
    'exec /app/ledgerctl "$@" --server 127.0.0.1:8888 --insecure --auth-token "$CLUSTER_SECRET"' sh "$@"
}
status() {
  local node="$1" ordinal output
  for ordinal in 0 1 2; do
    output="$(cli "$ordinal" cluster status --node-id "$node" --json --timeout 3s 2>/dev/null)" || continue
    jq -e '.raftStatus != null' <<<"$output" >/dev/null 2>&1 || continue
    printf '%s\n' "$output"
    return 0
  done
  return 1
}
health() {
  local output
  output="$(status 0)" || { echo '0 0'; return; }
  jq -r '"\(.leader // "0") \([.nodes[]? | select(.suffrage == "Voter")] | length)"' <<<"$output"
}
wait_consistent() {
  local maintenance="$1" deadline leader voters state horizon ordinal ready
  deadline=$(( $(date +%s) + 180 ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    read -r leader voters < <(health)
    if [ "$leader" != 0 ] && [ "$voters" -eq 3 ]; then
      state="$(status "$leader")" || { sleep 2; continue; }
      horizon="$(jq -r '.raftStatus.commit // 0' <<<"$state")"
      if [ "$horizon" != 0 ]; then
        ready=1
        for ordinal in 0 1 2; do
          state="$(status "$((ordinal+1))")" || { ready=0; break; }
          jq -e --argjson node "$((ordinal+1))" --argjson leader "$leader" \
            --argjson horizon "$horizon" --argjson maintenance "$maintenance" \
            '(.localNode | tonumber) == $node and (.leader | tonumber) == $leader and
             (.raftStatus.state == "Leader" or .raftStatus.state == "Follower") and
             (.raftStatus.lastPersistedIndex | tonumber) >= $horizon and
             ((.syncProgress.status // "normal") == "normal") and
             ((.maintenanceMode // false) == $maintenance)' <<<"$state" >/dev/null || { ready=0; break; }
          k wait --for=condition=Ready "pod/$(pod "$ordinal")" --timeout=3s >/dev/null 2>&1 || { ready=0; break; }
        done
        if [ "$ready" -eq 1 ]; then
          echo "all voters durable past commit $horizon; leader=$leader maintenance=$maintenance"
          return 0
        fi
      fi
    fi
    sleep 2
  done
  echo "cluster did not converge with maintenance=$maintenance" >&2
  return 1
}
account() { cli "$1" accounts get bank --ledger "$ledger" --json | jq -cS '{address,volumes,metadata}'; }
check_data() {
  local expected="$1" ordinal actual
  for ordinal in 0 1 2; do
    actual="$(account "$ordinal")" || return 1
    [ "$actual" = "$expected" ] || { echo "account mismatch on node $((ordinal+1))" >&2; return 1; }
  done
}
check_blocked() {
  local ordinal="$1" label="$2" output
  if output="$(cli "$ordinal" ledgers create "blocked-$label-$ordinal" 2>&1)"; then
    echo "ledger creation succeeded on node $((ordinal+1)) during maintenance" >&2
    exit 1
  fi
  grep -Eiq 'MAINTENANCE_MODE|maintenance mode' <<<"$output" || return 1
  if output="$(cli "$ordinal" transactions create --ledger "$ledger" --posting world,bank,1,USD \
    --reference "blocked-$label-$ordinal" 2>&1)"; then
    echo "transaction succeeded on node $((ordinal+1)) during maintenance" >&2
    exit 1
  fi
  grep -Eiq 'MAINTENANCE_MODE|maintenance mode' <<<"$output"
}
wait_statefulset() {
  local deadline=$(( $(date +%s) + 120 ))
  until k get statefulset "$statefulset" >/dev/null 2>&1; do
    [ "$(date +%s)" -lt "$deadline" ] || { echo 'operator did not create StatefulSet' >&2; return 1; }
    sleep 2
  done
}
pvc_ids() {
  k get pvc -o json | jq -r --arg name "$statefulset" \
    '.items[] | select(.metadata.name | contains($name)) | .metadata.name + " " + .metadata.uid' | sort
}

jq -n --arg name "$cluster" --arg repository "$UPGRADE_SOURCE_REPOSITORY" --arg tag "$UPGRADE_SOURCE_TAG" '
  {apiVersion:"ledger.formance.com/v1alpha1",kind:"Cluster",metadata:{name:$name},
   spec:{replicas:3,image:{repository:$repository,tag:$tag,pullPolicy:"Never"}}}' | k apply -f -
wait_statefulset
k rollout status "statefulset/$statefulset" --timeout=10m
wait_consistent false
for ordinal in 0 1 2; do
  k wait --for=condition=Ready "pod/$(pod "$ordinal")" --timeout=60s
done
before_pods="$(k get pods -o json | jq -r --arg name "$statefulset" \
  '.items[] | select(.metadata.name | startswith($name + "-")) | .metadata.name + " " + .metadata.uid' | sort)"
[ "$(wc -l <<<"$before_pods" | tr -d ' ')" -eq 3 ] || { echo 'missing source Pods' >&2; exit 1; }
before_pvcs="$(pvc_ids)"
[ "$(wc -l <<<"$before_pvcs" | tr -d ' ')" -ge 3 ] || { echo 'missing source PVCs' >&2; exit 1; }

ledger="upgrade-proof-$(date +%s)"
cli 0 ledgers create "$ledger" --idempotency-key "$ledger-create"
cli 0 transactions create --ledger "$ledger" --posting world,bank,100,USD --reference before-upgrade
wait_consistent false
before="$(account 0)"
jq -e '.address == "bank" and any(.volumes[]; .asset == "USD" and .volumes.balance == "100")' <<<"$before" >/dev/null
check_data "$before"
read -r leader _ < <(health)
cli "$((leader-1))" cluster maintenance enable
wait_consistent true
for ordinal in 0 1 2; do check_blocked "$ordinal" baseline; done

patch="$(jq -n --arg repository "$UPGRADE_TARGET_REPOSITORY" --arg tag "$UPGRADE_TARGET_TAG" \
  '{spec:{image:{repository:$repository,tag:$tag,pullPolicy:"Never"}}}')"
k patch cluster "$cluster" --type=merge -p "$patch"
echo "requested operator rollout $source_image -> $target_image"

deadline=$(( $(date +%s) + 600 ))
mixed_seen=0
mixed_gate=0
while [ "$(date +%s)" -lt "$deadline" ]; do
  pods="$(k get pods -o json)"
  counts="$(jq -r --arg prefix "$statefulset-" --arg source "$source_image" --arg target "$target_image" '
    [.items[] | select(.metadata.name | startswith($prefix)) |
      {image:([.spec.containers[] | select(.name == "ledger") | .image][0]),
       ready:([.status.conditions[]? | select(.type == "Ready") | .status][0] == "True"),
       ordinal:(.metadata.name | split("-")[-1] | tonumber)}] as $pods |
    "\($pods | length) \([$pods[] | select(.image == $source)] | length) \([$pods[] | select(.image == $target)] | length) \([$pods[] | select(.image == $source and .ready)] | length) \([$pods[] | select(.image == $target and .ready)] | length)"' <<<"$pods")"
  read -r total old new old_ready new_ready <<<"$counts"
  if [ "$old" -gt 0 ] && [ "$new" -gt 0 ]; then mixed_seen=1; fi
  if [ "$old_ready" -gt 0 ] && [ "$new_ready" -gt 0 ]; then
    # A rejected write on both binary versions proves the shared gate is live.
    old_node="$(jq -r --arg prefix "$statefulset-" --arg source "$source_image" \
      '.items[] | select((.metadata.name | startswith($prefix)) and
       ([.spec.containers[] | select(.name == "ledger") | .image][0] == $source) and
       ([.status.conditions[]? | select(.type == "Ready") | .status][0] == "True")) |
       .metadata.name | split("-")[-1]' <<<"$pods" | head -1)"
    new_node="$(jq -r --arg prefix "$statefulset-" --arg target "$target_image" \
      '.items[] | select((.metadata.name | startswith($prefix)) and
       ([.spec.containers[] | select(.name == "ledger") | .image][0] == $target) and
       ([.status.conditions[]? | select(.type == "Ready") | .status][0] == "True")) |
       .metadata.name | split("-")[-1]' <<<"$pods" | head -1)"
    if check_blocked "$old_node" mixed-old && check_blocked "$new_node" mixed-new; then mixed_gate=1; fi
  fi
  if [ "$total" -eq 3 ] && [ "$old" -eq 0 ] && [ "$new" -eq 3 ] && [ "$new_ready" -eq 3 ]; then break; fi
  sleep 2
done
[ "$mixed_seen" -eq 1 ] && [ "$mixed_gate" -eq 1 ] || { echo 'mixed-version maintenance window not observed' >&2; exit 1; }
[ "$total" -eq 3 ] && [ "$new" -eq 3 ] && [ "$new_ready" -eq 3 ] || { echo 'operator rollout did not converge' >&2; exit 1; }
k rollout status "statefulset/$statefulset" --timeout=120s
wait_consistent true
check_data "$before"
for ordinal in 0 1 2; do
  check_blocked "$ordinal" complete
  pod_json="$(k get pod "$(pod "$ordinal")" -o json)"
  jq -e --arg image "$target_image" --arg id "$UPGRADE_TARGET_IMAGE_ID" '
    ([.spec.containers[] | select(.name == "ledger") | .image][0] == $image) and
    ([.status.containerStatuses[] | select(.name == "ledger") | .imageID][0] | contains($id))' \
    <<<"$pod_json" >/dev/null
  old_uid="$(awk -v name="$(pod "$ordinal")" '$1 == name {print $2}' <<<"$before_pods")"
  new_uid="$(jq -r '.metadata.uid' <<<"$pod_json")"
  [ -n "$old_uid" ] && [ "$new_uid" != "$old_uid" ] || { echo "Pod $ordinal was not replaced" >&2; exit 1; }
done
[ "$(pvc_ids)" = "$before_pvcs" ] || { echo 'PVC identity changed during rollout' >&2; exit 1; }

read -r leader _ < <(health)
cli "$((leader-1))" cluster maintenance disable
wait_consistent false
cli 0 transactions create --ledger "$ledger" --posting world,bank,50,USD --reference after-upgrade
wait_consistent false
after="$(account 0)"
jq -e '.address == "bank" and any(.volumes[]; .asset == "USD" and .volumes.balance == "150")' <<<"$after" >/dev/null
check_data "$after"
for ordinal in 0 1 2; do
  echo "checking audit and derived store on node $((ordinal+1))"
  cli "$ordinal" store check
done
echo 'PASS: operator rollout, immutable image ID, retained PVCs, and committed data verified'
