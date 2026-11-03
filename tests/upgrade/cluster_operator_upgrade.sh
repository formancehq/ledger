#!/usr/bin/env bash
# Chainsaw owns the namespace. The operator owns the StatefulSet and PVCs.
set -euo pipefail

namespace="$1"
: "${UPGRADE_KUBECONFIG:?}" "${UPGRADE_KUBE_CONTEXT:?}" \
  "${UPGRADE_TARGET_OPERATOR_CHART:?}" "${UPGRADE_TARGET_OPERATOR_IMAGE:?}" \
  "${UPGRADE_TARGET_OPERATOR_IMAGE_ID:?}" "${UPGRADE_SOURCE_OPERATOR_IMAGE:?}" \
  "${UPGRADE_SOURCE_OPERATOR_IMAGE_ID:?}" \
  "${UPGRADE_SOURCE_REPOSITORY:?}" "${UPGRADE_SOURCE_TAG:?}" \
  "${UPGRADE_TARGET_REPOSITORY:?}" "${UPGRADE_TARGET_TAG:?}" \
  "${UPGRADE_TARGET_IMAGE_ID:?}" "${UPGRADE_EVIDENCE_DIR:?}" \
  "${UPGRADE_ROLLOUT:?}" "${UPGRADE_REJECTED_TAG:?}"
source_image="$UPGRADE_SOURCE_REPOSITORY:$UPGRADE_SOURCE_TAG"
target_image="$UPGRADE_TARGET_REPOSITORY:$UPGRADE_TARGET_TAG"
cluster=upgrade-cluster
statefulset=ledger-upgrade-cluster

k() { kubectl --kubeconfig "$UPGRADE_KUBECONFIG" --context "$UPGRADE_KUBE_CONTEXT" -n "$namespace" "$@"; }
operator_pod() {
  kubectl --kubeconfig "$UPGRADE_KUBECONFIG" --context "$UPGRADE_KUBE_CONTEXT" \
    -n ledger-system get pods -l app.kubernetes.io/instance=ledger-operator -o json
}
check_operator() {
  local image="$1" id="$2" output
  output="$(operator_pod)" || return 1
  jq -e --arg image "$image" --arg id "$id" '
    (.items | length) == 1 and
    (.items[0].spec.containers | any(.name == "operator" and .image == $image)) and
    (.items[0].status.containerStatuses | any(.name == "operator" and (.imageID | contains($id)) and .ready))
  ' <<<"$output" >/dev/null || return 1
  jq -r '.items[0].metadata.uid' <<<"$output"
}
wait_operator() {
  local image="$1" id="$2" deadline uid
  deadline=$(( $(date +%s) + 60 ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    if uid="$(check_operator "$image" "$id")"; then
      printf '%s\n' "$uid"
      return 0
    fi
    sleep 2
  done
  echo "operator Pod did not converge to $image" >&2
  return 1
}
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
  local deadline leader voters state horizon ordinal ready
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
            --argjson horizon "$horizon" \
            '(.localNode | tonumber) == $node and (.leader | tonumber) == $leader and
             (.raftStatus.state == "Leader" or .raftStatus.state == "Follower") and
             (.raftStatus.lastPersistedIndex | tonumber) >= $horizon and
             ((.syncProgress.status // "normal") == "normal") and
             ((.maintenanceMode // false) == false)' <<<"$state" >/dev/null || { ready=0; break; }
          k wait --for=condition=Ready "pod/$(pod "$ordinal")" --timeout=3s >/dev/null 2>&1 || { ready=0; break; }
        done
        if [ "$ready" -eq 1 ]; then
          echo "all voters durable past commit $horizon; leader=$leader"
          return 0
        fi
      fi
    fi
    sleep 2
  done
  echo 'cluster did not converge with maintenance disabled' >&2
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
record_raft() {
  local phase="$1" ordinal
  for ordinal in 0 1 2; do
    status "$((ordinal+1))" | jq -c --arg phase "$phase" --argjson node "$((ordinal+1))" \
      '{phase:$phase,node:$node,leader,raftStatus,syncProgress,maintenanceMode}' \
      >> "$UPGRADE_EVIDENCE_DIR/raft-status.jsonl"
  done
}
committed_index() {
  local state leader
  state="$(status 0)" || return 1
  leader="$(jq -r '.leader' <<<"$state")"
  status "$leader" | jq -r '.raftStatus.commit | tonumber'
}

jq -n --arg name "$cluster" --arg repository "$UPGRADE_SOURCE_REPOSITORY" --arg tag "$UPGRADE_SOURCE_TAG" '
  {apiVersion:"ledger.formance.com/v1alpha1",kind:"Cluster",metadata:{name:$name},
   spec:{replicas:3,image:{repository:$repository,tag:$tag,pullPolicy:"Never"}}}' | k apply -f -
wait_statefulset
k rollout status "statefulset/$statefulset" --timeout=10m
wait_consistent
for ordinal in 0 1 2; do
  k wait --for=condition=Ready "pod/$(pod "$ordinal")" --timeout=60s
done
before_pods="$(k get pods -o json | jq -r --arg name "$statefulset" \
  '.items[] | select(.metadata.name | startswith($name + "-")) | .metadata.name + " " + .metadata.uid' | sort)"
[ "$(wc -l <<<"$before_pods" | tr -d ' ')" -eq 3 ] || { echo 'missing source Pods' >&2; exit 1; }
before_sts_uid="$(k get statefulset "$statefulset" -o json | jq -r '.metadata.uid')"
before_pvcs="$(pvc_ids)"
[ "$(wc -l <<<"$before_pvcs" | tr -d ' ')" -ge 3 ] || { echo 'missing source PVCs' >&2; exit 1; }

ledger="upgrade-proof-$(date +%s)"
cli 0 ledgers create "$ledger" --idempotency-key "$ledger-create"
cli 0 transactions create --ledger "$ledger" --posting world,bank,100,USD --reference before-upgrade
wait_consistent
before="$(account 0)"
jq -e '.address == "bank" and any(.volumes[]; .asset == "USD" and .volumes.balance == "100")' <<<"$before" >/dev/null
check_data "$before"
before_commit="$(committed_index)"
record_raft before

old_operator_uid="$(wait_operator "$UPGRADE_SOURCE_OPERATOR_IMAGE" "$UPGRADE_SOURCE_OPERATOR_IMAGE_ID")"
helm upgrade ledger-operator "$UPGRADE_TARGET_OPERATOR_CHART" --namespace ledger-system \
  --kubeconfig "$UPGRADE_KUBECONFIG" --kube-context "$UPGRADE_KUBE_CONTEXT" \
  --set image.repository=ledger-operator --set "image.tag=${UPGRADE_TARGET_OPERATOR_IMAGE#*:}" \
  --set image.pullPolicy=Never --set leaderElection=false \
  --set pvcProtection.enabled=false --wait --timeout 5m
new_operator_uid="$(wait_operator "$UPGRADE_TARGET_OPERATOR_IMAGE" "$UPGRADE_TARGET_OPERATOR_IMAGE_ID")"
[ "$new_operator_uid" != "$old_operator_uid" ] || { echo 'operator Pod was not replaced' >&2; exit 1; }
echo "operator upgraded before Ledger: $old_operator_uid -> $new_operator_uid"
wait_consistent
check_data "$before"
after_operator_pods="$(k get pods -o json | jq -r --arg name "$statefulset" \
  '.items[] | select(.metadata.name | startswith($name + "-")) | .metadata.name + " " + .metadata.uid' | sort)"
[ "$after_operator_pods" = "$before_pods" ] || { echo 'Ledger Pods changed during operator upgrade' >&2; exit 1; }
[ "$(pvc_ids)" = "$before_pvcs" ] || { echo 'PVC identity changed during operator upgrade' >&2; exit 1; }

# A non-adjacent downgrade stays below the local target operator version, so
# this exercises RC adjacency rather than the independent version gate.
rejected_patch="$(jq -n --arg tag "$UPGRADE_REJECTED_TAG" '{spec:{image:{tag:$tag}}}')"
k patch cluster "$cluster" --type=merge -p "$rejected_patch"
deadline=$(( $(date +%s) + 60 ))
rejected=0
while [ "$(date +%s)" -lt "$deadline" ]; do
  if k get cluster "$cluster" -o json | jq -e '
    .metadata.generation as $generation |
    any(.status.conditions[]?;
      .type == "ConfigValid" and .status == "False" and
      .observedGeneration == $generation and
      (.message | contains("immediately next RC")))' >/dev/null; then
    rejected=1
    break
  fi
  sleep 2
done
[ "$rejected" -eq 1 ] || { echo 'non-adjacent RC was not rejected' >&2; exit 1; }
[ "$(k get statefulset "$statefulset" -o json | jq -r '.spec.template.spec.containers[] | select(.name == "ledger") | .image')" = "$source_image" ] || {
  echo 'rejected RC changed StatefulSet image' >&2; exit 1;
}
[ "$(pvc_ids)" = "$before_pvcs" ] || { echo 'rejected RC changed PVC identity' >&2; exit 1; }
[ "$(k get pods -o json | jq -r --arg name "$statefulset" '.items[] | select(.metadata.name | startswith($name + "-")) | .metadata.name + " " + .metadata.uid' | sort)" = "$before_pods" ] || {
  echo 'rejected RC replaced a Ledger Pod' >&2; exit 1;
}

patch="$(jq -n --arg repository "$UPGRADE_TARGET_REPOSITORY" --arg tag "$UPGRADE_TARGET_TAG" \
  '{spec:{image:{repository:$repository,tag:$tag,pullPolicy:"Never"}}}')"
k patch cluster "$cluster" --type=merge -p "$patch"
echo "requested operator rollout $source_image -> $target_image"

deadline=$(( $(date +%s) + 600 ))
if [ "$UPGRADE_ROLLOUT" = rolling ]; then
  mixed_seen=0
  while [ "$(date +%s)" -lt "$deadline" ]; do
    pods="$(k get pods -o json)"
    counts="$(jq -r --arg prefix "$statefulset-" --arg source "$source_image" --arg target "$target_image" '
      [.items[] | select((.metadata.name | startswith($prefix)) and .metadata.deletionTimestamp == null) |
        {image:([.spec.containers[] | select(.name == "ledger") | .image][0]),
         ready:([.status.conditions[]? | select(.type == "Ready") | .status][0] == "True")}] as $pods |
      "\($pods | length) \([$pods[] | select(.image == $source)] | length) \([$pods[] | select(.image == $target)] | length) \([$pods[] | select(.image == $source and .ready)] | length) \([$pods[] | select(.image == $target and .ready)] | length)"' <<<"$pods")"
    read -r total old new old_ready new_ready <<<"$counts"
    [ "$((old_ready + new_ready))" -ge 2 ] || { echo 'rolling upgrade lost ready quorum' >&2; exit 1; }
    k get cluster "$cluster" -o json | jq -e '.status.upgrade == null' >/dev/null || {
      echo 'rolling RC unexpectedly entered stopped-upgrade state' >&2; exit 1;
    }
    if [ "$old_ready" -gt 0 ] && [ "$new_ready" -gt 0 ]; then mixed_seen=1; fi
    if [ "$total" -eq 3 ] && [ "$old" -eq 0 ] && [ "$new" -eq 3 ] && [ "$new_ready" -eq 3 ]; then break; fi
    sleep 2
  done
  [ "$mixed_seen" -eq 1 ] || { echo 'mixed-version Ledger window not observed' >&2; exit 1; }
else
  stopped_seen=0
  barrier_seen=0
  empty_seen=0
  sts_absent_seen=0
  while [ "$(date +%s)" -lt "$deadline" ]; do
    pods="$(k get pods -o json)"
    counts="$(jq -r --arg prefix "$statefulset-" --arg source "$source_image" --arg target "$target_image" '
      [.items[] | select(.metadata.name | startswith($prefix)) |
        {image:([.spec.containers[] | select(.name == "ledger") | .image][0]),
         ready:([.status.conditions[]? | select(.type == "Ready") | .status][0] == "True")}] as $pods |
      "\($pods | length) \([$pods[] | select(.image == $source)] | length) \([$pods[] | select(.image == $target)] | length) \([$pods[] | select(.image == $target and .ready)] | length)"' <<<"$pods")"
    read -r total old new new_ready <<<"$counts"
    phase="$(k get cluster "$cluster" -o json | jq -r '.status.upgrade.phase // empty')"
    if [ -n "$phase" ]; then stopped_seen=1; fi
    case "$phase" in Retaining|Deleting|Recreating|Verifying|Unblocking) barrier_seen=1 ;; esac
    printf 'phase=%s total=%s old=%s new=%s readyNew=%s\n' "$phase" "$total" "$old" "$new" "$new_ready" >> "$UPGRADE_EVIDENCE_DIR/rollout-observations.txt"
    [ "$old" -eq 0 ] || [ "$new" -eq 0 ] || { echo 'new Pod appeared while an old Pod remained' >&2; exit 1; }
    if [ "$total" -eq 0 ]; then empty_seen=1; fi
    if ! k get statefulset "$statefulset" >/dev/null 2>&1; then sts_absent_seen=1; fi
    if [ "$total" -eq 3 ] && [ "$old" -eq 0 ] && [ "$new" -eq 3 ] && [ "$new_ready" -eq 3 ]; then break; fi
    sleep 1
  done
  [ "$stopped_seen" -eq 1 ] && [ "$barrier_seen" -eq 1 ] && [ "$empty_seen" -eq 1 ] && [ "$sts_absent_seen" -eq 1 ] || {
    echo 'stopped upgrade did not show drained barrier, empty Pods and absent StatefulSet' >&2; exit 1;
  }
fi
[ "$total" -eq 3 ] && [ "$new" -eq 3 ] && [ "$new_ready" -eq 3 ] || { echo 'operator rollout did not converge' >&2; exit 1; }
k rollout status "statefulset/$statefulset" --timeout=120s
if [ "$UPGRADE_ROLLOUT" = stopped ]; then
  deadline=$(( $(date +%s) + 120 ))
  until k get cluster "$cluster" -o json | jq -e '.status.upgrade == null' >/dev/null; do
    [ "$(date +%s)" -lt "$deadline" ] || { echo 'stopped upgrade did not release drain' >&2; exit 1; }
    sleep 2
  done
fi
wait_consistent
check_data "$before"
for ordinal in 0 1 2; do
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
after_sts_uid="$(k get statefulset "$statefulset" -o json | jq -r '.metadata.uid')"
if [ "$UPGRADE_ROLLOUT" = stopped ]; then
  [ "$after_sts_uid" != "$before_sts_uid" ] || { echo 'stopped upgrade did not recreate StatefulSet' >&2; exit 1; }
  replay_pod_uid="$(k get pod "$(pod 0)" -o json | jq -r '.metadata.uid')"
  k delete pod "$(pod 0)" --wait=true
  deadline=$(( $(date +%s) + 180 ))
  until k get pod "$(pod 0)" >/dev/null 2>&1; do
    [ "$(date +%s)" -lt "$deadline" ] || { echo 'target voter was not recreated for replay check' >&2; exit 1; }
    sleep 2
  done
  k wait --for=condition=Ready "pod/$(pod 0)" --timeout=180s
  [ "$(k get pod "$(pod 0)" -o json | jq -r '.metadata.uid')" != "$replay_pod_uid" ] || {
    echo 'target voter did not restart for replay check' >&2; exit 1;
  }
  wait_consistent
  check_data "$before"
else
  [ "$after_sts_uid" = "$before_sts_uid" ] || { echo 'rolling upgrade recreated StatefulSet' >&2; exit 1; }
fi

cli 0 transactions create --ledger "$ledger" --posting world,bank,50,USD --reference after-upgrade
wait_consistent
after="$(account 0)"
jq -e '.address == "bank" and any(.volumes[]; .asset == "USD" and .volumes.balance == "150")' <<<"$after" >/dev/null
check_data "$after"
after_commit="$(committed_index)"
[ "$after_commit" -gt "$before_commit" ] || { echo 'Raft commit did not advance after upgrade' >&2; exit 1; }
record_raft after
for ordinal in 0 1 2; do
  echo "checking audit and derived store on node $((ordinal+1))"
  cli "$ordinal" store check
done
echo "PASS: operator-first $UPGRADE_ROLLOUT upgrade, RC admission, retained PVCs, Raft/audit and committed data verified"
