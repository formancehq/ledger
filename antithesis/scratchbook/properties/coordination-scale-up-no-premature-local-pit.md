# coordination-scale-up-no-premature-local-pit — A joining replica cannot serve premature local history

| | |
|---|---|
| **Priority** | P1 |
| **Type** | Safety |
| **Property** | A newly created or reprovisioned replica must not return a successful stale-local PIT result until its primary store and process-local history builder have reconciled to the same authoritative source prefix; any earlier PIT attempt fails closed. |
| **Invariant** | Workload `assert.Always(!success || (view.ledgerID == ledger_id && view.requestedAt == selector.at && view.axis == selector.axis && view.logWatermark >= barrier_log && view.viewToken != "" && oracle_matches_at_view_watermark), "pit: joining replica never serves history before reconciliation", details)`. Send `barrier_log` as `minLogSequence`. The handler independently raises that bound to the receiving replica's current primary log head before opening history, so a successful immutable view is evidence that the process-local builder gate opened and history covered both the workload barrier and that local primary prefix. `Always` is the right semantics: the joining path may complete too quickly to observe a failure, but every success must be safe; learner-to-voter promotion is deliberately not a premise. |
| **Antithesis Angle** | Scale out while writes continue, kill a learner during checkpoint install/backfill, partition only its Raft or service link, and reuse an ordinal after prior scale-down. Probe the new pod directly with stale consistency as soon as its gRPC endpoint accepts connections. |
| **Why It Matters / Impact** | Kubernetes and Ledger health do not use PIT readiness. A pod can enter load balancing while its local projection is empty, behind, or restored from a different local lifecycle. Without the process-local gate, a stale request could return an incomplete historical result rather than an explicit building state. |
| **Confidence** | High. Repository lifecycle ordering, the Kubernetes probe contract, learner promotion, and direct ordinal addressing are all explicit at this revision. |

## Code evidence

- `internal/bootstrap/balance_history.go:27-40` states that PIT building/lag does not affect global health and opens a local peer store in the node data directory.
- `internal/application/balancehistory/builder.go:296-333` sets `ready=false` before starting boot/backfill.
- `internal/application/balancehistory/builder.go:428-434` makes readiness deliberately process-local so every restart must re-prove the source relationship.
- `internal/application/balancehistory/builder.go:465-541` reconciles the persisted manifest, repairs rollback/partial state, drains to a sampled source head, forces durability, and only then marks ready.
- `internal/bootstrap/balance_history_provider.go:59-66` returns `ErrBuilding` while that reconciliation has not completed.
- `internal/application/ctrl/controller_default.go:980-993` raises every PIT request's history bound to `max(request.minLogSequence, local primary log head)`; `internal/application/ctrl/volume_view.go:87-115` waits for that watermark and returns it in the immutable view token.
- `internal/infra/state/synchronizer.go:40-87` installs a leader checkpoint and rehydrates the primary FSM; this coordination lifecycle is separate from the ticker-driven history builder.
- `internal/bootstrap/module.go:926-945,1493-1526,1589-1602` makes learner registration a blocking preflight before the local Raft and API servers start. Therefore an application RPC cannot reach a fresh joining process before its learner membership has committed.
- `internal/infra/node/node.go:940-972` signals local readiness as soon as the Raft tasks are running, explicitly without waiting for FSM catch-up. `internal/adapter/http/server.go:66-84` makes that `Node.IsStarted()` signal the `/readyz` contract.
- `misc/operator/internal/controller/reconcile_statefulset.go:246-252,751-768` uses `OrderedReady` and probes `/readyz`; neither the probe nor `Node.IsStarted()` checks learner suffrage or balance-history readiness.
- `internal/infra/node/node.go:1611-1616,2702-2735` promotes learners asynchronously from the leader only after they are recent-active and within the configured commit-index threshold. There is no ordering edge from that promotion to Kubernetes Ready.
- `internal/bootstrap/module.go:1219-1273,1415-1418` starts the external gRPC and HTTP servers before the last-appended balance-history lifecycle hook; `Builder.Start()` is asynchronous, so external reachability and Kubernetes Ready do not attest `Builder.Ready()`.
- `misc/operator/internal/controller/reconcile_service_headless.go:35-58` publishes headless-service addresses even for not-ready pods. `tests/antithesis/k8s/workload.yaml:76-99` predeclares the seven ordinal FQDNs, matching the scaling range `OddReplicas = {3,5,7}` in `tests/antithesis/workload/internal/k8s.go:33-34`.
- `tests/antithesis/workload/internal/pernode.go:82-175` retains every single-target connection by `Addr` even when membership-based `NodeID` resolution is zero. Its retry interceptors are unsuitable for the first-attempt oracle, so the new probe must reuse the address list but construct a no-retry client.
- Existing structured and chaotic scaling drivers assert voter-count convergence only (`singleton_driver_scaling/main.go:69-97`, `singleton_driver_scaling_chaos/main.go:106-121`).

## Failure scenario

1. Build non-trivial history on a three-node cluster.
2. Commit a model-tracked transaction and retain its returned log sequence as `barrier_log`; choose a modelled PIT selector whose expected result can be recomputed for any returned history watermark.
3. Scale to five nodes while continuing writes and injecting failures during each new learner's snapshot synchronization.
4. Select each new ordinal's predeclared FQDN directly, construct a single-target client with no service-config or interceptor retries, and issue short-deadline stale-local PIT calls with `minLogSequence=barrier_log` as soon as its gRPC endpoint accepts connections. Do not wait for `DialPerNode.NodeID`, Kubernetes Ready, or voter promotion.
5. Classify pre-server `UNAVAILABLE`/deadline outcomes as reachability observations, not PIT decisions. Once the server responds, allow only explicit fail-closed outcomes appropriate to the phase: `LEDGER_NOT_FOUND` while the primary snapshot has not materialized the ledger, `HISTORY_BUILDING` while the process-local reconciliation gate is closed, `HISTORY_BEHIND` or deadline while a requested watermark is still being awaited, or success satisfying the invariant above. Any other successful or structurally incomplete response fails the property.
6. After primary and history catch-up, require at least one successful direct stale PIT from every new ordinal, then hand off to `coordination-quiescent-pit-convergence` for eventual cross-replica equality.

## Workload oracle

- Derive `node_id = ordinal + 1` from the same contract as the operator entrypoint, but use the ordinal FQDN—not membership discovery—to choose the socket. Record leader membership suffrage, pod Ready, local cluster state, and history-reconciliation events only as stage labels in assertion details; none is a safety premise.
- Attach `x-consistency: stale` and disable transparent retries so one assertion sample names exactly one receiving process and one attempt.
- Use an accepted log sequence from the authoritative write path as `barrier_log`, pass it as `minLogSequence`, and keep the per-attempt deadline bounded. The server also binds the request to its local primary head, preventing a successful view older than the primary snapshot used by that request.
- On success, decode the immutable PIT trailer and check the exact ledger ID, selector echo, non-empty token, and `logWatermark >= barrier_log`. Recompute the complete monetary result from the model through the returned `logWatermark`; do not compare only row counts or a digest produced by the SUT.
- Emit `Reachable("pit: joining ordinal accepted direct PIT RPC", details)` when a structured response first proves the target process was reached. Emit `Sometimes(success, "pit: joining ordinal eventually serves reconciled history", details)` after convergence. `HISTORY_BUILDING` observation is useful coverage but must not be required, because a small history may reconcile before the first probe.

## Instrumentation candidates and existing coverage

- **Missing — scale-up history boot:** add `Reachable("pit: joining replica entered history reconciliation", details)` and `Sometimes(ready, "pit: joining replica completed history reconciliation", details)` with node ID, ordinal, and whether boot began from an empty or persisted manifest. These signals label phases; the external success oracle remains independently checkable.
- **Partial — snapshot handoff:** lifecycle events `install_snapshot` and `sync_with_leader_complete` exist (`internal/infra/state/synchronizer.go:82-85,123-139`), but the existing assertion inventory does not connect them to PIT readiness.
- **Partial — scaling:** `"scaling patch applied"` and `"cluster should recover after chaotic scaling"` exercise the cluster transition, but stop at voter count.
- **Missing — PIT-specific:** `existing-assertions.md:31-48` reports no PIT SDK assertions.

## Open questions

None after repository investigation at `fb3f9f8334c7fbbcc27c2c211c77da36762e6aaf`.

### Investigation Log

#### Kubernetes Ready, join, promotion, and `Builder.Ready`

- **Examined:** `misc/operator/internal/controller/reconcile_statefulset.go`, `internal/adapter/http/server.go`, `internal/adapter/http/handlers_health.go`, `internal/bootstrap/module.go`, `internal/infra/node/node.go`, `internal/application/balancehistory/builder.go`, and `internal/bootstrap/balance_history.go`.
- **Found:** Fresh join registration completes before the local Raft/API lifecycle starts. Kubernetes readiness is then gated only by `Node.IsStarted()`. Learner promotion is a later, leader-driven tick conditioned on Raft progress, while the process-local history builder is started asynchronously by the last lifecycle hook after the external servers. Kubernetes Ready therefore does not promise promotion, primary FSM catch-up, or `Builder.Ready`. There is no fixed Ready-versus-promotion race winner: the probe has a five-second initial delay, so a fast promotion may be observed first, but nothing prevents Ready while the node remains a learner. The supported contract is the readiness predicate itself, not an incidental observed ordering.
- **Not found:** Any probe, pod condition, lifecycle dependency, or health aggregation that includes learner suffrage, synchronizer completion, or balance-history readiness.
- **Conclusion:** Learner membership commit precedes application reachability, but voter promotion and history readiness do not gate either gRPC reachability or Kubernetes Ready. The property must probe after learner admission and before/through the independent promotion and history-reconciliation windows, without treating voter status as required for safe success.

#### Direct addressability of a new ordinal

- **Examined:** `misc/operator/internal/controller/reconcile_service_headless.go`, `misc/operator/internal/controller/reconcile_statefulset.go`, `tests/antithesis/k8s/workload.yaml`, `tests/antithesis/workload/internal/k8s.go`, and `tests/antithesis/workload/internal/pernode.go`.
- **Found:** The headless Service sets `PublishNotReadyAddresses=true`; the workload manifest statically carries FQDNs for ordinals 0 through 6, exactly covering the supported 3/5/7 scale set; and the operator assigns `node_id = ordinal + 1`. `DialPerNode` creates and retains an address-keyed lazy connection even when membership lookup cannot yet fill `NodeID`.
- **Not found:** Any need to consult the leader membership list to derive or dial a new ordinal's socket. Conversely, the external service server cannot accept an RPC before the blocking join preflight has committed learner membership, so application-level PIT probes cannot precede membership admission even though the target address is known earlier.
- **Conclusion:** Target discovery is fully ordinal-derived and independent of membership. The property should use the predeclared address immediately and tolerate `NodeID=0` for early samples, but it needs a dedicated no-retry connection because the existing helper installs transparent `UNAVAILABLE` retries.
