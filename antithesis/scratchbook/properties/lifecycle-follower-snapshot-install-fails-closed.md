# lifecycle-follower-snapshot-install-fails-closed — Follower snapshot install closes PIT visibility

**Focus:** Lifecycle transitions

**Priority:** P1

**Type:** Safety

**Confidence:** High that primary replacement does not synchronously invalidate the process-local PIT gate and that stale reads can reach the affected replica; low that an honest same-cluster follower snapshot can produce an ahead or divergent success, because the snapshot protocol is forward-only and the controller independently gates history on the replacement primary's log head.

## Property

Before follower synchronization can activate a replacement primary store, it must invalidate the local PIT source-relationship gate. The gate must remain closed through primary activation, cache/state recovery, and spool replay, and may reopen only after the builder has sampled the replacement source and reconciled to that source head. If reconciliation detects rollback or hash divergence, reopening additionally requires the existing repair certification.

This property applies to every read consistency. In particular, `stale` is deliberately a replica-local read mode and does not become leader-forwarded merely because the applier is syncing.

## Invariant and assertion rationale

Use SUT-side `AlwaysOrUnreachable` named `pit: primary replacement keeps history read gate closed until reconciliation`. Give each replacement a monotonic source epoch and evaluate `!builder.Ready() || builderReconciledEpoch >= activePrimaryEpoch`:

- immediately before `IncomingRestoreFactory.Run` can activate the downloaded checkpoint;
- immediately after `RestoreCheckpoint` returns;
- after cache/state recovery and after spool replay;
- whenever the builder attempts to set `Ready` true.

Pair it with `Reachable("pit: follower primary replacement reached history gate")` at the pre-activation hook so the safety assertion is not vacuous, and `Sometimes("pit: history reconciled after follower snapshot install")` when the builder reopens the epoch-matched gate. A check only in `InstallSnapshot` is too early: that method updates Raft/FSM bookkeeping, while the actual Pebble swap occurs later inside `SynchronizeWithLeader`.

Use workload-side `AlwaysOrUnreachable` named `pit: snapshot-install window never serves incoherent history`. The workload cannot observe `Builder.Ready()` or the exact filesystem rename, so it must not infer that every success during the coarse `syncing` status is a gate violation. Instead, for every stable-head sample in the install/sync window, a successful direct PIT read must:

- carry a decodable `x-point-in-time-view-bin` trailer;
- have `PointInTimeView.log_watermark` exactly equal to the target replica's stable primary `GetIndexStatus.last_log_sequence`;
- return the same monetary result as a direct live aggregate on a workload-owned ledger whose effective timestamps are all before the PIT selector.

`HISTORY_BUILDING`, `HISTORY_BEHIND`, `HISTORY_SOURCE_MISSING`, and transport unavailability are fail-closed outcomes. Add `Sometimes("pit: direct probe observed follower snapshot-install window")` for targeting coverage and `Sometimes("pit: target-local PIT succeeds after follower history reconciliation")` after the node returns to `normal` and the stable-head equality oracle passes.

Distinct instrumentation candidates:

- `pit: follower primary replacement reached history gate`
- `pit: primary replacement keeps history read gate closed until reconciliation`
- `pit: follower snapshot replacement required history catch-up`
- `pit: history reconciled after follower snapshot install`

The existing lifecycle events `snapshot_received`, `install_snapshot`, `sync_snapshot_started`, and `sync_with_leader_complete` remain useful temporal anchors, but they do not enforce the gate.

## Workload targeting and oracle

1. Enable the feature in the campaign (`BALANCE_HISTORY_ENABLED=true`); the current Antithesis cluster manifest does not enable it. Create a dedicated ledger, write deterministic balances with effective timestamps before the chosen PIT timestamp, and wait for an initial successful PIT response.
2. Use the node list from `GetClusterState` to select a non-leader voter. `DialPerNode` already maps each single-target service address to its Raft `NodeID`, and Antithesis pod ordinals map to `NodeID = ordinal + 1`.
3. Keep probing every known target so a naturally faulted/restarted node is discovered when `GetClusterState{NodeId: targetID}.sync_progress.status` becomes `installing_snapshot`, `syncing`, or `out_of_sync`. For a deterministic local run, `run_model_test.sh` already keeps a known process down while writes force WAL compaction. In Kubernetes, do not treat one immediate `DeletePod` as proof that snapshot transfer occurred; use the observed target status as the coverage gate.
4. Add a DRY variant of the per-node dial helper that disables both the gRPC retry policy and retry interceptors. The existing `DialPerNode` performs up to 50 `UNAVAILABLE` retries and can move a probe outside the lifecycle window. Send `AggregateVolumes` and `GetIndexStatus` over that single-target connection with `WithStaleConsistency`; this bypasses ReadIndex and leader forwarding.
5. For one sample, read target-local status and `GetIndexStatus.last_log_sequence`, perform a direct live aggregate and a PIT aggregate while capturing the trailer, then read head/status again. Evaluate the safety oracle only when both head reads succeed, the `NodeID` is unchanged, both heads are equal, and both status samples remain in the same install/sync window. Otherwise the sample is inconclusive, not a failure.
6. After the target reports `normal`, poll the same no-retry local PIT request until it succeeds at a stable head, then require trailer watermark equality and live/PIT result equality. This is the externally implementable reconciliation oracle; the SUT epoch assertion supplies the exact activation-to-reopen proof that the public API cannot expose.

## Antithesis angle

Keep one follower unavailable long enough for the leader to compact past it, continue writes, then let it rejoin while continuously querying that exact service address. Scheduling pauses around `incomingRestore.Run` and the first builder tick enlarge the pre-invalidation and post-activation windows.

Do not construct a lower or hash-divergent checkpoint through the ordinary follower snapshot API: `PrepareSnapshot.minAppliedIndex` makes the leader wait until its FSM includes at least the follower's received Raft snapshot index, and Raft supplies the same committed prefix. Rollback-shaped primary replacement belongs to the backup/restore property; this property exercises the lifecycle gate and the forward-sync history-behind case.

## Impact

Follower synchronization replaces authoritative Pebble without restarting the process, but the balance-history builder is an independent peer-store worker. Without a handoff, `Builder.Ready()` can retain the value established against the old primary until its next source sample. On an honest forward snapshot, the controller's current-log watermark normally turns this into `HISTORY_BEHIND` or a bounded wait rather than a wrong response. A lifecycle gate is still required so source coherence does not depend on that secondary guard and so future or fault-induced replacement modes cannot expose an ahead or same-watermark-divergent manifest.

## Code evidence

- `internal/bootstrap/controller_routed.go:48-75` defines `stale` as an unconditional local-store route before any syncing/ReadIndex check; only linearizable reads fall back to the leader while syncing.
- `internal/adapter/grpc/consistency.go:12-19` documents `stale` as skipping ReadIndex and reading the local store directly.
- `internal/infra/node/applier.go:1100-1129` exposes `installing_snapshot`, `syncing`, and `out_of_sync` through `StatusString`/`GetSyncProgress`; `internal/infra/node/node.go:2090-2106` publishes that state in `ClusterState.sync_progress`.
- `internal/adapter/grpc/server_cluster.go:101-137` routes a non-zero `GetClusterState.node_id` to that exact node rather than to the leader.
- `tests/antithesis/workload/internal/pernode.go:28-181` already provides address/NodeID-bound single-target clients and `WithStaleConsistency`, but lines 95-115 install retry policy/interceptors and therefore are unsuitable for a precise no-retry window probe as written.
- `tests/antithesis/workload/internal/k8s.go:216-374` exposes pod ordinals, pod deletion, leader discovery, and the documented Antithesis `NodeID = ordinal + 1` mapping.
- `internal/infra/state/synchronizer.go:43-87` restores the leader checkpoint and FSM state without a balance-history gate callback; lines 94-116 hide activation and `RestoreCheckpoint` inside `IncomingRestoreFactory.Run`.
- `internal/infra/state/synchronizer.go:120-141` shows that `InstallSnapshot` changes only snapshot bookkeeping/caches; it is not the primary activation point.
- `misc/proto/snapshot.proto:19-25` and `internal/adapter/grpc/server_snapshot.go:68-89` require a follower-sync checkpoint at or beyond `minAppliedIndex`, excluding a normal rollback-shaped checkpoint.
- `internal/bootstrap/balance_history_provider.go:41-66` gates PIT solely on process-local `Builder.Ready()` before opening the peer store.
- `internal/application/balancehistory/builder.go:299-341,428-434` clears readiness on process start/stop, but there is no in-process primary-replacement hook; lines 937-991 detect rollback/divergence only after a later source sample.
- `internal/application/ctrl/controller_default.go:972-997` pins the replacement primary's current global log head and passes it as the required history watermark, providing the secondary forward-sync freshness guard.
- `internal/application/ctrl/controller_default.go:1293-1439` makes `GetIndexStatus.last_log_sequence` a direct primary-store observation suitable for bracketing the target-local oracle.
- `tests/antithesis/run_model_test.sh:470-499` keeps a known node down while writes continue specifically to force snapshot install; `tests/antithesis/k8s/cluster.yaml` currently has no balance-history enablement.

## Existing assertion cross-reference

`existing-assertions.md` contains no PIT-specific SUT or workload assertion for this transition. Its reusable pieces are per-node direct reads, exact-node `GetClusterState`, lifecycle reachability, and stable-index comparison. A no-retry per-node PIT probe and a source-epoch gate remain missing.

## Failure scenario and limits

1. Follower history is ready against primary source `(A,L)`.
2. The follower receives a same-cluster checkpoint at an applied index at least as new as the Raft snapshot and begins in-process primary replacement.
3. Pebble activation completes while `Builder.Ready()` still reflects the old source relationship.
4. A stale-local PIT request passes the provider's readiness check. If the history manifest is behind the replacement primary, the controller/store watermark gate fails closed with `HISTORY_BEHIND` or waits until catch-up.
5. An externally wrong success additionally requires history to be ahead of the replacement source or different at the same watermark. Repository evidence does not show how an honest follower snapshot can create that precondition; it would require corruption, a different restore mechanism, or a future relaxation of the forward-only checkpoint contract.

The missing synchronous gate is source-confirmed. A wrong response from the ordinary follower-sync protocol remains a hypothesis, not a confirmed running-system defect.

## Open questions

None.

### Investigation Log

#### Is stale-local PIT supported while a follower synchronizes, and who owns the gate?

- **Examined:** `RoutedController.readCtrl`, the gRPC consistency interceptor, applier status/read barriers, the PIT provider/controller, and builder readiness lifecycle.
- **Found:** `stale` returns the local controller unconditionally, before the sync-sensitive ReadIndex path. The PIT provider independently consults `Builder.Ready()`, while follower synchronization has no balance-history dependency or invalidation callback.
- **Not found:** No routing rule, API exception, or test rejects stale reads solely because the applier is `installing_snapshot`, `syncing`, or `out_of_sync`.
- **Conclusion:** Replica-local stale reads are intentional routing behavior. The narrow owner is the PIT source-relationship gate at the synchronizer/balance-history boundary; rejecting every stale API during sync would be a broader product-contract change.

#### Can the workload target the exact follower and avoid forwarding?

- **Examined:** per-node client construction, cluster-state forwarding, sync-status exposure, Kubernetes node/pod helpers, rolling-restart and local model campaigns, and client retry configuration.
- **Found:** A single-target address plus resolved `NodeID` identifies the replica; `GetClusterState{NodeId: id}` observes that node; `WithStaleConsistency` prevents read forwarding; sync status exposes the install/sync window. The local model campaign can force snapshot transfer to a known process.
- **Not found:** No workload-visible subscription to the exact `RestoreCheckpoint` publish rename, and no existing no-retry form of `DialPerNode`. Immediate Kubernetes pod deletion alone does not prove the leader compacted far enough to send a snapshot.
- **Conclusion:** Exact replica targeting and local read attribution are implementable now. Add a no-retry single-target dial variant, use status transitions as the workload coverage gate, and keep the precise activation/reopen boundary in SUT epoch instrumentation.
