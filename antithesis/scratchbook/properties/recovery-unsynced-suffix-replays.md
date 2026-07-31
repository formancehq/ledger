# recovery-unsynced-suffix-replays — Lost NoSync suffix is replayed exactly

## Catalog candidate

| | |
|---|---|
| **Priority** | P0 — the intentionally asynchronous history WAL permits this crash outcome in normal operation. |
| **Type** | Liveness (progress), with a companion safety check on every successful post-restart read. |
| **Property** | After a process or power failure loses balance-history publications newer than the last successful WAL barrier, the restarted replica remains fail closed until it replays the authoritative suffix and eventually returns the same monetary result and source watermark as a clean projection. |
| **Invariant** | Primary `Sometimes(lostSuffixObserved && replayedToH && resultEqualsOracleAtH)`, message `pit: lost unsynced history suffix is replayed exactly after restart`. One recovery episode sets `lostSuffixObserved` only when the same pod first served PIT at `H`, restarted with a new UID, and then returned `HISTORY_BUILDING` or `HISTORY_BEHIND` with `currentLogSequence < H`; this prevents a full-prefix recovery from satisfying the property. `Sometimes` is the right type because a real lost-suffix episode and its recovery must occur at least once. Companion workload `Always(!success || resultEqualsOracleAtReturnedWatermark)` prevents any partial successful result while replay is incomplete. |
| **Antithesis Angle** | Widen the durability interval, publish several source proposals, and use the existing Kubernetes `DeletePod(grace=0)` path to hard-delete one Ledger pod while its PIT publication watermark is ahead of its durability watermark. The StatefulSet recreates the pod on the same retained PVC. Branch on whether Pebble recovers the full visible prefix or an older consecutive prefix, and re-run on individual replicas while Raft authority remains available. |
| **Why It Matters** | The peer projection is rebuildable, but a lost suffix must produce bounded replay rather than missing or duplicate monetary effects. Silent partial success would make historical balances wrong after an ordinary crash. |
| **Confidence** | High — atomic publication, asynchronous durability, restart reconciliation, and a deterministic lost-suffix test are all present in current code; only real process/power-loss composition is missing. |

## Failure scenario and oracle

1. During a widened durability interval, publish distinctive monetary effects until the target pod successfully serves PIT through log head `H`. Stop this driver's writers and capture the clean oracle through exactly that returned watermark before killing the pod.
2. SUT-side `Sometimes(processedAudit > durableAudit)` establishes that an asynchronous publication window was reached; it guides kill placement but is not itself accepted as proof that this particular recovery lost bytes.
3. Immediately hard-delete that Ledger pod through the existing `DeletePod(grace=0)` helper. The operator-created StatefulSet recreates the same ordinal with a new UID while retaining its data PVC.
4. After the replacement pod becomes globally Ready, probe it directly with no retries and `minLogSequence=H`. A response with exact reason `HISTORY_BUILDING` or `HISTORY_BEHIND` and `currentLogSequence < H` proves that this same recovery episode lost a previously served suffix. Transport unavailability while the replacement endpoint reconnects is recorded separately and does not establish loss.
5. Every successful post-restart response must match the independent oracle through its returned watermark. Any reached application response other than success, `HISTORY_BUILDING`, or `HISTORY_BEHIND` fails a separately named safety assertion.
6. With the episode's writers stopped, require eventual success at `logWatermark >= H` with the exact oracle result through `H`.

The workload must not require that every hard kill loses the unsynced suffix: Pebble may recover the full prefix. A full-prefix recovery remains subject to the successful-read safety oracle but does not satisfy the primary `Sometimes`; only the correlated same-pod, old-UID/new-UID, pre-kill-success/post-restart-behind episode does. The workload continues attempts so Antithesis can search for at least one real loss branch.

## Code evidence

- `internal/storage/balancehistorystore/publish.go:281-285,369-417` writes run bytes, run metadata, manifest, and latest pointer atomically in one `pebble.NoSync` batch. This prevents a manifest from naming a partial run while explicitly allowing suffix loss.
- `internal/storage/balancehistorystore/store.go:176-188` uses `LogData(nil, pebble.Sync)` under `mutationMu` as the durability barrier.
- `internal/application/balancehistory/builder.go:470-541` starts with readiness closed, restores the persisted manifest, drains source batches, forces a barrier at the pinned head, and only then opens readiness.
- `internal/application/balancehistory/builder.go:786-925` resumes from the recovered manifest watermark and publishes only consecutive verified source ranges.
- `internal/storage/balancehistorystore/hardening_test.go:350-390` checkpoints a synced prefix, replaces the DB with that prefix after a NoSync publication, and proves that replay reconstructs the same digest and balances.
- `tests/antithesis/workload/internal/k8s.go:257-263` implements `DeletePod` with `GracePeriodSeconds: 0` and explicitly classifies it as fault injection rather than graceful shutdown.
- `tests/antithesis/workload/bin/cmds/main/singleton_driver_rolling_restart/main.go:113-139` already deletes one Ledger pod, verifies its UID changes, waits for the replacement to become Ready, and restores the voter count.
- `tests/antithesis/k8s/workload.yaml:19-23` grants the workload pod permission to delete Ledger pods.
- `tests/antithesis/k8s/cluster.yaml:48-54` configures the WAL, data, and cold-cache as persistent volumes; the PIT directory is under the data directory (`internal/bootstrap/balance_history.go:105-111`).
- `misc/operator/internal/controller/reconcile_statefulset.go:268-285,312-355` defaults StatefulSet PVC retention to `Retain` and mounts the data claim back at the configured data directory.
- `internal/bootstrap/balance_history.go:297-325` shows why the hard-delete matters: the graceful lifecycle path calls `Builder.Stop()`, which forces the final WAL barrier, while `DeletePod(grace=0)` is specifically the non-graceful path.

## Candidate SUT instrumentation

Existing status: **missing**. `existing-assertions.md` states there are no PIT-specific SDK assertions. The builder exports published/source/durable sequence metrics, but the checked-in Antithesis workload has no metrics scraper and a post-restart metric sample cannot by itself distinguish NoSync loss from ordinary pre-crash lag. Public PIT errors expose only the current/required log sequences, while successful responses expose the resolved audit/log watermarks. Those surfaces are sufficient for the fail-closed and final correctness oracle, but not for precise branch guidance.

- Add `Sometimes(processedAudit > durableAudit)`, `pit: history publication is visible before its durability barrier`, immediately after a successful `Publish`. This exposes the kill window without a test-only API, but it is guidance only; the workload's per-pod recovery episode supplies the proof of loss.
- On the first successful source-head sample during boot, retain the pre-replay manifest position in process-local recovery state. Emit `Reachable`, `pit: hard restart recovered a persisted history prefix behind source`, when a non-empty recovered manifest is behind the sampled authoritative head. This confirms the internal replay branch, while the workload correlates it to pre-kill `H` by observing `currentLogSequence < H`.
- Add `AlwaysOrUnreachable(replayedAudit >= recoveredAudit && replayedAudit <= batchHeadAudit && replayedLog >= recoveredLog && replayedLog <= batchHeadLog)`, `pit: hard restart replay stays within recovered and current source batch bounds`, after each boot publication while that recovery state is active. Use that source batch's immutable head, not the boot's first sampled head, because later batches may legitimately observe concurrent source growth.
- After the forced catch-up barrier succeeds and readiness opens, add `Sometimes(recoveredBehind && finalManifest == latestSampledHead)`, `pit: hard restart replay reached and durably covered sampled source head`. This is the SUT-side replay-completion anchor; the workload's quiescent `H` remains the public liveness target.
- In the no-retry per-node workload, add `Always(!success || resultEqualsOracleAtReturnedWatermark)`, `pit: successful post-restart history read matches replay oracle`, for every response after the pod UID changes.
- Separately add `Always(!applicationResponseReached || success || reason == HISTORY_BUILDING || reason == HISTORY_BEHIND)`, `pit: hard restart history probe returns only success or fail-closed lag`, so the success oracle is not vacuous for failures. Connection refusal, DNS transition, cancellation, and deadline while the replacement endpoint reconnects are classified as transport outcomes and do not satisfy or fail this application-response assertion.
- Maintain one workload recovery record keyed by pod ordinal, old UID, new UID, pre-kill `H`, and its oracle. Only `currentLogSequence < H` from an accepted lag response sets `lostSuffixObserved`; only later exact success through the same `H` satisfies the primary `Sometimes`.

## Open questions

- Does the target Antithesis storage model discard un-fsynced Pebble bytes on a grace-zero pod deletion, or is a node/disk fault required to reach the true lost-suffix branch? `(needs human input)`

### Investigation Log

#### Can the Antithesis environment hard-terminate Ledger while preserving its PVC and bypassing graceful `Builder.Stop()`?

- **Examined:** `tests/antithesis/workload/internal/k8s.go:257-263`, `tests/antithesis/workload/bin/cmds/main/singleton_driver_rolling_restart/main.go:113-139`, `tests/antithesis/k8s/workload.yaml:19-23`, `tests/antithesis/k8s/cluster.yaml:48-54`, `misc/operator/internal/controller/reconcile_statefulset.go:268-285,312-355`, and `internal/bootstrap/balance_history.go:297-325`.
- **Found:** the checked-in workload is authorized to delete pods and already calls `DeletePod` with grace zero. The helper explicitly says it is for fault injection, not graceful shutdown. The StatefulSet recreates the same ordinal with a new UID, its WAL/data/cold-cache claims are PVC-backed, and the operator's default retention policy is `Retain`. The normal PIT quiesce path would call `Builder.Stop()` and force a final barrier, so the grace-zero path is the required ungraceful contrast.
- **Not found:** a checked-in deterministic disk/power fault that guarantees the host loses un-fsynced Pebble bytes. A hard pod kill can legally recover either the full NoSync prefix or only a durable prefix.
- **Conclusion:** the original process/PVC question is resolved: the environment can exercise the ungraceful process/PVC-preserving restart. Whether this termination model can produce actual un-fsynced-byte loss is a separate environment-semantics question recorded below.

#### Does grace-zero pod deletion discard un-fsynced Pebble bytes, or is a node/disk fault required?

- **Examined:** every checked-in Kubernetes fault helper and operational driver under `tests/antithesis`, the Kubernetes deployment manifests, PIT WAL publication/barrier code, and the deterministic lost-prefix test in `internal/storage/balancehistorystore/hardening_test.go:349-387`.
- **Found:** the repository supplies hard pod deletion and a deterministic in-process test that replaces the live database with an older synced checkpoint. It does not define the Antithesis hypervisor's host page-cache, virtual-disk, or PVC crash semantics. Therefore repository evidence cannot establish whether a pod-only kill ever discards bytes already written with `NoSync`.
- **Not found:** a node-power, disk-reset, page-cache-drop, or explicit persisted-volume rollback fault in the checked-in harness, nor repository documentation defining the external platform's storage-fault semantics.
- **Conclusion:** `(needs human input)`. Confirm the target Antithesis environment's storage semantics. If grace-zero deletion cannot lose un-fsynced bytes, add a supported node/disk fault or deterministic test seam; do not weaken the primary condition to ordinary restart lag.

#### Must recovery expose a test-only persisted-manifest diagnostic?

- **Examined:** `internal/application/balancehistory/builder.go:296-417,470-541,786-925`, `internal/pkg/tailworker/gauges.go:17-53`, `internal/storage/balancehistorystore/errors.go:20-52`, `internal/application/ctrl/volume_view.go:34-43,74-117`, `misc/proto/bucket.proto:1262-1274`, the Antithesis workload clients/manifests, and `existing-assertions.md`.
- **Found:** the SUT already has process-local published/source/durable gauges, but the checked-in workload does not scrape them. `HISTORY_BUILDING` and `HISTORY_BEHIND` expose current/target or required log sequences through error metadata, and every successful PIT response exposes audit/log watermarks in `PointInTimeView`. A workload record of the target pod's pre-kill successful watermark `H`, followed after UID replacement by `currentLogSequence < H`, identifies the relevant loss episode without exposing the manifest. If replay wins the race before the probe, the attempt is inconclusive rather than a false positive and the driver retries.
- **Not found:** a public manifest-status API, a workload metrics-scraping helper, or an existing PIT SDK assertion at the recovered-manifest/source-head comparison.
- **Conclusion:** resolved without a new diagnostic surface. Keep the workload oracle black-box and add narrow SUT-side SDK reachability/bounds/completion assertions at the existing boot comparison and publication points. This follows the repository's established SUT-assertion pattern and avoids making physical manifest state part of the public PIT contract.
