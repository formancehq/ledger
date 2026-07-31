# boot-readiness-reconciles-persisted-history — Ordinary boot reconciles persisted history before serving

## Catalog candidate

| | |
|---|---|
| **Priority** | P1 — the safety check remains mandatory, while ordinary boot is less search-dependent than restore/repair crash windows. |
| **Type** | Safety, with a liveness companion for reopening local reads after reconciliation. |
| **Property** | On every ordinary process restart that reopens a complete persisted PIT manifest without a persistent failure marker, the process-local PIT read gate remains closed until a fresh authoritative source snapshot has been sampled, the manifest has caught up exactly to that sampled audit/log head with its hash relationship validated, and the forced boot WAL barrier succeeds; only then may local PIT reads succeed. |
| **Invariant** | At the sole transition that opens readiness, assert `AlwaysOrUnreachable(!ordinaryPersistedBoot || (freshSourceSnapshotValidated && caughtUp && manifest.SourceComplete && manifest.AuditWatermark == sampledHead.AuditSequence && manifest.LogWatermark == sampledHead.LogSequence && auditHashRelationshipValidated && bootWALBarrierSucceeded && !rebuildPending && !sourceMissing))`, message `pit: ordinary boot opens persisted history only after fresh source reconciliation`. Use a process-local boot trace: none of these facts may be inferred merely from the recovered manifest. Add `Reachable(ordinaryPersistedBoot && !ready)`, message `pit: ordinary boot loaded persisted history with read gate closed`, to prove the relevant branch is exercised. Companion workload `Sometimes(restartedSamePVC && exactPITSucceededAtOrBeyondH)`, message `pit: ordinary restart reconciles persisted history and reopens local reads`, supplies liveness. |
| **Antithesis Angle** | Seed a target replica through a known PIT watermark `H`, quiesce the property ledger, gracefully restart that pod while retaining its PVC, and probe the replacement UID directly without retries. `HISTORY_BUILDING` and `HISTORY_BEHIND` are permitted before reconciliation; transport unavailability is inconclusive. Every successful response must carry a valid resolved watermark at or beyond `H` and match the independent monetary oracle. Repeat across replicas and interleave unrelated cluster activity so that boot races public availability without manufacturing an unsynced-loss premise. |
| **Why It Matters** | A complete local manifest is only a peer projection, not authority. Process restart discards the previous process's readiness proof, so trusting retained bytes before a new source comparison could serve history that no longer agrees with the audit-backed primary state. |
| **Confidence** | High for the contract and assertion point: the gate, fresh source reads, exact watermark/hash validation, forced boot durability barrier, sole readiness-open transition, and deterministic restart tests are explicit. Medium for observing the short pre-ready interval through public RPCs, so the SUT assertion is authoritative and observing `HISTORY_BUILDING` is not required for workload success. |

## Scope and distinction from adjacent properties

This property covers an ordinary restart of the same replica and retained PIT store when the recovered manifest is complete and no persistent failure marker requires repair. A valid manifest may be behind and tail forward, but the episode must not require reset or rebuild from genesis.

It is deliberately distinct from `recovery-unsynced-suffix-replays`: that property requires proof that a hard failure lost a suffix which the same pod had previously served. Here, the restart is graceful, the old process completes its final durability barrier, and the recovered complete manifest need not regress below `H`. It is also distinct from restore-ahead reconciliation: if the fresh source is behind or hash-divergent and `resetIfRolledBack` enters reset/rebuild, classify the episode under the restore/repair property rather than satisfying this ordinary-boot premise. A fresh-PVC backup restore is outside scope as well.

The safety property does not require the workload to observe a public fail-closed response. Boot can reconcile between endpoint availability and the first probe. The required evidence is the SUT assertion immediately before the readiness transition; the public workload proves only that a same-PVC ordinary restart eventually reopens exact reads.

## Failure scenario and oracle

1. On one directly addressable replica, create distinctive transactions in a driver-owned ledger and wait until a no-retry PIT request succeeds with resolved log watermark `H`. Freeze that ledger and retain an independent transaction/volume oracle through `H`.
2. Gracefully stop and restart the same StatefulSet ordinal, preserving its data PVC. Correlate the episode with the old and new pod UIDs. Condition the ordinary-boot premise on the old process completing `Builder.Stop()` and its final PIT durability barrier; a timed-out or hard termination is not evidence for this property.
3. During the new process's boot, SUT instrumentation records that it loaded a non-empty, `SourceComplete` manifest with no persistent failure marker while readiness remained false. This is the non-vacuity anchor.
4. Probe the replacement pod through a single-target, no-retry client with `minLogSequence=H`. Before the gate opens, exact application reasons `HISTORY_BUILDING` and `HISTORY_BEHIND` are permitted. Connection refusal, endpoint transition, cancellation, and deadline are transport outcomes and neither satisfy nor fail the application assertion.
5. Immediately before the builder opens readiness, assert that this process—not the previous one—successfully sampled source authority, validated the recovered cursor's sequence/log/hash relationship, reached that sampled head, and completed the forced WAL barrier. A reset, rebuild, source-missing state, head mismatch, source-read error, or sync error must leave readiness closed.
6. Require eventual success from the replacement UID at `logWatermark >= H`. The returned audit/log trailer must be valid and the balance result through the returned watermark must equal the independent oracle. Do not require an observed `HISTORY_BUILDING` response: a fast correct reconciliation is legal.

## Exact assertion rationale

`AlwaysOrUnreachable` is appropriate because an ordinary persisted-manifest resume is conditional: some campaign processes start with an empty store or enter an explicit repair path. Whenever the ordinary branch reaches the readiness-open statement, every conjunct is mandatory. A plain workload `Always` cannot prove the internal source comparison or WAL barrier, and a standalone `Sometimes(ready)` would prove only liveness.

`freshSourceSnapshotValidated` must be a process-local boot-episode fact set only by a successful `processOnce` path that used new `Source.Head`/`Source.Read` snapshots. Reusing manifest equality as its value would make the assertion circular. `auditHashRelationshipValidated` should record the successful same-cursor hash check or the verified consecutive read path, so equality of sequence numbers alone cannot pass. `bootWALBarrierSucceeded` must be set only after `syncDurability(true)` returns nil in the caught-up boot path.

The assertion belongs immediately before the single production `ready.Store(true)` in `markReadyAfterReconciliation`. At that point the method already requires a complete manifest, exact equality with the latest sampled audit/log head, and absence of rebuild/source-missing flags. The boot trace adds the facts currently implicit across earlier calls: this process made the source observations, validated their relationship to the recovered manifest, and successfully crossed the forced durability barrier.

## Code evidence

- `internal/application/balancehistory/builder.go:296-333` closes readiness before the worker boot callback starts, even if the in-memory flag had previously been true. The comment identifies restart-time reconciliation as the gate for persisted history.
- `internal/application/balancehistory/builder.go:428-434` documents readiness as process-local: every process restart must prove the source relationship before exposing the persisted provider.
- `internal/application/balancehistory/builder.go:465-541` loads the persisted manifest, restores persistent repair state, repeatedly reconciles through `processOnce`, forces `syncDurability(true)` at catch-up, and only then calls `markReadyAfterReconciliation`. Boot errors fall into retry with readiness still false.
- `internal/application/balancehistory/builder.go:556-591` also closes readiness on steady-state build/sync errors or lag and reopens it only after reconciliation.
- `internal/application/balancehistory/builder.go:751-779` requires `SourceComplete`, exact audit/log equality with the latest sampled source head, and no rebuild/source-missing state before the sole production `ready.Store(true)` call.
- `internal/application/balancehistory/builder.go:786-925` samples source head and reads from the recovered cursor. An empty batch counts as caught up only when the cursor is unchanged and the sampled head is not ahead; non-empty batches publish consecutive input before reporting catch-up.
- `internal/application/balancehistory/source_hot.go:28-105` obtains fresh primary read handles for `Head` and `Read`; at an equal audit sequence, `Read` requires exact log watermark and audit hash equality before returning an empty caught-up batch.
- `internal/application/balancehistory/source_hotcold.go:63-125,136-176,301-335` independently opens fresh combined hot/chapter-registry snapshots and applies the same ahead, log, and hash cursor validation.
- `internal/application/balancehistory/builder.go:937-991` sends an ahead or same-sequence/hash-divergent manifest through source-missing reset/rebuild. That branch is excluded from this ordinary no-rebuild property rather than accepted as reconciliation success.
- `internal/bootstrap/balance_history_provider.go:12-15,59-66` explicitly treats the builder gate as fail closed: even a readable persisted store returns `ErrBuilding` until the current builder reports ready.
- `internal/bootstrap/module.go:1219-1264,1415-1418` registers the external gRPC lifecycle before the balance-history lifecycle, so public server availability can race asynchronous PIT boot. Kubernetes readiness therefore is not proof of PIT readiness.
- `internal/bootstrap/balance_history.go:285-325` starts the builder asynchronously and, on graceful quiesce, stops it through `Builder.Stop`; that stop closes the gate and forces the final durability sync.
- `internal/application/balancehistory/builder_test.go:490-579` deliberately starts a new builder with a readable persisted manifest and a failing/blocked fresh source head. `Start` immediately clears a stale true readiness flag; readiness remains false after the source error and opens only after lower-source reconciliation/reset completes.
- `internal/application/balancehistory/builder_test.go:710-733` verifies that caught-up boot forces a WAL sync, while `internal/bootstrap/balance_history_provider_test.go:61-93` verifies that the provider rejects a directly readable persisted store until the new builder is ready.
- `tests/antithesis/workload/internal/k8s.go:257-263` offers only a grace-zero pod deletion helper today and explicitly calls it fault injection. An ordinary-restart helper with positive grace is therefore needed rather than reusing the unsynced-loss driver.
- `tests/antithesis/workload/internal/pernode.go:82-125` supplies direct per-node dialing but installs retry behavior. Exact boot probes need a factored no-retry option so transient fail-closed responses are not hidden.
- `antithesis/scratchbook/existing-assertions.md` records that the repository currently has no PIT-specific SDK assertions and that existing retrying clients can hide `HISTORY_BUILDING`/`HISTORY_BEHIND` responses.

All code references above are to commit `fb3f9f8334c7fbbcc27c2c211c77da36762e6aaf`.

## Candidate SUT instrumentation

Existing status: **missing**. The deterministic tests exercise the gate, but the Antithesis build has no assertion at the readiness transition.

- Add a process-local boot trace owned by `Builder`, reset at the start of each boot episode. Record a boot/process identifier; recovered manifest version, source-complete state, audit/log/hash position; persistent-failure presence; latest fresh source audit/log/hash sample; cursor/hash validation; whether reset/rebuild was entered; caught-up result; and successful forced boot sync. Do not persist this trace and do not expose it as a product API.
- After loading a non-empty complete manifest with no persistent marker and before the first source sample, emit `Reachable`, `pit: ordinary boot loaded persisted history with read gate closed`, including node/boot ID and recovered watermarks. This proves that the assertion's antecedent is exercised.
- Immediately before `ready.Store(true)`, emit the primary `AlwaysOrUnreachable` assertion using the boot trace and current manifest/head. If reset/rebuild occurred, clear the ordinary-boot antecedent so that the repair property owns that episode; do not let a later completed rebuild masquerade as ordinary resume.
- After the same transition, emit `Reachable`, `pit: ordinary boot reconciled persisted history before opening read gate`, with recovered and final positions. This is diagnostic branch coverage, not the workload's liveness proof.
- Add a positive-grace restart helper that waits for old-process quiesce completion, retains the same PVC, verifies a new pod UID for the same ordinal, and records an episode as inconclusive if graceful shutdown does not complete. Keep the existing grace-zero helper for the separate unsynced-suffix property.
- Factor the per-node dialer so callers can select no-retry behavior without duplicating connection setup. The ordinary-restart driver must see the first application outcome from the target replica rather than an interceptor's later retry.
- In the workload, add `Sometimes(restartedSamePVC && exactPITSucceededAtOrBeyondH)`, `pit: ordinary restart reconciles persisted history and reopens local reads`. Add the companion `Always(!success || validTrailerAtOrBeyondH && resultEqualsOracleAtReturnedWatermark)`, `pit: successful ordinary-restart history read matches its resolved watermark`.

## Open questions

None. The repository resolves the safety contract and assertion point. The missing positive-grace restart helper, no-retry per-node mode, and PIT SDK assertion are implementation gaps, not unresolved semantic questions.

### Investigation Log

#### What proves fresh reconciliation instead of trusting the persisted manifest?

- **Examined:** `Builder.Start`, `Builder.boot`, `Builder.processOnce`, `Builder.markReadyAfterReconciliation`, both hot and hot/cold source implementations, the restart-readiness tests, and every production `ready.Store(true)` occurrence at commit `fb3f9f8334c7fbbcc27c2c211c77da36762e6aaf`.
- **Found:** readiness is reset before boot; boot obtains fresh source snapshots, validates cursor sequence/log/hash relationships, catches the manifest up, forces the WAL barrier, and opens readiness at one production call site only after exact head equality and clean state flags. Source errors and barrier errors retain fail-closed readiness.
- **Not found:** an alternate production path that opens readiness directly from persisted-manifest state or a persisted readiness bit that survives process restart.
- **Conclusion:** the exact, non-circular SUT assertion belongs immediately before the sole readiness-open transition and must carry explicit process-local evidence from the preceding source and sync calls.

#### How is ordinary boot different from unsynced suffix loss and restore-ahead repair?

- **Examined:** graceful `Builder.Stop`, boot durability handling, `resetIfRolledBack`, the existing `recovery-unsynced-suffix-replays` candidate, Kubernetes restart helpers, StatefulSet/PVC topology, and backup/restore property boundaries in the synthesis.
- **Found:** graceful stop closes the PIT gate and forces a final sync, while the existing hard-delete helper uses grace zero specifically for fault injection. Unsynced recovery requires observed loss below a previously served `H`; ordinary boot requires only a retained complete manifest and a fresh proof before reuse. Ahead/divergent state deliberately enters source-missing reset/rebuild and belongs to repair, not this no-rebuild premise.
- **Not found:** an existing positive-grace same-PVC workload helper that records completion of the old process's PIT durability barrier.
- **Conclusion:** use a graceful same-PVC restart and condition the episode on completed quiesce. Do not reuse the hard-kill/lost-prefix trigger, and exclude reset/rebuild episodes.

#### Can the workload prove the pre-serving reconciliation boundary by itself?

- **Examined:** bootstrap lifecycle ordering, the balance-history provider, its tests, gRPC PIT error behavior summarized in `existing-assertions.md`, and direct per-node client construction.
- **Found:** the external server may be reachable before asynchronous PIT boot finishes, and the provider returns fail-closed `ErrBuilding` while the persisted store is locally readable. However, boot can complete before the first public probe, retry interceptors can hide lag responses, and no public field proves which source snapshot or WAL barrier justified readiness.
- **Not found:** a public readiness transition event, a no-retry per-node client option, or an existing PIT-specific SDK assertion.
- **Conclusion:** the workload should prove eventual exact local service and treat any observed fail-closed response as permitted evidence, not mandatory evidence. The internal assertion at `ready.Store(true)` is the authoritative safety oracle.
