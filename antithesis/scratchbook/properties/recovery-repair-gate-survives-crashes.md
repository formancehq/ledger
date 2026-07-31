# recovery-repair-gate-survives-crashes — Repair never reopens before durable certification

## Catalog candidate

| | |
|---|---|
| **Priority** | P0 — clearing the gate early can serve incomplete or corrupt history after a second crash. |
| **Type** | Safety (correctness). |
| **Property** | Once `SOURCE_MISSING`, `QUARANTINED`, or `REBUILDING` is persisted, crashes at any repair subphase leave PIT fail closed until a genesis rebuild reaches the pinned source head, the rebuilt source prefix is covered by a successful WAL barrier, semantic certification succeeds for that required head, and the marker deletion itself succeeds synchronously. |
| **Invariant** | SUT `assert.AlwaysOrUnreachable(!clearAttempt || (resetComplete && manifestCoversRequiredHead && replayPrefixWALBarrierSucceededFor(requiredAuditSequence, requiredLogSequence) && certificationSucceededFor(requiredAuditSequence, requiredLogSequence)), "pit: repair marker clear attempted only for a durable certified source prefix", details)`. `AlwaysOrUnreachable` fits a rare optional repair path: a run that never repairs may pass, but every marker-clear attempt must satisfy all prerequisites. The marker clear remains a `pebble.Sync` write, which is the final durability barrier before reopening. No-retry workload companion `assert.Always(!rpcReachedReplica || (readyPublished ? exactResultAtRequiredHead : isExactRepairFailClosedReason(err)), "pit: repair stays fail closed until durable certification", details)` guards the public boundary before and after process-local readiness opens. |
| **Antithesis Angle** | Kill the replica after marker persistence, after reset, during bounded replay, after the forced WAL barrier, during semantic replay, and after marker deletion but before the next readiness observation. Restart from the same PVC at every branch and combine source/MinIO outages with the kill. |
| **Why It Matters** | A repair marker is the durable authority preventing known-bad projection state from being served. Process-local flags alone cannot survive a crash, and certification without a durability barrier can be lost by the next crash. |
| **Confidence** | High — the ordered repair mechanism and deterministic crash-state tests are explicit in code, while Antithesis adds real process crashes between subphases. |

## Required subphase trace

The proposed SUT trace should carry one repair ID and monotonic phase enum:

1. `failure_persisted`
2. `derived_state_reset`
3. `source_head_pinned`
4. `genesis_replay_caught_up`
5. `replay_prefix_wal_barrier_succeeded`
6. `semantic_certification_succeeded_for_required_head`
7. `failure_marker_sync_cleared`
8. `ready_published`

Crashes may repeat a phase. They must never skip forward over a missing prerequisite. This trace is SDK-only instrumentation; it must not become production recovery authority or a test-only API/status surface.

## Code evidence

- `internal/storage/balancehistorystore/store.go:303-375` persists failure state synchronously and increments the generation so open/pinned views fail closed.
- `internal/storage/balancehistorystore/store.go:378-446` resets all derived data in a synchronous batch while preserving a durable `REBUILDING` or `SOURCE_MISSING` marker and invalidating archive proofs.
- `internal/application/balancehistory/builder.go:679-702` reloads failure state after restart and schedules a genesis rebuild.
- `internal/application/balancehistory/builder.go:556-592` forces `syncDurability` after the pinned head is caught up and returns on a barrier failure before certification or readiness.
- `internal/application/balancehistory/builder.go:705-748` requires the caught-up head and full `Certify` before calling `CompleteRebuild` or `ClearFailure`.
- `internal/application/balancehistory/builder.go:751-779` requires exact manifest/source equality and no pending repair flags before setting process-local readiness.
- `internal/storage/balancehistorystore/store.go:176-188` serializes `LogData(nil, pebble.Sync)` with every manifest mutation, making all preceding asynchronous history-store writes durable.
- `internal/storage/balancehistorystore/store.go:318-346,449-476` rechecks the required head under `mutationMu` and clears markers with `pebble.Sync`; rebuild completion also verifies the latest physical store. The synchronous marker deletion is the final barrier for mutations that may complete after the builder's pre-certification WAL barrier.
- `internal/application/balancehistory/verifier.go:535-647` pins the actual manifest it certifies and performs scratch replay without mutating the production history store. Concurrent maintenance can publish a later physical layout, but cannot change the certified audit/log prefix.
- `internal/application/balancehistory/builder_test.go:766-944` verifies fail-closed bounded repair and restart recovery with WAL sync before certification and with certification retry.
- `docs/technical/architecture/subsystems/read-path/point-in-time-balances.md:411-423,503-519` documents the forced repair barrier, certification, locked watermark recheck, and marker-clear order.

## Candidate SUT instrumentation

Existing status: **missing**. The current metrics count rebuilds and durability failures, but `existing-assertions.md` records no PIT subphase assertions. Repository Antithesis code places assertions in the SUT and workload; it exposes no PIT test-only phase/status API. Keep the phase trace SDK-only and let the workload observe the public fail-closed/result boundary.

- `Reachable` at each phase above, each with a unique message such as `pit: repair reached durable WAL barrier` and `pit: repair semantic certification succeeded`.
- The primary `AlwaysOrUnreachable` immediately before `ClearFailure` and `CompleteRebuild`, using the successful reset state, current manifest coverage, and audit/log watermarks captured when the forced WAL barrier and subsequent `Certify` call each returned successfully. `LastDurableAuditSequence` is supporting evidence, but the assertion should retain both required watermarks rather than treating the audit watermark alone as the complete phase identity.
- `Reachable`, `pit: repair marker cleared synchronously after certification`, only after the `pebble.Sync` marker deletion returns successfully.
- The no-retry workload `Always` above, which treats no-response transport faults as inconclusive but requires exact repair failure reasons from a reached replica and an exact oracle result once repair completes.

## Open questions

- None.

### Investigation Log

#### Should repair phase state use only SDK assertions or a test-only status surface?

- Examined: `antithesis/scratchbook/existing-assertions.md`; all SUT-side Antithesis imports under `internal/`; workload driver conventions in `tests/antithesis/README.md` and `docs/technical/contributing/testing.md`; PIT provider, metrics, gRPC/HTTP adapters, and repository searches for Antithesis build tags and test-only diagnostic endpoints.
- Found: the repository's established split is SUT-side SDK assertions for dangerous internal transitions plus workload assertions for public behavior. PIT exposes readiness only through the provider's public fail-closed behavior and bounded metrics; no PIT repair-phase API or Antithesis-only server endpoint exists. SDK assertions are catalogued before a crash and therefore already provide the cross-process timeline anchors the property needs.
- Not found: any repository requirement or precedent that internal repair phases must be exposed through a test-only status API.
- Conclusion: resolved in favor of SDK-only phase instrumentation. A new status surface is unnecessary for this property and would create a second observational contract without becoming recovery authority.

#### Does the pre-certification `SyncWAL` cover every write used by certification?

- Examined: `Builder.tick`, `syncDurability`, `completeCaughtUpHistory`, and `markReadyAfterReconciliation`; `Store.SyncWAL`, `ClearFailure`, and `CompleteRebuild`; `HistoryVerifier.Certify` / `verifyPinned`; compaction/tiering NoSync publication comments; repair tests; and the PIT architecture durability section.
- Found: `SyncWAL` holds `mutationMu` and synchronously appends `LogData`, so it covers every production-store mutation completed before that barrier, including the rebuilt audit/log prefix. `Certify` writes only to an isolated scratch store, but its verification view may pin a maintenance layout published after the pre-certification barrier. Both marker-clear methods recheck the required head under `mutationMu`, and their successful `pebble.Sync` marker deletion is a later barrier that durably orders every preceding local history mutation before the fail-closed marker disappears. `CompleteRebuild` additionally runs `verifyLatest` before that deletion.
- Not found: a guarantee that the pre-certification barrier alone covers a physical maintenance manifest published after it; the code does not need that stronger claim because marker deletion supplies the final synchronous barrier.
- Conclusion: resolved by refining the invariant. Track the pre-certification barrier as proof that the rebuilt source prefix is durable, and treat successful synchronous marker deletion as the final durability boundary before readiness.
