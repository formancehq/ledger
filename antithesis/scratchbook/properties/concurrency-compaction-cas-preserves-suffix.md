# concurrency-compaction-cas-preserves-suffix — Prepared compaction cannot lose concurrent publications

## Catalog entry

| | |
|---|---|
| **Priority** | P0 |
| **Type** | Safety |
| **Property** | Compaction prepared outside `mutationMu` may replace only its exact selected inputs; it must retain every concurrently published run and preserve the semantic monetary state. |
| **Invariant** | Workload `assert.Always(!compactionCompleted || (postView.LogWatermark >= preView.LogWatermark && resultMatchesOracleAt(postView.LogWatermark)), "pit: compaction CAS preserves concurrent history publications", details)`, with the oracle checking all accepted effects through the returned watermark. `Always` catches any single lost suffix or duplicate merge. |
| **Antithesis Angle** | Schedule builder publication and reset between compaction view open, run reservation, streamed chunk commits, view close, manifest revalidation, and final publication; kill the node around NoSync batches. |
| **Why It Matters** | A stale compaction CAS can silently drop a newly acknowledged monetary effect or publish a run ID that collides with another mutation. |
| **Confidence** | High — the code performs generation and exact-input checks twice and reserves run IDs durably in manifest order; Antithesis adds real crash/scheduling variance. |

**Open Questions:**

- None. The distinction is internal, so add separate SUT `Reachable` signals
  for successful publication and stale prepared-run discard; public results
  remain the correctness oracle.

## Evidence

- `internal/storage/balancehistorystore/compact.go:27-82` streams outside `mutationMu`, uses one `compactionMu`, and publishes only after revalidation.
- `internal/storage/balancehistorystore/compact_stream.go:74-134` checks `generation` and exact inputs, advances `NextRunID` in a manifest reservation, then marks the ID prepared.
- `internal/storage/balancehistorystore/compact_stream.go:328-403` rechecks generation and exact inputs under `mutationMu`, builds from the latest manifest (thereby retaining concurrent new runs), and publishes the replacement atomically.
- `internal/storage/balancehistorystore/compact_stream.go:406-447` discards a stale prepared run without deleting a run that became live.
- `internal/storage/balancehistorystore/tier_test.go:601` and `:635` cover prepared-run GC/crash behavior; `hardening_test.go:581` covers concurrent views/compaction/GC. No test uses process kills or Antithesis assertions.

## SDK instrumentation status

- **Existing:** no PIT SDK instrumentation.
- **Missing workload assertion:** the exact `assert.Always` above.
- **Required SUT guidance:** emit
  `assert.Reachable("pit: compaction CAS published replacement", details)` on
  success and `assert.Reachable("pit: compaction CAS discarded a stale prepared run", details)`
  on generation/input rejection. Immediately before the final batch, assert
  exact selected-input presence with `AlwaysOrUnreachable`.
