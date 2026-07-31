# concurrency-builder-source-snapshot-archive-purge — Builder source batches survive chapter confirmation and hot purge

## Catalog entry

| | |
|---|---|
| **Priority** | P0 |
| **Type** | Safety |
| **Property** | A builder/verifier source read chooses hot-versus-cold chapter topology from one primary snapshot and returns one consecutive, internally complete audit/log batch even if chapter confirmation purges the corresponding hot keys concurrently. |
| **Invariant** | SUT `assert.AlwaysOrUnreachable(batchIsConsecutiveAndComplete(batch, after), "pit: hot-cold source batch remains complete across concurrent archive purge", details)` after `HotColdSource.Read`. `AlwaysOrUnreachable` fits the optional archived-source path; every reached batch must be coherent. |
| **Antithesis Angle** | Request archive through the real asynchronous archiver, pause source reading after its chapter-registry snapshot, confirm/purge hot ranges, evict/refetch cold chapter readers, and continue builder and verifier replays. |
| **Why It Matters** | Losing one audit item or resolved log during the handoff either corrupts PIT money or permanently marks the source missing despite a valid archive. |
| **Confidence** | High — the snapshot and cold-reader leases are explicit and direct deterministic tests cover both purge and eviction races. |

**Open Questions:**

- None. Wait for the real archiver's `ARCHIVED` registry state and add a
  correlated SUT `Reachable` spanning source-snapshot pin through observed hot
  purge. Public lifecycle state alone does not prove the overlap.

## Evidence

- `internal/application/balancehistory/source_hotcold.go:24-32` documents the snapshot contract: the primary snapshot pins the chapter registry while concurrent confirmation may purge live ranges.
- `internal/application/balancehistory/source_hotcold.go:136-176` opens one primary read handle, reads/validates chapter topology through it, and derives the combined head from the same snapshot.
- `internal/application/balancehistory/source_hotcold.go:344-405` resolves proposal headers using the snapshot-selected archived prefix and hot tail.
- `internal/application/balancehistory/source_hotcold.go:610-687` resolves referenced logs from those same archived ranges before falling back to the pinned hot snapshot and fails closed if any remain unresolved.
- `internal/application/balancehistory/source_hotcold.go:753-797` serializes verified cold-reader acquisition and holds a lease through all iterator work; a pointer change after eviction forces checksum/content revalidation.
- `internal/application/balancehistory/source_hotcold_test.go:205` purges hot keys after snapshot creation and still reads the complete batch; `:241` exercises concurrent cold eviction under a lease.

## SDK instrumentation status

- **Existing:** no PIT SDK assertion. The existing `chapter archive completed` workload assertion proves only Raft confirmation, not cold readability, as `existing-assertions.md` notes.
- **Missing SUT assertion:** the exact `assert.AlwaysOrUnreachable` above after all proposal logs resolve.
- **Required reachability:** record chapter ID and source-read probe ID when the
  primary snapshot pins the registry, then emit
  `assert.Reachable("pit: hot-cold source snapshot survived concurrent chapter purge", details)`
  only when the same read completes after that chapter's hot purge. Never use
  the workload's manual confirmation path.
- **Missing workload check:** archive via the real archiver, prove a cold audit/log read, and compare post-catch-up PIT results to the monetary oracle.
