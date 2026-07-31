# concurrency-tier-lease-precedes-local-delete — Tiering deletes local bytes only after archive durability and lease drain

## Catalog entry

| | |
|---|---|
| **Priority** | P0 |
| **Type** | Safety |
| **Property** | A tier pass may mark a run cold-only only after every archive part is verified durable and while its run lease count is zero; a view opened before the decision keeps its local bytes. |
| **Invariant** | SUT `assert.AlwaysOrUnreachable(!removeLocal || (run.Archived && len(run.ArchiveParts) > 0 && runLeaseCount == 0), "pit: tiering removes local bytes only after durable archive and lease drain", details)`. `AlwaysOrUnreachable` is correct because cold tiering is optional, but every reached local-delete decision must satisfy the condition. |
| **Antithesis Angle** | Pause archive upload/verification and local-delete publication while opening/closing views, compacting the same run, reconfiguring the archive, killing the node, and faulting MinIO. |
| **Why It Matters** | Deleting too early turns a recoverable cache/object-store interruption into PIT data loss or makes an in-flight query read a partially removed run. |
| **Confidence** | High — the two-phase Sync/NoSync protocol and lock order are explicit and have deterministic tests. |

**Open Questions:**

- None. Use an Antithesis-only pause after remote verification and before
  archive-reference publication, keyed by run/probe ID; do not change the
  production MinIO interface.

## Evidence

- `internal/storage/balancehistorystore/tier.go:391-421` holds `archiveGate` for reading across epoch advance, upload, and publication, rejecting stale tiering configuration.
- `internal/storage/balancehistorystore/tier.go:485-524` snapshots and verifies the local run, archives bounded parts, validates total record count, then publishes.
- `internal/storage/balancehistorystore/tier.go:663-732` first publishes durable archive references with `pebble.Sync` while keeping local bytes, checks `runLeases` under `leaseMu` while still holding `mutationMu`, and only then commits local deletion.
- `internal/storage/balancehistorystore/view.go:181-220` acquires the run lease under `mutationMu`, so view-open and local-delete form one linear order.
- `internal/storage/balancehistorystore/tier_test.go:341` proves the lease deferral and later cold read; `:676` revalidates delayed archives before deletion.

## SDK instrumentation status

- **Existing:** none PIT-specific.
- **Missing SUT assertion:** the exact `assert.AlwaysOrUnreachable` above at the phase-two decision.
- **Required reachability:** the post-verification/pre-publication pause and
  `assert.Reachable("pit: tiering deferred local deletion for an active view lease", details)`
  when `runLeaseCount > 0`.
- **Missing workload oracle:** a long PIT read concurrent with tiering must either match the returned watermark oracle or fail with an exact fail-closed reason.
