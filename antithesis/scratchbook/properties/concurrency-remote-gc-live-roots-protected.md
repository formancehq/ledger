# concurrency-remote-gc-live-roots-protected — Remote GC never deletes a live or ambiguously leased root

## Catalog entry

| | |
|---|---|
| **Priority** | P0 |
| **Type** | Safety |
| **Property** | Remote GC must never call `Delete` for a digest referenced by the latest manifest, and it must perform no deletion while any manifest lease makes reset/version ABA ambiguous. |
| **Invariant** | SUT `assert.AlwaysOrUnreachable(!rooted[digest] && activeManifestLeaseCount == 0, "pit: remote GC deletes only unrooted objects with no ambiguous view lease", details)` immediately before each remote delete. `AlwaysOrUnreachable` fits optional cold/GC paths while forbidding one unsafe delete. |
| **Antithesis Angle** | Age candidates through two scans, then interleave view open/close, reset with manifest-version reuse, compaction, archive upload, object deletion, node kill, and delete-before-ack retry. |
| **Why It Matters** | A single false-positive remote delete can permanently destroy the only bytes for a cold run and make valid historical money unavailable. |
| **Confidence** | High — root capture and the conservative all-view block are explicit and regression-tested. |

**Open Questions:**

- None. The current property deliberately requires the conservative all-delete
  block while any manifest lease exists. Generation-aware selective deletion
  is a future optimization and must define a separate property if implemented.

## Evidence

- `internal/storage/balancehistorystore/remote_gc.go:207-224` takes `archiveGate` for writing before root capture and all deletion work.
- `internal/storage/balancehistorystore/remote_gc.go:251-299` captures roots while holding `mutationMu` and `leaseMu`, filters rooted candidates, and acknowledges rooted entries without deleting them.
- `internal/storage/balancehistorystore/remote_gc.go:301-336` performs remote deletion outside `mutationMu` but keeps the archive writer gate, then durably acknowledges each idempotent delete.
- `internal/storage/balancehistorystore/remote_gc.go:740-776` adds every latest manifest archive part and rejects the entire collection call if any manifest lease exists because lease identity is version-only and reset can reuse versions.
- `internal/storage/balancehistorystore/remote_gc_test.go:215` exercises reset/version ABA with an active view; `:247` proves current roots are retired from the queue without deletion.

## SDK instrumentation status

- **Existing:** no PIT SDK assertion; deterministic tests are not catalogued Antithesis properties.
- **Missing SUT assertion:** the exact `assert.AlwaysOrUnreachable` above.
- **Missing guidance:** `assert.Reachable("pit: remote GC blocked deletion because an active view made roots ambiguous", details)` on `ErrRemoteGCRootsUnavailable`, and `assert.Reachable("pit: remote GC retried an idempotent delete after missing acknowledgement", details)` on the persisted retry path.
- **Missing workload validation:** after every GC campaign, query every retained cold watermark and compare to the oracle; exact `SOURCE_MISSING` is a failure unless the campaign intentionally removed the object outside GC.
