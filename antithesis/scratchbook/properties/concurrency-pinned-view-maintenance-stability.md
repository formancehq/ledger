# concurrency-pinned-view-maintenance-stability — Maintenance never creates a mixed pinned view

## Catalog entry

| | |
|---|---|
| **Priority** | P0 |
| **Type** | Safety |
| **Property** | A view that remains valid while publication, compaction, tiering, local GC, and remote GC run concurrently returns exactly the monetary state of its pinned manifest; an explicit reset/failure transition may only make it fail closed. |
| **Invariant** | Workload `assert.Always(outcome.Success ? resultMatchesOracleAt(outcome.View.LogWatermark) : isExactPITFailClosedReason(outcome.Err), "pit: pinned view never returns a mixed maintenance snapshot", details)`. `Always` is appropriate because partial success is forbidden on every evaluated request. |
| **Antithesis Angle** | Stretch a PIT read with cold fetch or a large account set while concurrent drivers publish runs, compact, tier, collect local garbage, scan/delete remote objects, and inject MinIO faults. |
| **Why It Matters** | A mixed view can omit or double-count money while all individual files and manifests appear valid. |
| **Confidence** | High — snapshots and leases directly encode the intended guarantee and deterministic concurrency tests exist, but real process/object-store timing remains untested. |

**Open Questions:**

- None. Add an Antithesis-only pause immediately after the history view pins
  its manifest and leases, release it only after a maintenance publication is
  observed, then complete the public aggregate.

## Evidence

- `internal/storage/balancehistorystore/view.go:27-40` defines a view as one immutable manifest plus the Pebble sequence containing its runs.
- `internal/storage/balancehistorystore/view.go:181-220` creates the snapshot and acquires manifest/run leases under `mutationMu`.
- `internal/storage/balancehistorystore/view.go:337-357` releases cold readers/cache leases, the Pebble snapshot, and manifest/run leases only on `Close`.
- `internal/storage/balancehistorystore/gc.go:30-56` preserves every latest, leased, and prepared run before deleting unreachable bytes.
- `internal/storage/balancehistorystore/hardening_test.go:581` races eight readers with compaction and local GC; `tier_test.go:341` verifies tier defers local removal for a pinned view. These are non-SDK, single-process tests.

## Exact supporting guidance

Add SUT-side `assert.Reachable("pit: pinned view completed after concurrent manifest publication", details)` when a successful view operation finishes with `v.manifest.Version` different from the current latest version. That signal helps Antithesis checkpoint the useful interleaving; it is not a substitute for the workload `Always` oracle.

## SDK instrumentation status

- **Existing:** none for PIT. Generic compaction workload assertions only cover primary-store compaction.
- **Required:** the workload `Always`, the SUT reachability assertion above,
  and the post-pin pause keyed by a workload probe ID. A naturally slow query
  is not accepted as proof of overlap.
