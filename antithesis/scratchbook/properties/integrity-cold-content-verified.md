# integrity-cold-content-verified — Cold bytes are content-verified or PIT fails closed

## Catalog entry

| | |
|---|---|
| **Priority** | P0 |
| **Type** | Safety |
| **Property** | A PIT query never returns success from a missing, truncated, reordered, misrouted, or checksum-mismatched cold run part; verified content-addressed bytes are the only cold bytes admitted to a view. |
| **Invariant** | `Always(success_implies_all_intersecting_cold_parts_verified, "pit: successful cold view uses only verified content-addressed parts")`. Missing content must yield source-missing and corrupt content must yield corruption/quarantine, never a partial aggregate. |
| **Antithesis Angle** | Inject asymmetric MinIO network faults, delete or replace one multipart object, kill during download/cache publication, and concurrently evict/refetch a cached object while requests touch different key ranges. |
| **Why It Matters** | Cold history holds old monetary effects that may no longer exist locally. Trusting partial or wrong bytes silently changes historical balances. |
| **Confidence** | High |
| **Focus** | Data Integrity |

**Open Questions:**

- None. Add a workload helper that uses the campaign's existing MinIO endpoint
  and credentials to copy, replace, delete and restore one exact owned object
  while MinIO data is PVC-backed. Mutating the bucket/PVC as a whole is outside
  this property.

## Evidence trail

- `internal/storage/balancehistoryarchive/store.go:177-267` checks existence, expected checksum, content address, streamed checksum, and encoded archive validity before cache admission.
- `internal/storage/balancehistoryarchive/codec.go:183-276` verifies format/header/trailer, digest, strict record order, and counts while opening an archive.
- `internal/storage/balancehistorystore/view.go:166-168` documents lazy verified cold fetch with archive leases held for the view lifetime.
- `internal/storage/balancehistorystore/view.go:222-245` rejects local-removed runs without an archive configuration/reference.
- `internal/storage/balancehistorystore/verify.go:618-738` maps archive missing vs corrupt conditions into source-missing vs integrity failures.
- `internal/storage/balancehistorystore/tier.go:426-448` verifies every already-archived part still exists before completing tier work.
- `internal/storage/balancehistorystore/tier_test.go:170-228` distinguishes missing required multipart parts from corrupt required parts.
- `internal/storage/balancehistoryarchive/store_test.go:64-116` covers corrupt bytes and checksum mismatches; `store_test.go:314-330` ignores crash temporary files.

## Failure scenario to explore

1. Tier a run into several route-bounded objects and ensure local bytes are removed.
2. Query an account whose keys map to one specific part while delaying or corrupting another part.
3. Delete, truncate, or replace the intersecting part, and separately exercise a non-intersecting part fault.
4. Require exact success for unaffected ranges only when no required object is read; required missing content must return `HISTORY_SOURCE_MISSING`, and corrupt content must return `HISTORY_CORRUPT`/quarantine.
5. Restore the object and verify the documented repair behavior without permitting any partial result.

## Instrumentation status

- **Existing SDK instrumentation:** missing. The archive/store error paths are covered only by ordinary Go tests.
- **Missing SUT-side guidance:** add unique `Reachable` assertions for cold fetch verified, missing part detected, and corrupt part detected; an `Unreachable` should mark any path that returns a record before checksum/codec verification completes.
- **Workload-side check:** missing exact `ErrorInfo.reason` classification and a no-retry client; the current global interceptor would blur the evidence.
