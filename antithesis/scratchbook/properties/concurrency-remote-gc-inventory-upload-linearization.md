# concurrency-remote-gc-inventory-upload-linearization — Inventory proof is invalidated before object creation

## Catalog entry

| | |
|---|---|
| **Priority** | P0 |
| **Type** | Safety |
| **Property** | A completed remote inventory is valid only for the archive mutation epoch it scanned; every upload-capable or reset transition advances that epoch before remote I/O, so an in-flight page is discarded and restarted rather than certifying a stale empty namespace. |
| **Invariant** | SUT `assert.AlwaysOrUnreachable(!cycleCompleted || state.CompletedInventoryEpoch == binding.MutationEpoch, "pit: completed remote inventory is bound to the current archive mutation epoch", details)`. `AlwaysOrUnreachable` is correct because remote inventory is optional, but any completed proof must match the current epoch. |
| **Antithesis Angle** | Pause after listing but before page sync, after mutation-epoch persistence but before upload, after upload but before manifest CAS, and across reset/rebuild; inject process kills at each point. |
| **Why It Matters** | A stale empty proof can authorize archive destination rotation while uploaded bytes are abandoned, escaping both the live manifest and future collection. |
| **Confidence** | High — the gate/epoch design and all principal interleavings are represented in code and deterministic regression tests. |

**Open Questions:**

- None. Add SUT assertions at durable inventory-page commit, epoch-change scan
  restart and destination-rotation refusal. The workload correlates them by a
  non-secret epoch/destination digest and then validates the public outcome.

## Evidence

- `internal/storage/balancehistorystore/tier.go:34-37` states that runtime uploads must flow through tiering so the durable mutation epoch advances before remote I/O.
- `internal/storage/balancehistorystore/tier.go:391-421` holds the shared archive gate and synchronously advances the epoch before calling the archive.
- `internal/storage/balancehistorystore/store.go:381-410`, `418-446`, and `557-578` advance the same epoch atomically with rebuild/source-repair/plain reset.
- `internal/storage/balancehistorystore/remote_gc.go:342-405` holds the archive writer gate across remote listing and durable page synchronization.
- `internal/storage/balancehistorystore/remote_gc.go:503-641` compares `ScanEpoch` to the binding epoch; a mismatch resets cursor and partial counters and refuses to complete the stale page.
- `internal/storage/balancehistorystore/remote_gc_test.go:716`, `:776`, `:827`, and `:1132` cover reset-after-list, inventory/upload serialization, CAS-loss orphan inventory, and upload-then-reset proof invalidation.

## SDK instrumentation status

- **Existing:** no PIT SDK signal for epoch advance, scan restart, or inventory completion.
- **Missing SUT assertion:** the exact `assert.AlwaysOrUnreachable` above at completed-page persistence.
- **Required guidance:** emit `Reachable` on durable inventory completion,
  `errRemoteGCScanEpochChanged`, destination-rotation refusal, and tier upload
  CAS loss. Details include the mutation/scan epoch and a destination digest,
  never credentials or object-store secrets.
- **Missing workload check:** after inducing the window, a destination disable/rotation attempt must fail until a fresh, matching-epoch empty inventory completes.
