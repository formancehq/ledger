# replay-remote-delete-ack-is-idempotent — Remote delete-before-ack recovery is idempotent

## Candidate catalog entry

- **Type:** Liveness
- **Priority:** P1
- **Assertion:** `Sometimes(reclaimedAndCandidateCleared)` — after reaching a
  delete-before-durable-ack crash and entering a fault-free window, the repeated
  delete succeeds idempotently and the durable candidate queue clears.
- **Confidence:** High for the intended protocol; medium for black-box
  observability without SUT instrumentation.

## Evidence

The collector deletes the content-addressed object outside the store mutation
lock and only then synchronously acknowledges candidates. A crash between those
steps leaves the candidate durable. Retrying exact `DeleteObject` is intended to
be safe when the object is already absent.

Relevant paths:

- `internal/storage/balancehistorystore/remote_gc.go:235-340`
- `internal/storage/balancehistorystore/remote_gc_test.go:102-190`
- `internal/infra/coldstorage/s3.go:208-269`

## Failure scenario and oracle

Create an unreferenced object in the replica-owned namespace, let it survive two
complete inventories and grace, then terminate the owning Ledger after remote
delete and before local acknowledgement. On restart and without further faults,
the collector should retry, treat absence as success, remove the candidate and
continue inventory progress. Current/pinned objects remain covered by the
separate root-safety property.

## Instrumentation status

No existing SDK assertion identifies candidate eligibility, remote-delete
completion, delete-before-ack or acknowledgement recovery. Add stable SUT-side
`Reachable` signals for the crash window and retry, plus a `Sometimes` condition
that includes both remote absence and durable candidate removal. A workload-only
check cannot inspect the candidate queue.

## Open questions

- None for the non-versioned MinIO profile. Versioned-bucket physical reclaim is
  a separate lifecycle-policy concern.
