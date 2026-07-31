# out-of-order-chapters-recover — PIT recovers after an out-of-order archive prefix becomes complete

## Candidate catalog entry

- **Type:** Liveness
- **Priority:** P1
- **Assertion:** `Sometimes(exactPITAvailableOnEveryReplica)` — after chapter N
  is also archived and faults stop, every replica eventually serves the exact
  result at the common required watermark.
- **Confidence:** Medium. The builder retries and can rebuild, but the combined
  valid-out-of-order transition has no current test.

## Evidence

This property is the progress complement of
`out-of-order-chapters-fail-closed`. Once the archived prefix is
complete, the hot/cold topology validator has a consecutive source again. The
builder is intended to reset, replay, certify and reopen readiness.

Relevant paths:

- `internal/application/balancehistory/source_hotcold.go:217-283`
- `internal/application/balancehistory/builder.go:621-779`
- `internal/storage/balancehistorystore/store.go:303-476`

## Workload and assertion rationale

Use a final quiet period after archiving N. Establish cluster quiescence, then
poll direct per-node PIT reads with a fixed `minLogSequence`. `Sometimes` fits
because the meaningful condition — exact availability on every replica — must
be reached at least once after recovery. Exactness of every individual success
remains an `Always` property in the safety catalog.

## Instrumentation status

The existing harness has generic chapter and cross-node reachability assertions,
but no PIT rebuild/certification or out-of-order-source signal. SUT-side
`Reachable` for certification after this source-invalid path is missing.

## Open questions

- None.
