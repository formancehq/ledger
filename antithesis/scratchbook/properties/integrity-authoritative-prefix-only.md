# integrity-authoritative-prefix-only — PIT publishes only a verified consecutive source prefix

## Catalog entry

| | |
|---|---|
| **Priority** | P0 |
| **Type** | Safety |
| **Property** | Every successful PIT view represents a consecutive audit prefix whose referenced fresh logs are present exactly once, hash-chain valid, and fully included through the response watermarks. |
| **Invariant** | `Always(valid_prefix, "pit: successful view contains exactly one verified consecutive audit and log prefix")`. For each successful response, the workload must independently replay accepted audit items through the trailer's audit/log watermarks and require exact monetary equality. `Always` is appropriate because one omitted, duplicated, or unauthenticated effect is a correctness failure, not an optional-path outcome. |
| **Antithesis Angle** | Kill or partition replicas while proposals, audit items, logs, chapter reads, and the asynchronous builder interleave. Target gaps at batch boundaries and failures/no-log proposals that advance only the audit watermark. |
| **Why It Matters** | Advancing a cursor past one missing item or fresh log would make all later PIT reads silently omit money while still returning success. |
| **Confidence** | High |
| **Focus** | Data Integrity |

**Open Questions:**

- None. The workload maintains its own accepted-operation journal and resolves
  commitment from public audit/log references through the returned watermark;
  a SUT assertion independently proves sequence/log/hash completeness before
  publication. The workload never imports the PIT reducer.

## Evidence trail

- `internal/application/balancehistory/builder.go:1025-1202` validates audit sequence continuity, item count/order, the audit hash chain, fresh-log coverage, duplicate fresh logs, and the source-reported next position before returning effects.
- `internal/application/balancehistory/builder.go:782-925` keeps reduction temporary and calls `Publish` only after the whole bounded source batch verifies.
- `internal/storage/balancehistorystore/publish.go:242-278` rejects backward watermarks, effects outside the newly covered audit interval, log sequences beyond coverage, and coverage-only mutations that are inconsistent with the current manifest.
- `internal/application/balancehistory/source_hotcold.go:408-478` requires consecutive audit entries and consecutive fresh-log ranges while traversing physical source ranges.
- `internal/application/balancehistory/source_hotcold.go:610-687` refuses to return a source batch while any referenced log remains unresolved across cold and hot readers.
- Deterministic regression anchors already exist in `source_hot_test.go` (`TestHotSourceRejectsAuditGap`, `TestHotSourceRejectsMissingItemOrLog`, `TestHotSourceValidatesFreshLogCoverage`) and `source_hotcold_test.go` (`TestHotColdSourceCrossesColdHotBoundaryAtomically`).

## Failure scenario to explore

1. Generate a mix of successful monetary proposals, failed proposals, and non-monetary proposals.
2. Kill or isolate one replica while its local builder is between source reads and publication.
3. Archive/purge a chapter crossing the builder cursor while another replica remains hot-only.
4. Query the affected replica without retries until it either fails closed or returns a complete view trailer.
5. For every success, replay the authoritative prefix through the returned watermarks and compare all account/asset/color input and output totals. A response that skips a missing prefix is never acceptable.

## Instrumentation status

- **Existing SDK instrumentation:** missing. `existing-assertions.md` records no PIT-specific assertion in the builder or store.
- **Missing SUT-side guidance:** add a unique `Always` immediately after `reduceVerifiedBatch` and before `Publish` checking that the verified `next` equals the source batch's `Next` and that every fresh log slot was consumed. This is redundant with returned errors but provides Antithesis search guidance for the dangerous boundary.
- **Workload-side check:** missing. It needs a no-retry per-node PIT client plus an independent audit-prefix monetary oracle keyed by returned watermarks.

## Why existing tests are insufficient

The deterministic tests inject malformed snapshots directly. They do not combine real process death, independent replica lag, Raft advancement, and chapter movement while the builder repeatedly opens fresh primary snapshots.
