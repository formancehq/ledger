# integrity-layout-independent-semantics — Compaction and tiering preserve logical PIT state

## Catalog entry

| | |
|---|---|
| **Priority** | P0 |
| **Type** | Safety |
| **Property** | For an unchanged source watermark, publication batching, run IDs, compaction layout, multipart boundaries, and hot/cold placement never change any PIT monetary result or its canonical semantic digest. |
| **Invariant** | `AlwaysOrUnreachable(before_digest == after_digest && before_results == after_results, "pit: layout rewrite preserves semantic history")` at successful compaction/tier publication. The path is workload-dependent, but every execution must preserve semantics, hence `AlwaysOrUnreachable`. |
| **Antithesis Angle** | Interleave publication with streaming compaction, kill after prepared output but before manifest CAS, tier during compaction, and restart between the two tier phases. Vary object-store latency so multipart cold inputs are hydrated mid-compaction. |
| **Why It Matters** | Layout maintenance is not business activity. If it changes balances, replica-local physical differences become user-visible monetary divergence. |
| **Confidence** | High |
| **Focus** | Data Integrity |

**Open Questions:**

- None. The public, bounded timestamp/account matrix is the mandatory oracle.
  A SUT-side semantic-digest assertion is an additional high-frequency
  diagnostic and must not replace public result comparison.

## Evidence trail

- `internal/storage/balancehistorystore/view.go:64-140` reconstructs deltas from cumulative records, merges duplicate logical keys across runs, drops zero deltas, and hashes a layout-independent stream.
- `internal/storage/balancehistorystore/publish.go:187-208` excludes physical run IDs from canonical record checksums.
- `internal/storage/balancehistorystore/compact.go:27-80` verifies selected inputs, streams outside `mutationMu`, then performs a CAS-like publication and discards stale prepared output.
- `internal/storage/balancehistorystore/compact.go:97-120` requires the exact selected descriptors to remain present before publication.
- `internal/storage/balancehistorystore/tier.go:663-732` revalidates the run after upload and uses a two-phase local-to-cold transition.
- `internal/storage/balancehistorystore/semantic_digest_test.go:18-93` compares separate vs combined publication batching and pre/post compaction digests.
- `internal/storage/balancehistorystore/semantic_digest_test.go:139-190` exercises semantic digest over multipart cold-only runs.
- `internal/storage/balancehistorystore/tier_test.go:635-716` covers prepared-run crash orphans and delayed archive revalidation.

## Failure scenario to explore

1. Build a history containing backdated effects, multiple assets/colors, and several ledger IDs.
2. Capture public PIT results at boundary timestamps and the source/trailer watermark.
3. Force compaction and tiering while concurrent writes continue, then quiesce at the original or a later known watermark.
4. Compare each result against the oracle for that exact watermark. If an internal diagnostic is available, also compare the semantic digest before/after every successful layout transition.
5. Repeat after killing the node at prepared-output, upload-before-CAS, archive-reference publication, and local-removal points.

## Instrumentation status

- **Existing SDK instrumentation:** missing; deterministic digest and maintenance tests are not Antithesis SDK assertions.
- **Required SUT-side guidance:** place one unique `AlwaysOrUnreachable` at successful compaction publication and another at completed tier local removal. Capture the pinned input semantic digest and compare to the newly published view only in Antithesis-enabled/test builds if production cost is too high.
- **Required workload check:** implement a bounded fixture containing every
  supported monetary option, account selector, asset/color and boundary
  timestamp used by the campaign. Compare the full matrix through public APIs
  before and after each reached rewrite at the same source watermark.
