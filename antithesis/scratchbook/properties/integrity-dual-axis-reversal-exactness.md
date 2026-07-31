# integrity-dual-axis-reversal-exactness — Effective and insertion axes fold each monetary effect exactly once

## Catalog entry

| | |
|---|---|
| **Priority** | P0 |
| **Type** | Safety |
| **Property** | At every queried timestamp, effective-time and insertion-time PIT aggregates equal an independent fold of resolved postings and compensating reversal postings, with boundary timestamps included atomically and no double inversion. |
| **Invariant** | `Always(pit_result == oracle_fold(axis, timestamp, returned_watermark), "pit: effective and insertion axes exactly fold resolved postings and reversals")`. Every successful monetary response must be exact; eventual or reachability semantics would be too weak. |
| **Antithesis Angle** | Reorder effective time relative to insertion time, commit reversals while builders lag, and kill around same-timestamp multi-effect publication. Explicit common-watermark cross-replica comparison and observed compaction/tiering coverage remain follow-up extensions. |
| **Why It Matters** | The two axes answer different accounting questions. Applying a reversal on the wrong axis, exposing half of a same-timestamp pair, or reversing an already-compensating posting changes historical balances without any storage error. |
| **Confidence** | High |
| **Focus** | Data Integrity |

**Implementation:** Implemented. `first_default_ledger` seeds the isolated
`pitaxis-oracle` corpus before faults; `parallel_driver_pit_dual_axis` samples
one direct-replica boundary during chaos; `eventually_pit_dual_axis` exhausts
every boundary and account on every resolved replica through the complete log
watermark after writers stop. This first implementation deliberately freezes
the effect corpus before chaos; mutations and ambiguous retries during faults
remain an incremental extension and are not claimed by the current campaign.

**Open Questions:**

- None. The first P0 corpus uses explicit postings and reversals, which the
  workload owns independently. Numscript/mirror attempts enter the oracle only
  after the workload fetches their committed resolved transaction through the
  public API; an unreachable generation path is reported as missing coverage,
  never approximated with SUT reducer code.

## Evidence trail

- `internal/domain/balancehistory/reducer.go:78-94` records both timestamps and emits one source-output plus destination-input effect per resolved posting.
- `internal/domain/balancehistory/reducer.go:288-303` feeds already-compensating reversal postings through the normal reduction path and explicitly avoids a second inversion.
- `internal/storage/balancehistorystore/publish.go:55-84` materializes every effect on both effective and insertion axes and both account and asset scopes.
- `internal/storage/balancehistorystore/publish.go:143-166` sorts by timestamp/audit/order/log and combines all same-timestamp deltas before emitting one cumulative boundary, preventing half-visible same-time effects.
- `internal/storage/balancehistorystore/store_test.go:66-110` proves a backdated insertion changes the effective axis earlier while the insertion axis changes only at commit time.
- `internal/query/pit_v2_compatibility_test.go:105-172` compares an independent move fold across boundary timestamps, normal reversals, and `at_effective_date` reversals.
- `internal/query/pit_v2_compatibility_test.go:174-183` confirms transient/ephemeral accounts remain in additive history even if absent from the live projection.

## Failure scenario to explore

1. Generate one property-owned batch containing backdated, future and same-insertion-time direct transactions. Add Numscript and mirror variants only after authenticating their resolved public transactions.
2. Revert some normally and some at the original effective date.
3. Query immediately before, exactly at, and immediately after every effective/insertion boundary while faults interrupt builder progress.
4. Compare account and ledger-wide asset totals to an independent committed-log oracle through the response watermark; also require total inputs equal total outputs per asset/color.
5. Future extension: prove compaction/tiering occurred, then repeat across
   replicas pinned to one explicit common watermark.

## Instrumentation status

- **Implemented fixture and reachability:** `SeedPITDualAxisFixture` commits four
  explicit transactions in one proposal, including backdated and future
  effective timestamps, then commits one normal reversal and one
  `at_effective_date` reversal. Three unique `Reachable` assertions prove all
  semantic forms were accepted before chaos starts.
- **Implemented independent oracle:** `LoadPITDualAxisOracle` authenticates the
  property-owned postings, reversal directions, contiguous ledger-local IDs,
  effective timestamps, the shared insertion timestamp of the four-order
  proposal, and the six outer global log sequences through `ListLogs`, without
  calling the SUT reducer or reading the balance-history store.
  `FoldPITDualAxis` includes a proposal only after its actual global end
  watermark; unrelated global logs before, between or after the three property
  proposals remain legal cutoffs. Compensating postings are treated as already
  reversed.
- **Implemented fault-time safety:** `parallel_driver_pit_dual_axis` samples a
  direct replica, both axes, every `t-1/t/t+1` neighborhood and either the full
  ledger or one exact account. Every successful response fires
  `Always(..., "pit: effective and insertion axes exactly fold resolved postings and reversals")`;
  classified fail-closed responses are inconclusive.
- **Implemented quiescent completeness:** `eventually_pit_dual_axis` requires
  every generated boundary/scope case on every resolved replica with
  `minLogSequence=oracle.MaxLogSequence()`, the authenticated global sequence
  of the final property log. Each replica may legitimately return a later
  watermark; this is a per-replica exact fold, not yet an explicit
  common-watermark comparison. Ledger-wide samples additionally assert
  input/output conservation independently for every asset/color bucket.
- **Future maintenance coverage:** the current command does not assert that a
  compaction or tiering event occurred. A later campaign must add independent
  maintenance evidence before claiming layout-transition coverage.
- **Optional SUT-side guidance:** reducer-side markers are no longer required
  for coverage because the fault-free first command deterministically commits
  all three semantic forms. They may still improve search localization in a
  future campaign, but the public workload `Always` remains authoritative.
