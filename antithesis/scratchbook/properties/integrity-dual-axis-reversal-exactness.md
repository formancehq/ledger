# integrity-dual-axis-reversal-exactness — Effective and insertion axes fold each monetary effect exactly once

## Catalog entry

| | |
|---|---|
| **Priority** | P0 |
| **Type** | Safety |
| **Property** | At every queried timestamp, effective-time and insertion-time PIT aggregates equal an independent fold of resolved postings and compensating reversal postings, with boundary timestamps included atomically and no double inversion. |
| **Invariant** | `Always(pit_result == oracle_fold(axis, timestamp, returned_watermark), "pit: effective and insertion axes exactly fold resolved postings and reversals")`. Every successful monetary response must be exact; eventual or reachability semantics would be too weak. |
| **Antithesis Angle** | Reorder effective time relative to insertion time, commit reversals while builders lag, kill around same-timestamp multi-effect publication, and compare replicas with different publication/compaction layouts at a common source watermark. |
| **Why It Matters** | The two axes answer different accounting questions. Applying a reversal on the wrong axis, exposing half of a same-timestamp pair, or reversing an already-compensating posting changes historical balances without any storage error. |
| **Confidence** | High |
| **Focus** | Data Integrity |

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

1. Generate direct, Numscript, and mirror-resolved transactions with insertion times after/before varied effective timestamps.
2. Revert some normally and some at the original effective date.
3. Query immediately before, exactly at, and immediately after every effective/insertion boundary while faults interrupt builder progress.
4. Compare account and ledger-wide asset totals to an independent committed-log oracle through the response watermark; also require total inputs equal total outputs per asset/color.
5. Repeat after compaction/tiering and across replicas at a common watermark.

## Instrumentation status

- **Existing SDK instrumentation:** missing. Compatibility and store tests are deterministic Go assertions only.
- **Missing SUT-side guidance:** workload-side `Always` is authoritative. Add SUT `Reachable` markers for backdated effect, normal reversal, and at-effective-date reversal reduction so Antithesis can seek the semantic boundaries.
- **Workload-side check:** volume consistency helpers exist, but no dual-axis resolved-log oracle or PIT trailer-aware comparison exists.
