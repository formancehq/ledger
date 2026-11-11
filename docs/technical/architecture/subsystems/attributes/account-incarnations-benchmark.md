# Account incarnation benchmark prototype

This branch measures the cost of a cache-backed `AccountIncarnation` attribute before account lifecycle rules change.

## Write path

Admission declares an `AccountIncarnation` key for each non-`world` account whose volume a posting can touch. The key is `(ledger, address)`. The FSM checks the declared key through the proposal-wide coverage gate after processing the orders. If a normal account has no incarnation, the FSM writes `{opened_at_raft_index, persistence}` to the attribute and cache zones in the same batch as the volume update. Later postings read the attribute without rewriting it.

The attribute uses the standard two-generation cache. Rotation can evict the in-memory entry; a subsequent admission loads it from the primary attribute zone. There is no always-resident map of accounts in the FSM.

This prototype has no dedicated Bloom filter for incarnation keys. A cache miss checks Pebble, including when the account has no incarnation yet.

The probe runs only when the execution plan contains an incarnation key. This lets older hand-built test plans retain their existing shape. Production admission adds the key through `addVolumeNeed`.

## Deliberate limits

This is a benchmark prototype, not a release-ready account model. It creates incarnations only for normal accounts touched by postings. It does not cover accounts created only by metadata, close an incarnation, enforce a pinned type, or change ephemeral and transient volume behavior. It does not yet verify the new projection in the checker or define an upgrade from existing persisted state. Do not ship this branch as a behavior change.

## Local benchmark

Run the paired admission benchmarks on the same checkout and host:

```bash
go test ./internal/infra/plan -run '^$' -bench 'BenchmarkAdmissionCoverage' -benchtime=1s -count=3 -benchmem
go test ./internal/infra/state -run '^$' -bench '^BenchmarkEnsureAccountIncarnationsHot$' -benchtime=100x -count=3 -benchmem
```

`BenchmarkAdmissionCoverage` is the volume-only baseline. `WithIncarnations` adds a hot incarnation key per account. `WithColdIncarnations` persists those keys in Pebble but leaves them outside the cache, modeling the first admission after rotation. The benchmark builds coverage, resolves preloads, and assigns coverage bits. It excludes Raft apply, Pebble commit, and network latency; its ratios must not be interpreted as end-to-end transaction latency.

On an Apple M5 Pro (Darwin arm64), the median of three 100-iteration runs was:

| Shape | Volume-only | Hot incarnations | Cold incarnations |
| --- | ---: | ---: | ---: |
| 10 orders, 1 posting each | 17.9 µs | 25.8 µs | 53.5 µs |
| 50 orders, 2 postings each | 90.4 µs | 139.7 µs | 355.4 µs |
| 100 orders, 5 postings each | 351.8 µs | 663.6 µs | Not measured |

The hot 50-order case rises from 1,226 to 1,755 allocations per operation. These synthetic orders use a new source and destination address for every posting, so they represent a high-cardinality case. A follow-up benchmark should measure repeated addresses and the full FSM commit path before deciding whether every normal account needs this attribute.

The isolated FSM step with hot incarnation entries measured 0.5 µs for 2 accounts, 5.8 µs for 20 accounts, and 31.3 µs for 100 accounts (medians of three runs). It includes volume-key collection, account deduplication, coverage checks, and cache reads. It excludes posting arithmetic and persistence.
