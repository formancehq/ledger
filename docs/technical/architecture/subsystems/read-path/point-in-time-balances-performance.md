# Point-in-Time Balance Performance Evidence

This page is the durable benchmark log for the implementation described in
[Point-in-Time Balance Queries](point-in-time-balances.md). Results are kept
separate from design targets: a target is not reported as achieved until the
corresponding before/after and percentile run appears here.

## Reproduction rules

- Record the exact commit, working-tree state, OS, architecture, CPU, command,
  sample count, duration, and dataset generator parameters.
- Report live and PIT reads separately.
- Report effective and insertion axes separately.
- Report hot and cold sources separately; a warmed cold cache is a hot result.
- Use the same primary-store dataset and write workload for before/after write
  comparisons.
- Preserve raw machine-readable output alongside the summary when running the
  full performance suite.
- Do not use account, asset, ledger, or requested timestamp as metric labels.

## Pre-implementation live read baseline

Environment:

```text
date:       2026-07-30
branch:     release/v3.0
base HEAD:  d73fec7ce
os/arch:    darwin/arm64
cpu:        Apple M5 Pro
mode:       current primary Pebble, warm unfiltered AggregateAllVolumes
```

Command:

```bash
nix develop --command bash -c \
  "GOROOT= go test ./internal/query -run '^$' \
  -bench '^BenchmarkAggregateAllVolumes$' -benchmem \
  -benchtime=500ms -count=5"
```

Observed five-sample ranges:

| Current volume keys | Time/op | Bytes/op | Allocs/op |
|---:|---:|---:|---:|
| 1,000 | 0.300-0.311 ms | 548,066-548,162 | 11,897 |
| 10,000 | 2.776-2.816 ms | 5,381,593-5,382,005 | 117,663-117,664 |
| 100,000 | 27.24-28.41 ms | 53,668,728-53,671,076 | 1,175,284-1,175,298 |

This is a microbenchmark baseline, not an end-to-end p50/p95/p99 latency
distribution. It establishes the CPU/allocation cost of the existing hot,
unfiltered aggregate path before the history builder is wired.

## Initial history-store microbenchmark

The first logical-run prototype was measured with one ledger, one asset/color
bucket, 1,000 accounts, one immutable run, a pinned view, and a warm local
Pebble cache.

Command:

```bash
nix develop --command bash -c \
  "GOROOT= go test ./internal/storage/balancehistorystore -run '^$' \
  -bench '^BenchmarkHistoryReadByRunCount/runs=1$' \
  -benchmem -benchtime=100ms -count=1"
```

Observed result:

```text
BenchmarkHistoryReadByRunCount/runs=1-15  73174  1540 ns/op  602 B/op  25 allocs/op
```

This measures only the unfiltered per-asset summary lookup. It excludes API,
filter compilation, builder lag, sync publication, cold fetch, and concurrent
write interference, so it is not evidence for the production latency target.

## Implementation evidence snapshot

The final local evidence below was collected on 2026-07-30/31 on macOS arm64,
an Apple M5 Pro, and Go 1.26.5. Every JSON artifact records the base commit,
base tree, dirty-path count, and a SHA-256 fingerprint of the complete tracked
diff plus untracked files. The base commit for all runs is
`d73fec7ce8c310937372775e6d3202c4e94e452f`.

The fingerprints differ because evidence was captured while the accepted
implementation converged. In particular, the final write gate was frozen after
its first valid local result and was not rerun to seek a pass after test-harness
or documentation changes. Production-path changes represented by that artifact
are the final four-target fan-out, 200 ms ticker, one-second maintenance, two
compactions per pass, threshold four, and five-second history WAL barrier.

| Artifact | Scope | Working-tree fingerprint |
|---|---|---|
| `build/perf/pit-builder-local-2026-07-30.json` | Final local write gate, lag, verifier diagnostic | `3f2b60370453669b95d64a5e750780b4b805bf0c46d289f1be8624fbcf01a91c` |
| `build/perf/pit-fsm-fanout-local-2026-07-30.json` | Four-target delivered FSM versus rejected five-target option | `6995d73a3ff9655756e8090abcdc694ad0ce9da7919c356a0bbb16bd091cc9a8` |
| `build/perf/pit-store-local-2026-07-30.json` | Two-year hot/cold read matrix and store capacity | `78b74af13a982bbb498f386a0d02146cde2ed3d534c9f7d73ed550135b6f87dc` |
| `build/perf/pit-archive-local-2026-07-30.json` | 8.46 MB immutable archive | `78b74af13a982bbb498f386a0d02146cde2ed3d534c9f7d73ed550135b6f87dc` |
| `build/perf/pit-archive-full-2026-07-30.json` | 33.85 MB immutable archive | `78b74af13a982bbb498f386a0d02146cde2ed3d534c9f7d73ed550135b6f87dc` |
| `build/perf/pit-builder-full-phase-only-2026-07-30.json` | 100,000-audit backfill and rebuild only | `fd48863e303e627b37612dff2d750b58d0cac7165b0733c71e064c983e859ab0` |
| `build/perf/pit-store-full-timeout-2026-07-30.txt` | Unchanged full store profile timeout log | `78b74af13a982bbb498f386a0d02146cde2ed3d534c9f7d73ed550135b6f87dc` |

The principal reproduction commands are:

```bash
# Builder acceptance evidence. Do not overwrite the frozen dated artifact.
PIT_PERF=1 PIT_PERF_PROFILE=local PIT_PERF_ENFORCE=1 \
PIT_BUILDER_PERF_OUTPUT="$PWD/build/perf/pit-builder-local-YYYY-MM-DD.json" \
go test ./internal/application/balancehistory \
  -run '^TestBuilderLocalPerformanceEvidence$' -count=1

# Store matrix. The full profile needs an explicit timeout above 30 minutes or
# a phase-splitting redesign; the captured unchanged full profile timed out.
PIT_PERF=1 PIT_PERF_PROFILE=local PIT_PERF_ENFORCE=1 \
PIT_PERF_OUTPUT="$PWD/build/perf/pit-store-local-YYYY-MM-DD.json" \
go test ./internal/storage/balancehistorystore \
  -run '^TestPITLocalPerformanceEvidence$' -count=1

# Backfill/rebuild only: deliberately returns before write-gate measurement.
PIT_PERF=1 PIT_PERF_PROFILE=full PIT_PERF_PHASE_ONLY=1 \
PIT_BUILDER_PERF_OUTPUT="$PWD/build/perf/pit-builder-full-phase-only-YYYY-MM-DD.json" \
go test ./internal/application/balancehistory \
  -run '^TestBuilderLocalPerformanceEvidence$' -count=1
```

Machine identity and the three `PIT_PERF_GIT_*` values should be supplied as
environment variables for a publishable run; the harness writes `unknown` when
they are omitted.

## Synchronous write path and asynchronous lag

PIT does not add a fifth synchronous notification target. The production
fan-out remains at four targets and the builder polls on its 200 ms ticker.
Logical maintenance runs every second and performs at most two threshold-four
merges. Static configuration validation rejects settings whose maximum run
retirement capacity is lower than the ticker publication rate:

```text
maxCompactionsPerPass * (threshold - 1) * TickInterval
    >= MaintenanceInterval
```

The isolated fan-out microbenchmark measured 41 ns p50 / 42 ns p99 for four
targets and 42 ns p50 / 83 ns p99 for the rejected five-target option. At the
full local `Machine.ApplyEntries + SyncWAL` boundary, four targets measured
3.924 ms p50 / 5.987 ms p99 and five targets measured 3.905 ms p50 / 5.911 ms
p99 over 300 samples. That difference is noise-dominated; the important result
is structural: the delivered PIT runtime uses four targets, so it adds no
synchronous fan-out operation to the FSM path. This harness is below Raft
transport/admission and does not include HTTP or gRPC.

The final primary-write gate used five interleaved A/B/A trials. Each steady
trial and each baseline side contributed 1,000 `Commit + SyncWAL` samples on a
separately preconditioned primary store. The steady case ran the real ticker
builder, a proposal every 5 ms, bounded maintenance, and the history store's
five-second WAL barrier. The full verifier was excluded from this acceptance
gate.

| Primary write sample | Samples | p50 | p95 | p99 | Throughput |
|---|---:|---:|---:|---:|---:|
| Combined A baseline | 10,000 | 3.720 ms | 4.641 ms | 6.106 ms | 264.5/s |
| Final PIT steady state | 5,000 | 3.624 ms | 4.576 ms | 6.473 ms | 271.3/s |

The pooled p99 regression is **+6.02%**, which **fails** the `<5%` acceptance
target. The pooled before/after baseline drift is 6.83%, inside its 10% validity
limit. Per-trial p99 regressions ranged from -4.79% to +28.67%, with a +2.20%
median, so the tail remains noisy even though the pooled bracket is valid. This
result must not be rounded into a pass. PIT should remain opt-in and should not
be enabled broadly until a production-like canary confirms the write p99 on the
actual storage topology.

Builder publication lag passes its local gate:

| Tail metric | p50 | p95 | p99 | Target |
|---|---:|---:|---:|---:|
| Audit append to published manifest | 199.87 ms | 201.85 ms | 213.52 ms | p99 `<500 ms` |

At 200 source proposals per second, the maximum transient run count observed at
one level was four, below the calculated burst bound of eight. After producer
stop, ordinary maintenance reconverged to at most three runs per level, below
the threshold of four.

## Backfill, rebuild, and restart

The separate phase-only profile prevents the 100,000-audit run from
recalculating the frozen write gate. It uses the real builder reducer, audit
verification, batches of 200, history publication, and reset/rebuild path.
The source fixture holds fully formed audit/log records in memory, so the rates
below isolate reducer and history-store work; they exclude primary-Pebble or
cold-source fetch latency.

| Phase | Audits | Effects | Elapsed | Audits/s | Effects/s | Resulting runs |
|---|---:|---:|---:|---:|---:|---:|
| Initial backfill | 100,000 | 199,998 | 5.165 s | 19,360 | 38,719 | 500 |
| Reset and rebuild | 100,000 | 199,998 | 5.056 s | 19,779 | 39,558 | 500 |

The initial backfill occupied 85.7 MB, or 428 bytes per effect, before
post-catch-up maintenance. The rebuild's reported 152.6 MB / 763 bytes per
effect is **not** an incremental live-size estimate: it reuses the same Pebble
directory after `Reset`, so obsolete physical files have not necessarily been
reclaimed. Capacity planning must use a fresh-store result or post-compaction
live-size measurement, not that rebuild number.

The 500-run result is a rollout risk. The phase ends before the maintenance
worker reconverges. An ideal threshold-four fold requires approximately 164
merges; at the default two merges per one-second pass that consumes at least 82
seconds of maintenance budget with no new publications. Readiness and canary
tests must measure this post-backfill convergence and must not expose a
500-run interactive view as normal steady state.

The smaller 2,000-audit local run measured a restart verify/resume p99 of
4.89 ms over 50 samples. It does not replace a crash/recovery or cold-source
restart test at production scale.

## PIT read latency: yesterday versus six months

The complete local dataset contains 731 synthetic days, 64 accounts, eight
asset buckets, four colors, 32 postings per day, 25,440 postings / 50,880
effects, and 1% backdated transactions. Daily publication and logical
compaction produced 11 final runs, below the formula limit of 18 and with at
most three runs at any level.

For hot, unfiltered reads, the answer to “is yesterday the same performance as
six months ago?” is: **the implementation avoids a linear age penalty, but it
does not promise identical latency**.

| Axis | Timestamp age | p50 | p95 | p99 |
|---|---:|---:|---:|---:|
| Effective | 1 day | 5.578 ms | 6.826 ms | 7.389 ms |
| Effective | 6 months | 2.710 ms | 2.978 ms | 3.133 ms |
| Effective | 2 years | 2.675 ms | 2.867 ms | 3.015 ms |
| Insertion | 1 day | 2.729 ms | 2.957 ms | 3.125 ms |
| Insertion | 6 months | 2.769 ms | 3.123 ms | 3.493 ms |
| Insertion | 2 years | 2.848 ms | 3.785 ms | 4.149 ms |

The effective six-month/one-day p95 ratio is 0.436, passing the `<=1.20`
target. Six months happened to be faster in this dataset; that is not an age
speedup guarantee. The configured account/asset/color cardinality is equal,
but the timestamp changes included effects, materialized values, and cache
behavior. The intended invariant is bounded run topology, not equal clock time.

Query shape dominates age in the same hot dataset:

| Effective query shape | 1-day p95 | 6-month p95 | 2-year p95 |
|---|---:|---:|---:|
| Unfiltered summary | 6.826 ms | 2.978 ms | 2.867 ms |
| Current-filtered, 16 accounts | 40.436 ms | 21.174 ms | 19.893 ms |
| Grouped | 81.191 ms | 82.014 ms | 78.427 ms |

Grouped maximum-precision color collapse measured 87.43 ms p95; unfiltered
color collapse measured 2.99 ms p95. Short, one-run cardinality probes also
showed increasing p95 with volume identity count: 3.6 microseconds for
64x1x1, 14.4 microseconds for 256x8x1, and 63.8 microseconds for 256x8x4.
Those probes isolate in-run lookup cost and must not be read as two-year API
latencies.

Backdated-rate probes at 0%, 1%, and 50% remained in an 84.9-93.0 microsecond
p95 band across effective and insertion axes on their smaller 180-day
datasets. This supports the no-rewrite design but is not a production
backdating saturation result.

## Cold reads and archive cost

The cold lifecycle used the real composite view and immutable archive codec,
but its object backend was the local filesystem. It retained seven hot and four
cold logical runs; the composite query opened eight archive parts per cold-miss
sample. Fetches are sequential by intersecting run/part.

| Effective query | Cold miss p95: 1d / 6mo / 2y | Verified cache-hit p95: 1d / 6mo / 2y |
|---|---:|---:|
| Unfiltered | 141.9 / 138.9 / 144.9 ms | 24.2 / 19.9 / 20.0 ms |
| Current-filtered, 16 accounts | 222.9 / 224.3 / 219.9 ms | 80.2 / 67.7 / 70.0 ms |
| Grouped | 317.0 / 324.0 / 327.2 ms | 170.1 / 143.3 / 136.6 ms |

Across all cold-miss shapes, the harness observed 2,880 local-file fetches,
eight per query, with 0.795 ms fetch p95. This does **not** measure S3 request,
TLS, network, or cross-AZ latency, and the OS page cache was not flushed. Since
the current fetch path is sequential, remote round-trip latency can dominate
the result roughly once per intersecting part; “yesterday equals six months”
cannot be promised for a real cold deployment until its object store is
measured.

The independent full archive profile encoded, published, and verified a
33.85 MB immutable object in 136.5 ms. Its 200 local-filesystem reads measured
137.7 ms cold-miss p95 and 53.1 ms verified cache-hit p95. The local 8.46 MB
profile measured 42.0 ms and 13.7 ms respectively. These are codec/cache
measurements, not end-to-end PIT API latency.

## Storage, compaction, and replica convergence

The two-year hot dataset occupied 54.4 MB: 2,139 bytes per posting or
1,069 bytes per effect. These values include Pebble representation and the
specific synthetic account/asset/color distribution; they are not a universal
retention formula. The cold lifecycle produced 17.1 MB of archive objects but
did not reduce the immediate local Pebble footprint in this run. Logical
tiering is therefore proven, while physical local-disk reclamation remains to
be measured after obsolete-file cleanup.

Daily aging already left no threshold-eligible run, so the final explicit
compaction diagnostic performed zero logical merges. Pebble reported cumulative
write amplification of 1.157, but this run does not close the compaction
throughput or concurrent-read acceptance row.

Two independent local stores ingested identical effects with different
publication layouts: one store published once and retained one run; the other
published daily, compacted, and retained six runs. Their logical effect digest
and served semantic digest were equal. This proves deterministic local logical
convergence across physical layouts, not networked multi-replica recovery.

The unchanged `full` store profile used 731 days, 512 accounts, 16 asset
buckets, eight colors, 256 postings/day, 1,000 primary samples, and 200 shape
samples. It timed out first at the Go default 10 minutes and once more at the
allowed 30-minute limit, both while executing grouped hot reads through
`AggregateHistoricalVolumes` and Pebble block decompression. It produced no
complete JSON and no assertions. No third attempt was made. This is a **failed
shape/capacity result**, not generic pending work; the harness or query path
must be partitioned/profiled before claiming support for that matrix.

## Full-verifier interference

The semantic verifier is intentionally outside the interactive query path, but
it reads authoritative history, builds a scratch Pebble projection, and streams
both timelines. Its scratch directory is below `--balance-history-dir`.

The final local diagnostic measured primary-write p99 rising from 5.896 ms to
7.720 ms, or **+30.95%**, while a 2,000-proposal full verification took
280.6 ms. `overlappedAllWrites=false`: the verifier completed before all 300
writes, so this is not acceptance evidence and may underestimate or distort a
fully overlapping run. It is nevertheless a real contention signal.

Production rollout should put balance-history/verifier I/O on a dedicated
volume where possible, schedule full replay away from peak write traffic, and
measure shared-volume and dedicated-volume cases. Cold replay bytes, peak
scratch bytes, cleanup behavior, and a fully overlapping write distribution
remain unmeasured.

## Decision and remaining rollout gates

The local implementation evidence supports bounded age behavior, both time
axes, hot/cold/cache paths, 100k audit reconstruction, logical run boundedness,
and deterministic logical convergence. It does **not** support broad GA:

- the final steady write p99 gate fails at +6.02% versus a `<5%` target;
- the full grouped/cardinality store profile does not complete within 30
  minutes;
- a 100k boot/rebuild leaves 500 runs before background maintenance;
- the verifier shows +30.95% local p99 interference in a partial-overlap
  diagnostic;
- real object-store, multi-node, HTTP/gRPC, Raft admission, crash recovery,
  and deployed-volume measurements remain outstanding.

The accepted safe rollout remains opt-in, canary-first, and capacity-planned.
Metadata and all other non-monetary attributes remain current-state-only and do
not add historical storage or write work.

## Completion matrix

| Evidence | Result | Status |
|---|---|---|
| Synchronous writes | Final local A/B/A p99 +6.02%; target `<5%` | **Fail** |
| Builder tail | Local p99 213.52 ms; target `<500 ms` | Pass local |
| Builder backfill/rebuild | 100k audits in 5.17/5.06 s; 500-run convergence debt remains | Partial |
| Full verifier | Local partial-overlap p99 +30.95%; deployed shared/dedicated volumes unmeasured | Diagnostic / pending deployment |
| PIT age | Hot six-month/one-day effective p95 ratio 0.436; no linear age penalty observed | Pass local |
| PIT axis | Effective and insertion measured at 1d/6mo/2y | Pass local |
| PIT shape | Local shapes measured; full grouped profile timed out at 30m | **Fail / timeout** |
| PIT source | Hot, local-filesystem cold miss, verified cache hit measured; real object network absent | Partial |
| Backdating | 0%, 1%, and 50% local matrices measured | Pass local |
| Compaction | Run-count bounds pass; explicit final merge had zero work | Partial |
| Capacity | Local bytes/effect measured; cold physical reclamation and production formula absent | Partial |
| Multi-replica | Equal local logical/semantic digests across layouts; real cluster absent | Partial |
