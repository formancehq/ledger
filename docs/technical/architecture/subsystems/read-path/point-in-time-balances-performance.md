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
- Run the `full` profile as explicit `PIT_PERF_PHASES` partitions when the
  monolithic profile cannot finish inside the declared test timeout. Keep one
  JSON artifact per partition; never concatenate samples from different
  machines, commits, profiles, or dataset parameters.
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
# phase partitioning; the captured unchanged monolithic full profile timed out.
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

The store harness schema version 2 accepts comma-separated `PIT_PERF_PHASES`
and `PIT_PERF_CASES` selections. An unset value (or `all`) preserves the
original complete sequential run. The available phases are:

```text
hot-unfiltered  hot-filtered  hot-grouped  hot-shapes
compaction      replica-digest
cold-unfiltered cold-filtered cold-grouped
cardinality     backdating    write
```

The case selector applies to the hot/cold age matrices and accepts:

```text
effective-1d  effective-6mo  effective-2y
insertion-1d  insertion-6mo  insertion-2y
```

Each artifact records `complete`, `selectedPhases`, `selectedCases`, and
`harnessElapsedMs`. Partial artifacts also carry an explicit warning in
`pending`. For the `full` profile, run every hot/cold shape one matrix case at
a time so an expensive grouped read cannot erase another completed case:

```bash
for phase in hot-unfiltered hot-filtered hot-grouped \
             cold-unfiltered cold-filtered cold-grouped
do
  for matrix_case in effective-1d effective-6mo effective-2y \
                     insertion-1d insertion-6mo insertion-2y
  do
    PIT_PERF=1 PIT_PERF_PROFILE=full \
    PIT_PERF_PHASES="$phase" PIT_PERF_CASES="$matrix_case" \
    PIT_PERF_ENFORCE=1 \
    PIT_PERF_OUTPUT="$PWD/build/perf/pit-store-full-${phase}-${matrix_case}-YYYY-MM-DD.json" \
    go test ./internal/storage/balancehistorystore \
      -run '^TestPITLocalPerformanceEvidence$' -count=1 -timeout=30m || exit 1
  done
done

for phase in hot-shapes compaction replica-digest cardinality backdating write
do
  PIT_PERF=1 PIT_PERF_PROFILE=full PIT_PERF_PHASES="$phase" \
    PIT_PERF_ENFORCE=1 \
    PIT_PERF_OUTPUT="$PWD/build/perf/pit-store-full-${phase}-YYYY-MM-DD.json" \
    go test ./internal/storage/balancehistorystore \
      -run '^TestPITLocalPerformanceEvidence$' -count=1 -timeout=30m || exit 1
done
```

The phase artifacts form one evidence set only when their code provenance,
machine identity, profile, and dataset parameters are identical. The phase
split removes the all-or-nothing reporting failure; it does not make a slow
query faster or turn a timed-out phase into a passing result.

Machine identity and the three `PIT_PERF_GIT_*` values should be supplied as
environment variables for a publishable run; the harness writes `unknown` when
they are omitted.

### Current-branch post-bootstrap rerun

The local profile was rerun on 2026-07-31 after the empty-source bootstrap fix
and the Antithesis PIT workload landed. The source commit was `dbec9ee6f` on
`feat/point-in-time-balances-v3`; the only dirty paths were pre-existing user
files outside the PIT scope. The machine was an Apple M5 Pro running
darwin/arm64 and Go 1.26.5.

| Artifact | SHA-256 | Test duration |
|---|---|---:|
| `build/perf/pit-store-local-2026-07-31-post-bootstrap.json` | `94d31c8328d23af00b733740e2f92d30cef2190faf7dab3b70b887be523d7f91` | 325.66 s |
| `build/perf/pit-builder-local-2026-07-31-post-bootstrap.json` | `c788fd19a3ac7fa5661ce278bb92829ba81ad7cbb7cfc59cc035ff67bd605266` | 214.31 s |

Both tests ran with `PIT_PERF_ENFORCE=1` and passed every acceptance
assertion. The raw artifacts are intentionally ignored build outputs; the
checksums above bind the summarized local evidence without treating generated
samples as source files.

### Current-branch functional validation

The completed local implementation was revalidated on 2026-07-31 at source
commit `63328307d6d9ca1e89447b4aa89deaf14c8c209e`, on the same branch and
five-unrelated-dirty-path boundary. Every command below passed:

```bash
# Entire default-feature root module.
nix develop --command bash -c \
  'go test ./... -count=1 -timeout=20m'

# Operator module.
nix develop --command bash -c \
  'cd misc/operator && go test ./... -count=1 -timeout=20m'

# Every Antithesis workload command plus its shared-unit tests.
nix develop --command bash -c \
  'cd tests/antithesis/workload && go test ./... -count=1 -timeout=20m'

# Native business contract: both axes, reversal timing, transformations,
# provenance trailer, read-after-write floor, allowlist and failure modes.
nix develop --command bash -c \
  'go test -tags e2e ./tests/e2e/business -run "^TestBusiness$" \
    -count=1 -timeout=10m -ginkgo.focus="Point-in-time balances" -ginkgo.v'

# Three-node forwarding and follower-local convergence.
nix develop --command bash -c \
  'go test -tags e2e ./tests/e2e/cluster -run "^TestCluster$" \
    -count=1 -timeout=10m \
    -ginkgo.focus="Point-in-time balances forwarding" -ginkgo.v'

# Required repository checks and compilation.
nix develop --command bash -c 'just pre-commit && GOROOT= go build ./...'
```

The local Compose configuration was also rendered against the filtered PIT
workload images and `snouty validate` observed a ready three-node cluster,
`setup_complete`, one `first_`, two driver, and one `eventually_` command. This
proves packaging and bootstrap only. It is not an Antithesis fault-search
result.

### Current-branch Antithesis fault-search evidence

Two two-hour cloud campaigns completed on 2026-07-31. Antithesis records the
requested 120-minute duration as `custom.duration="2"`; the completion times
include approximately nine minutes of finalization after fault exploration.
Use `snouty runs show <run_id> --web` to open either report without copying its
signed URL.

| Role | Run ID | Result |
|---|---|---|
| Pre-fix scope-equivalence baseline | `dd4951b02c3adea9de3a066efdbb816c-58-10` | Completed; 69/89 properties passing, but setup is invalid |
| Corrected dual-axis campaign | `93d98919f9f174b5ab20109b8cf00314-58-10` | Completed; 82/100 properties passing and every targeted PIT property passing |

The baseline is not PIT correctness evidence. Its `first_default_ledger`
command failed 6,289 times because the old convergence assets
`PIT-CONVERGENCE-1-PRE` and `PIT-CONVERGENCE-2-POST` violate the v3 asset
grammar. The resulting setup abort also made the scope fixture ledger absent.
Commit `566268741` replaced those assets with valid identifiers before the
targeted image was built. The same baseline also observed two shutdowns with
`closing store: leaked iterators: current`; this is a distinct, non-attributed
shutdown signal to investigate separately, not a demonstrated PIT result.

The corrected campaign used workload commit `a3c179468` plus fixture fix
`566268741`, config tag `pit-dual-axis-20260731-d8b65cf`, Ledger image digest
`sha256:a88f0f1afa141df669eff291b0d42f4514d17bb5a6db1ddefe5cf22528af26d5`,
and workload digest
`sha256:9ea4a56d7fd6948d0ccf30bbad7511fa4b22d215f851191f7c1dbaee1588f76c`.
Its targeted observations were:

| Property | Passing evaluations | Counterexamples |
|---|---:|---:|
| Exact effective/insertion fold across postings and reversals | 28,746 | 0 |
| View watermark is an acknowledged atomic log boundary | 28,749 | 0 |
| Canonical flat volume buckets | 28,746 | 0 |
| Per-asset and per-color monetary conservation | 10,492 | 0 |
| Fault-time immutable view reached | 21,862 | Property passed; 8,252 inconclusive evaluations |
| Every boundary and scope covered on every replica | 7,042 | Property passed; 68 incomplete evaluations |
| Backdated effect, effective-date reversal, and normal reversal fixtures | 4,740 each | 0 |

`first_default_ledger` completed successfully 4,865 times and the report's
unexpected-container-exit property passed. The 18 report-level failures do not
contain a PIT safety counterexample: one is the environment image-age
metaproperty, fourteen are unrelated assertion sites that were never reached,
and three are generic `Sometimes` coverage probes that never became true. The
campaign therefore closes the targeted three-node Antithesis fault-search gate for the
dual-axis monetary projection; it does not close real object-store latency,
production-volume interference, or every unrelated repository-wide coverage
probe.

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

The current-branch rerun used the same five-trial A/B/A method and produced a
valid, passing bracket:

| Primary write sample | Samples | p50 | p95 | p99 |
|---|---:|---:|---:|---:|
| Combined A baseline | 10,000 | 3.627 ms | 4.333 ms | 5.347 ms |
| PIT steady state | 5,000 | 3.431 ms | 4.155 ms | 5.281 ms |

The rerun's pooled p99 change is **-1.22%**, passing the `<5%` target, and its
before/after baseline drift is 1.63%, inside the 10% validity bound. Per-trial
p99 changes ranged from -9.00% to +1.92%, with a -0.15% median. This newer run
supersedes the local acceptance state for the current commit, but does not
erase the earlier +6.02% failure: together they show why a production-like
canary remains a rollout gate rather than allowing a broad default enablement.

Builder publication lag passes its local gate:

| Tail metric | p50 | p95 | p99 | Target |
|---|---:|---:|---:|---:|
| Audit append to published manifest | 199.87 ms | 201.85 ms | 213.52 ms | p99 `<500 ms` |

The current-branch rerun measured 199.98 ms p50, 201.41 ms p95, and 206.84 ms
p99 over 60 samples, also passing the `<500 ms` gate. Its 2,000-audit local
backfill processed 21,590 audits/s, rebuild processed 28,871 audits/s, and
restart verify/resume measured 4.90 ms p99 over 50 samples.

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

The current-branch rerun confirms the bounded-age conclusion with a different
latency ordering:

| Axis | Timestamp age | p50 | p95 | p99 |
|---|---:|---:|---:|---:|
| Effective | 1 day | 8.638 ms | 10.312 ms | 13.300 ms |
| Effective | 6 months | 5.205 ms | 11.081 ms | 14.819 ms |
| Effective | 2 years | 3.183 ms | 4.261 ms | 6.704 ms |
| Insertion | 1 day | 3.751 ms | 4.424 ms | 5.258 ms |
| Insertion | 6 months | 5.540 ms | 9.402 ms | 12.320 ms |
| Insertion | 2 years | 3.824 ms | 5.431 ms | 8.925 ms |

For the effective axis, six months is 1.0745x the one-day p95, or 7.45% slower,
and still passes the `<=1.20x` gate. Therefore the supported claim is not
"identical latency regardless of age". It is "no linear age penalty while the
logical run topology remains bounded". Query shape, intersecting runs, cache
state, and result cardinality remain the primary latency variables.

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

The current-branch rerun again opened eight parts per unfiltered cold miss. Its
effective-axis p95 was 168.2 ms at one day, 217.3 ms at six months, and 164.0 ms
at two years. Verified cache-hit p95 was 49.0 ms, 28.6 ms, and 22.6 ms
respectively. This local-filesystem variance reinforces that cold latency is
driven by intersecting object reads and cache behavior, not by timestamp age
alone; it is not an S3 latency claim.

The independent full archive profile encoded, published, and verified a
33.85 MB immutable object in 136.5 ms. Its 200 local-filesystem reads measured
137.7 ms cold-miss p95 and 53.1 ms verified cache-hit p95. The local 8.46 MB
profile measured 42.0 ms and 13.7 ms respectively. These are codec/cache
measurements, not end-to-end PIT API latency.

### Full cold matrices after case partitioning

The complete full-profile cold matrix was captured on 2026-07-31 at commit
`b38a18e106a54433287ede9929f27dc0617800f7`, tree
`c05b700ed498063ba68a895a593f705d9b707b0c`, on Apple M5 Pro,
darwin/arm64, Go 1.26.5. The five dirty paths were the same unrelated
instruction and local-environment files. Every case used the same 731-day,
512-account, 16-asset, eight-color, 256-postings/day dataset. It contained
505,344 effects and retained 11 logical runs: seven hot and four cold. The
archive held 32 immutable parts; every cold-miss sample opened eight
intersecting parts sequentially.

Each artifact is one intentionally partial phase/case selection
(`complete=false`), not an incomplete execution. All 18 artifacts are valid
schema-version-2 JSON, contain 50 cold-miss and 200 verified cache-hit samples,
and have matching provenance and dataset parameters. Their filename pattern is
`build/perf/pit-store-full-cold-<shape>-<axis>-<age>-b38a18e10.json`:

| Shape | Axis / age | SHA-256 |
|---|---|---|
| Unfiltered | Effective / 1 day | `b5aceec94e7ec55733d4be897741a10d48859b57aaa3775fb4c8a797e4a8f526` |
| Unfiltered | Effective / 6 months | `cd9a7b733df897f827b5f1cbdcae24856c63d858fc1af5f7724f3deb45501455` |
| Unfiltered | Effective / 2 years | `6aa0a19f13cf9abe5d441c29bc05d4c4785f40fd8aed9751f380940000412ca5` |
| Unfiltered | Insertion / 1 day | `8138955ab4c5a3b137ebcefd07f8b0ca20af5b6991759a5122ac8abc91efb985` |
| Unfiltered | Insertion / 6 months | `9c464c69224a3c76a06c8c3ea22ad7796a8011f3059ef5e6e95ea414ca7445a3` |
| Unfiltered | Insertion / 2 years | `e5faa681ec8b8a5c84ee427a7ad0aa4ded40b86bd69fd7acbbc7eec6ef7991fb` |
| Filtered, 16 accounts | Effective / 1 day | `fe53726c167f3418248ca62fdea684ac90f2d1d25ed50a2dee8297ddc3d3d573` |
| Filtered, 16 accounts | Effective / 6 months | `de64b81ea23d9b23c29014f5eecdabf3c74ad8da98efa209e70ed6c0c1a31307` |
| Filtered, 16 accounts | Effective / 2 years | `dcf76a54b08b04a5a465bd46e93c853136159589f230831100ca209d8fba6f01` |
| Filtered, 16 accounts | Insertion / 1 day | `53bf4eb22bbaabec26efb47233ba889a8551505d2d62afbc2317a46701ee5985` |
| Filtered, 16 accounts | Insertion / 6 months | `6ac4add3f807a2d5c6656b5bd68f9580ce62c97eead531839a45cda55301bf93` |
| Filtered, 16 accounts | Insertion / 2 years | `165675e4c25d23db4f4a35fa6577eb47722ab01f40aaf5b741b32c9dd0499949` |
| Grouped | Effective / 1 day | `976d6fb161a37524bb5ff1d3b372459e3ccfcda62b8831340bc9bc4ae7451a0c` |
| Grouped | Effective / 6 months | `358fae779318bb4fc089c920d42f907885f193260257927cfff49a3ae3b52730` |
| Grouped | Effective / 2 years | `f4a43a8418155f940f0cb32f41381bb0c2e4e0033d58bba7edcf27464fc84aaa` |
| Grouped | Insertion / 1 day | `77f223720b038d18209e4ea6ced3309fc869ec51f36c4a406107c2b8f21e1fe1` |
| Grouped | Insertion / 6 months | `5bb6b71300b134429ff97d197641d384e120ff80b77498a9c8ec24d10bc6269c` |
| Grouped | Insertion / 2 years | `eb2665b99e902a328ee980a3457f25a49879bb12575ee2ba0471fe10758db37f` |

The unfiltered case harnesses completed in 77.3-83.6 seconds, filtered cases
in 205.1-210.5 seconds, and grouped cases in 695.7-782.3 seconds:

| Shape | Axis | Age | Cold miss p50 | p95 | p99 | Cache hit p50 | p95 | p99 |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| Unfiltered | Effective | 1 day | 291.904 ms | 306.043 ms | 310.007 ms | 115.332 ms | 119.930 ms | 134.365 ms |
| Unfiltered | Effective | 6 months | 273.123 ms | 282.912 ms | 298.934 ms | 103.440 ms | 107.839 ms | 121.503 ms |
| Unfiltered | Effective | 2 years | 290.104 ms | 298.943 ms | 307.003 ms | 115.771 ms | 121.662 ms | 132.944 ms |
| Unfiltered | Insertion | 1 day | 293.029 ms | 299.014 ms | 302.902 ms | 121.062 ms | 132.461 ms | 146.072 ms |
| Unfiltered | Insertion | 6 months | 275.918 ms | 282.071 ms | 288.996 ms | 107.821 ms | 112.750 ms | 121.280 ms |
| Unfiltered | Insertion | 2 years | 292.963 ms | 319.091 ms | 336.909 ms | 119.052 ms | 131.234 ms | 134.706 ms |
| Filtered, 16 accounts | Effective | 1 day | 1.057 s | 1.101 s | 1.139 s | 516.163 ms | 546.494 ms | 584.583 ms |
| Filtered, 16 accounts | Effective | 6 months | 1.073 s | 1.189 s | 1.206 s | 516.312 ms | 545.734 ms | 854.947 ms |
| Filtered, 16 accounts | Effective | 2 years | 1.059 s | 1.102 s | 1.138 s | 514.935 ms | 568.229 ms | 585.322 ms |
| Filtered, 16 accounts | Insertion | 1 day | 1.063 s | 1.095 s | 1.129 s | 516.218 ms | 545.277 ms | 586.964 ms |
| Filtered, 16 accounts | Insertion | 6 months | 1.064 s | 1.098 s | 1.123 s | 517.200 ms | 600.476 ms | 685.805 ms |
| Filtered, 16 accounts | Insertion | 2 years | 1.057 s | 1.116 s | 1.138 s | 505.824 ms | 529.299 ms | 544.466 ms |
| Grouped | Effective | 1 day | 2.915 s | 3.303 s | 3.391 s | 2.348 s | 2.559 s | 2.606 s |
| Grouped | Effective | 6 months | 3.404 s | 4.012 s | 5.084 s | 2.327 s | 2.878 s | 2.911 s |
| Grouped | Effective | 2 years | 3.385 s | 3.966 s | 4.374 s | 2.805 s | 3.023 s | 3.130 s |
| Grouped | Insertion | 1 day | 2.903 s | 3.415 s | 3.438 s | 2.327 s | 2.548 s | 2.679 s |
| Grouped | Insertion | 6 months | 3.192 s | 3.264 s | 3.512 s | 2.317 s | 2.780 s | 2.933 s |
| Grouped | Insertion | 2 years | 3.172 s | 3.235 s | 3.250 s | 2.311 s | 2.684 s | 2.727 s |

The p95 age ratios make the supported claim precise:

| Shape / axis | Miss 6mo/1d | Miss 2y/1d | Hit 6mo/1d | Hit 2y/1d |
|---|---:|---:|---:|---:|
| Unfiltered / effective | 0.924 | 0.977 | 0.899 | 1.014 |
| Unfiltered / insertion | 0.943 | 1.067 | 0.851 | 0.991 |
| Filtered / effective | 1.080 | 1.000 | 0.999 | 1.040 |
| Filtered / insertion | 1.002 | 1.019 | 1.101 | 0.971 |
| Grouped / effective | 1.215 | 1.201 | 1.125 | 1.182 |
| Grouped / insertion | 0.956 | 0.947 | 1.091 | 1.053 |

There is no monotonic or linear age penalty: some older cutoffs are faster,
and the largest observed age effect is the grouped effective six-month cold
miss at 1.215x the one-day p95. That comparison is not the formal hot
unfiltered age gate, and it is evidence that arbitrary shapes cannot promise
identical latency. Query shape dominates age. Across both axes and all ages,
cold-miss p95 is 282-319 ms unfiltered, 1.095-1.189 seconds filtered, and
3.235-4.012 seconds grouped. Cache-hit p95 is 108-132 ms, 529-600 ms, and
2.548-3.023 seconds respectively.

The shape cost is also visible in allocations. Cold misses allocate
21.1-21.3 MB unfiltered, 136.7-137.1 MB filtered, and 485.2-501.1 MB grouped
per operation. Verified cache hits still allocate 10.7-10.9 MB, 71.8-72.3 MB,
and 420.4-436.2 MB respectively. Cold-miss object-fetch p95 was 1.35-1.42 ms
for unfiltered reads and 7.2-7.6 ms for filtered/grouped reads, far below the
end-to-end grouped latency. Fetch caching therefore removes most backend I/O
but not archive decoding, historical merge, transformation, or result
materialization. Cold-miss p95 uses 50 query samples while cache-hit p95 uses
200, so the two distributions do not have equal statistical confidence. Most
cache-hit cases still observed eight backend fetches across 200 samples after
cache churn; their per-fetch percentile is an eight-sample diagnostic, not a
network-latency estimate.

The equivalent all-hot stores occupied 91.82-91.89 MB. After tiering and
flushing, the primary history stores occupied 80.19-84.99 MB, a 7.50-12.68%
local reduction, while the immutable cold objects occupied 156.70 MB. Tiering
therefore removes local hot-run bytes, but the combined retained footprint is
larger for this fixture. Capacity planning still needs a production retention
formula covering primary data, object encoding, cache, obsolete-file cleanup,
and replication.

This remains a local-filesystem result. The OS page cache was not flushed, and
there is no S3 request, TLS, network, throttling, or cross-AZ latency. A real
object store adds deployment-specific behavior around the same decode and
merge work; these local results do not predict its total latency. It cannot
eliminate the measured grouped CPU/allocation cost. Production cold acceptance
still requires the deployment-specific object-store matrix.

## Storage, compaction, and replica convergence

The two-year hot dataset occupied 54.4 MB: 2,139 bytes per posting or
1,069 bytes per effect. These values include Pebble representation and the
specific synthetic account/asset/color distribution; they are not a universal
retention formula. The initial smaller cold lifecycle produced 17.1 MB of
archive objects but did not reduce its immediate local Pebble footprint. The
later full cold matrix does show a 7.50-12.68% flushed primary-store reduction,
together with a 156.70 MB archive. Logical tiering and some local reclamation
are therefore measured, while production retention and obsolete-file cleanup
remain open.

Daily aging already left no threshold-eligible run, so the final explicit
compaction diagnostic performed zero logical merges. Pebble reported cumulative
write amplification of 1.157, but this run does not close the compaction
throughput or concurrent-read acceptance row.

Two independent local stores ingested identical effects with different
publication layouts: one store published once and retained one run; the other
published daily, compacted, and retained six runs. Their logical effect digest
and served semantic digest were equal. This proves deterministic local logical
convergence across physical layouts, not networked multi-replica recovery.

### Current full auxiliary phases

The remaining full-profile store phases were rerun on 2026-07-31 at commit
`3390e3415a826a52c92b9df594c2c5d38026f36f`, tree
`cea6078d9027653341fe78ba60dc1c769c1b573e`, on the same Apple M5 Pro,
darwin/arm64, Go 1.26.5 boundary. Each artifact is an intentionally selected
phase (`complete=false`), with the same five unrelated dirty paths:

| Phase | Artifact | SHA-256 |
|---|---|---|
| Compaction | `build/perf/pit-store-full-compaction-3390e3415.json` | `762fd5b0ec95e5b85e059b77e21a066125e3b18fada7aed3a1d4cff9aebf34c7` |
| Active compaction | `build/perf/pit-store-full-active-compaction-a20765832.json` | `0e2b5b160ee79c1c98d45e4e2f94335287464223c161830c41f6ddfc9810e4bf` |
| Replica digest | `build/perf/pit-store-full-replica-digest-3390e3415.json` | `87d545e89c8e9697964cb8ef952fb21084e1ea6a6b215bc3bbb68a6b743265e6` |
| Cardinality | `build/perf/pit-store-full-cardinality-3390e3415.json` | `4a6afcb4bfe2f286df97e7b0129cc8c7c02ccf8c2711f9b168d822075714853f` |
| Backdating | `build/perf/pit-store-full-backdating-3390e3415.json` | `5e412c6214d9b34c658174bfe5c56912efc9cae82c739aa99ed9d36d38f12278` |

The full compaction phase rebuilt the 731-day, 505,344-effect fixture and
again found zero threshold-eligible logical merges: the 11-run topology was
already converged. The explicit Pebble flush reduced physical bytes from
91,827,365 to 90,226,084 and compaction debt from 15,051,391 bytes to zero;
cumulative Pebble write amplification was 1.577. Unfiltered p95 was 26.046 ms
before and 26.955 ms after the flush (+3.49%); p99 was 26.763 ms and 27.597 ms
(+3.12%). This measures converged-state flush/cleanup, not logical compaction
throughput or read interference while real merges are active, so the
concurrent-compaction rollout gate was still open at that point.

The active-compaction phase was then rerun on 2026-07-31 at commit
`a2076583299ed204a0eb81d00875cefd9c6b1e67`, tree
`d58ac4f3698484af9f01903e63638f80916f27c7`, with the same machine and five
unrelated dirty paths. It forced eight additional L0 runs containing 1,048,576
effects. The real compactor completed three logical merges in 2.301 seconds,
reducing the manifest from 19 to 10 runs. One pre-compaction pinned view
returned the exact reference protobuf result before, during, and after those
merges; 31 successful read-call intervals overlapped successful `Compact`
calls.

| State | Samples | p50 | p95 | p99 |
|---|---:|---:|---:|---:|
| Before compaction | 200 | 41.023 ms | 49.103 ms | 73.692 ms |
| During active logical merges | 31 | 76.610 ms | 105.704 ms | 118.705 ms |
| After compaction | 200 | 16.070 ms | 26.050 ms | 29.444 ms |

Active compaction increased p95 by 115.27% relative to the pre-compaction
backlog. Once the run count had converged, p95 was 46.95% below that same
backlog baseline. This concurrent distribution is deliberately wall-clock
latency only: allocation, Go CPU, and database I/O are omitted because the
reader and compactor share the measurement interval and those resources cannot
be attributed honestly per read. The result closes the local active-merge
evidence gap, but it does not predict interference on deployed shared or
dedicated volumes.

The replica-digest phase used 90 identical days. Replica A published once and
retained one run; replica B published daily, compacted, and retained six runs.
Both the logical effect digest and the served semantic digest were exactly
equal while the physical shapes differed. The
`simulated_replica_digest_convergence` assertion passed. This is deterministic
two-store evidence, not a networked lag, crash, or recovery test.

The cardinality phase used one-run probes with 300 samples each:

| Accounts x assets x colors | p50 | p95 | p99 |
|---:|---:|---:|---:|
| 64 x 1 x 1 | 6.208 us | 9.750 us | 15.083 us |
| 256 x 8 x 1 | 11.959 us | 13.875 us | 26.083 us |
| 256 x 8 x 4 | 57.459 us | 71.167 us | 108.375 us |

These probes isolate in-run identity lookup and materialization. They do not
represent the multi-run, filtered, grouped, cold, or end-to-end API costs.

The backdating phase also used 300 samples per row on its bounded fixture:

| Backdated rate | Effective p95 | Insertion p95 |
|---:|---:|---:|
| 0% | 77.875 us | 82.500 us |
| 1% | 76.833 us | 93.083 us |
| 50% | 81.625 us | 82.792 us |

The 76.8-93.1 microsecond p95 band shows no monotonic read penalty from the
backdated percentage in this fixture. It supports the no-rewrite
read-scalability hypothesis, but it is not a production write-saturation or
adversarial backdating result.

The historical unchanged monolithic `full` store profile used 731 days, 512 accounts, 16 asset
buckets, eight colors, 256 postings/day, 1,000 primary samples, and 200 shape
samples. It timed out first at the Go default 10 minutes and once more at the
allowed 30-minute limit, both while executing grouped hot reads through
`AggregateHistoricalVolumes` and Pebble block decompression. It produced no
complete JSON and no assertions. No third attempt was made. This is a **failed
shape/capacity result**, not generic pending work; the harness or query path
had to be partitioned/profiled before claiming support for that matrix. The
harness now supports the phase-and-case-partitioned procedure above. The
effective-axis grouped cases below supersede the timeout for that bounded slice;
the historical timeout remains authoritative for the unpartitioned full suite.

### Full grouped age matrices after case partitioning

#### Effective axis

The comparable case-level series was captured on 2026-07-31 at commit
`30208aa1c153b54b9f73db8d470932b4e4c1c3db`. The five dirty paths were the
pre-existing instruction and local-environment files outside the PIT build;
the harness itself was committed. The ignored raw artifacts and SHA-256 values
are:

| Case | Artifact | SHA-256 |
|---|---|---|
| Effective 1 day | `build/perf/pit-store-full-hot-grouped-effective-1d-30208aa1c.json` | `7af3cb751082508e0c36d48015f5725c1429a7477482775d66d4be80694f1999` |
| Effective 6 months | `build/perf/pit-store-full-hot-grouped-effective-6mo-30208aa1c.json` | `dac11e94e3fe6e49a97b33f1ab7020c8ed9e64a842ed0f1ad407044c7a513d44` |
| Effective 2 years | `build/perf/pit-store-full-hot-grouped-effective-2y-30208aa1c.json` | `b79f07f1ff72743c68f28f1f0a4fa50e95ccd98092e597c42d180b91c12835f0` |

Each case used the complete 731-day, 512-account, 16-asset, eight-color,
256-postings/day dataset. It retained 11 logical runs and occupied 91.8 MB
before the measurement. Each artifact contains 200 samples of one effective-axis
grouped cutoff:

| Age | Harness elapsed | p50 | p95 | p99 | Max | Operations/s |
|---:|---:|---:|---:|---:|---:|---:|
| 1 day | 341.0 s | 1.469 s | 1.817 s | 2.260 s | 3.826 s | 0.655 |
| 6 months | 314.5 s | 1.381 s | 1.467 s | 1.494 s | 1.502 s | 0.718 |
| 2 years | 314.0 s | 1.376 s | 1.445 s | 1.486 s | 1.565 s | 0.722 |

The six-month/one-day p95 ratio is **0.807**, and the two-year/one-day ratio is
**0.795**. At this cardinality, older effective cutoffs were about 19-20% faster
at p95; there is no linear age penalty. This is not an age speedup guarantee:
the one-day tail was more variable, and the requested cutoff changes how many
effects and values intersect the query.

Case-level partitioning fixes the all-or-nothing evidence problem for this
shape, but it does not establish acceptable interactive latency. The cases
allocated 171.6-179.2 MB and approximately 2.78-3.12 million objects per
operation, with roughly 92-94 million Pebble block-cache misses per 200-sample
artifact. Query shape and cardinality dominate timestamp age.

#### Insertion axis

The insertion-axis series was captured on 2026-07-31 at commit
`6c8237c4adcda98e19a6df3b94fcfd2831b5b440`, with the same machine and
five unrelated dirty paths. Its raw artifacts are:

| Case | Artifact | SHA-256 |
|---|---|---|
| Insertion 1 day | `build/perf/pit-store-full-hot-grouped-insertion-1d-6c8237c4a.json` | `3a77afde43880de0b4f22ac568d47856d69c141e6fd758123fe9bc83d950ff5e` |
| Insertion 6 months | `build/perf/pit-store-full-hot-grouped-insertion-6mo-6c8237c4a.json` | `73493d0ca14edda6871183f88c10d24f977f9404ab929cf79de4977b3997f37c` |
| Insertion 2 years | `build/perf/pit-store-full-hot-grouped-insertion-2y-6c8237c4a.json` | `a3d1c7205b7ca111d3399922eddbe690543c926dfc5388b1c0d7b10e1b1bb66e` |

| Age | Harness elapsed | p50 | p95 | p99 | Max | Operations/s |
|---:|---:|---:|---:|---:|---:|---:|
| 1 day | 404.5 s | 1.790 s | 2.188 s | 2.329 s | 2.371 s | 0.550 |
| 6 months | 461.4 s | 2.019 s | 2.362 s | 2.510 s | 2.831 s | 0.492 |
| 2 years | 423.2 s | 1.862 s | 2.363 s | 2.568 s | 3.342 s | 0.528 |

The insertion six-month/one-day p95 ratio is **1.080**, and the
two-year/one-day ratio is also **1.080**. Older insertion cutoffs were about 8%
slower at p95 in this series, and remain below the same `<=1.20x` comparison
used by the unfiltered age gate. Together with the effective matrix's opposite
ordering, this reinforces the supported claim: age is not a monotonic cost
driver and does not guarantee identical latency.

### Full unfiltered fast path and write stress

The full unfiltered and write-only phases were captured on 2026-07-31 at
commit `9763d1667e40de2220075dd48970ee39bd475120`, with the same machine
and unrelated dirty-path boundary as the grouped matrices:

| Phase | Artifact | SHA-256 |
|---|---|---|
| Hot unfiltered | `build/perf/pit-store-full-hot-unfiltered-9763d1667.json` | `8d1e0d0fe4fa63da1dc716f17ac7c9fa17ebbf0a27e108e2075ec5df83c026bd` |
| Write stress | `build/perf/pit-store-full-write-9763d1667.json` | `c8ab6ccdf8ca3110d95ab4dd5b91b9bc8ce152398c42115e59c6a2ea007f6ee2` |

The unfiltered phase ran 1,000 samples per case and completed in 115.1 seconds:

| Axis | Age | p50 | p95 | p99 | Operations/s |
|---|---:|---:|---:|---:|---:|
| Effective | 1 day | 14.304 ms | 29.637 ms | 35.438 ms | 56.7 |
| Effective | 6 months | 13.996 ms | 16.295 ms | 20.307 ms | 69.5 |
| Effective | 2 years | 13.562 ms | 15.135 ms | 16.246 ms | 72.6 |
| Insertion | 1 day | 13.906 ms | 15.936 ms | 18.061 ms | 70.4 |
| Insertion | 6 months | 14.796 ms | 18.570 ms | 23.220 ms | 65.2 |
| Insertion | 2 years | 16.988 ms | 22.027 ms | 29.701 ms | 56.6 |

The effective six-month/one-day p95 ratio is **0.550**, so the enforced
`<=1.20x` gate passes. Insertion orders differently: six months is **1.165x**
and two years **1.382x** the one-day p95. The insertion two-year comparison is
not part of the current enforced gate, but it is material evidence against any
claim of age-independent latency across both axes. The growth is not linear in
history length; it is nevertheless a real 38.2% tail increase for this shape.

### Full filtered and transformed shapes

The remaining hot filtered and transformed phases were captured on 2026-07-31
at commit `1120a3ec4ae40c57101efcd1a1842bb18f644875`, tree
`67c087ddfda166292fae8e6f009df5e1748860ba`, on the same Apple M5 Pro,
darwin/arm64, Go 1.26.5 boundary. The five dirty paths were the same unrelated
instruction and local-environment files. The dataset contained 731 days, 512
accounts, 16 assets, eight colors, 256 postings/day, 505,344 effects, and 11
final logical runs.

The current-filtered phase selected 16 accounts and ran 200 samples per
case. Each case completed independently in 34.1-39.0 seconds:

| Case | Artifact | SHA-256 |
|---|---|---|
| Effective 1 day | `build/perf/pit-store-full-hot-filtered-effective-1d-1120a3ec4.json` | `2aaf87dba684df02addefc680f2573e6788c198c7d175b70a48502a25fe03d4d` |
| Effective 6 months | `build/perf/pit-store-full-hot-filtered-effective-6mo-1120a3ec4.json` | `4f2c3ca68fe2b08450edd8761c1c4cd712167a29262a7145996f87c546fde16b` |
| Effective 2 years | `build/perf/pit-store-full-hot-filtered-effective-2y-1120a3ec4.json` | `60077e62fa727dd5b362fa4d2a753262752500c535877a4820afb48fc1e982a1` |
| Insertion 1 day | `build/perf/pit-store-full-hot-filtered-insertion-1d-1120a3ec4.json` | `745cfb78b48df2146710458ae0e163781261aa644a7ea8677459ebbcaf11e628` |
| Insertion 6 months | `build/perf/pit-store-full-hot-filtered-insertion-6mo-1120a3ec4.json` | `0a10f56e2daa443fde9ae7bcd442e7163493f04475dfe8e66d9afee023015094` |
| Insertion 2 years | `build/perf/pit-store-full-hot-filtered-insertion-2y-1120a3ec4.json` | `f4da36c0f3a673100e73be6c5a99faf572f43b2fe4bdf51e17792d4eb33766aa` |

| Axis | Age | p50 | p95 | p99 | Max | Operations/s |
|---|---:|---:|---:|---:|---:|---:|
| Effective | 1 day | 57.711 ms | 106.284 ms | 124.098 ms | 137.644 ms | 14.89 |
| Effective | 6 months | 45.929 ms | 95.783 ms | 104.134 ms | 110.587 ms | 17.27 |
| Effective | 2 years | 87.899 ms | 112.774 ms | 120.719 ms | 125.626 ms | 12.56 |
| Insertion | 1 day | 50.865 ms | 89.360 ms | 94.955 ms | 96.190 ms | 16.57 |
| Insertion | 6 months | 63.258 ms | 112.316 ms | 121.788 ms | 151.880 ms | 13.88 |
| Insertion | 2 years | 85.112 ms | 123.116 ms | 130.517 ms | 152.571 ms | 12.47 |

For effective time, the six-month/one-day p95 ratio is **0.901** and the
two-year/one-day ratio is **1.061**. For insertion time, the corresponding
ratios are **1.257** and **1.378**. This is the same qualitative result as the
unfiltered matrix: the bounded run topology avoids a linear history scan, but
age is not latency-neutral and the insertion axis has a material older-cutoff
tail in this dataset. The filtered cases allocated 4.73-4.96 MB and
88.7-99.1 thousand objects per operation.

The transformed-shape artifact is
`build/perf/pit-store-full-hot-shapes-1120a3ec4.json`, SHA-256
`72019c629bed678d042ad0641ffdf3f9599a1ce699072c156cdf970f863b64d0`.
It completed in 1,098.3 seconds and ran 200 one-day effective-time samples for
each shape:

| Effective one-day shape | p50 | p95 | p99 | Max | Operations/s | Bytes/op | Allocs/op |
|---|---:|---:|---:|---:|---:|---:|---:|
| Grouped + max precision + collapsed colors | 3.551 s | 9.716 s | 24.584 s | 93.018 s | 0.190 | 179.2 MB | 3.12 M |
| Unfiltered + collapsed colors | 35.326 ms | 54.314 ms | 60.878 ms | 81.264 ms | 24.52 | 0.775 MB | 18.7 k |

The combined grouped transform is **178.9x** slower at p95 and **403.8x**
slower at p99 than unfiltered color collapse. It allocates 231.2x more bytes
and 167.1x more objects per operation, and caused 92.5 million Pebble block
cache misses over 200 samples versus 0.88 million for unfiltered collapse.
This is a severe shape/capacity warning, not evidence that old timestamps are
intrinsically slow. These partial-phase artifacts have no shape-specific
acceptance assertion: `PIT_PERF_ENFORCE=1` verifies applicable gates, while
successful completion alone does not make multi-second interactive latency
acceptable.

The write-only phase completed in 32.4 seconds and used 1,000 samples per row:

| Primary-store case | p50 | p95 | p99 | Throughput |
|---|---:|---:|---:|---:|
| Durable-write baseline | 3.013 ms | 4.045 ms | 4.966 ms | 316.7/s |
| Manual steady history cadence | 3.010 ms | 3.999 ms | 5.193 ms | 318.9/s |
| Manual history-store saturation | 3.003 ms | 3.998 ms | 6.081 ms | 317.8/s |

Steady cadence changes p99 by **+4.56%**, p95 by -1.14%, and throughput by
+0.70% versus the baseline. This is just inside the 5% comparison, but it is a
single manual-cadence diagnostic rather than the builder harness's bracketed
A/B/A acceptance gate. Forced store saturation changes p99 by **+22.45%** and
raises allocations per operation by 256.6x. It is not a steady-state rollout
result; it quantifies why uncontrolled backfill/catch-up and verifier overlap
must remain canary and scheduling concerns.

## Full-verifier interference

The semantic verifier is intentionally outside the interactive query path, but
it reads authoritative history, builds a scratch Pebble projection, and streams
both timelines. Its scratch directory is below `--balance-history-dir`.

The original local diagnostic measured primary-write p99 rising from 5.896 ms to
7.720 ms, or **+30.95%**, while a 2,000-proposal full verification took
280.6 ms. `overlappedAllWrites=false`: the verifier completed before all 300
writes, so this is not acceptance evidence and may underestimate or distort a
fully overlapping run. It is nevertheless a real contention signal.

The current-branch diagnostic measured +28.97% p99 interference and completed
the verifier in 263.7 ms, again with `overlappedAllWrites=false`. It confirms
the contention signal but still does not qualify as a fully overlapping
acceptance test.

Production rollout should put balance-history/verifier I/O on a dedicated
volume where possible, schedule full replay away from peak write traffic, and
measure shared-volume and dedicated-volume cases. Cold replay bytes, peak
scratch bytes, cleanup behavior, and a fully overlapping write distribution
remain unmeasured.

## Decision and remaining rollout gates

The local implementation evidence supports bounded age behavior, both time
axes, hot/cold/cache paths, 100k audit reconstruction, logical run boundedness,
and deterministic logical convergence. It does **not** support broad GA:

- the current steady-write rerun passes at -1.22%, but an earlier valid run
  failed at +6.02%, so production-topology variance is not closed;
- the historical monolithic full profile and the six-case hot grouped phase
  both time out at 30 minutes; all six partitioned hot grouped cases complete,
  but at 1.445-2.363-second p95;
- the full current-filtered matrix completes at 89.360-123.116 ms p95, while
  grouped max-precision color collapse reaches 9.716 seconds p95 and 24.584
  seconds p99;
- the full local-filesystem cold matrix completes, but cold-miss p95 rises from
  282-319 ms unfiltered to 1.095-1.189 seconds filtered and 3.235-4.012
  seconds grouped; a real object store remains unmeasured;
- the full store-stress phase is +4.56% p99 at steady history cadence, but
  +22.45% under forced history-store saturation;
- active logical compaction raises local unfiltered p95 from 49.103 ms to
  105.704 ms (+115.27%) while it merges a forced backlog, then reduces p95 to
  26.050 ms after convergence; deployed-volume interference remains open;
- a 100k boot/rebuild leaves 500 runs before background maintenance;
- the verifier shows roughly +29-31% local p99 interference in partial-overlap
  diagnostic;
- real object-store, multi-node, HTTP/gRPC, Raft admission, crash recovery,
  and deployed-volume measurements remain outstanding.

The accepted safe rollout remains opt-in, canary-first, and capacity-planned.
Metadata and all other non-monetary attributes remain current-state-only and do
not add historical storage or write work.

## Completion matrix

| Evidence | Result | Status |
|---|---|---|
| Synchronous writes | Current A/B/A p99 -1.22%; earlier valid run +6.02%; full store stress +4.56% steady and +22.45% saturated | Pass current local / saturation and canary pending |
| Builder tail | Current local p99 206.84 ms; target `<500 ms` | Pass local |
| Builder backfill/rebuild | 100k audits in 5.17/5.06 s; 500-run convergence debt remains | Partial |
| Full verifier | Local partial-overlap p99 roughly +29-31%; deployed shared/dedicated volumes unmeasured | Diagnostic / pending deployment |
| PIT age | Hot full ratios vary by shape/axis; cold full ratios span 0.851-1.215 with no monotonic growth | Effective hot gate pass / shape and axis dependent |
| PIT axis | Effective and insertion measured at 1d/6mo/2y locally for full unfiltered, filtered, and grouped shapes | Pass local / full hot and cold matrices |
| PIT shape | Hot full p95 ranges from 15.135 ms unfiltered to 9.716 s grouped transform; cold full miss p95 ranges from 282 ms unfiltered to 4.012 s grouped | Complete local hot/cold shapes / severe grouped capacity warning |
| PIT source | Full local-filesystem cold miss and verified cache hit matrices complete; real object network absent | Pass local / pending deployment |
| PIT cardinality | One-run p95 grows from 9.750 us at 64 x 1 x 1 to 71.167 us at 256 x 8 x 4 | Diagnostic local |
| Backdating | Full 0%, 1%, and 50% matrix remains in a 76.8-93.1 us p95 band | Pass local |
| Compaction | Forced full backlog completes 3 logical merges; 31 overlapping reads preserve exact results, with p95 +115.27% during merges and -46.95% after convergence versus the backlog baseline | Pass local / pending deployed-volume interference |
| Capacity | Full tiering reduces flushed primary bytes 7.50-12.68% but adds 156.70 MB of archive objects; production retention formula remains open | Partial |
| Multi-replica | Equal 90-day logical/semantic digests across one-run and six-run local layouts; real cluster absent | Partial |
