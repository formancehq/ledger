# 0002 — FSM apply pipeline throughput ceiling on single-node

**Status:** Investigation closed (2026-07-08). Ceiling of ~150k tx/s
accepted on single-node Raft with the `perf-world-to-bank` workload.
Revisit if the workload profile shifts, if we move away from single-node
Raft (sharding, multi-writer), or if a fork of `etcd-io/raft` becomes
worthwhile.

## Context

The `perf-world-to-bank` benchmark on `eks-acme-dev-euw1-01` targets a
single-node Raft ledger with:

- **Pod:** 28 CPU / 224 GiB / SSD-backed, dedicated node
- **Workload:** k6 `1m:80 → 5m:100 (later 200/400) → 1m:0` VUs,
  `BULK_SIZE=50` (each bulk = 50 transactions), `USE_NUMSCRIPT=false`
- **Cluster:** single-node Raft (no follower acks, no cross-node fsync)
- **Baseline throughput:** ~155k tx/s at 100 VUs steady state

The perf effort aimed to raise this ceiling by attacking whichever stage
of the FSM apply pipeline (decoder → applier.main → committer) showed
CPU headroom or blocking behavior. This ADR consolidates what we tried,
what moved, and — importantly — what did not.

## What we tried

Five interventions were prototyped, each starting from a specific
observation in Pyroscope or Grafana. Each was benchmarked against the
baseline. Result column reports steady-state TPS delta.

| # | Change | Merged? | TPS |
|---|--------|---------|-----|
| 1 | Prefetch `DecodeEntries` in its own goroutine, pipeline before `PrepareDecodedEntries` (#1533) | Ready | ±0 |
| 2 | pprof labels on applier.main / applier.decoder / applier.committer (#1533) | Ready | n/a (observability) |
| 3 | Parallelize the 11 independent `Derived.<Store>.Merge()` calls in `WriteSet.Merge` via `errgroup` (#1535) | Closed | ±0 |
| 4 | Shard `Transactions.Merge` overlay drain across 8 workers via `WithParallelism` construction option (#1535) | Closed | ±0 |
| 5 | Deepen the applier.main → committer channel from `buffered(1)` to `buffered(4)` (#1536) | Closed | ±0 |
| 6 | pprof label on `handleBulk` + per-phase histograms in `Admit` (resolve_batch, orders_preparation, scripts, response_resolution) (#1537) | Draft | n/a (observability) |

**Every code change that targeted throughput came back at 150-155k tx/s.**
Only the observability changes (#1533 labels, #1537 histograms) landed
as useful.

## Measurements after all changes

### CPU utilization

Per stage of the FSM pipeline, measured over multiple 2-3 min windows
at steady-state 150k tx/s (Pyroscope, `process_cpu/cpu` with the pprof
labels from #1533):

| Component | CPU | Fraction of 1 core |
|-----------|-----|--------------------|
| `applier.decoder` | 6.9% | negligible |
| `applier.main` | ~60% | not saturated |
| `applier.committer` | ~48% | not saturated |
| Pod total (`admission.http` + FSM + others) | ~4% | of 28 available cores |

**No stage of the pipeline is CPU-bound.** The pod uses roughly one
core's worth of CPU total across ~30 cores available. Adding more
CPU-side optimizations cannot help — the constraint is elsewhere.

### Admission phase decomposition

At 150k tx/s the six histogrammed phases of `Admission.Admit`
(`admissionMetrics: true` in the LedgerService CRD) break down as
follows (averages over 10 min, native Prometheus histograms via Thanos):

| Phase | Avg | Share |
|-------|-----|-------|
| `admission.resolve_batch` (sig verify + unmarshal) | 2.4 µs | 0.005% |
| `admission.orders_preparation` (`requestsToOrders` + `extractPreloadNeeds`) | 215 µs | 0.4% |
| `admission.scripts` (numscript, off in this workload) | 0.7 µs | 0.001% |
| `admission.preload` (build preload) | 56 µs | 0.1% |
| `admission.propose` (Raft accept wait) | 748 µs | 1.5% |
| **`admission.fsm_future_wait` (FSM apply wait)** | **47.5 ms** | **98.2%** |
| `admission.response_resolution` (post-apply log reads; idempotent replays only) | ~0 µs | ~0% |
| `admission.command_duration` (total) | 48.3 ms | — |

Sanity check: sum of the seven phase averages = 48.5 ms vs total 48.3 ms
(within 0.4%). The decomposition is complete — there is no "hidden"
time between phases. `response_resolution` is ~0 here because this
workload issues no idempotent replays; on a replay-heavy workload it
captures the `ReadLogBySequence` reads that resolve `ReferenceSequence`
results into concrete logs, which no other phase measures.

### Where the time actually goes

`fsm_future_wait` is measured from when Raft accepts a proposal to when
the applier's committer resolves the future. By Little's law at
steady-state:

- Arrival rate: **3000 proposals/s**
- Avg wait: **47 ms**
- ⇒ **~141 proposals in-flight** in the FSM pipeline at any time

That is the fundamental picture: 141 proposals continuously queueing
inside the FSM stage even though the three stage goroutines each have
CPU headroom.

### What is NOT the bottleneck (ruled out)

- **CPU saturation** — pod at 4% of 28 cores. Not it.
- **Applier.main CPU** — 60% of 1 core with 40% headroom. Not it.
- **Committer CPU** — 48% of 1 core. Not it.
- **Pebble memtable insert** (`findSpliceForLevel`, `bytes.Compare`) —
  visible in committer CPU but committer isn't saturated, so improving
  it (e.g., two-pass sorted flush of attribute + cache zones) would
  save CPU but not raise throughput. See "Deferred optimizations".
- **WAL fsync** — commits are `pebble.NoSync`; the WAL writer is
  asynchronous. `walMinSyncInterval: 100ms` sets the background sync
  cadence, not commit blocking. Confirmed by reading `machine.go`
  comments and Pebble code.
- **User-level `sync.Mutex` / `sync.RWMutex` contention** — Pyroscope
  `mutex/delay` shows `sync.(*Mutex).Unlock` = **438 ms cumulated over
  135 s (0.32% wall-clock)** across the whole process. No hidden lock
  is significant enough to gate the pipeline.
- **`IndexTracker` mutex** in `plan.Builder.AcquireProposalGuard`:
  ~30 µs held per proposal × 3000/s = 9% utilization. Not saturated.
- **Propose queue depth** — `admission.propose_queue.load` p95 tops
  out around 400 out of a 4096 cap (10%). The queue never fills.
- **HTTP admission code path** — `admission.http` component CPU is
  ~125% of 1 core (across 100-200 goroutines) but dominated by
  bytedance/sonic JSON marshal/unmarshal. Parallelizable trivially,
  and the phase histograms confirm every non-`fsm_future_wait` phase
  is sub-ms.
- **Applier→committer channel depth** — increasing `commitCh` buffer
  from 1 to 4 redistributed the block/delay from `chansend1` to
  `chanrecv1` but did not move TPS (#1536). Confirms the coupling
  wasn't the actual gate.

### What the bottleneck likely IS

Two candidates remain, both structurally hard to attack from application
code:

1. **`etcd-io/raft` ordering loop.** The Raft library runs a single
   goroutine that orders proposals, generates committed entries, and
   fires them via the `Ready` channel. The rate at which this goroutine
   produces committed entries is bounded by its internal
   tick + heartbeat cadence and batching heuristics. It is not
   labeled in our pprof setup and adding visibility would require
   forking or wrapping the library.

2. **Serial pipeline structure with in-order semantics.** Even with
   CPU headroom on each stage, the applier must apply entries in
   Raft order per ledger. Any real parallelism inside a batch is
   bounded by ordering. The 141 in-flight proposals are the natural
   depth of the pipeline given per-stage service times of 200-400 µs
   each × 5 stages (Raft, decoder buffer, decoder, applier.main,
   committer).

We did not confirm which of these two is dominant. Both would require
substantial work to attack.

## Decision

**Accept 150k tx/s as the operational ceiling** for single-node Raft on
this workload profile. Merge the two observability PRs (#1533 pprof
labels + prefetch, #1537 admission decomposition) because they are
independently valuable for future diagnostics.

Close the throughput-targeted PRs that did not move the metric
(#1535 parallel merge / transactions fan-out, #1536 commitCh buffer).
Preserve the commit history via git for later revisits.

## Deferred optimizations

Legitimate but not throughput-moving in the current regime:

- **Two-pass sorted flush in `flushAttributeAndCache`** — splitting
  the interleaved `attr.Set` (0xF1) + `writeCacheRaw` (0xFF) writes
  into two passes each sorted by their zone's natural key ordering
  would make Pebble skiplist inserts monotone within each zone and
  reduce `findSpliceForLevel` cost. Estimated 20-30% committer CPU
  savings. Committer is not the bottleneck today, so this is CPU
  savings without throughput impact. Worth doing if committer ever
  becomes the gate (e.g., when the FSM apply latency drops through
  a Raft-side fix).

- **Reduce JSON marshal/unmarshal CPU on `admission.http`** — >50% of
  the HTTP handler CPU is `bytedance/sonic` encoding/decoding. A move
  to a binary transport (gRPC + proto or Connect) would cut this. Not
  a throughput lever today (handlers run in parallel across many
  goroutines; JSON cost doesn't gate), but a resource efficiency
  win at scale.

## When to revisit

- Any change that moves us **off single-node Raft** (sharding by
  ledger, multi-writer replication, cluster read-replicas): the
  throughput ceiling assumptions in this ADR no longer hold.
- A workload profile that pushes CPU saturation on any pipeline
  stage — the CPU headroom analysis above is workload-specific.
- Upstream `etcd-io/raft` changes (batch-tick tuning, alternative
  scheduler) or a decision to fork it.
- A benchmark run at significantly higher load where propose queue
  load starts approaching the 4096 cap — that would flip the
  diagnosis toward admission-side saturation.

## Related work

- #1524 — ADR 0001 (vtprotobuf `unmarshal_unsafe`, rejected for the
  same workload)
- #1533 — decode prefetch + pprof labels (ready)
- #1537 — admission observability (draft)
- Closed: #1535 (parallel merge + transactions fan-out), #1536
  (commitCh buffer deepening)
