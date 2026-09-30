# RocksDB POC — replacing Pebble under the main store

Status: draft / proof of concept. Not authoritative for current behaviour.

## Why

Cockroach Labs moved Pebble development to a private repository on
2026-09-15. The public repository stays as a historical snapshot and
existing releases keep their license, but there is no longer a public
development branch to follow. The ledger currently pins
`github.com/cockroachdb/pebble/v2 v2.1.7`, likely one of the last public
releases.

This POC evaluates RocksDB (via `github.com/linxGnu/grocksdb`) as a
replacement engine. The baseline it must beat is "fork Pebble v2.1.7 and
maintain it ourselves", which remains a viable option.

## Scope

The POC covers the **main store only** (`internal/storage/dal`). It is
the most constrained area (write-session capabilities, restore, query
checkpoints) and exercises the widest slice of the engine API. The read
store and usage store are estimated at the end of the POC, not ported.

Ledger v3 is unreleased, so there is no on-disk migration to design: a
node on the new engine is rebuilt from the Raft log / backup.

## Engine surface used today

Non-test references to Pebble, whole repository:

| Symbol | Count | RocksDB mapping | Risk |
|---|---|---|---|
| `IterOptions` (bounded scans) | 58 | `ReadOptions` with iterate bounds | low |
| `NoSync` / `Sync` | 34 | `WriteOptions.SetSync` | low |
| `DB` / `Open` / `Options` | 54 | `OpenDb` / `Options` | low |
| `Iterator` | 25 | `Iterator` | low |
| `ErrNotFound` | 21 | nil slice on Get | low |
| `Snapshot` | 14 | `Snapshot` + `ReadOptions.SetSnapshot` | low |
| `Checkpoint(dir, WithFlushedWAL())` | 8 call sites | `Checkpoint.CreateCheckpoint` | medium |
| `Comparer.Split` (readstore, usagestore) | 2 | `SliceTransform` prefix extractor | high |
| `Merger` (check/replay_store) | 1 | `MergeOperator` | medium |
| `Experimental.ValueSeparationPolicy` | 1 | BlobDB (`enable_blob_files`) | low |
| `WALFailoverOptions` | 1 | none | accept / ops |
| `EventListener` + `vfs` wrapper (metrics) | 2 | `Statistics` tickers + properties | medium |
| per-level compression (`pebblecfg`) | 1 | `compression_per_level` | low |

## Build chain — the real blocker

RocksDB needs cgo. Today every artifact is built with `CGO_ENABLED=0`:

- `Dockerfile` (scratch image)
- `.goreleaser.yml` (three builds, linux/darwin, amd64/arm64)
- `flake.nix` pins the toolchain; RocksDB, a C++ compiler, zstd, snappy,
  lz4 and bz2 would need to enter the Nix environment.

grocksdb v1.11.x targets RocksDB 11.x and requires GCC 11+ / C++20.

## Steps

1. **Build chain** (2–3 days). Add RocksDB to the Nix shell, build a
   hello-world binary with grocksdb, produce a statically linked Docker
   image and a multi-arch goreleaser build. Stop here if the pipeline
   cannot be made reproducible.
2. **Risky mappings spike** (2 days). Reproduce the variable-length
   `Split` of the read store as a `SliceTransform`, and the replay-store
   `Merger` as a `MergeOperator`, in an isolated package with tests.
3. **`kv` abstraction** (1 week). Extract a minimal engine interface from
   `dal`: Open, Get, bounded NewIter, Batch with sync/nosync, Snapshot,
   Checkpoint, Close, ErrNotFound. Pebble implementation first, existing
   `dal` suite must pass unchanged; then the grocksdb implementation.
4. **Benchmarks** (3 days). Same harness for both engines: apply write
   throughput, iterator scans, checkpoint and restore time, restart time,
   RSS (RocksDB heap is outside the Go GC — measure process RSS, not the
   Go profile), binary size.
5. **Ops and invariants** (3 days). Bucket restore via Raft-log rebuild,
   follower sync, race suite, a short Antithesis run. The deterministic
   FSM invariant is untouched as long as the engine stays below the
   abstraction.

## Go / no-go criteria

- Reproducible multi-arch build in Nix and CI.
- Functional parity on checkpoint/restore and per-ledger bloom filtering.
- Write and scan performance within ±15 % of Pebble.
- Process RSS under control with the block cache sized like today.
- Cost of porting readstore and usagestore estimated.
- Overall cost lower than maintaining a Pebble fork.

## Log

- 2026-09-29: branch `codex/rocksdb-poc` created from `release/v3.0`; steps 1–5 executed the same day, findings below.

## Step 1 findings — build chain

Validated on 2026-09-29 with `misc/rocksdb-poc/hello` (own `go.mod`, so the
main module stays untouched):

- **Version pin.** grocksdb ties itself to an exact RocksDB C API:
  v1.10.8 → 10.10.1, v1.11.0 → 11.0.4, v1.11.1 → 11.1.2. Compiling
  grocksdb v1.11.0 against RocksDB 10.10.1 headers fails (missing
  `rocksdb_fifo_compaction_options_*` symbols). One version everywhere is
  mandatory: **RocksDB 11.0.4 / grocksdb v1.11.0**, chosen because Alpine
  (the ledger base and runtime image) ships exactly 11.0.4.
- **Nix.** nixpkgs (stable, unstable, master) is at 10.10.1 with no cached
  11.x, so `flake.nix` overrides the derivation to 11.0.4. First
  `nix develop` compiles RocksDB from source (several minutes), then it is
  cached locally. The shell exports `ROCKSDB_CGO_CFLAGS` /
  `ROCKSDB_CGO_LDFLAGS`; regular builds keep `CGO_ENABLED=0`.
  `just rocksdb-hello` builds and runs the spike on the host.
- **Docker.** `just rocksdb-hello-docker [platform]` builds with
  `golang:1.27-alpine` + `rocksdb-dev` and runs on `alpine` +
  `rocksdb`. Verified on linux/arm64 and linux/amd64. Alpine has no
  `rocksdb-static` package, so the binary links `librocksdb.so`
  dynamically; the runtime image already is Alpine today, so this is not
  a regression. A fully static binary would require compiling RocksDB in
  the builder stage (long, but cacheable) — deferred.
- **macOS.** Dynamic link against the Nix store dylibs works; the
  resulting binary is host-only, which is fine for development.
- **Open for goreleaser.** Cross-compiling cgo for darwin from a Linux
  runner (and vice versa) is the remaining unknown; the likely answer is
  one native runner per OS/arch or Zig as a cross C toolchain.

## Step 2 findings — risky mappings (`misc/rocksdb-poc/spike`)

Both mappings work; run with `just rocksdb-test`.

**Comparer.Split → prefix extractor.** The read/usage store Split is a
65-byte fixed prefix for ledger-scoped keys and "whole key" for internal
singletons. RocksDB's native `FixedPrefixTransform(65)` reproduces the
useful part with no cgo callback: singleton keys shorter than 65 bytes fall
out of the prefix domain and stay covered by whole-key filtering, longer
ones get a 65-byte prefix that is never used for scans. Verified with 16
ledgers in 16 SST files: three scans skip exactly 46 foreign files and open
the 2 home files (`rocksdb.non.last.level.seek.filtered` /
`seek.filter.match`); without an extractor nothing is skipped. A Go
`SliceTransform` porting Split byte-for-byte also works and costs +7 % on
ingestion (2.8 µs vs 2.6 µs per single Put on an M-series laptop), so the
native transform is the recommended mapping.

**Merger → MergeOperator.** Pebble folds operands through
MergeNewer/MergeOlder and `Finish(includesBase)`; RocksDB hands
`FullMerge` the base (nil is definitive: key absent) plus operands
oldest-first, and calls `PartialMerge` when a compaction cannot reach the
base. The Pebble `includesBase=false` deferral therefore maps to
`PartialMerge` re-emitting an ordered op batch. Verified: additive volume
folding across flushed files, ordered tx ops resolved on Get and on
iteration, and a compaction that really invokes `PartialMerge` (universal
compaction with merge width 2 so the operand runs compact without the base
run — the C API cannot pin a file to a level, and a level-style
`CompactRange` on a fresh DB leaves the base in L1 where every later L0
compaction sees it).

**grocksdb footguns found on the way** (each cost a segfault):

- `rocksdb_options_set_merge_operator`, `set_prefix_extractor` and
  `set_universal_compaction_options` take ownership of the C object;
  grocksdb's `Options.Destroy` / `UniversalCompactionOptions.Destroy`
  free it again. grocksdb's own tests never destroy such options. The
  `kv` abstraction must own this lifecycle once.
- `grocksdb.TickerType` mirrors the C enum by iota and drifts across
  RocksDB versions: with 11.0.4 `TickerType_BLOOM_FILTER_PREFIX_*` reads an
  unrelated counter. Read tickers by name from `GetStatisticsString()`.
- The legacy `bloom.filter.prefix.*` tickers stay at zero on RocksDB ≥ 7;
  prefix-bloom skips are reported by `*.seek.filtered`.
- A single `db.Put` costs ~2.6 µs (cgo round trip + memtable); the main
  store already writes through batches, which amortises this, but every
  Get/Seek/Next is a cgo call too. Step 4 must measure scan-heavy paths.

## Step 3 findings — engine abstraction and the main store on RocksDB

`internal/storage/engine` is the contract; `pebbleengine` and
`rocksengine` (build tag `rocksdb`) both pass `enginetest.Run`, including
under `-race`. `dal.Store` runs on either through `Config.Engine`;
`LEDGER_TEST_ENGINE=rocksdb go test -tags rocksdb` replays the existing
suites on RocksDB. Result: **dal, state (FSM), attributes, backup, checker,
query, membership and bloom suites are green on RocksDB**, with three
Pebble-specific tests skipped (OPTIONS-file fault injection, MaxOpenFiles,
Pebble-shaped metrics).

Migration cost, measured: 109 files, +1.9k/-0.5k lines, one working day,
almost all mechanical (`pebble.IterOptions` → `engine.IterOptions`,
`*pebble.Iterator` → `engine.Iterator`, `pebble.ErrNotFound` →
`engine.ErrNotFound`, write-session `Set`/`DeleteRange` losing their
Pebble write-options argument). The read and usage stores were wrapped
rather than ported: they keep their Pebble open path and comparers, and
hand a `pebbleengine.DB` to the shared helpers.

Semantic differences absorbed in `rocksengine`:

- `SeekLT` is "last key < k" in Pebble, `SeekForPrev` is "<= k" in RocksDB:
  an exact hit steps back once.
- `SeekPrefixGE` confinement is decided per iterator in RocksDB
  (`prefix_same_as_start`); the wrapper enforces it on `Valid()` instead.
- `Checkpoint` uses `log_size_for_flush = MaxUint64` to mirror Pebble's
  `WithFlushedWAL` (WAL synced and copied, memtable untouched), refuses an
  existing destination, and creates missing parent directories (RocksDB
  does not).
- Iterator bounds are copied: RocksDB references the bound bytes for the
  iterator's lifetime and callers reuse key buffers.
- The write batch is encoded in Go (RocksDB wire format, 12-byte header +
  tagged records) and handed to `rocksdb_writebatch_create_from` at commit:
  one cgo call per batch instead of one per operation.
- `ValueAndErr` never polls `rocksdb_iter_get_error` per key (a cgo call
  plus a heap escape); errors surface through `Error()`.
- `Options.Destroy` is skipped when a prefix extractor or merge operator
  was set (grocksdb double free, see step 2).

Still Pebble-only: `dal.NewStore` option building (event listener, VFS
metrics wrapper, WAL failover, value separation, per-level compression),
`servicepb.PebbleMetrics` (nil on RocksDB), write-stall signalling (never
stalls on RocksDB), and the read/usage store comparers.

## Step 4 findings — benchmarks (Apple M5 Pro, 48 GB, single process)

Engine-level harness: `enginetest.RunBenchmarks` (ledger-shaped keys:
zone/sub/32-byte hash for attributes, zone/sub/u64 for history; 200-op
batches, NoSync with a WAL sync every 16 batches; 256 MB block cache, 64 MB
memtable, 10-bit bloom). `go test -bench Engine` on both packages,
`-benchtime=2s -count=3`, medians:

| Benchmark | Pebble 2.1.7 | RocksDB 11.0.4 | RocksDB / Pebble |
|---|---|---|---|
| ApplyBatches (ops/s) | 440 k | 455 k | 1.03× |
| ApplyBatches max RSS | 255 MB | 131 MB | 0.51× |
| PointGetHot (ns) | 707 | 651 | 0.92× |
| PointGetMissing (ns) | 436 | 159 | 0.36× |
| ScanHistory1000 (keys/s) | 30.6 M | 8.5 M | **3.6× slower** |
| SnapshotIterAttributes 1000 keys (µs) | 31 | 93 | **3.0× slower** |
| CheckpointAndReopen, 49 MB DB (ms) | 130 | 61 | 0.47× |

Application-level, same suites on both engines through
`LEDGER_TEST_ENGINE`:

| Benchmark | Pebble | RocksDB |
|---|---|---|
| dal `BenchmarkStoreGet` 64 B / 512 B / 4 KB (ns) | 506 / 573 / 1035 | 340 / 409 / 745 |
| state `BenchmarkAuditWrite/Monolithic` n=1 / 10 / 100 / 500 (µs) | 1.1 / 2.2 / 20.5 / 98 | 2.9 / 3.5 / 10.3 / 33 |

Reading: writes, point lookups, checkpoints and memory are at parity or
better on RocksDB; RocksDB's lower RSS comes from its block cache living
outside the Go heap (which also means Go memory profiles no longer show
it). The one structural loss is iteration: every `Next`/`Key`/`Value` is a
cgo transition (~40 ns each), so scan-heavy paths — read-store queries,
cache restore, checker replays, backup rebuild — run 3–4× slower per key.
Tiny commits pay a fixed ~2 µs write-path cost (RocksDB's mutex-guarded
writer vs Pebble's lock-free commit pipeline); batches of 50+ operations,
which is what an FSM apply produces, are faster on RocksDB. Test-suite wall
time confirms the scan penalty: state suite 72 s on RocksDB vs 50 s on
Pebble, backup 19 s vs 11 s.

Binary: 91.0 MB vs 90.8 MB, plus a 12 MB `librocksdb` shared library at
runtime. Build time roughly +20 s for the cgo wrapper (cached afterwards).

## Step 5 findings — ops and invariants

Everything below ran with `LEDGER_TEST_ENGINE=rocksdb` and `-tags rocksdb`
against the unchanged test-suites:

- `-race` on dal, state and backup: green.
- e2e cluster suite (in-process nodes, Ginkgo): **294 / 294 specs**, including
  learner join, follower sync through checkpoint streaming, restore from
  backup and from a stale cache, query checkpoints, protocol-version and
  rolling-config scenarios. e2e business suite: **549 / 549**.
- Docker: `docker build --build-arg STORAGE_ENGINE=rocksdb .` produces the
  regular image with cgo on and Alpine's `librocksdb.so.11.0.4` in the
  runtime layer (184 MB). A single node started with
  `--storage-engine rocksdb --bootstrap` created a ledger, applied a
  transaction, served the account, survived a restart with WAL replay and
  wrote SSTs under `live/`.
- Two more Pebble/RocksDB differences surfaced only at this level and are
  now covered by the conformance suite: RocksDB does not create parent
  directories on open nor on checkpoint.

Not run: an Antithesis session. The workload images build from the same
Dockerfile, so `STORAGE_ENGINE=rocksdb` is the only change needed; launching
one is a paid, explicit step left to the team.

## Go / no-go assessment

| Criterion | Result |
|---|---|
| Reproducible multi-arch build | Yes for Nix (macOS/Linux) and Docker (amd64/arm64). goreleaser darwin/linux cross-compilation with cgo remains untested. |
| Functional parity (checkpoint/restore, per-ledger bloom) | Yes: full dal/state/backup/e2e suites green; prefix bloom verified in step 2. |
| Writes and scans within ±15 % | Writes, point gets, checkpoints: at parity or better. **Scans: 3–4× slower per key**, outside the target. |
| RSS under control | Yes, and lower than Pebble at equal cache size (block cache outside the Go heap). |
| Cost of read/usage store port | Estimated 1–2 days each: same mechanical pattern as the main store, plus `FixedPrefixTransform(65)` for the comparers. Value separation has a BlobDB equivalent; WAL failover has none. |
| Cheaper than maintaining a Pebble fork | Not established. The engine abstraction (this branch) is the reusable part either way. |

Recommendation: keep the `engine` abstraction and the RocksDB
implementation behind its build tag, do not switch the default. The scan
penalty hits exactly the paths the read store, cache restore, checker and
backup rebuild lean on; the batched-iteration shim below recovers a third of
it, the rest is RocksDB's own iterator cost. Meanwhile the
abstraction makes a Pebble fork, a later RocksDB switch, or another pure-Go
engine equally reachable.

## Follow-up — batched iteration across the cgo boundary

Implemented in `rocksengine/iter_fill.go`: a C shim (`ledger_iter_fill`)
copies consecutive entries into a Go buffer and advances the iterator in one
call; `Next`/`Key`/`Value` are then served from Go memory. The window
starts at 2 KiB after a seek and doubles per refill up to 64 KiB (a fixed
64 KiB first fill made every short scan pay for ~500 entries), buffers are
pooled, and backward steps re-seek the C iterator to the logical position.
The conformance suite gained a long mixed-direction scan and now pins
Pebble's edge semantics (Next after running off the start re-enters at the
first key, and symmetrically). All RocksDB suites stay green.

| | before shim | after shim | Pebble |
|---|---|---|---|
| NewIter + First + Close (µs) | 35.6 (eager 64 KiB fill) / ~1.6 (direct) | 2.0 | 1.2 |
| ScanHistory1000 (keys/s) | 8.5 M | 12.1 M | 30.6 M |
| SnapshotIterAttributes 1000 keys (µs) | 93 | 100 | 31 |

The cgo transitions were about a third of the gap. A CPU profile of the
scan benchmark puts 88 % of the time inside the single C call, i.e. in
RocksDB's own iterator (`DBIter::Next`, block delta decoding, upper-bound
check): ~80 ns per key on this machine against ~30 ns for Pebble's columnar
blocks. Block size, delta encoding and restart interval were tried and move
the result by under 10 %. **Scans stay ~2.5× slower per key**; the go/no-go
criterion on scans remains unmet, and the remaining gap is RocksDB's, not
the binding's.

