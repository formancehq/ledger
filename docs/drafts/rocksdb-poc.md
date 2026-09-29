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

- 2026-09-29: branch `codex/rocksdb-poc` created from `release/v3.0`;
  step 1 started.

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

