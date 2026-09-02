# Agent module download cache

## Operational requirement

Candidate validation needs to reuse verified Go module downloads without
giving two candidates a shared extracted module tree. A cold isolated module
setup is material to PR-loop latency, but ordinary `go build` and `go test`
trust an extracted tree already present in `GOMODCACHE`. Sharing that tree lets
one candidate change dependency source consumed by another candidate without a
checksum failure.

The required boundary is therefore:

```text
base-pinned trusted module graph
        -> shared .zip/.mod seed
        -> copy/reflink by value
        -> unique run-local GOMODCACHE
        -> Go checksum verification
        -> run-local extracted trees
```

`GOCACHE` remains unique per validation run. Local `replace` targets remain in
their worktrees and never enter the module download seed.

## Alternatives evaluated

### A. Direct `file://` module proxy

- **TRUST:** Go verifies proxy archives, but the candidate receives the shared
  filesystem path in `GOPROXY`. Under the same UID it can mutate version lists,
  metadata, or artifacts while another run resolves modules.
- **PORTABILITY:** Go-native and supported on every repository platform.
- **SETUP_COST:** Low; no seed copy.
- **COPY_COST:** None.
- **DISK_COST:** One shared archive set plus each run's extracted cache.
- **CONCURRENT_BEHAVIOR:** Concurrent readers are simple, but candidate writes
  cross the authority boundary.
- **PRIVATE_MODULE_FUTURE_COMPATIBILITY:** Would need separate credentials and
  no-sumdb policy; unsafe as an automatic shared path.
- **COMPLEXITY:** Low mechanically, unacceptable authority boundary.

### B. Copy verified downloads into each run (selected)

- **TRUST:** The shared generation is never a `GOMODCACHE`. Only `.zip` and
  `.mod` files are copied. `.ziphash`, proxy lists, checksum-database tiles,
  locks, temporary files, and extracted directories are excluded. The trusted
  base graph then runs `go mod download` in the run-local cache. Missing
  `.ziphash` files force Go to hash archives and check `go.sum` / `GOSUMDB`
  before extraction. Seed entries not verified by that graph are removed.
- **PORTABILITY:** Uses pinned Go, GNU coreutils, findutils, and Bash from the
  existing Nix shell on Linux and macOS.
- **SETUP_COST:** A cold generation costs the ordinary trusted download once;
  repeated runs retain only hashing and extraction.
- **COPY_COST:** `cp --reflink=auto` uses copy-on-write where supported and a
  normal byte copy elsewhere.
- **DISK_COST:** One content-addressed download generation plus one complete
  run-local module cache per active validation. Reflink-capable filesystems
  reduce physical duplication but are not required for correctness.
- **CONCURRENT_BEHAVIOR:** Cold runs may download in parallel. A complete
  run-local publication is atomically renamed into place; no run waits for or
  consumes a partial generation.
- **PRIVATE_MODULE_FUTURE_COMPATIBILITY:** Sharing disables completely when
  `GOPRIVATE`, `GONOSUMDB`, or `GONOPROXY` is non-empty, or when `GOSUMDB` is
  empty/off. Such runs retain fully isolated module resolution.
- **COMPLEXITY:** One focused preparation helper and the existing validation
  wrapper; no daemon, mount, or new service.

### C. Immutable prepopulation in the Nix store

- **TRUST:** Strong filesystem immutability and no candidate write authority.
- **PORTABILITY:** Supported by the repository's Nix environments.
- **SETUP_COST:** Good after realization, but every module-graph change needs a
  new fixed-output derivation and hash.
- **COPY_COST:** Still requires a run-local extraction or module cache.
- **DISK_COST:** Nix generations plus run-local caches.
- **CONCURRENT_BEHAVIOR:** Nix realizes derivations safely.
- **PRIVATE_MODULE_FUTURE_COMPATIBILITY:** Fixed-output fetching and credentials
  would require a separate design.
- **COMPLEXITY:** Disproportionate maintenance for frequently changing root and
  nested module graphs, so rejected.

### D. Shared raw download subtree, overlay, or cache daemon

- **TRUST:** A symlink or bind-mounted `GOMODCACHE/cache/download` also shares
  `.ziphash` state and lets `go clean -modcache` affect peers. A daemon could
  enforce read-only serving but creates a new service boundary.
- **PORTABILITY:** Overlay/mount behavior varies by host; a daemon adds process
  lifecycle and networking requirements.
- **SETUP_COST:** Potentially low.
- **COPY_COST:** None for overlays/daemon serving.
- **DISK_COST:** Low shared storage, at the cost of the stronger coupling.
- **CONCURRENT_BEHAVIOR:** Requires mount or service coordination and crash
  handling.
- **PRIVATE_MODULE_FUTURE_COMPATIBILITY:** Requires credential, tenant, and
  checksum-exemption policy.
- **COMPLEXITY:** Exceeds the saved setup time and violates the no-service
  constraint, so rejected.

## Authority and verification boundary

`ai-pr-loop` and `ai-pr-adopt-candidate` invoke the base-pinned
`agent-validation-env` before any candidate process. The wrapper consumes
`SHARE_DOWNLOAD_CACHE_ONLY=1`, unsets it before executing the requested
command, and never exports the shared generation path. The helper derives the
shared root from the trusted worktree's Git common directory; candidate input
does not select it.

Filesystem secrecy or same-UID permissions are not treated as integrity. A
candidate that deliberately locates and edits the shared directory can cause a
cache miss or validation failure, but cannot authorize bytes for another run:

1. reuse copies regular `.zip` and `.mod` files by value into a unique run;
2. no shared `.ziphash` is copied;
3. the trusted base's root and every tracked nested module execute `go mod
   download` before candidate execution;
4. Go recreates `.ziphash` only after the archive matches the trusted
   `go.sum`, consulting `GOSUMDB` for sums not already recorded;
5. copied entries without a freshly created run-local `.ziphash` are removed;
6. newly introduced candidate dependencies are absent from the allowlisted
   run-local seed and use the unchanged `GOPROXY` chain.

Wrong archives, same-size/same-mtime changes, changed module files, and
candidate-injected entries therefore fail or are discarded before reuse. A
candidate's later changes to its own extracted tree, archive cache, or
`.ziphash` remain confined to that run.

## State and lifecycle

Shared generations live at:

```text
<git-common-dir>/ledger-agent-module-download-cache/v1/<go-version>/<manifest-fingerprint>/
```

The fingerprint covers pinned Go version, `GOPROXY`, `GOSUMDB`, and every
tracked root/nested `go.mod` and `go.sum` blob. Source-only changes reuse the
same generation; dependency, proxy, checksum-database, or toolchain changes
select a new generation. `GOPROXY` is used only as input to that opaque digest;
the shared generation metadata never persists its raw value because proxy URLs
may contain credentials.

Cold preparation downloads through the existing Go configuration into the
current run's private `GOMODCACHE`, then publishes only verified `.zip`/`.mod`
pairs. Publication uses an atomic no-clobber rename. Concurrent cold runs may
duplicate the initial network work, which is cheaper and safer than a lock or
daemon; one complete generation wins. A crash before rename leaves no visible
generation because staging lives inside the disposable run directory.

Run cleanup continues to remove the complete validation directory, including
all extracted modules. Shared generations are deliberately outside that tree.
They are content-addressed and may be removed as a whole for garbage
collection; active runs already hold private copies. An unexpected incomplete
or corrupt generation is never repaired in place: remove that generation and
the next trusted preparation recreates it.

## Private and no-sumdb policy

The optimization is fail-closed. Any effective non-empty `GOPRIVATE`,
`GONOSUMDB`, or `GONOPROXY`, or an empty/off `GOSUMDB`, disables all shared
reuse for the run. This conservative policy avoids leaking private archives or
silently extending shared trust to modules without the current checksum
database contract. Public and private dependencies can be split later only
with a separately reviewed classification mechanism; this helper does not try
to infer privacy from module names.

## Measured behavior

Measurements used two clean worktrees at the same target commit, pinned Go
1.26.5, all tracked root and nested modules, and a distinct empty `GOCACHE` for
every run. Times vary with network and filesystem state, so they describe the
observed envelope rather than a fixed budget:

| Scenario | Module setup wall time |
| --- | ---: |
| Fully isolated baseline | 45.18 s |
| Cold shared-generation creation | 31.19 s |
| Repeated reuse, sample 1 | 31.49 s |
| Repeated reuse, sample 2 | 23.32 s |
| Two concurrent isolated runs | 49.36 s |
| Two concurrent reuse runs | 50.38 s |

Repeated single-run setup improved by 1.43-1.94x (13.69-21.86 seconds).
Concurrent wall time was neutral within observed variance because both runs
still perform independent hashing and extraction. Copy/reflink setup took
0.79 seconds for 354 MiB of shared downloads. Each complete run-local cache
was approximately 1.51 GiB; the shared download-only generation added 355 MiB.
A source-only candidate reused the generation and its subsequent `go mod
download` took 0.13 seconds. A candidate with a new public dependency fetched
that dependency into its own cache in 0.34 seconds. No measurement reused or
changed `GOCACHE`.

Uncontrolled post-rebase spot checks ranged from 32.30 to 63.83 seconds for
reuse against a 41.21-second isolated sample. The wide range coincided with
heavy temporary-cache cleanup and is retained here as a caution: the cache
removes repeated network transfer and showed a material controlled-worktree
improvement, but it does not eliminate local hashing/extraction cost or
guarantee lower latency under host I/O contention.
