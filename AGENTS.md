# CLAUDE.md - AI Agent Instructions

This document contains rules and conventions for AI agents working on this codebase. Detailed documentation lives in `docs/` - see [docs/README.md](docs/README.md) for navigation.

## Invariants

**CRITICAL**: These rules are non-negotiable and must never be violated.

### Architecture

1. **Cache is the source of authority** — The in-memory cache must NEVER diverge between nodes. Every node must see identical cache state for the same applied index.
2. **FSM must be deterministic** — The finite state machine (Raft apply path) must produce identical results on every node for the same input. No randomness, no time-dependent logic, no node-local state.
3. **No Pebble reads in FSM / hot path** — The FSM apply path must never read from Pebble. All data needed for apply must come from the cache or the command itself. Pebble is write-only on the hot path. The hot path receives a `dal.WriteSessionFactory` parameter and opens a `*dal.WriteSession` — this type deliberately has no `Get`/`NewIter`, so the invariant is compiler-enforced for any code that holds a session. The hot-path FSM `Machine` itself holds NO Pebble read capability and NO `*dal.Store`: boot/recovery reads live on `state.Recovery` (which owns the only `dal.RecoveryReader`), follower-sync coordination lives on `state.Synchronizer` (which owns the only `dal.IncomingRestoreFactory`). Post-commit sentinel checks (debug mode) read through `dal.SentinelFactory.Run(fn)` — a scoped callback so the reader never escapes the check.
4. **Pebble writes only from the hot path or declared lifecycle paths** — `*dal.Store.OpenWriteSession()` is the only producer of `*dal.WriteSession`. Outside the FSM hot path, only declared lifecycle paths may call it: `internal/bootstrap/config_validation.go`, `internal/infra/backup/`, `internal/infra/attributes/prepare.go`. This is enforced by a `forbidigo` rule in `.golangci.yaml`; new call sites must be added to the exclusions block with a justification.
5. **Never delete cache entries outside of rotations** — Cache entries must only be evicted during generation rotations (Gen0 → Gen1 → discard). Deleting individual entries breaks the cache prediction mechanism (bloom filters, tombstones).
6. **Every FSM `Registry.X.Get(...)` must have a matching preload, declared by the component that proposes the command** — The FSM apply path reads from the in-memory cache; a cache miss turns the read into a silent no-op. Each component that emits a proposal (the metadata converter, the index builder, the cluster-config reconciler, the idempotency-eviction scheduler, the mirror worker, admission) is responsible for declaring its own `preload.Needs` covering every key its apply path will read. There is NO central proposal→needs registry — coupling the preload package to every proposal type creates a single point that easily falls behind. The component knows what it reads; the component declares it. The shared `proposeTechnical` helper takes a `*preload.Needs` parameter the caller fills in (pass nil or an empty `Needs` when the apply path has no cache-keyed reads, e.g. cluster config / idempotency eviction). The preload populates the cache via `MirrorPreload` with a fresh value read at propose time (Pebble fallback on cache miss), and `PredictedIndex` catches mutations between propose and apply.
7. **Never silently skip a "should not happen" branch** — A branch that is reachable only if an invariant is violated (nil where the contract says non-nil, a state we believe unreachable, a cache miss after a guaranteed preload, etc.) MUST surface a loud signal: `return fmt.Errorf("invariant: ...")` so it bubbles up, or `assert.Unreachable(...)` for SUT-level invariants exercised under antithesis. A silent `return nil` / `continue` on these branches hides real bugs — particularly catastrophic in the FSM apply path, where a no-op desyncs nodes from each other. Branches that represent genuine runtime conditions (cache miss as an expected outcome, stale proposal, deleted entity) keep their soft `return nil`. The distinction is whether the case is *expected* (soft skip OK) or *impossible by design* (must fail loudly). The comment must say *why* the case is impossible so a reader can decide whether to add a hard fail or relax the rule.

8. **The audit log is the only source of truth — every other persisted dataset is a projection and must be verified by the checker** — Only `AuditEntry` (zone `Cold`, sub `Audit`) is cryptographically bound, via the hash chain that `state.BuildHashedHeaderPayload` + `processing.HashGenerator` produce and `checker.verifyAuditHashChain` verifies on every Check() run. Everything else stored in Pebble — `Log`, `AuditItem`, `AppliedProposal`, `LedgerLog.PurgedVolumes`, attribute caches (`Volume`, `Metadata`, `Transaction`, `Reference`, `Boundary`, etc.), reversion bitsets, idempotency keys, mirror cursors, chapters, bloom filters, signing keys, the read-side index — is a *projection* of orders that already live in the audit chain. Projections are rebuildable from the audit on demand, so we deliberately do NOT extend the hash chain to cover them (refactor over hash binding — see `feedback_audit_is_source_of_truth`). In exchange, **`internal/application/check/checker.go` MUST verify every projection it persists**: re-derive the value the projection should hold by replaying the audit (`ReplayLedgerLog`, `SimulateEphemeralPurge`, `partitionVolumes`, etc.) and compare to what is stored, emitting the matching `CHECK_STORE_ERROR_TYPE_*` event on divergence. A projection that the checker does not verify is a tampering vector — adding a new persisted projection without a matching compare* / collect* pass in the checker is the violation. The current passes are `compareVolumes`, `compareMetadata`, `compareTransactions`, `compareExclusionProjections` (AppliedProposal.TransientVolumes + LedgerLog.PurgedVolumes), `checkReversionInvariants`, `verifySealingHash`, `compareIdempotencyOutcomes` (frozen idempotency outcomes in SubIdempKeys vs the hash-chained AuditFailure/AuditSuccess that wrote them — the failure kind is re-derived from the chain-bound reason via `domain.KindForReason`, never stored), and `compareIndexes` (SubAttrIndex registry vs CreateIndex/DropIndex/RemovedMetadataFieldType/DeleteLedger logs — covers presence + identity; BuildStatus is intentionally excluded because it is purely informational on the cluster-wide registry entry — queries gate on the per-replica `IndexVersionState.CurrentVersion`, not on BuildStatus); extend the list as new persisted projections land.

9. **Never bypass the FSM coverage gate** — Every cache-attribute read on the FSM hot path MUST go through `Scope.GetX(...)` so the per-order `coverage_bits` admit it. Reading the underlying `Registry.X.KeyStore().M` (or any other parent-cache iterator) directly skips the gate and produces non-deterministic FSM behavior: the gate is what binds the order to the admission-declared preload set, and a direct read silently sees keys the proposer never declared. There is NO documented exception — paths that need to iterate (e.g. cascade-on-delete) MUST either declare the relevant `preload.Needs` upfront, defer the work to a lifecycle path (`batch.deleteLedgerData` + `MarkLedgerForCleanup`), or be rejected at design review. New helpers that scan the parent KeyStore from inside an order/TU handler are the violation, even when wrapped in a method on `WriteSet`. The coverage gate exists precisely so admission's declared key set is the FSM's only legitimate read horizon — under no circumstances should the apply path widen it on the fly.

### Code style

7. **Prefer parameters over separate methods** — When adding a boolean mode (dry run, force, preview), add it as a parameter to the existing method rather than creating a new method.
8. **Numscript syntax** — Literal account names require `@` prefix (e.g., `@funding:pool`). Multiple `send` blocks per script are supported. Variables don't use `@`.

## Reference Implementation

**The reference implementation is `github.com/formancehq/ledger`.** Follow its patterns for application structure, dependency injection (fx), lifecycle management, HTTP/gRPC servers, OpenTelemetry, and error handling.

## Documentation Maintenance

**CRITICAL**: Always maintain documentation when making changes.

- **Document new technical mechanisms** — when introducing a new technical mechanism, subsystem, or non-obvious invariant, add a dedicated page under `docs/technical/architecture/` and link it from the corresponding `README.md`
- **Update `docs/technical/contributing/api-comparison.md`** when adding, modifying, or removing API endpoints
- **Update `docs/ops/cli.md`** when modifying CLI commands, flags, or behavior
- **Update `openapi.yml`** if HTTP endpoints change
- **Update code comments** if interfaces or behavior change
- **Keep documentation in English**
- **Regenerate demo GIFs** after CLI changes: `just generate-demo`

## Pre-commit Checks

**CRITICAL**: Before completing any task, run pre-commit checks.

```bash
# Preferred: uses nix develop for reproducible toolchain
nix develop --command bash -c "just pre-commit"

# Alternative: direnv-based
direnv allow && eval "$(direnv export bash)" && GOROOT= just pre-commit
```

This runs `go generate ./...`, `go mod tidy`, and `golangci-lint run --fix`.

Always verify compilation with `GOROOT= go build ./...` before submitting. The `GOROOT=` prefix is required to avoid Go toolchain version mismatch errors when nix is not active.

## Mock Generation

**CRITICAL**: After any change to interfaces annotated with `//go:generate mockgen`, regenerate mocks immediately with `go generate ./...`.

Interfaces with mockgen: `Transport` (`internal/infra/node/transport.go`), `Controller` (`internal/application/ctrl/controller.go`), `Admission` (`internal/application/ctrl/controller_default.go`), `Spool` (`internal/storage/spool/spool.go`), `WAL` (`internal/storage/wal/wal.go`), `InMemoryStore` (`internal/domain/processing/store.go`), `Checker` (`internal/infra/health/healthcheck.go`), `Proposer` (`internal/infra/state/metadata_converter.go`).

## JSON Property Naming

**CRITICAL**: All JSON properties must use **camelCase** (OpenAPI spec and Go struct tags).

## Protocol Buffers

**CRITICAL**: After modifying any `.proto` file, **immediately** run `just generate-proto`. Realign field numbers sequentially when adding/removing fields.

See [docs/technical/contributing/protobuf.md](docs/technical/contributing/protobuf.md) for full details (file locations, vtprotobuf, Uint256 wire format, adding new command models).

## Conventions

For full conventions with examples, see [docs/technical/contributing/conventions.md](docs/technical/contributing/conventions.md).

Key rules:
1. **One file per command** and **one file per HTTP handler**
2. **No global variables** for flags - use structs
3. **Group variable declarations** in `var (...)` blocks
4. **No type aliases** - use original types directly
5. **Never ignore errors** - handle explicitly or `_ = ...` with comment
6. **Struct methods colocation** - all methods in same file as struct. If a file grows large, extract sub-types (composition) rather than splitting methods across files
7. **Build into `build/`** directory - never leave binaries in repo root

## File Structure

- **Server**: `cmd/server/` - main server binary entry point
- **CLI**: `cmd/ledgerctl/` - one file per sub-command. See [docs/ops/cli.md](docs/ops/cli.md).
- **Domain**: `internal/domain/` - value objects, **business errors emitted by the FSM**, domain services (`processing/`, `accounttype/`, `analysis/`, `replay/`), and cryptographic primitives (`crypto/signing/`, `crypto/keystore/`). Errors in this package are FSM-generated business outcomes (e.g. `ErrInsufficientFund`, `ErrEmptyTransaction`, `ErrLedgerNameRequired`). Admission / integration / config validators live in `internal/application/<layer>/errors.go` and use `domain.NewValidationSentinel` to build their own sentinels — do NOT pile non-FSM errors into `internal/domain`.
- **Bootstrap**: `internal/bootstrap/` - composition root (fx wiring, config, TLS, persisted config)
- **Application**: `internal/application/` - use cases (`admission/`, `ctrl/`, `events/`, `check/`, `indexbuilder/`, `mirror/`)
- **Infrastructure**: `internal/infra/` - consensus (`node/`, `state/`), caching (`cache/`, `attributes/`), transport, health, monitoring, `backup/`, `bloom/`, `coldstorage/`, `preload/`, `receipt/`
- **Utilities**: `internal/pkg/` - zero/low-dependency utilities (`kv/`, `signal/`, `futures/`, `commands/`, `bitset/`, `bytesize/`, `filterexpr/`, `semver/`, `tarutil/`, `vtmarshal/`, `worker/`)
- **Storage**: `internal/storage/` - Pebble DAL, WAL, spool, `readstore/`, `pebblecfg/`
- **Query**: `internal/query/` - CQRS read-side queries
- **Adapters**: `internal/adapter/` - transport layer (`grpc/` primary API, `http/` REST compat, `json/` serialization, `auth/` JWT/Ed25519 authentication, `v2/` v2 compatibility layer)
- **Proto definitions**: `misc/proto/` -> generated code in `internal/proto/`
- **Demos**: `misc/demo/` - VHS tape files for CLI demos
- **Numscript examples**: `misc/numscript/examples/`
- **Public packages**: `pkg/` - public API (`actions/`, `scenario/`, `testserver/`)
- **Tests**: `tests/` - test suites (`e2e/`, `scenarios/`, `antithesis/`, `perf/`, `schemathesis/`)
- **Operator**: `misc/operator/` - Kubernetes operator (separate Go module). CRD types (`api/v1alpha1/`), controllers (`internal/controller/`), Helm charts (`helm/`), kubectl plugin (`cmd/kubectl-ledger/`), web UI (`ui/`), e2e tests (`e2e/`)

## Build Tags (Optional Features)

The default build (`go build .`) produces a **light binary** (~60 MB) without heavy optional dependencies. To include optional features, use positive build tags:

| Tag | Feature | Heavy dependencies |
|-----|---------|-------------------|
| `kafka` | Kafka event sink | `IBM/sarama` |
| `nats` | NATS JetStream event sink | `nats-io/nats.go`, `nats-io/nats-server` |
| `clickhouse` | ClickHouse event sink | `ClickHouse/clickhouse-go` |
| `databricks` | Databricks event sink | `databricks/databricks-sql-go` |
| `s3` | S3 cold storage & backup | `aws-sdk-go-v2` |
| `azure` | Azure Blob Storage backup | `azure-sdk-for-go/sdk/storage/azblob`, `azure-sdk-for-go/sdk/azidentity` |
| `pyroscope` | Pyroscope continuous profiling | `grafana/pyroscope-go` |

Build with all features: `just build-full` or `go build -tags "kafka,nats,clickhouse,databricks,s3,azure,pyroscope" .`

Scenario tests use a separate build tag: `go test -tags scenario ./tests/scenarios/... -timeout 20m`

Tests with event-sink feature tags (`kafka`, `clickhouse`) start Testcontainers from `TestMain`, so Docker access is required even for compile-only checks such as `-run '^$'`.

## Testing Conventions

See [docs/technical/contributing/testing.md](docs/technical/contributing/testing.md) for full testing guidelines.

Key rules:
- **Never use `time.Sleep`** in tests - use `require.Eventually`
- **Always use `t.Parallel()`** in unit tests
- **Use gRPC client** (`servicepb.BucketServiceClient`) in integration tests
- **Use helper functions** from `tests/e2e/testutil/` (helpers and server setup)
- **E2E tests** use the `e2e` build tag and Ginkgo/Gomega framework: `go test -tags e2e ./tests/e2e/... -timeout=600s`
- **Never hand-roll mocks** — if a test needs to fake an interface, add a `//go:generate mockgen` directive on the interface (see [Mock Generation](#mock-generation) above for the standard flag set), run `go generate ./...`, and use the generated `MockXxx` in the test. Hand-rolled fakes drift from the interface, lose call recording for free, and duplicate effort.

### Running all tests (with all optional features)

```bash
# Unit tests with all features
just test-full
# or: go test -tags "kafka,nats,clickhouse,databricks,s3,pyroscope" ./... -timeout 20m

# E2E tests with all features
just test-e2e-full
# or: go test -tags "e2e,kafka,nats,clickhouse,databricks,s3,pyroscope" ./tests/e2e/... -timeout 20m

# E2E tests for a specific feature (e.g., ClickHouse sink)
go test -tags "e2e,clickhouse" ./tests/e2e/... -timeout 20m
```

## Configuration Safety Checks

The server persists critical config (`node-id`, `cluster-id`, `idempotency-ttl`, `storage-schema-version`) in Pebble under the Global zone on first boot and validates on subsequent boots. Mismatch on `node-id`/`cluster-id` is fatal. Use `--unsafe-skip-config-validation` to bypass (dangerous). Schema version mismatches are never bypassable, even with `--unsafe-skip-config-validation`. See [docs/ops/deployment.md](docs/ops/deployment.md) and [docs/ops/cli.md](docs/ops/cli.md) for details.

Key files: `internal/bootstrap/persisted_config.go`, `internal/bootstrap/config_validation.go`, `internal/bootstrap/module.go`.

## Request Signing

Ed25519 request signing for authenticity and integrity. See [docs/ops/signing.md](docs/ops/signing.md) for operations and [docs/ops/maintenance-mode.md](docs/ops/maintenance-mode.md) for maintenance mode.

## Architecture

See [docs/technical/architecture/](docs/technical/architecture/) for detailed architecture documentation. Key design principles:

- **Single Raft group** manages all ledgers
- **FSMs must be fast** - they run in the critical path of Raft consensus
- **Uber fx** for dependency injection - see [docs/technical/contributing/getting-started.md](docs/technical/contributing/getting-started.md)
- **Formance go-libs** for service lifecycle, OTLP, HTTP server

- I would like you to respect the concepts of DRY (Don't Repeat Yourself).
