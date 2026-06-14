# CLAUDE.md - AI Agent Instructions

This document contains rules and conventions for AI agents working on this codebase. Detailed documentation lives in `docs/` - see [docs/README.md](docs/README.md) for navigation.

## Invariants

**CRITICAL**: These rules are non-negotiable and must never be violated.

### Architecture

1. **Cache is the source of authority** — The in-memory cache must NEVER diverge between nodes. Every node must see identical cache state for the same applied index.
2. **FSM must be deterministic** — The finite state machine (Raft apply path) must produce identical results on every node for the same input. No randomness, no time-dependent logic, no node-local state.
3. **No Pebble reads in FSM / hot path** — The FSM apply path must never read from Pebble. All data needed for apply must come from the cache or the command itself. Pebble is write-only on the hot path.
4. **Never delete cache entries outside of rotations** — Cache entries must only be evicted during generation rotations (Gen0 → Gen1 → discard). Deleting individual entries breaks the cache prediction mechanism (bloom filters, tombstones).

### Code style

5. **Prefer parameters over separate methods** — When adding a boolean mode (dry run, force, preview), add it as a parameter to the existing method rather than creating a new method.
6. **Numscript syntax** — Literal account names require `@` prefix (e.g., `@funding:pool`). Multiple `send` blocks per script are supported. Variables don't use `@`.

## Reference Implementation

**The reference implementation is `github.com/formancehq/ledger`.** Follow its patterns for application structure, dependency injection (fx), lifecycle management, HTTP/gRPC servers, OpenTelemetry, and error handling.

## Documentation Maintenance

**CRITICAL**: Always maintain documentation when making changes.

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
- **Domain**: `internal/domain/` - value objects, errors, domain services (`processing/`, `accounttype/`, `analysis/`, `replay/`), and cryptographic primitives (`crypto/signing/`, `crypto/keystore/`)
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
