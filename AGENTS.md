# CLAUDE.md - AI Agent Instructions

This document contains rules and conventions for AI agents working on this codebase. Detailed documentation lives in `docs/` - see [docs/README.md](docs/README.md) for navigation.

## Reference Implementation

**The reference implementation is `github.com/formancehq/ledger`.** Follow its patterns for application structure, dependency injection (fx), lifecycle management, HTTP/gRPC servers, OpenTelemetry, and error handling.

## Documentation Maintenance

**CRITICAL**: Always maintain documentation when making changes.

- **Update `docs/dev/api-comparison.md`** when adding, modifying, or removing API endpoints
- **Update `docs/ops/cli.md`** when modifying CLI commands, flags, or behavior
- **Update `openapi.yml`** if HTTP endpoints change
- **Update code comments** if interfaces or behavior change
- **Keep documentation in English**
- **Regenerate demo GIFs** after CLI changes: `just generate-demo`

## Pre-commit Checks

**CRITICAL**: Before completing any task, run pre-commit checks.

```bash
# For Nix environments
direnv allow && eval "$(direnv export bash)" && GOROOT= just pre-commit
```

This runs `go generate ./...`, `go mod tidy`, and `golangci-lint run --fix`.

Always verify compilation with `go build ./...` before submitting.

## Mock Generation

**CRITICAL**: After any change to interfaces annotated with `//go:generate mockgen`, regenerate mocks immediately with `go generate ./...`.

Interfaces with mockgen: `WAL` (`internal/raft/node.go`), `Transport` (`internal/raft/transport.go`), `Controller` (`internal/ctrl/controller.go`), `Engine` (`internal/ctrl/controller_default.go`), `Spool` (`internal/storage/spool/spool.go`), `WAL` (`internal/storage/wal/wal.go`), `Store` (`internal/service/processing/processor.go`), `Checker` (`internal/health/healthcheck.go`).

## JSON Property Naming

**CRITICAL**: All JSON properties must use **camelCase** (OpenAPI spec and Go struct tags).

## Protocol Buffers

**CRITICAL**: After modifying any `.proto` file, **immediately** run `just generate-proto`. Realign field numbers sequentially when adding/removing fields.

See [docs/dev/protobuf.md](docs/dev/protobuf.md) for full details (file locations, vtprotobuf, Uint256 wire format, adding new command models).

## Conventions

For full conventions with examples, see [docs/dev/conventions.md](docs/dev/conventions.md).

Key rules:
1. **One file per command** and **one file per HTTP handler**
2. **No global variables** for flags - use structs
3. **Group variable declarations** in `var (...)` blocks
4. **No type aliases** - use original types directly
5. **Never ignore errors** - handle explicitly or `_ = ...` with comment
6. **Struct methods colocation** - all methods in same file as struct
7. **Build into `build/`** directory - never leave binaries in repo root

## File Structure

- **CLI**: `cmd/ledgerctl/` - one file per sub-command. See [docs/ops/cli.md](docs/ops/cli.md).
- **HTTP handlers**: `internal/compat/http/` - one file per handler
- **Proto definitions**: `misc/proto/` -> generated code in `internal/proto/`
- **Demos**: `misc/demo/` - VHS tape files for CLI demos
- **Numscript examples**: `misc/numscript/examples/`

## Testing Conventions

See [docs/dev/testing.md](docs/dev/testing.md) for full testing guidelines.

Key rules:
- **Never use `time.Sleep`** in tests - use `require.Eventually`
- **Always use `t.Parallel()`** in unit tests
- **Use gRPC client** (`servicepb.LedgerServiceClient`) in integration tests
- **Use helper functions** from `tests/e2e/helpers.go`

## Configuration Safety Checks

The server persists critical config (`node-id`, `cluster-id`, `audit-enabled`) in Pebble (key prefix `0xFE`) on first boot and validates on subsequent boots. Mismatch on `node-id`/`cluster-id` is fatal; `audit-enabled` mismatch is a warning. Use `--unsafe-skip-config-validation` to bypass (dangerous). See [docs/ops/deployment.md](docs/ops/deployment.md) and [docs/ops/cli.md](docs/ops/cli.md) for details.

Key files: `internal/storage/data/persisted_config.go`, `internal/application/config_validation.go`, `internal/application/module.go`.

## Request Signing

Ed25519 request signing for authenticity and integrity. See [docs/ops/signing.md](docs/ops/signing.md) for operations and [docs/ops/maintenance-mode.md](docs/ops/maintenance-mode.md) for maintenance mode.

## Architecture

See [docs/dev/architecture/](docs/dev/architecture/) for detailed architecture documentation. Key design principles:

- **Single Raft group** manages all ledgers
- **FSMs must be fast** - they run in the critical path of Raft consensus
- **Uber fx** for dependency injection - see [docs/dev/getting-started.md](docs/dev/getting-started.md)
- **Formance go-libs** for service lifecycle, OTLP, HTTP server

- I would like you to respect the concepts of DRY (Don't Repeat Yourself).
