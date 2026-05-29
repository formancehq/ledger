# Development Guide

## Overview

This guide provides the information needed to contribute to the Ledger v3 POC project, understand the code Structure, and follow project conventions.

## Project Structure

```
ledger/
├── cmd/                    # Entry points
│   ├── server/            # Main server
│   └── ledgerctl/         # CLI client (ledgerctl)
├── internal/               # Internal code (not exported)
│   ├── domain/            # Domain layer (value objects, errors, processing)
│   │   └── processing/    # Transaction/log processing (Numscript)
│   ├── bootstrap/         # Composition root (fx wiring, config, TLS)
│   ├── application/       # Use cases
│   │   ├── admission/     # Admission service (preload, AttributeLoader)
│   │   ├── ctrl/          # Controller layer (transaction processing)
│   │   ├── events/        # Event dispatch
│   │   └── check/         # Integrity checking
│   ├── adapter/           # Transport adapters
│   │   ├── auth/          # Authentication middleware
│   │   ├── grpc/          # gRPC servers and client
│   │   ├── http/          # HTTP REST handlers
│   │   ├── json/          # JSON serialization
│   │   └── v2/            # v2 API compatibility
│   ├── infra/             # Infrastructure
│   │   ├── attributes/    # Attribute types, U128 hashing
│   │   ├── backup/        # Backup support
│   │   ├── bloom/         # Bloom filter
│   │   ├── cache/         # FSM attribute cache (generation-based)
│   │   ├── coldstorage/   # Cold storage (S3)
│   │   ├── health/        # Health checks
│   │   ├── monitoring/    # Observability (OTLP, Pyroscope, tracing, diskusage)
│   │   ├── node/          # Raft node lifecycle and transport
│   │   ├── preload/       # Preloading
│   │   ├── receipt/       # Receipt handling
│   │   ├── state/         # FSM state machine and snapshots
│   │   └── transport/     # gRPC connection pool
│   ├── pkg/               # Pure utilities (zero/low internal deps)
│   │   ├── bitset/        # Bitset utilities
│   │   ├── bytesize/      # Byte size formatting
│   │   ├── commands/      # Raft command builders
│   │   ├── filterexpr/    # Filter expressions
│   │   ├── futures/       # Async futures for proposal results
│   │   ├── kv/            # Key-value map utilities
│   │   ├── semver/        # Semantic versioning
│   │   ├── signal/        # Signal utilities
│   │   ├── tarutil/       # Tar archive extraction
│   │   ├── vtmarshal/     # VT protobuf marshal utilities
│   │   └── worker/        # Worker pool
│   ├── query/             # CQRS read-side queries
│   ├── storage/           # Storage layer
│   │   ├── dal/           # Data access layer (Pebble)
│   │   ├── pebblecfg/    # Pebble configuration
│   │   ├── readstore/    # Read-side store
│   │   ├── spool/         # Spool for sync buffering (Raft entries)
│   │   └── wal/           # Write-ahead log (etcd/raft)
│   └── proto/             # Generated protobuf types
│       ├── auditpb/       # Audit types
│       ├── clusterpb/     # Cluster state types
│       ├── commonpb/      # Common types (Posting, Transaction, Log)
│       ├── eventspb/      # Event types
│       ├── raftcmdpb/     # FSM command types
│       ├── rafttransportpb/ # Raft transport types
│       ├── restorepb/     # Restore types
│       ├── servicepb/     # gRPC service definitions
│       ├── signaturepb/   # Signature types
│       └── snapshotpb/    # Snapshot service types
├── pkg/                    # Exported packages
│   ├── actions/           # Action helpers
│   ├── scenario/          # Scenario helpers
│   └── testserver/        # Test helpers
├── misc/                   # Miscellaneous files
│   ├── demo/              # VHS tape files for CLI demos
│   ├── devenv/            # Development environment
│   ├── numscript/         # Numscript examples
│   ├── pg-import/         # PostgreSQL import tools
│   └── proto/             # Protocol Buffer definitions
├── tests/                 # Tests
│   ├── antithesis/        # Antithesis tests
│   ├── e2e/               # End-to-end tests
│   │   ├── business/      # Single-node business logic tests
│   │   ├── cluster/       # Multi-node cluster tests
│   │   └── testutil/      # E2E test utilities
│   ├── perf/              # Performance tests
│   ├── scenarios/         # Scenario tests
│   └── schemathesis/      # Schemathesis API tests
└── docs/                  # Technical documentation
```

## Code Conventions

### File Organization

#### HTTP handlers

Each HTTP handler has its own file:
- `handlers_create_ledger.go`
- `handlers_get_ledger.go`
- `handlers_delete_ledger.go`
- `handlers_create_transaction.go`
- `handlers_save_transaction_metadata.go`
- `handlers_delete_transaction_metadata.go`
- `handlers_save_account_metadata.go`
- `handlers_delete_account_metadata.go`
- `handlers_bulk.go`
- etc.

#### CLI Commands (`cmd/ledgerctl/`)

Commands are organized into sub-packages, each containing their own command definitions:

```
cmd/ledgerctl/
  main.go
  accounts/       # Account commands
  accounttypes/   # Account type commands
  audit/          # Audit commands
  auth/           # Authentication commands
  cluster/        # Cluster management
  cmdutil/        # Command utilities
  events/         # Event/sink commands
  ledgers/        # Ledger commands
  logs/           # Log commands
  numscripts/     # Numscript commands
  periods/        # Period commands
  profile/        # Profile commands
  provision/      # Provisioning commands
  queries/        # Prepared query commands
  querycheckpoint/ # Query checkpoint commands
  restore/        # Restore commands
  signing/        # Signing commands
  store/          # Store commands
  transactions/   # Transaction commands
  upgrade/        # Upgrade commands
```

### Naming

- **Packages**: lowercase, single word
- **Types**: PascalCase
- **Public functions**: PascalCase
- **Private functions**: camelCase
- **Constants**: PascalCase or UPPER_SNAKE_CASE

### Documentation

- All public types and functions must have documentation
- Use `//` for line comments
- Use `/* */` for block comments (rare)

## Code Architecture

### Dependency Injection with fx

The project uses Uber's `fx` for dependency injection. Components are provided through `fx.Provide()` and lifecycle hooks are registered via `fx.Invoke()`.

### Lifecycle Management

All components with a lifecycle use `fx.Lifecycle` to register `OnStart` and `OnStop` hooks.

## Adding a New Feature

### Example: Adding an HTTP Endpoint

1. **Create the handler** in `internal/adapter/http/handlers_*.go`
2. **Register the route** in `internal/adapter/http/handler.go`
3. **Add to OpenAPI** in `openapi.yml`

### Example: Adding an FSM Command

1. **Define the protobuf** in `misc/proto/raft_cmd.proto`

2. **Regenerate protobufs** using `just generate-proto`

3. **Create the command function** in `internal/pkg/commands/command.go`

4. **Add the handler in the FSM** in `internal/infra/state/machine.go`

5. **Update `ApplyEntries`** to route the command to the handler

## Tests

### Test Structure

- **Unit tests**: In the same package with suffix `_test.go`
- **Integration tests**: In `*_integration_test.go`
- **E2E tests**: In `tests/e2e/` sub-packages with tag `//go:build e2e`
  - `tests/e2e/business/` - Single-node business logic tests
  - `tests/e2e/cluster/` - Multi-node cluster tests

### Write a Unit Test

Unit tests follow the Arrange-Act-Assert pattern and are placed in the same package with the `_test.go` suffix.

### Write an E2E Test

E2E tests are organized in sub-packages under `tests/e2e/` with the `//go:build e2e` tag:
- `tests/e2e/business/` for single-node business logic tests
- `tests/e2e/cluster/` for multi-node cluster tests

They typically set up a cluster, run tests, and clean up.

### Test Helpers

The package `pkg/testserver` provides helpers for creating test servers with configurable options.

## Protocol Buffers

### Structure

- **`misc/proto/common.proto`**: Common types (Posting, Transaction, Log, etc.)
- **`misc/proto/raft_cmd.proto`**: FSM command types (CreateLedger, DeleteLedger, CreateLog, etc.)
- **`misc/proto/bucket.proto`**: gRPC service definitions (BucketService)
- **`misc/proto/cluster.proto`**: Cluster state messages
- **`misc/proto/snapshot.proto`**: Snapshot service definitions
- **`misc/proto/raft_transport.proto`**: Raft transport messages
- **`misc/proto/audit.proto`**: Audit types
- **`misc/proto/signature.proto`**: Signature types
- **`misc/proto/events.proto`**: Event types
- **`misc/proto/restore.proto`**: Restore types

### Regenerate protobufs

Use `just generate-proto` to regenerate Go code from `.proto` files. The generated files are placed in the correct directories according to `go_package`.

### Modify a Protobuf

1. Modify the `.proto` file
2. Regenerate: `just generate-proto`
3. Update the Go code that uses the types
4. Check that everything compiles

## OpenAPI and SDK

### Modify the API

1. Modify `openapi.yml`
2. Validate the YAML
3. Update the tests if necessary

## Design Principles

### FSM: Performance First

**CRITICAL**: FSMs must be fast as they are called in the critical path of Raft consensus.

**Why performance matters**:
- FSMs are invoked synchronously during entry application
- Slow FSMs block the Raft consensus loop
- Performance directly impacts transaction throughput and latency

**Best practices**:
- Prefer in-memory operations when possible (orders of magnitude faster)
- Minimize I/O operations during entry application
- Use efficient data structures for lookups (maps instead of linear scans)
- Batch operations when possible
- Consider async I/O for non-critical operations

**Note**: While I/O is not strictly forbidden, it should be minimized and optimized. The ledger FSM performs I/O to update Store, but this is done efficiently and is necessary for maintaining balances.

### Request Forwarding

When a node receives a write request but is not the leader:

1. Check `IsLeader()`
2. If not leader, get the leader: `GetLeader()`
3. If no leader, return `ErrNoLeader`
4. Forward to the leader via gRPC
5. Return the response

### Error Handling

- **Business errors**: Return appropriate HTTP codes (400, 404, 409)
- **System errors**: Return 500 or 503 with details
- **No leader**: Return 503 with `Retry-After`

## Development Tools

### Justfile

The project uses `just` for common commands. See the `justfile` for available commands including `build`, `test`, `docker-build`, and `generate-proto`.

### Nix

For a reproducible environment, use `nix develop` to enter the environment and `nix build` to build the application.

### Debugging

#### Logs

Enable debug logs by setting `DEBUG=true` when running the server.

#### Tracing

OpenTelemetry is integrated. Configure the OTLP endpoint to see traces.

#### Profiling

Use `pprof` for profiling by accessing the pprof endpoint at `/debug/pprof/profile`.

## Checklist for a Pull Request

- [ ] Code compiles without errors
- [ ] Tests pass (Unit, integration, e2e)
- [ ] Documentation updated if necessary
- [ ] OpenAPI updated if new API
- [ ] Protobufs regenerated if modified
- [ ] No `time.Sleep` in tests (Use `Eventually`)
- [ ] Error handling appropriate
- [ ] Structured logs with context
- [ ] Minimize I/O in FSMs -- I/O is not strictly forbidden but should be minimized and optimized

## References

- [AGENTS.md](../../../CLAUDE.md): Project structure and conventions
- [Architecture](../architecture/core/architecture.md): General architecture
- [Raft Consensus](../architecture/core/raft-consensus.md): Raft details
- [API](../architecture/api/api.md): API documentation
- [CLI Reference](../../ops/cli.md): CLI client documentation
- [Numscript Examples](../../../misc/numscript/examples/README.md): Example Numscript files

## Next Steps

To contribute effectively:

1. Read [AGENTS.md](../../../CLAUDE.md) for conventions
2. Explore existing code to understand patterns
3. Write tests for your feature
4. Document important changes
