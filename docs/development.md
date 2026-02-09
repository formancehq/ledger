# Development Guide

## Overview

This guide provides the information needed to contribute to the Ledger v3 POC project, understand the code Structure, and follow project conventions.

## Project Structure

```
ledger-v3-poc/
├── cmd/                    # Entry points of the application
│   ├── server/            # Main server
│   └── ledgerctl/         # CLI client (ledgerctl)
├── numscript/              # Numscript resources
│   └── examples/          # Example Numscript files
├── internal/               # Internal code (not exported)
│   ├── application/       # Application module (fx wiring, gRPC servers)
│   ├── raft/              # Raft implementation (single group)
│   ├── ctrl/              # Controller layer (transaction processing)
│   ├── service/           # Business services
│   │   ├── admission/     # Admission service (preload computation, AttributeLoader)
│   │   ├── attributes/    # Attribute types, U128 hashing, collision detection
│   │   ├── cache/         # FSM attribute cache (generation-based)
│   │   ├── commands/      # Raft command builders
│   │   ├── futures/       # Async futures for proposal results
│   │   ├── kv/            # Key-value map utilities
│   │   ├── node/          # Raft node lifecycle and transport
│   │   ├── processing/    # Transaction/log processing
│   │   ├── state/         # FSM state machine and snapshots
│   │   └── transport/     # gRPC connection pool
│   ├── compat/            # Compatibility layer
│   │   ├── http/          # HTTP handlers
│   │   └── json/          # JSON utilities
│   ├── storage/           # Storage layer
│   │   ├── data/          # Main data store (Pebble)
│   │   ├── spool/         # Spool for sync buffering
│   │   └── wal/           # Write-ahead log
│   ├── transport/         # gRPC transport and connection pool
│   ├── monitoring/        # Observability modules
│   │   ├── otlplogs/      # OpenTelemetry logs
│   │   ├── pyroscope/     # Continuous profiling
│   │   └── tracesampling/ # Trace sampling
│   ├── proto/             # Generated protobuf types
│   │   ├── commonpb/      # Common types (Posting, Transaction, Log, etc.)
│   │   ├── raftcmdpb/     # FSM command types
│   │   ├── servicepb/     # gRPC service definitions
│   │   ├── clusterpb/     # Cluster state types
│   │   └── snapshotpb/    # Snapshot service types
│   └── utils/             # Utility functions
├── pkg/                    # Exported packages
│   └── testserver/        # Test helpers
├── misc/                   # Miscellaneous files
│   └── proto/             # Protocol Buffer definitions
├── tests/                 # Tests
│   └── e2e/               # End-to-end tests
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

Each CLI command has its own file:
- `ledgers.go` - Parent command for ledger operations
- `ledgers_create.go` - Create a ledger
- `ledgers_list.go` - List all ledgers
- `ledgers_get.go` - Get a specific ledger
- `accounts.go` - Parent command for account operations
- `accounts_get.go` - Get an account with volumes
- `transactions.go` - Parent command for transaction operations
- `transactions_create.go` - Create a transaction
- `transactions_get.go` - Get a transaction by ID
- `store.go` - Parent command for store operations
- `store_metrics.go` - Get Pebble storage metrics

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

1. **Create the handler** in `internal/compat/http/handlers_*.go`
2. **Register the route** in `internal/compat/http/handler.go`
3. **Add to OpenAPI** in `openapi.yml`

### Example: Adding an FSM Command

1. **Define the protobuf** in `misc/proto/raftcmd.proto`

2. **Regenerate protobufs** using `just generate-proto`

3. **Create the command function** in `internal/raft/command.go`

4. **Add the handler in the FSM** in `internal/raft/fsm.go`

5. **Update `ApplyEntries`** to route the command to the handler

## Tests

### Test Structure

- **Unit tests**: In the same package with suffix `_test.go`
- **Integration tests**: In `*_integration_test.go`
- **E2E tests**: In `tests/e2e/` with tag `//go:build e2e`

### Write a Unit Test

Unit tests follow the Arrange-Act-Assert pattern and are placed in the same package with the `_test.go` suffix.

### Write an E2E Test

E2E tests are placed in `tests/e2e/` with the `//go:build e2e` tag. They typically set up a cluster, run tests, and clean up.

### Test Helpers

The package `pkg/testserver` provides helpers for creating test servers with configurable options.

## Protocol Buffers

### Structure

- **`misc/proto/common.proto`**: Common types (Posting, Transaction, Log, etc.)
- **`misc/proto/raftcmd.proto`**: FSM command types (CreateLedger, DeleteLedger, CreateLog, etc.)
- **`misc/proto/service.proto`**: gRPC service definitions (LedgerService)
- **`misc/proto/cluster.proto`**: Cluster state messages
- **`misc/proto/snapshot.proto`**: Snapshot service definitions
- **`misc/proto/raft_transport.proto`**: Raft transport messages

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

The project uses `just` for common commands. See the `justfile` for available commands including `build`, `test`, `docker-up`, and `generate-proto`.

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
- [ ] SDK regenerated if OpenAPI modified
- [ ] No `time.Sleep` in tests (Use `Eventually`)
- [ ] Error handling appropriate
- [ ] Structured logs with context
- [ ] No I/O in FSMs

## References

- [AGENTS.md](../AGENTS.md): Project structure and conventions
- [Architecture](./architecture/architecture.md): General architecture
- [Raft Consensus](./architecture/raft-consensus.md): Raft details
- [API](./architecture/api.md): API documentation
- [CLI Reference](./cli.md): CLI client documentation
- [Numscript Examples](../numscript/examples/README.md): Example Numscript files

## Next Steps

To contribute effectively:

1. Read [AGENTS.md](../AGENTS.md) for conventions
2. Explore existing code to understand patterns
3. Write tests for your feature
4. Document important changes
