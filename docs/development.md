# Development Guide

## Overview

This guide provides the information needed to contribute to the Ledger v3 POC project, understand the code Structure, and follow project conventions.

## Project Structure

```
ledger-v3-poc/
├── cmd/                    # Entry points of the application
│   ├── server/            # Main server
│   └── client/             # CLI client
├── internal/               # Internal code (not exported)
│   ├── application/       # Application module main
│   ├── raft/              # Raft implementation
│   │   ├── system/        # System Raft group
│   │   └── ledger/        # Ledger Raft groups
│   ├── service/           # Business services
│   ├── http/              # HTTP handlers
│   ├── grpc/              # gRPC server
│   ├── transport/         # gRPC transport
│   ├── ledgerpb/          # Ledger protobuf types
│   └── otlplogs/          # OpenTelemetry logs
├── pkg/                    # Exported packages
│   ├── client/            # Generated client SDK
│   └── testserver/        # Test helpers
├── proto/                 # Protocol Buffer definitions
│   └── commands/          # FSM command definitions
├── tests/                 # Tests
│   └── e2e/               # End-to-end tests
├── deployments/           # Deployment configurations
│   └── chart/             # Helm chart
└── docs/                  # Technical documentation
```

## Conventions de Code

### organisation des Files

#### HTTP handlers

Each HTTP handler has its own file:
- `handlers_create_ledger.go`
- `handlers_get_ledger.go`
- `handlers_delete_ledger.go`
- `handlers_create_transaction.go`
- `handlers_save_account_metadata.go`
- `handlers_bulk.go`
- etc.

#### CLI Commands

Each CLI command has its own file:
- `ledgers_create.go`
- `ledgers_list.go`
- `ledgers_get.go`
- `ledgers_delete.go`
- `ledgers_raft_state.go`
- `cluster.go` (contains snapshot and cluster-state commands)

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

The project uses Uber's `fx` for dependency injection:

```go
func Module() fx.Option {
    return fx.Options(
        fx.Provide(
            // Provide dependencies
            system.NewNode,
            httphandler.NewServer,
        ),
        fx.Invoke(
            // Invoke lifecycle hooks
            func(lc fx.Lifecycle, node *system.Node) {
                lc.Append(fx.Hook{
                    OnStart: func(ctx context.Context) error {
                        return node.Start()
                    },
                    OnStop: func(ctx context.Context) error {
                        return node.Stop(ctx)
                    },
                })
            },
        ),
    )
}
```

### Lifecycle Management

All components with a lifecycle use `fx.Lifecycle`:

```go
func NewComponent(lc fx.Lifecycle, deps...) (*Component, error) {
    component := &Component{...}
    
    lc.Append(fx.Hook{
        OnStart: func(ctx context.Context) error {
            return component.Start(ctx)
        },
        OnStop: func(ctx context.Context) error {
            return component.Stop(ctx)
        },
    })
    
    return component, nil
}
```

## Adding a New Feature

### Example: Adding an HTTP Endpoint

1. **Create the handler** in `internal/http/handlers_*.go`

```go
func (s *Server) handleNewEndpoint(w http.ResponseWriter, r *http.Request) {
    // Implementation
    api.Ok(w, response)
}
```

2. **Register the route** in `internal/http/handler.go`

```go
r.With(contentTypeMiddleware).Group(func(r chi.Router) {
    // ... existing routes ...
    r.Get("/new-endpoint", server.handleNewEndpoint)
})
```

3. **Add to OpenAPI** in `openapi.yml`

```yaml
/new-endpoint:
  get:
    summary: New endpoint
    responses:
      '200':
        description: Success
```

4. **Regenerate the SDK** (if necessary)

```bash
just generate-sdk
```

### Example: Adding an FSM Command

1. **Define the protobuf** in `proto/commands/*.proto`

For system FSM commands, add to `proto/commands/system_commands.proto`:
```protobuf
message NewCommand {
  string field = 1;
}
```

For ledger FSM commands, add to `proto/commands/ledger_commands.proto`.

2. **Regenerate protobufs**

```bash
just generate-proto
```

3. **Create the command function** in `internal/raft/system/command.go` (or `internal/raft/ledger/command.go`)

```go
func NewNewCommand(field string) (*raft.Command, error) {
    cmdProto := &NewCommand{Field: field}
    data, err := proto.Marshal(cmdProto)
    if err != nil {
        return nil, err
    }
    return &raft.Command{
        ID:   generateRandomID(),
        Type: CommandTypeNew,
        Data: data,
        Date: time.Now(),
    }, nil
}
```

4. **Add the handler in the FSM** in `internal/raft/system/fsm.go` (or `internal/raft/ledger/fsm.go`)

```go
func (fsm *FSM) handleNewCommand(cmd raft.Command) error {
    var newCmd NewCommand
    if err := UnmarshalCommandData(cmd.Data, &newCmd); err != nil {
        return err
    }
    
    // Process the command
    // Update the FSM state
    
    return nil
}
```

5. **Update `ApplyEntries`** to route the command

```go
func (fsm *FSM) ApplyEntries(ctx context.Context, commands ...raft.Command) []raft.ApplyResult {
    results := make([]raft.ApplyResult, 0, len(commands))
    for _, cmd := range commands {
        switch cmd.Type {
        case CommandTypeNew:
            err := fsm.handleNewCommand(cmd)
            results = append(results, raft.ApplyResult{Error: err})
        // ...
        }
    }
    return results
}
```

## Tests

### Test Structure

- **Unit tests**: In the same package with suffix `_test.go`
- **Integration tests**: In `*_integration_test.go`
- **E2E tests**: In `tests/e2e/` with tag `//go:build e2e`

### Write a Unit Test

```go
func TestMyFunction(t *testing.T) {
    // Arrange
    input := "test"
    
    // Act
    result := MyFunction(input)
    
    // Assert
    assert.Equal(t, "expected", result)
}
```

### Write an E2E Test

```go
//go:build e2e

func TestFeature(t *testing.T) {
    // Setup cluster
    servers := setupCluster(t, 3)
    defer cleanupCluster(t, servers)
    
    // Test
    result, err := servers[0].client.DoSomething()
    require.NoError(t, err)
    assert.NotNil(t, result)
}
```

### Test Helpers

The package `pkg/testserver` provides helpers for creating test servers:

```go
server := testserver.New(
    cmdserver.NewRootCommand,
    testserver.WithInstruments(
        testserver.WithNodeID(1),
        testserver.WithHTTPPort(9000),
    ),
)
```

## Protocol Buffers

### Structure

- **`proto/ledger.proto`**: Common types (Posting, Transaction, Log)
- **`proto/system.proto`**: System service (ledgers)
- **`proto/raft_transport.proto`**: Raft transport messages
- **`proto/commands/`**: FSM commands
  - `commands.proto`: Base command structure
  - `system_commands.proto`: System FSM commands (create/delete ledger)
  - `ledger_commands.proto`: Ledger FSM commands (insert log)

### Regenerate protobufs

```bash
just generate-proto
```

This command:
1. Generates Go code from the `.proto` files
2. Places the files in the correct directories according to `go_package`

### Modify a Protobuf

1. Modify the `.proto` file
2. Regenerate: `just generate-proto`
3. Update the Go code that uses the types
4. Check that everything compiles

## OpenAPI and SDK

### Modify the API

1. Modify `openapi.yml`
2. Validate the YAML
3. Regenerate the SDK: `just generate-sdk`
4. Update the tests if necessary

### Retry Configuration

The retry configuration is in `openapi.yml`:

```yaml
x-speakeasy-retries:
  strategy: backoff
  backoff:
    initialInterval: 500
    maxInterval: 60000
    maxElapsedTime: 3600000
    exponent: 1.5
  statusCodes:
    - 503
  retryConnectionErrors: true
```

## Design Principles

### FSM: No I/O

**CRITICAL**: FSMs must never perform I/O (File, Network, database).

**Why**:
- FSMs must be deterministic
- I/O introduces non-determinism
- I/O can fail, making the FSM unreliable

**What to do**:
- Store all data in memory in the FSM
- Perform I/O during snapshot creation
- Restore from snapshots at startup

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

The project uses `just` for common commands:

```bash
just build          # Compile the application
just test           # Run tests
just docker-up      # Start the Docker cluster
just generate-proto # Regenerate protobufs
just generate-sdk   # Regenerate the client SDK
```

### Nix

For a reproducible environment:

```bash
nix develop         # Enter the environment
nix build           # Build the application
```

### Debugging

#### Logs

Enable debug logs:

```bash
DEBUG=true go run ./cmd/server ...
```

#### Tracing

OpenTelemetry is integrated. Configure the OTLP endpoint to see traces.

#### Profiling

Use `pprof` for profiling:

```bash
go tool pprof http://localhost:9000/debug/pprof/profile
```

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
- [Architecture](./architecture.md): General architecture
- [Raft Consensus](./raft-consensus.md): Raft details
- [API](./api.md): API documentation

## Next Steps

To contribute effectively:

1. Read [AGENTS.md](../AGENTS.md) for conventions
2. Explore existing code to understand patterns
3. Write tests for your feature
4. Document important changes

