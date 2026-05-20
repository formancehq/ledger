# Code Conventions

## File Organization

### One file per command (CLI)

Each CLI sub-command has its own file in a sub-package under `cmd/ledgerctl/`:
- `cmd/ledgerctl/ledgers/root.go` - Parent command
- `cmd/ledgerctl/ledgers/create.go` - `ledgers create`
- `cmd/ledgerctl/ledgers/list.go` - `ledgers list`
- `cmd/ledgerctl/ledgers/get.go` - `ledgers get`

### One file per HTTP handler

Each HTTP handler has its own file in `internal/adapter/http/`:
- `handlers_create_ledger.go` - `POST /{ledgerName}`
- `handlers_get_ledger.go` - `GET /{ledgerName}`
- `handlers_create_transaction.go` - `POST /{ledgerName}/transactions`

### Struct methods colocation

All methods (receiver functions) for a given struct must be defined in the **same file** as the struct definition. Standalone functions (constructors, helpers) may live in separate files within the same package.

## Naming

- **Packages**: lowercase, single word
- **Types**: PascalCase
- **Public functions**: PascalCase
- **Private functions**: camelCase
- **JSON properties**: camelCase (e.g., `ledgerName`, `nextLogId`, `createdAt`)

## Variable Declarations

Group variable declarations in a block:

```go
// Good
var (
    address      string
    metadataJSON string
    volumesJSON  string
)

// Bad
var address string
var metadataJSON string
var volumesJSON string
```

## Error Handling

Always handle errors explicitly:

```go
// Good
data, err := json.Marshal(obj)
if err != nil {
    return fmt.Errorf("failed to marshal: %w", err)
}

// Good - explicit discard with reason
_ = file.Close() // Best effort cleanup

// Bad - silent ignore
data, _ := json.Marshal(obj)
```

## No Type Aliases

Never use type aliases (`type X = Y`). Always use the original type directly.

## No Global Variables

Avoid global variables for command flags. Use a struct to hold options and extract values in the `RunE` function.

## Pre-commit Checks

Before completing any task:

```bash
# For Nix environments
direnv allow && eval "$(direnv export bash)" && GOROOT= just pre-commit
```

This runs:
1. `go generate ./...` - regenerate mocks and protobuf files
2. `go mod tidy` - clean up dependencies
3. `golangci-lint run --fix` - lint and auto-fix

## Mock Generation

After any change to interfaces annotated with `//go:generate mockgen`, regenerate mocks:

```bash
go generate ./...
```

Interfaces with mockgen annotations:

| Interface | File |
|-----------|------|
| `Transport` | `internal/infra/node/transport.go` |
| `Controller` | `internal/application/ctrl/controller.go` |
| `Admission` | `internal/application/ctrl/controller_default.go` |
| `Spool` | `internal/storage/spool/spool.go` |
| `WAL` | `internal/storage/wal/wal.go` |
| `InMemoryStore` | `internal/domain/processing/store.go` |
| `Checker` | `internal/infra/health/healthcheck.go` |
| `Proposer` | `internal/infra/state/metadata_converter.go` |

## Adding a New CLI Command

1. Create `cmd/ledgerctl/<group>/<action>.go` with:
   - Command variable (`var fooBarCmd = &cobra.Command{...}`)
   - `init()` function to register it under the parent
   - `runFooBar()` function with the logic

2. The command is automatically registered via `init()`.

## Adding a New HTTP Handler

1. Create `internal/adapter/http/handlers_<action>.go` with the handler function
2. Register the route in `internal/adapter/http/handler.go`

## PR Checklist

- [ ] Code compiles without errors
- [ ] Tests pass (unit, integration, e2e)
- [ ] Documentation updated if necessary
- [ ] OpenAPI updated if new API
- [ ] Protobufs regenerated if modified
- [ ] No `time.Sleep` in tests
- [ ] Errors handled appropriately
