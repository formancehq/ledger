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

All methods (receiver functions) for a given struct must be defined in the **same file** as the struct definition. Standalone functions (constructors, helpers) may live in separate files within the same package. If a file grows large because its struct has many methods, extract sub-types (composition) rather than splitting methods across files.

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

## Method Design

Prefer adding parameters to existing methods over creating separate methods. When adding a boolean mode (dry run, force, preview), add it as a parameter to the existing method rather than creating a new method.

## No Type Aliases

Never use type aliases (`type X = Y`). Always use the original type directly.

## No Global Variables

Avoid global variables for command flags. Use a struct to hold options and extract values in the `RunE` function.

## Pre-commit Checks

Before completing a PR candidate, run the base-sensitive local selector:

```bash
AI_REVIEW_BASE_SHA=<base-sha> bash scripts/agent-check-pr
```

For relevant inputs this runs the repository's straightforward pre-commit
recipe once, followed by focused validation. Documentation-only changes skip
Go normalization. The pinned Nix entry point clears inherited `GOROOT` and does
not use a login shell. See [Local validation](local-validation.md).

## Mock Generation

After any change to interfaces annotated with `//go:generate mockgen`, regenerate mocks:

```bash
go generate ./...
```

Do not maintain a static interface inventory here; annotations are the source
of truth and new interfaces are added regularly. Discover them with:

```bash
rg -n '^//go:generate mockgen' --glob '*.go'
```

Always run `go generate ./...` rather than generating only the interface you
remember changing, so every annotated package stays synchronized.

Use mockgen source mode (`-source <file>`) for interfaces declared in this
repository, including interfaces from another package (`-source
../../infra/backup/storage.go`). Package mode (an import path or `.` followed
by interface names) type-checks the whole dependency graph from source on every
call and costs roughly a second per directive; source mode parses one file
in about 0.1 seconds.

Source mode mocks every interface in the file and ignores trailing interface
names. Exclude the others with `-exclude_interfaces`, for example the server
interfaces in a `*_grpc.pb.go` file. Keep package mode only where source mode
cannot produce the mock: interfaces from external modules, such as
`google.golang.org/grpc` streams, and type aliases of generic instantiations,
such as `readstore.EntityIterator`. Generate several interfaces from one
package-mode import path in a single directive so the package loads once.

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
- [ ] No `time.Sleep` in tests outside a `testing/synctest` bubble
- [ ] Errors handled appropriately
