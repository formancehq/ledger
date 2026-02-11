# AGENTS.md - CLI Client Structure

This document describes the organizational structure of the CLI client commands.

## Reference Implementation

**The reference implementation for this project is `github.com/formancehq/ledger`.**

The ledger service implementation follows the same patterns and best practices as `github.com/formancehq/ledger`. When implementing new features or fixing bugs, refer to the original implementation for guidance on:
- Application structure and organization
- Dependency injection patterns (fx)
- Service startup and lifecycle management
- HTTP and gRPC server configuration
- OpenTelemetry instrumentation
- Configuration management
- Error handling and logging patterns

## Documentation Maintenance

**CRITICAL**: Always maintain the documentation when making any changes to the codebase.

When implementing new features, fixing bugs, or refactoring code:
- **Update relevant documentation files** in the `docs/` directory
- **Update API documentation** in `openapi.yml` if endpoints change
- **Update `docs/api-comparison.md`** when adding, modifying, or removing API endpoints to track feature parity with the original ledger
- **Update `docs/cli.md`** when adding, modifying, or removing CLI commands, flags, or behavior
- **Update code comments** if interfaces or behavior change
- **Update examples** if usage patterns change
- **Keep documentation in English** - all technical documentation must be in English

The documentation should always reflect the current state of the codebase. Outdated documentation is worse than no documentation.

### API Comparison Document

The `docs/api-comparison.md` file tracks feature parity between this POC and the original `github.com/formancehq/ledger`. When implementing new endpoints:

1. **Update the Summary table** - Add or modify the feature row with ✅/❌/⚠️ status
2. **Add implementation details** - Document the endpoint in "Features Implemented in POC" section
3. **Update Read Features table** - If it's a read endpoint, update the comparison table
4. **Remove from Missing Features** - If the feature was previously listed as missing, remove or update it

### CLI Documentation

The `docs/cli.md` file documents all CLI commands and their usage. When modifying the CLI (`cmd/ledgerctl/`):

1. **Update command documentation** - Add or modify the command section with usage, flags, and examples
2. **Update global flags** - If adding new persistent flags, document them in the Global Flags section
3. **Update examples** - Ensure examples are accurate and demonstrate common use cases
4. **Update Numscript examples** - If adding new Numscript-related features, update `misc/numscript/examples/`
5. **Update CLI demo** - If adding new commands or flags that should be showcased, update `misc/demo/demo.tape` and `misc/demo/simple-demo.tape`
6. **Regenerate demo GIFs** - After any CLI change (new commands, flags, output format, error messages), regenerate the demo GIFs by running `just generate-demo`. This ensures the recorded demos always reflect the current CLI behavior.

## Pre-commit Checks

**CRITICAL**: Before completing any task, you MUST run pre-commit checks to ensure code quality.

Run pre-commit checks:
```bash
just pre-commit
```

**For Nix environments**: This project uses Nix with direnv to provide the correct toolchain (Go, golangci-lint, protoc, etc.). Before running pre-commit, you MUST activate the Nix environment by running `direnv allow` first, then unset GOROOT to avoid conflicts with IntelliJ's configuration:
```bash
direnv allow && eval "$(direnv export bash)" && GOROOT= just pre-commit
```

This command:
1. Runs `go generate ./...` to regenerate mocks and protobuf files
2. Runs `go mod tidy` to clean up dependencies
3. Runs `golangci-lint run --fix` to check and auto-fix linting issues

**Note for AI agents**:
- Always activate the Nix environment first with `direnv allow && eval "$(direnv export bash)"`, then run `GOROOT= just pre-commit` after completing any code changes
- Always verify that the code compiles with `go build ./...` before submitting
- If pre-commit fails, fix the errors before completing the task

## Mock Generation

**CRITICAL**: After any change to interfaces annotated with `//go:generate mockgen`, you MUST regenerate the mocks immediately.

Interfaces annotated with mockgen:
- `WAL` in `internal/raft/node.go`
- `Transport` in `internal/raft/transport.go`
- `Controller` in `internal/ctrl/controller.go`
- `Engine` in `internal/ctrl/controller_default.go`
- `Spool` in `internal/storage/spool/spool.go`
- `WAL` in `internal/storage/wal/wal.go`
- `Store` in `internal/service/processing/processor.go`
- `Checker` in `internal/health/healthcheck.go`

To regenerate all mocks, run:
```bash
go generate ./...
```

Or regenerate mocks for a specific package:
```bash
go generate ./internal/raft/...
go generate ./internal/ctrl/...
go generate ./internal/storage/...
go generate ./internal/service/...
```

**Note for AI agents**: Always regenerate mock files automatically after modifying any interface annotated with `//go:generate mockgen`. Use `go generate` command with the appropriate file path to regenerate the mocks.

## JSON Property Naming Convention

**CRITICAL**: All JSON objects in the API must use **camelCase** for property names.

- **All properties in OpenAPI specification** (`openapi.yml`) must be in camelCase
- **All JSON tags in Go structs** used for HTTP request/response bodies must use camelCase
- **Examples**: `ledgerName`, `nextLogId`, `nextTransactionId`, `createdAt`, `idempotencyKey`, etc.

This convention ensures consistency across the API and matches common JavaScript/TypeScript conventions.

## File Structure

The client CLI and HTTP handlers are organized into separate files to improve maintainability and code readability.

### Client CLI (`cmd/ledgerctl/`)

The client CLI (`ledgerctl`) uses gRPC to communicate with the server.

**Entry point:**
- **`main.go`** : Main entry point, creates the root command and initializes all sub-commands

**Ledger commands:**
- **`ledgers.go`** : Parent command for ledger operations
- **`ledgers_list.go`** : `ledgers list` command to list all ledgers via gRPC
- **`ledgers_get.go`** : `ledgers get` command to retrieve a specific ledger via gRPC
- **`ledgers_create.go`** : `ledgers create` command to create a new ledger via gRPC
- **`ledgers_delete.go`** : `ledgers delete` command to delete a ledger via gRPC

**Account commands:**
- **`accounts.go`** : Parent command for account operations
- **`accounts_get.go`** : `accounts get` command to retrieve an account with its volumes via gRPC

**Transaction commands:**
- **`transactions.go`** : Parent command for transaction operations
- **`transactions_get.go`** : `transactions get` command to retrieve a transaction by ID via gRPC
- **`transactions_create.go`** : `transactions create` command to create a new transaction via gRPC

**Store commands:**
- **`store.go`** : Parent command for store operations
- **`store_metrics.go`** : `store metrics` command to retrieve Pebble storage metrics via gRPC
- **`store_check.go`** : `store check` command to verify store integrity (hash chain + derived data) via gRPC
- **`store_backup.go`** : `store backup` command to download a point-in-time backup as a tar archive via gRPC

**Audit commands:**
- **`audit.go`** : Parent command for audit log operations
- **`audit_list.go`** : `audit list` command to list audit entries (success + failure) via gRPC streaming

**Shared files:**
- **`common.go`** : Shared functions (gRPC client creation, context management, formatting utilities)

### CLI Demo (`misc/demo/`)

VHS tape files for generating animated GIF demos of the CLI. Each demo is self-contained (creates its own ledger, runs the scenario, cleans up):
- **`demo_getting_started.tape`** : Create a ledger, interactive wizard, list transactions
- **`demo_numscript.tape`** : Payment with fees, escrow with dynamic accounts
- **`demo_transactions.tape`** : Force transactions, revert transactions
- **`demo_metadata.tape`** : Account and transaction metadata CRUD
- **`demo_operations.tape`** : Cluster status, store integrity check, store backup
- **`demo_audit.tape`** : Audit log listing, failures-only filter, ledger filter
- **`README.md`** : Documentation for generating demos

**Generating demos:**
```bash
# Generate all demos (starts a temporary server automatically)
just generate-demo

# Generate a single demo
just generate-demo-only demo_numscript
```

**Note**: Update these files when adding new CLI commands or flags that should be showcased in the demo.

### Numscript Examples (`misc/numscript/examples/`)

Example Numscript files for use with the CLI:
- **`simple_transfer.num`** : Basic transfer between two accounts
- **`world_funding.num`** : Create money by funding from `@world`
- **`multi_destination.num`** : Split payment to multiple destinations
- **`multi_source.num`** : Pay from multiple sources (fallback pattern)
- **`bounded_overdraft.num`** : Allow overdraft up to a specific limit
- **`unbounded_overdraft.num`** : Allow unlimited overdraft
- **`payment_with_fees.num`** : Payment with platform fee calculation
- **`escrow_funding.num`** : Fund an escrow account with dynamic address

### Numscript Features

All experimental Numscript features are **enabled by default** in this implementation. The feature flags are defined in `internal/service/processing/processor.go` in the `numscriptFeatureFlags` variable.

**Enabled features:**
- **`experimental-account-interpolation`** : Dynamic account addresses using variable interpolation (e.g., `@escrow:$order_id`)
- **`experimental-asset-colors`** : Asset coloring for tracking fund origins
- **`experimental-get-amount-function`** : `get_amount()` function to extract amount from monetary values
- **`experimental-get-asset-function`** : `get_asset()` function to extract asset from monetary values
- **`experimental-mid-script-function-call`** : Mid-script function calls (e.g., balance queries during execution)
- **`experimental-oneof`** : `oneof` source/destination selector for conditional routing
- **`experimental-overdraft-function`** : `overdraft()` function for dynamic overdraft calculation

**Updating features:**

When new Numscript features are added to the `github.com/formancehq/numscript` library:
1. Update the `numscriptFeatureFlags` map in `internal/service/processing/processor.go`
2. Update this documentation section
3. Add appropriate tests if needed

**Building the client:**
```bash
# Using just
just build-client

# Or directly
go build -o build/ledgerctl ./cmd/ledgerctl
```

### Build Directory

All generated files and build artifacts are stored in the `build/` directory at the project root. This directory is gitignored.

Any script or tool that generates files should place them in the `build/` directory to keep the repository clean.

**CRITICAL**: Never leave built binaries in the repository root or any tracked directory. Always either:
- Build binaries into the `build/` directory: `go build -o build/ledgerctl ./cmd/client`
- Delete binaries after testing: `rm -f ledgerctl ledger-v3-poc`
- Use `just build-client` or `just build-server` which output to `build/`

Built binaries pollute the repository and should never be committed.

### HTTP Handlers (`internal/compat/http/`)

HTTP handlers are organized into separate files, with **one handler per file**. This convention ensures clear separation of concerns and makes it easy to locate and maintain individual handlers.

- **`server.go`** : Main file that defines the `Server` struct
- **`handler.go`** : Main router file that registers all routes and middleware
- **`error_handler.go`** : Shared error handling utilities
- **`response.go`** : Response helper functions
- **`handlers_health.go`** : `handleHealth` handler for GET /health
- **`handlers_create_ledger.go`** : `handleCreateLedger` handler for POST /{ledgerName}
- **`handlers_get_ledger.go`** : `handleGetLedger` handler for GET /{ledgerName}
- **`handlers_delete_ledger.go`** : `handleDeleteLedger` handler for DELETE /{ledgerName}
- **`handlers_list_all_ledgers.go`** : `handleListAllLedgers` handler for GET /
- **`handlers_create_transaction.go`** : `handleCreateTransaction` handler for POST /{ledgerName}/transactions
- **`handlers_get_transaction.go`** : `handleGetTransaction` handler for GET /{ledgerName}/transactions/{id}
- **`handlers_get_account.go`** : `handleGetAccount` handler for GET /{ledgerName}/accounts/{address}
- **`handlers_save_account_metadata.go`** : `handleSaveAccountMetadata` handler for POST /{ledgerName}/accounts/{address}/metadata
- **`handlers_delete_account_metadata.go`** : `handleDeleteAccountMetadata` handler for DELETE /{ledgerName}/accounts/{address}/metadata/{key}
- **`handlers_save_transaction_metadata.go`** : `handleSaveTransactionMetadata` handler for POST /{ledgerName}/transactions/{id}/metadata
- **`handlers_delete_transaction_metadata.go`** : `handleDeleteTransactionMetadata` handler for DELETE /{ledgerName}/transactions/{id}/metadata/{key}
- **`handlers_revert_transaction.go`** : `handleRevertTransaction` handler for POST /{ledgerName}/transactions/{id}/revert
- **`handlers_bulk.go`** : `handleBulk` handler for POST /{ledgerName}/_bulk

## Conventions

1. **One file per command** : Each sub-command (create, list, get, delete) has its own file
2. **One file per HTTP handler** : Each HTTP handler has its own file. This ensures clear separation of concerns and makes it easy to locate and maintain individual handlers.
3. **Parent file** : Each command group (ledgers, accounts, transactions) has a main file that defines the parent command
4. **No global variables** : Avoid using global variables for command flags. Instead, use a struct to hold command options and extract values from flags in the `RunE` function.
5. **Group variable declarations** : When initializing multiple variables, group them in a block using parentheses. This improves readability and consistency.
6. **No type aliases** : Never use type aliases (e.g., `type X = Y`). Always use the original type directly. This improves code clarity and avoids confusion about which type is actually being used.
7. **Never ignore errors** : Always handle errors explicitly. Use `_` to discard an error only when there is genuinely nothing to do with it, but never silently ignore errors.

   **Example**:
   ```go
   // ✅ Good: Error is handled
   data, err := json.Marshal(obj)
   if err != nil {
       return fmt.Errorf("failed to marshal: %w", err)
   }

   // ✅ Good: Error is explicitly discarded with comment when appropriate
   _ = file.Close() // Best effort cleanup, error doesn't affect outcome

   // ❌ Bad: Error silently ignored
   data, _ := json.Marshal(obj) // What if marshaling fails?

   // ❌ Bad: Error not checked at all
   json.Marshal(obj)
   ```

   **Example**:
   ```go
   // Good: Grouped variables
   var (
       address          string
       metadataJSON     string
       firstUsageStr    sql.NullString
       insertionDateStr sql.NullString
       updatedAtStr     sql.NullString
       volumesJSON      string
   )
   
   // Bad: Separate declarations
   var address string
   var metadataJSON string
   var firstUsageStr sql.NullString
   var insertionDateStr sql.NullString
   var updatedAtStr sql.NullString
   var volumesJSON string
   ```

## Example: Adding a New CLI Command

To add a new `ledgers delete` command:

1. Create `cmd/client/ledgers_delete.go` with:
   - Definition of `ledgersDeleteCmd`
   - `init()` function to configure flags
   - `runDeleteLedger()` function that implements the command logic

   Example structure:
   ```go
   var ledgersDeleteCmd = &cobra.Command{
       Use:   "delete <ledger-name>",
       Short: "Delete a ledger",
       Args:  cobra.ExactArgs(1),
       RunE:  runDeleteLedger,
   }

   func init() {
       ledgersCmd.AddCommand(ledgersDeleteCmd)
   }

   func runDeleteLedger(cmd *cobra.Command, args []string) error {
       ledgerName := args[0]
       // Implementation...
   }
   ```

2. The command is automatically added via `init()` function

## Example: Adding a New HTTP Handler

To add a new `handleUpdateLedger` handler:

1. Create `internal/compat/http/handlers_update_ledger.go` with:
   - `handleUpdateLedger` function implementation
   - Any request/response structures specific to this handler
   - All necessary imports for the handler

2. Modify `internal/compat/http/handler.go` in the route registration section to register the route:
   ```go
   r.With(contentTypeMiddleware).Group(func(r chi.Router) {
       // ... existing routes ...
       r.Put("/{ledgerName}", server.handleUpdateLedger) // PUT /{ledgerName}
   })
   ```

**Important**: Follow the "one handler per file" convention. Each handler should have its own file to ensure clear separation of concerns and easy maintenance.

## Protocol Buffers and gRPC Code Generation

The Raft transport layer and ledger service use gRPC for communication. Protocol buffer definitions are stored in the `misc/proto/` directory, while the generated Go code is placed in the appropriate internal packages.

### File Locations

- **Protocol definitions**: 
  - `misc/proto/raft_transport.proto` - Raft transport messages
  - `misc/proto/common.proto` - Common types (Posting, Transaction, Log, etc.)
  - `misc/proto/raftcmd.proto` - FSM command types (CreateLedger, DeleteLedger, CreateLog, etc.)
  - `misc/proto/service.proto` - gRPC service definitions (LedgerService)
  - `misc/proto/cluster.proto` - Cluster state messages
  - `misc/proto/snapshot.proto` - Snapshot service definitions
  - `misc/proto/audit.proto` - Audit log messages (AuditEntry, AuditSuccess, AuditFailure)
- **Generated code**: 
  - `internal/raft/raft_transport.pb.go` and `internal/raft/raft_transport_grpc.pb.go` - Raft transport
  - `internal/proto/commonpb/` - Common types (common.pb.go, etc.)
  - `internal/proto/raftcmdpb/` - FSM command types (raftcmd.pb.go, etc.)
  - `internal/proto/servicepb/` - gRPC service (service.pb.go, service_grpc.pb.go, etc.)
  - `internal/proto/clusterpb/` - Cluster state (cluster.pb.go, etc.)
  - `internal/proto/snapshotpb/` - Snapshot service (snapshot.pb.go, snapshot_grpc.pb.go)
  - `internal/proto/auditpb/` - Audit log types (audit.pb.go)

### Regenerating Code

To regenerate the gRPC code from the protocol buffer definitions:

```bash
just generate-proto
```

This command:
1. Reads the `.proto` files from the `proto/` directory
2. Generates Go code using `protoc` with the `protoc-gen-go` and `protoc-gen-go-grpc` plugins
3. Places the generated files in the appropriate directories (`internal/raft/` or `internal/service/`) based on the `go_package` option specified in each `.proto` file

### Prerequisites

- `protoc` (Protocol Buffer Compiler) must be installed
- Go plugins: `protoc-gen-go` and `protoc-gen-go-grpc` must be available in your PATH

To install the Go plugins:
```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
```

### Modifying Protocol Definitions

**CRITICAL**: When modifying any `.proto` file, you MUST regenerate the protobuf code immediately after making the changes.

When modifying any `.proto` file:
1. **Realign field numbers sequentially** when adding/removing fields to avoid gaps, and remove obsolete `reserved` entries.
1. Edit the `.proto` file in the `proto/` directory
2. **IMMEDIATELY run `just generate-proto` to regenerate the Go code for all proto files** - Do not skip this step!
3. Update any code that uses the generated types if the API has changed
4. Rebuild the project to ensure everything compiles

**Note for AI agents**: Always regenerate protobuf files automatically after modifying any `.proto` file. Use `just generate-proto` command with `required_permissions: ['all']` to regenerate the code.

### Adding New Command Models

To add a new command model (e.g., for a new FSM command):

1. **Add the message definition to `misc/proto/raftcmd.proto`**

2. **Example**: Adding a new `UpdateLedgerCommand`:
   ```protobuf
   message UpdateLedgerCommand {
     string name = 1;
     map<string, string> config = 2;
   }
   ```

3. **Regenerate the protobuf code**:
   ```bash
   just generate-proto
   ```

4. **Update the Go code**:
   - Create a `NewUpdateLedgerCommand` function in `internal/raft/command.go` that:
     - Creates the protobuf command
     - Marshals the protobuf message
     - Returns a `*raftcmdpb.Command`
   - Update `UnmarshalCommandData` in `internal/raft/command.go` to handle the new command type
   - Add a handler method in `internal/raft/fsm.go` (e.g., `handleUpdateLedger`)

5. **Example implementation**:
   ```go
   // In internal/raft/command.go
   func NewUpdateLedgerCommand(cmd *raftcmdpb.UpdateLedgerCommand) *raftcmdpb.Command {
       data, err := proto.Marshal(cmd)
       if err != nil {
           panic(err)
       }
       return &raftcmdpb.Command{
           Id:   generateRandomID(),
           Type: raftcmdpb.ActionType_UpdateLedger,
           Data: data,
           Date: timestamppb.Now(),
       }
   }
   ```

6. **Rebuild and test**:
   ```bash
   go build ./...
   go test ./...
   ```

### Command Serialization Format

Commands are now serialized using Protocol Buffers instead of `gob`. This provides:
- **Better performance**: Protobuf is faster and produces smaller binary sizes
- **Language interoperability**: Commands can be read/written from other languages
- **Schema evolution**: Protobuf supports backward compatibility for schema changes
- **Type safety**: Generated code provides compile-time type checking

The serialization flow:
1. Go struct → Protobuf message (using conversion functions)
2. Protobuf message → Binary (using `proto.Marshal`)
3. Binary → Protobuf message (using `proto.Unmarshal`)
4. Protobuf message → Go struct (using conversion functions)

## Finite State Machine (FSM) Design Principles

### Single Raft Architecture

The system uses a **single Raft group** to manage all ledgers and their transactions. The FSM (`internal/raft/fsm.go`) handles:
- `CreateLedgerCommand`: Create a new ledger
- `DeleteLedgerCommand`: Delete an existing ledger
- `CreateLogCommand`: Insert a log (transaction, metadata changes, reversions) into any ledger

The FSM maintains a unified state containing all ledgers:
```go
type State struct {
    Ledgers map[string]*LedgerState  // All ledgers indexed by name
}
```

### Performance First

**CRITICAL**: FSMs should be fast as they are called in the critical path of Raft consensus.

**Why performance matters**:
- FSMs are invoked synchronously during entry application
- Slow FSMs block the Raft consensus loop
- Performance directly impacts transaction throughput and latency

**Best practices**:
- Keep ledger state in memory (the FSM state map)
- Minimize I/O operations during entry application
- Use efficient data structures for lookups
- Batch operations when possible

**Note**: The FSM performs I/O to update the RuntimeStore during log application. This is necessary for maintaining balances and is done efficiently.

## Dependency Injection with fx

The application uses Uber's `fx` (functional dependency injection) framework for managing dependencies and lifecycle, following the same patterns as `github.com/formancehq/ledger`.

### Architecture

The application is structured using fx modules:

- **`internal/application/module.go`**: Main application module that provides all core dependencies (logger, Raft cluster, ledger service, HTTP server, gRPC server)
- **`internal/transport/module.go`**: Transport module that provides gRPC connection pool
- **`internal/monitoring/otlplogs/module.go`**: OpenTelemetry logs module for structured logging
- **`internal/monitoring/pyroscope/module.go`**: Pyroscope continuous profiling module
- **`internal/monitoring/tracesampling/module.go`**: Trace sampling module

### Module Pattern

Each module exports a `Module()` function that returns `fx.Option`:

```go
func Module() fx.Option {
    return fx.Options(
        fx.Provide(
            // Provide dependencies
        ),
        fx.Invoke(
            // Invoke lifecycle hooks
        ),
    )
}
```

### Lifecycle Management

Components use `fx.Lifecycle` hooks for startup and shutdown:

```go
func NewComponent(lc fx.Lifecycle, dependencies...) (*Component, error) {
    component := &Component{...}
    
    lc.Append(fx.Hook{
        OnStart: func(ctx context.Context) error {
            // Start component
            return component.Start(ctx)
        },
        OnStop: func(ctx context.Context) error {
            // Stop component
            return component.Stop(ctx)
        },
    })
    
    return component, nil
}
```

### Application Startup

The application uses `github.com/formancehq/go-libs/v3/service` to manage startup and lifecycle, following the same pattern as `github.com/formancehq/ledger`:

```go
import (
    "github.com/formancehq/go-libs/v3/service"
    "github.com/formancehq/ledger-v3-poc/internal/application"
)

func runServer(cmd *cobra.Command, args []string) error {
    cfg, err := loadConfig(cmd)
    if err != nil {
        return err
    }

    // Create fx application options
    opts := []fx.Option{
        fx.Provide(func() *config.Config { return cfg }),
        application.Module(),
    }

    // Create service app
    app := service.New(os.Stdout, opts...)

    // Run the application (handles startup, signal handling, and graceful shutdown)
    return app.Run(cmd)
}

func main() {
    rootCmd := newRootCommand()
    service.Execute(rootCmd) // Binds env vars to flags and executes
}
```

The `service.Execute()` function:
- Automatically binds environment variables to command flags (converting `NODE_ID` → `--node-id`, etc.)
- Handles command execution and error reporting

The `app.Run()` function:
- Creates and starts the fx application
- Handles interrupt signals (SIGTERM, SIGINT)
- Manages graceful shutdown with configurable grace period
- Provides proper error handling and exit codes

### Adding a New Component

To add a new component using fx:

1. **Create a provider function**:
   ```go
   func NewMyComponent(lc fx.Lifecycle, dependencies...) (*MyComponent, error) {
       component := &MyComponent{...}
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

2. **Add to module**:
   ```go
   func Module() fx.Option {
       return fx.Options(
           fx.Provide(NewMyComponent),
           // ...
       )
   }
   ```

3. **Use in other components**:
   ```go
   func NewOtherComponent(myComponent *MyComponent) *OtherComponent {
       return &OtherComponent{component: myComponent}
   }
   ```

### Benefits

- **Automatic dependency resolution**: fx automatically resolves dependencies based on function parameters
- **Lifecycle management**: Components are started and stopped in the correct order
- **Testability**: Easy to provide mock dependencies in tests
- **Modularity**: Each module can be tested independently
- **Consistency**: Follows the same patterns as other Formance projects

### Using Formance Libraries

The application uses Formance's `go-libs` library for common functionality:

#### OpenTelemetry (OTLP)

OpenTelemetry is configured directly using `github.com/formancehq/go-libs/v3/otlp` and `github.com/formancehq/go-libs/v3/otlp/otlptraces` modules:

```go
// In cmd/server/main.go
import (
    "github.com/formancehq/go-libs/v3/otlp"
    "github.com/formancehq/go-libs/v3/otlp/otlptraces"
)

func newRootCommand() *cobra.Command {
    rootCmd := &cobra.Command{...}
    
    // Add OpenTelemetry flags from go-libs
    otlp.AddFlags(rootCmd.Flags())
    otlptraces.AddFlags(rootCmd.Flags())
    
    return rootCmd
}

func runServer(cmd *cobra.Command, args []string) error {
    // Set default service name if not provided via flags
    serviceName, _ := cmd.Flags().GetString(otlp.OtelServiceNameFlag)
    if serviceName == "" {
        defaultServiceName := fmt.Sprintf("ledger-v3-poc-node-%d", cfg.NodeID)
        cmd.Flags().Set(otlp.OtelServiceNameFlag, defaultServiceName)
    }
    
    opts := []fx.Option{
        // Add OpenTelemetry modules from go-libs (using flags)
        otlp.FXModuleFromFlags(cmd, otlp.WithServiceVersion(version)),
        otlptraces.FXModuleFromFlags(cmd),
        // ... other options
    }
    
    app := service.New(os.Stdout, opts...)
    return app.Run(cmd)
}
```

The `otlp.FXModuleFromFlags()` and `otlptraces.FXModuleFromFlags()` functions automatically read configuration from command flags and create the appropriate fx modules. The flags are automatically bound to environment variables (e.g., `OTEL_SERVICE_NAME`, `OTEL_TRACES_EXPORTER_OTLP_ENDPOINT`, etc.).

#### Pyroscope Continuous Profiling

Pyroscope is configured via CLI flags defined in `cmd/server/pyroscope.go`:

```go
// In cmd/server/server.go
func NewRunCommand() *cobra.Command {
    runCmd := &cobra.Command{...}
    
    // Add Pyroscope profiling flags
    addPyroscopeFlags(runCmd.Flags())
    
    return runCmd
}

func runServer(cmd *cobra.Command, args []string) error {
    // Configure Pyroscope profiling
    pyroscopeCfg := pyroscopeConfigFromFlags(cmd)
    
    opts := []fx.Option{
        // Add Pyroscope profiling module
        pyroscope.Module(pyroscopeCfg),
        // ... other options
    }
    
    return service.NewWithLogger(logger, opts...).Run(cmd)
}
```

The Pyroscope module (`internal/monitoring/pyroscope/`) provides:
- `Config` struct with all Pyroscope configuration options
- `Module(cfg Config) fx.Option` to integrate with fx lifecycle
- Automatic profiler start/stop with application lifecycle

Key environment variables:
- `PYROSCOPE_ENABLED`: Enable/disable profiling
- `PYROSCOPE_SERVER_ADDRESS`: Pyroscope server URL
- `PYROSCOPE_APPLICATION_NAME`: Application name in Pyroscope
- `PYROSCOPE_PROFILE_TYPES`: Comma-separated profile types (cpu, alloc_objects, etc.)

#### HTTP Server

The HTTP server is configured using `github.com/formancehq/go-libs/v3/httpserver`:

```go
// In internal/application/module.go
import (
    "github.com/formancehq/go-libs/v3/httpserver"
    httphandler "github.com/formancehq/ledger-v3-poc/internal/compat/http"
)

func StartHTTPServerHook(lc fx.Lifecycle, cfg *config.Config, handler http.Handler) {
    lc.Append(httpserver.NewHook(handler,
        httpserver.WithAddress(fmt.Sprintf(":%d", cfg.HTTPPort)),
    ))
}
```

The `httpserver.NewHook()` function returns an `fx.Hook` that manages the HTTP server lifecycle (start/stop) using `serverport` for port management. The handler is created separately using `httphandler.NewHandler()` which returns an `http.Handler`.

**Benefits**:
- **Standardized configuration**: Uses Formance's standard patterns for HTTP servers
- **Port management**: Automatic port binding and management via `serverport`
- **Lifecycle management**: Proper startup and shutdown hooks integrated with fx
- **Consistency**: Same patterns as other Formance services

### Using the Service Package

The application uses `github.com/formancehq/go-libs/v3/service` for application lifecycle management, following the same pattern as `github.com/formancehq/ledger`:

```go
// In cmd/server/main.go
import (
    "github.com/formancehq/go-libs/v3/service"
)

func main() {
    rootCmd := newRootCommand()
    service.Execute(rootCmd) // Binds env vars to flags and executes
}

func runServer(cmd *cobra.Command, args []string) error {
    // Load config from flags (env vars are automatically bound)
    cfg := loadConfig(cmd)
    
    // Create fx options
    opts := []fx.Option{
        fx.Provide(func() *config.Config { return cfg }),
        application.Module(),
    }
    
    // Create and run service app
    app := service.New(os.Stdout, opts...)
    return app.Run(cmd)
}
```

**Key features**:
- **Automatic env var binding**: `service.Execute()` automatically binds environment variables to flags (e.g., `NODE_ID` → `--node-id`)
- **Standard flags**: `service.AddFlags()` adds common flags like `--debug`, `--grace-period`, `--total-stop-timeout`
- **Lifecycle management**: `app.Run()` handles:
  - Application startup
  - Signal handling (SIGTERM, SIGINT)
  - Graceful shutdown with configurable grace period
  - Proper error handling and exit codes
- **Logging integration**: Automatically creates logger based on debug flag and OTLP configuration
- **Context management**: Provides lifecycle context with ready/stopped channels

**Reference**: See `github.com/formancehq/ledger` for examples of how the service package is used in production.

## Testing Conventions

### Avoid `time.Sleep` in Tests

**CRITICAL**: Never use `time.Sleep` in tests. Always use `require.Eventually` from testify instead.

**Why**:
- `time.Sleep` causes flaky tests - timing can vary between machines and CI environments
- `time.Sleep` wastes time - tests wait the full duration even when the condition is met earlier
- `require.Eventually` polls until a condition is met or times out, making tests both faster and more reliable

**Example**:
```go
// ❌ Bad: Using time.Sleep
time.Sleep(500 * time.Millisecond)
// check condition...

// ✅ Good: Using require.Eventually
require.Eventually(t, func() bool {
    // Return true when condition is met
    ledgers, err := store.ListLedgers(ctx)
    if err != nil {
        return false
    }
    return len(ledgers) > 0
}, 5*time.Second, 100*time.Millisecond, "condition should be met")
```

### Using gRPC in Tests

**CRITICAL**: Always use the gRPC client (`servicepb.LedgerServiceClient`) in integration tests.

**Rules**:
- **Use the gRPC client methods** for all ledger operations
- **Use helper functions** defined in `tests/e2e/helpers.go` for common operations (e.g., `createLedgerAction`, `createTransactionAction`)
- **gRPC methods are type-safe** and provide better error handling

**Example**:
```go
// ✅ Good: Using gRPC client
resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
    Actions: []*servicepb.Action{
        createLedgerAction("ledger1", nil),
    },
})

// For read operations
ledger, err := client.GetLedger(ctx, &servicepb.GetLedgerRequest{
    Ledger: &servicepb.LedgerNameOrId{Type: &servicepb.LedgerNameOrId_Name{Name: "ledger1"}},
})
```

### Parallel Tests

**CRITICAL**: Always use `t.Parallel()` in unit tests to enable parallel execution and reduce test suite runtime.

**Rules**:
- **Add `t.Parallel()` at the beginning of every test function** that doesn't share state with other tests
- **Add `t.Parallel()` to subtests** (`t.Run()`) when they are independent of each other
- **Do NOT call `t.Parallel()` twice** on the same test (e.g., if a helper function already calls it)
- **Tests that share global state** (rare) should not use `t.Parallel()`

**Example**:
```go
// ✅ Good: Parallel test
func TestMyFeature(t *testing.T) {
    t.Parallel()

    // Test code...
}

// ✅ Good: Parallel subtests
func TestMyFeature(t *testing.T) {
    t.Parallel()

    t.Run("subtest1", func(t *testing.T) {
        t.Parallel()
        // Test code...
    })

    t.Run("subtest2", func(t *testing.T) {
        t.Parallel()
        // Test code...
    })
}

// ❌ Bad: Missing t.Parallel()
func TestMyFeature(t *testing.T) {
    // Test code runs sequentially...
}
```

**Benefits**:
- **Faster test execution**: Tests run concurrently on multiple CPU cores
- **Better resource utilization**: CI/CD pipelines complete faster
- **Identifies race conditions**: Parallel tests help detect hidden concurrency bugs

- I would like you to respect the concepts of DRY (Don't Repeat Yourself).
