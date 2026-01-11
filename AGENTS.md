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
- **Update code comments** if interfaces or behavior change
- **Update examples** if usage patterns change
- **Keep documentation in English** - all technical documentation must be in English

The documentation should always reflect the current state of the codebase. Outdated documentation is worse than no documentation.

## OpenAPI and SDK Regeneration

**CRITICAL**: After any change to `openapi.yml`, you MUST regenerate the SDK immediately.

Run:
```bash
just generate-sdk
```

## JSON Property Naming Convention

**CRITICAL**: All JSON objects in the API must use **camelCase** for property names.

- **All properties in OpenAPI specification** (`openapi.yml`) must be in camelCase
- **All JSON tags in Go structs** used for HTTP request/response bodies must use camelCase
- **Examples**: `logStoreDriver`, `runtimeStoreDriver`, `logStoreConfig`, `runtimeStoreConfig`, `snapshotThreshold`, `createdAt`, `deletedAt`, etc.

This convention ensures consistency across the API and matches common JavaScript/TypeScript conventions.

## File Structure

The client commands and HTTP handlers are organized into separate files to improve maintainability and code readability.

### Ledger Commands

Ledger management commands are separated into individual files:

- **`ledgers.go`** : Main file that defines the parent `ledgers` command and initializes all sub-commands
- **`ledgers_create.go`** : `ledgers create` command to create a new ledger
- **`ledgers_list.go`** : `ledgers list` command to list all ledgers
- **`ledgers_get.go`** : `ledgers get` command to retrieve a specific ledger
- **`ledgers_raft_state.go`** : `ledgers raft-state` command to retrieve ledger Raft cluster state
- **`ledgers_delete.go`** : `ledgers delete` command to delete a ledger

### Cluster Commands

Raft cluster-related commands are separated into individual files:

- **`cluster.go`** : Raft cluster-related commands (`snapshot`, `cluster-state`)

### Shared Files

- **`main.go`** : Main entry point, defines `rootCmd` and initializes all commands
- **`common.go`** : Shared functions (SDK client creation, debug HTTP client)
- **`completions.go`** : Shell completion support

### Build Directory

All generated files and build artifacts are stored in the `build/` directory at the project root. This directory is gitignored.

Any script or tool that generates files should place them in the `build/` directory to keep the repository clean.

### HTTP Handlers

HTTP handlers are organized into separate files, with **one handler per file**. This convention ensures clear separation of concerns and makes it easy to locate and maintain individual handlers.

- **`server.go`** : Main file that defines the `Server` struct, routes, and middleware
- **`handler.go`** : Main router file that registers all routes and middleware
- **`error_handler.go`** : Shared error handling utilities
- **`handlers_types.go`** : Shared types (e.g., `LedgerResponse`)
- **`handlers_snapshot.go`** : `handleSnapshot` handler for POST /snapshot
- **`handlers_health.go`** : `handleHealth` handler for GET /health
- **`handlers_cluster_state.go`** : `handleClusterState` handler for GET /cluster/state
- **`handlers_create_ledger.go`** : `handleCreateLedger` handler for POST /{ledgerName}
- **`handlers_get_ledger.go`** : `handleGetLedger` handler for GET /{ledgerName}
- **`handlers_delete_ledger.go`** : `handleDeleteLedger` handler for DELETE /{ledgerName}
- **`handlers_get_ledger_raft_state.go`** : `handleGetLedgerRaftState` handler for GET /{ledgerName}/raft/state
- **`handlers_list_all_ledgers.go`** : `handleListAllLedgers` handler for GET /
- **`handlers_create_transaction.go`** : `handleCreateTransaction` handler for POST /{ledgerName}/transactions
- **`handlers_save_account_metadata.go`** : `handleSaveAccountMetadata` handler for POST /{ledgerName}/accounts/{address}/metadata
- **`handlers_bulk.go`** : `handleBulk` handler for POST /{ledgerName}/bulk and POST /{ledgerName}/_bulk
- **`bulking/`** : Directory containing bulk operation implementation files

## Conventions

1. **One file per command** : Each sub-command (create, list, get, delete) has its own file
2. **One file per HTTP handler** : Each HTTP handler has its own file. This ensures clear separation of concerns and makes it easy to locate and maintain individual handlers.
3. **Parent file** : Each command group (buckets, ledgers) has a main file that defines the parent command and calls `init()` for each sub-command
4. **No global variables** : Avoid using global variables for command flags. Instead, use a struct to hold command options and extract values from flags in the `RunE` function. This improves testability and avoids state pollution.
5. **`init()` function** : Each command file uses `init()` to define its flags and mark required flags
6. **Group variable declarations** : When initializing multiple variables, group them in a block using parentheses. This improves readability and consistency.
7. **No type aliases** : Never use type aliases (e.g., `type X = Y`). Always use the original type directly. This improves code clarity and avoids confusion about which type is actually being used.

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

## Example: Adding a New Command

To add a new `ledgers update` command:

1. Create `ledgers_update.go` with:
   - A struct to hold command options (e.g., `updateLedgerOptions`)
   - Definition of `ledgersUpdateCmd`
   - `init()` function to configure flags (bind flags to a local variable in `init()`, not a global)
   - `runUpdateLedger()` function that extracts flag values and uses the options struct

   Example structure:
   ```go
   type updateLedgerOptions struct {
       name   string
       driver string
   }

   var ledgersUpdateCmd = &cobra.Command{
       Use:   "update",
       RunE:  runUpdateLedger,
   }

   func init() {
       opts := &updateLedgerOptions{}
       ledgersUpdateCmd.Flags().StringVar(&opts.name, "name", "", "Ledger name")
       ledgersUpdateCmd.Flags().StringVar(&opts.driver, "driver", "", "Driver name")
   }

   func runUpdateLedger(cmd *cobra.Command, args []string) error {
       opts := &updateLedgerOptions{}
       opts.name, _ = cmd.Flags().GetString("name")
       opts.driver, _ = cmd.Flags().GetString("driver")
       // Use opts...
   }
   ```

2. Modify `ledgers.go` to add:
   ```go
   ledgersCmd.AddCommand(ledgersUpdateCmd)
   ```

## Example: Adding a New HTTP Handler

To add a new `handleUpdateLedger` handler:

1. Create `handlers_update_ledger.go` with:
   - `handleUpdateLedger` function implementation
   - Any request/response structures specific to this handler (or add to `handlers_types.go` if shared)
   - All necessary imports for the handler

2. Modify `handler.go` in the route registration section to register the route:
   ```go
   r.With(contentTypeMiddleware).Group(func(r chi.Router) {
       // ... existing routes ...
       r.Put("/{ledgerName}", server.handleUpdateLedger) // PUT /{ledgerName}
   })
   ```

**Important**: Follow the "one handler per file" convention. If a handler file contains multiple handlers, split them into separate files. For example, if `handlers_get_ledger.go` contains both `handleGetLedger` and `handleGetLedgerRaftState`, create separate files:
- `handlers_get_ledger.go` for `handleGetLedger`
- `handlers_get_ledger_raft_state.go` for `handleGetLedgerRaftState`

This structure enables easy maintenance and clear separation of responsibilities.

## Protocol Buffers and gRPC Code Generation

The Raft transport layer and ledger service use gRPC for communication. Protocol buffer definitions are stored in the `proto/` directory, while the generated Go code is placed in the appropriate internal packages.

### File Locations

- **Protocol definitions**: 
  - `proto/raft_transport.proto` - Raft transport messages
  - `proto/system.proto` - System service messages
  - `proto/ledger.proto` - Ledger service messages (contains Posting, Transaction, Log types)
  - `proto/commands/` - Directory containing all FSM command definitions:
    - `proto/commands/commands.proto` - Base command structure for FSM commands
    - `proto/commands/system_commands.proto` - Commands for the system FSM (create/delete ledger)
    - `proto/commands/ledger_commands.proto` - Commands for ledger FSM (insert log)
- **Generated code**: 
  - `internal/raft/raft_transport.pb.go` and `internal/raft/raft_transport_grpc.pb.go` - Raft transport
  - `internal/raft/commands.pb.go` - Base command protobuf types
  - `internal/raft/system/system_commands.pb.go` - System FSM command protobuf types
  - `internal/raft/ledger/ledger_commands.pb.go` - Ledger FSM command protobuf types
  - `internal/ledgerpb/` - Directory containing ledger protobuf types (ledger.pb.go, ledger_grpc.pb.go, etc.)

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

1. **Add the message definition to the appropriate `.proto` file**:
   - For system FSM commands: add to `proto/commands/system_commands.proto`
   - For ledger FSM commands: add to `proto/commands/ledger_commands.proto`
   - For base command structure: modify `proto/commands/commands.proto` if needed

2. **Example**: Adding a new `UpdateLedgerCommand` to `proto/commands/system_commands.proto`:
   ```protobuf
   message UpdateLedgerCommand {
     string name = 1;
     google.protobuf.Struct config = 2;
   }
   ```

3. **Regenerate the protobuf code**:
   ```bash
   just generate-proto
   ```

4. **Update the Go code**:
   - Create a `NewUpdateLedgerCommand` function in `internal/raft/system/command.go` that:
     - Converts Go types to protobuf types
     - Marshals the protobuf message
     - Returns a `*raft.Command`
   - Update `UnmarshalCommandData` in `internal/raft/system/command.go` to handle the new command type
   - Add a handler method in `internal/raft/system/fsm.go` (e.g., `HandleUpdateLedger`)

5. **Example implementation**:
   ```go
   // In internal/raft/system/command.go
   func NewUpdateLedgerCommand(name string, config map[string]interface{}) (*raft.Command, error) {
       configStruct, err := structpb.NewStruct(config)
       if err != nil {
           return nil, err
       }
       cmdProto := &UpdateLedgerCommand{
           Name: name,
           Config: configStruct,
       }
       data, err := proto.Marshal(cmdProto)
       if err != nil {
           return nil, err
       }
       return &raft.Command{
           ID:   generateRandomID(),
           Type: CommandTypeUpdateLedger,
           Data: data,
           Date: time.Now(),
       }, nil
   }
   
   // Update UnmarshalCommandData to handle UpdateLedgerCommand
   func UnmarshalCommandData(data []byte, v interface{}) error {
       switch cmd := v.(type) {
       case *CreateLedgerCommand:
           return proto.Unmarshal(data, cmd)
       case *DeleteLedgerCommand:
           return proto.Unmarshal(data, cmd)
       case *UpdateLedgerCommand:
           return proto.Unmarshal(data, cmd)
       default:
           return proto.Unmarshal(data, v.(proto.Message))
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

### No I/O Operations

**CRITICAL**: FSMs must never perform any I/O operations (file system, network, database, etc.). All data must be stored in memory and accessed directly from the FSM's internal state.

**Rationale**: 
- FSMs are deterministic state machines that must produce identical results when replaying the same sequence of commands
- I/O operations introduce non-determinism (network delays, file system state, etc.)
- I/O operations can fail, making the FSM unreliable
- Performance: in-memory operations are orders of magnitude faster

**What to do instead**:
- Store all necessary data in the FSM's internal state (maps, slices, etc.)
- Access data directly from memory structures
- If you need to persist data, do it during snapshot creation (which happens outside the FSM)
- If you need to read persisted data, restore it from snapshots during FSM initialization

**Example**: Instead of calling `logReader.GetLogWithIdempotencyKey()` in the FSM, maintain an `idempotencyKeys map[string]IdempotencyKeyInfo` in the FSM state and look up directly from this map.

**Note**: The ledger FSM maintains idempotency keys in memory for fast lookups during transaction processing.

## Dependency Injection with fx

The application uses Uber's `fx` (functional dependency injection) framework for managing dependencies and lifecycle, following the same patterns as `github.com/formancehq/ledger`.

### Architecture

The application is structured using fx modules:

- **`internal/application/module.go`**: Main application module that provides all core dependencies (logger, Raft cluster, ledger service, HTTP server, gRPC server)
- **`internal/transport/module.go`**: Transport module that provides gRPC connection pool
- **`internal/otlplogs/module.go`**: OpenTelemetry logs module for structured logging

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

#### HTTP Server

The HTTP server is configured using `github.com/formancehq/go-libs/v3/httpserver`:

```go
// In internal/application/module.go
import (
    "github.com/formancehq/go-libs/v3/httpserver"
    httphandler "github.com/formancehq/ledger-v3-poc/internal/http"
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

### Using the SDK in Tests

**CRITICAL**: Always use the generated SDK client (`pkg/client`) in tests instead of making manual HTTP requests.

**Rules**:
- **Never use `http.NewRequest` or `http.DefaultClient` directly in tests** - Always use the SDK client methods
- **If a method doesn't exist in the SDK**, it means the OpenAPI specification is incomplete and needs to be updated
- **After updating OpenAPI**, regenerate the SDK using `just generate-sdk` before writing tests
- **SDK methods are type-safe** and provide better error handling than manual HTTP calls
- **SDK usage ensures consistency** between tests and actual client usage

**Example**:
```go
// ✅ Good: Using SDK
resp, err := client.Accounts.SaveAccountMetadata(ctx, operations.SaveAccountMetadataRequest{
    LedgerName:  ledgerName,
    Address:     "account-1",
    RequestBody: map[string]string{"key": "value"},
})

// ❌ Bad: Manual HTTP request
req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(data))
resp, err := http.DefaultClient.Do(req)
```

**Benefits**:
- **Type safety**: Compile-time checking of request/response types
- **Consistency**: Tests use the same client as external users
- **Maintainability**: Changes to API automatically reflected in tests after SDK regeneration
- **Error handling**: SDK provides structured error types
- **Documentation**: SDK methods are self-documenting

- I would like you to respect the concepts of DRY (Don't Repeat Yourself).
