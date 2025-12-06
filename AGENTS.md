# AGENTS.md - CLI Client Structure

This document describes the organizational structure of the CLI client commands.

## File Structure

The client commands are organized into separate files to improve maintainability and code readability.

### Bucket Commands

Bucket management commands are separated into individual files:

- **`buckets.go`** : Main file that defines the parent `buckets` command and initializes all sub-commands
- **`buckets_create.go`** : `buckets create` command to create a new bucket
- **`buckets_list.go`** : `buckets list` command to list all buckets
- **`buckets_get.go`** : `buckets get` command to retrieve a bucket with its Raft state
- **`buckets_delete.go`** : `buckets delete` command to delete a bucket

### Ledger Commands

Ledger management commands are separated into individual files:

- **`ledgers.go`** : Main file that defines the parent `ledgers` command and initializes all sub-commands
- **`ledgers_create.go`** : `ledgers create` command to create a new ledger in a bucket
- **`ledgers_list.go`** : `ledgers list` command to list all ledgers in a bucket
- **`ledgers_get.go`** : `ledgers get` command to retrieve a specific ledger

### Shared Files

- **`main.go`** : Main entry point, defines `rootCmd` and initializes all commands
- **`common.go`** : Shared functions (SDK client creation, debug HTTP client)
- **`cluster.go`** : Raft cluster-related commands (`snapshot`, `cluster-state`)

## Conventions

1. **One file per command** : Each sub-command (create, list, get, delete) has its own file
2. **Parent file** : Each command group (buckets, ledgers) has a main file that defines the parent command and calls `init()` for each sub-command
3. **Global variables** : Flag variables are defined in the corresponding command file
4. **`init()` function** : Each command file uses `init()` to define its flags and mark required flags

## Example: Adding a New Command

To add a new `buckets update` command:

1. Create `buckets_update.go` with:
   - Global variables for flags
   - Definition of `bucketsUpdateCmd`
   - `init()` function to configure flags
   - `runUpdateBucket()` function for the implementation

2. Modify `buckets.go` to add:
   ```go
   bucketsCmd.AddCommand(bucketsUpdateCmd)
   ```

This structure enables easy maintenance and clear separation of responsibilities.

## Protocol Buffers and gRPC Code Generation

The Raft transport layer and ledger service use gRPC for communication. Protocol buffer definitions are stored in the `proto/` directory, while the generated Go code is placed in the appropriate internal packages.

### File Locations

- **Protocol definitions**: 
  - `proto/raft_transport.proto` - Raft transport messages
  - `proto/ledger.proto` - Ledger service messages
- **Generated code**: 
  - `internal/raft/raft_transport.pb.go` and `internal/raft/raft_transport_grpc.pb.go` - Raft transport
  - `internal/service/ledger.pb.go` and `internal/service/ledger_grpc.pb.go` - Ledger service

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

When modifying any `.proto` file:
1. Edit the `.proto` file in the `proto/` directory
2. Run `just generate-proto` to regenerate the Go code for all proto files
3. Update any code that uses the generated types if the API has changed
4. Rebuild the project to ensure everything compiles
