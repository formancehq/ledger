# AGENTS.md - CLI Client Structure

This document describes the organizational structure of the CLI client commands.

## Reference Implementation

The ledger service implementation is based on the reference implementation from `github.com/formancehq/ledger`. When implementing new features or fixing bugs, refer to the original implementation for guidance on patterns and best practices.

## File Structure

The client commands and HTTP handlers are organized into separate files to improve maintainability and code readability.

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

### HTTP Handlers

HTTP handlers are organized into separate files, with one handler per file:

- **`server.go`** : Main file that defines the `Server` struct, routes, and middleware
- **`handlers.go`** : Shared utility functions (e.g., `isLeader()`)
- **`handlers_types.go`** : Shared types (e.g., `LedgerResponse`)
- **`handlers_snapshot.go`** : `handleSnapshot` handler for POST /snapshot
- **`handlers_health.go`** : `handleHealth` handler for GET /health
- **`handlers_cluster_state.go`** : `handleClusterState` handler for GET /cluster/state
- **`handlers_create_ledger.go`** : `handleCreateLedger` handler for POST /{ledgerName}
- **`handlers_get_ledger.go`** : `handleGetLedger` handler for GET /{ledgerName}
- **`handlers_list_all_ledgers.go`** : `handleListAllLedgers` handler for GET /
- **`handlers_create_transaction.go`** : `handleCreateTransaction` handler for POST /{ledgerName}/transactions
- **`handlers_list_buckets.go`** : `handleListBuckets` handler for GET /buckets
- **`handlers_create_bucket.go`** : `handleCreateBucket` handler for POST /buckets/{bucketName}
- **`handlers_get_bucket.go`** : `handleGetBucket` handler for GET /buckets/{bucketName}
- **`handlers_delete_bucket.go`** : `handleDeleteBucket` handler for DELETE /buckets/{bucketName}
- **`handlers_create_bucket_snapshot.go`** : `handleCreateBucketSnapshot` handler for POST /buckets/{bucketName}/snapshot

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

## Example: Adding a New HTTP Handler

To add a new `handleUpdateBucket` handler:

1. Create `handlers_update_bucket.go` with:
   - `handleUpdateBucket` function implementation
   - Any request/response structures specific to this handler (or add to `handlers_types.go` if shared)

2. Modify `server.go` in the `Start()` method to register the route:
   ```go
   r.Put("/buckets/{bucketName}", s.handleUpdateBucket)
   ```

This structure enables easy maintenance and clear separation of responsibilities.

## Protocol Buffers and gRPC Code Generation

The Raft transport layer and ledger service use gRPC for communication. Protocol buffer definitions are stored in the `proto/` directory, while the generated Go code is placed in the appropriate internal packages.

### File Locations

- **Protocol definitions**: 
  - `proto/common.proto` - Common types shared across services (Posting, Transaction)
  - `proto/raft_transport.proto` - Raft transport messages
  - `proto/ledger.proto` - Ledger service messages (imports common.proto)
  - `proto/commands/` - Directory containing all FSM command definitions:
    - `proto/commands/commands.proto` - Base command structure for FSM commands
    - `proto/commands/fsm_commands.proto` - Commands for the main FSM (create/delete bucket)
    - `proto/commands/bucket_commands.proto` - Commands for bucket FSM (create ledger, insert log, imports common.proto)
- **Generated code**: 
  - `internal/service/common.pb.go` - Common protobuf types (Posting, Transaction)
  - `internal/raft/raft_transport.pb.go` and `internal/raft/raft_transport_grpc.pb.go` - Raft transport
  - `internal/service/ledger.pb.go` and `internal/service/ledger_grpc.pb.go` - Ledger service
  - `internal/service/commands.pb.go` - Base command protobuf types
  - `internal/raft/fsm/fsm_commands.pb.go` - FSM command protobuf types
  - `internal/raft/bucketfsm/bucket_commands.pb.go` - Bucket FSM command protobuf types

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

### Adding New Command Models

To add a new command model (e.g., for a new FSM command):

1. **Add the message definition to the appropriate `.proto` file**:
   - For main FSM commands: add to `proto/commands/fsm_commands.proto`
   - For bucket FSM commands: add to `proto/commands/bucket_commands.proto`
   - For base command structure: modify `proto/commands/commands.proto` if needed

2. **Example**: Adding a new `UpdateBucketCommand` to `proto/commands/fsm_commands.proto`:
   ```protobuf
   message UpdateBucketCommand {
     string name = 1;
     google.protobuf.Struct config = 2;
   }
   ```

3. **Regenerate the protobuf code**:
   ```bash
   just generate-proto
   ```

4. **Update the Go code**:
   - Create a `NewUpdateBucketCommand` function in `internal/raft/fsm/command.go` that:
     - Converts Go types to protobuf types
     - Marshals the protobuf message
     - Returns a `*service.Command`
   - Update `UnmarshalCommandData` in `internal/raft/fsm/command.go` to handle the new command type
   - Add a handler method in `internal/raft/fsm/fsm.go` (e.g., `HandleUpdateBucket`)

5. **Example implementation**:
   ```go
   // In internal/raft/fsm/command.go
   func NewUpdateBucketCommand(name string, config map[string]interface{}) (*service.Command, error) {
       configStruct, err := structpb.NewStruct(config)
       if err != nil {
           return nil, err
       }
       cmdProto := &UpdateBucketCommand{
           Name: name,
           Config: configStruct,
       }
       data, err := proto.Marshal(cmdProto)
       if err != nil {
           return nil, err
       }
       return &service.Command{
           ID:   service.GenerateRandomID(),
           Type: CommandTypeUpdateBucket,
           Data: data,
           Date: time.Now(),
       }, nil
   }
   
   // Update UnmarshalCommandData to handle UpdateBucketCommand
   func UnmarshalCommandData(data []byte, v interface{}) error {
       switch cmd := v.(type) {
       case *CreateBucketCommand:
           return proto.Unmarshal(data, cmd)
       case *DeleteBucketCommand:
           return proto.Unmarshal(data, cmd)
       case *UpdateBucketCommand:
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
