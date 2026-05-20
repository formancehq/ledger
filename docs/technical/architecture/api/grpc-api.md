# gRPC API

## Overview

The gRPC API provides a programmatic interface for interacting with the ledger cluster. It is used for:

1. **Client applications**: Direct programmatic access to ledger operations
2. **Inter-node communication**: Request forwarding from followers to the leader
3. **CLI tools**: The `ledgerctl` command-line tool uses gRPC

## Connection

### Default Port

The gRPC service server listens on port `8888` by default (configurable via `--grpc-port`).

> **Note**: There are two separate gRPC servers:
> - **Service server** (port 8888): External client-facing API (BucketService, ClusterService, Health)
> - **Raft server** (port 7777): Internal inter-node communication (RaftTransport, SnapshotService)

### Connection Example

```go
import (
    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"
    "github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
)

conn, err := grpc.NewClient(
    "localhost:8888",
    grpc.WithTransportCredentials(insecure.NewCredentials()),
)
if err != nil {
    log.Fatal(err)
}
defer conn.Close()

client := servicepb.NewBucketServiceClient(conn)
```

## Service Definition

The main service is `BucketService`:

> **Note:** The listing below is a partial excerpt. The actual `BucketService` in `misc/proto/bucket.proto` defines 30+ RPCs including prepared queries, numscript library, period management, signing keys, index management, analysis, and more. See `bucket.proto` for the full definition.

```protobuf
service BucketService {
  // Read operations
  rpc ListLedgers(ListLedgersRequest) returns (stream LedgerInfo);
  rpc GetLedger(GetLedgerRequest) returns (LedgerInfo);
  rpc GetAccount(GetAccountRequest) returns (Account);
  rpc GetTransaction(GetTransactionRequest) returns (GetTransactionResponse);
  rpc ListTransactions(ListTransactionsRequest) returns (stream Transaction);

  // Write operations (unified Apply method)
  rpc Apply(ApplyRequest) returns (ApplyResponse);

  // Diagnostics
  rpc GetPrimaryMetrics(GetPrimaryMetricsRequest) returns (GetPrimaryMetricsResponse);
  rpc GetSecondaryMetrics(GetSecondaryMetricsRequest) returns (GetSecondaryMetricsResponse);
  rpc CheckStore(CheckStoreRequest) returns (stream CheckStoreEvent);

  // Audit
  rpc ListAuditEntries(ListAuditEntriesRequest) returns (stream AuditEntry);

  // ... and 20+ more RPCs (see bucket.proto for the full list)
}
```

## Read Operations

### ListLedgers

Streams all ledgers in the cluster.

```go
stream, err := client.ListLedgers(ctx, &servicepb.ListLedgersRequest{})
if err != nil {
    return err
}

for {
    ledger, err := stream.Recv()
    if err == io.EOF {
        break
    }
    if err != nil {
        return err
    }
    fmt.Printf("Ledger: %s (ID: %d)\n", ledger.Name, ledger.Id)
}
```

### GetLedger

Retrieves a ledger by name.

```go
ledger, err := client.GetLedger(ctx, &servicepb.GetLedgerRequest{
    Ledger: "my-ledger",
})
if err != nil {
    return err
}
fmt.Printf("Ledger: %s, Created: %v\n", ledger.Name, ledger.CreatedAt)
```

### GetAccount

Retrieves an account with its volumes.

```go
account, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
    Ledger:  "my-ledger",
    Address: "users:alice",
})
if err != nil {
    return err
}

fmt.Printf("Account: %s\n", account.Address)
for asset, volumes := range account.Volumes {
    fmt.Printf("  %s: input=%s, output=%s, balance=%s\n",
        asset, volumes.Input, volumes.Output, volumes.Balance)
}
```

### GetTransaction

Retrieves a transaction by ID.

```go
resp, err := client.GetTransaction(ctx, &servicepb.GetTransactionRequest{
    Ledger:        "my-ledger",
    TransactionId: 1,
})
if err != nil {
    return err
}

tx := resp.Transaction
fmt.Printf("Transaction %d: %d postings\n", tx.Id, len(tx.Postings))
for _, p := range tx.Postings {
    fmt.Printf("  %s -> %s: %s %s\n", p.Source, p.Destination, p.Amount, p.Asset)
}
```

## Write Operations

All write operations go through the unified `Apply` method, which accepts a batch of requests.

### Request Structure

```protobuf
message ApplyRequest {
  repeated Request requests = 1;
  bool skip_response = 2;  // Strip log payloads from response (only sequence returned)
}

message Request {
  string idempotency_key = 1;  // Optional idempotency key
  oneof type {
    LedgerApplyRequest apply = 2;        // Ledger operations
    CreateLedgerRequest create_ledger = 3;
    DeleteLedgerRequest delete_ledger = 4;
  }
}

message LedgerApplyRequest {
  string ledger = 1;
  oneof data {
    CreateTransactionPayload create_transaction = 2;
    SaveMetadataCommand add_metadata = 3;
    RevertTransactionPayload revert_transaction = 4;
    DeleteMetadataCommand delete_metadata = 5;
  }
}
```

### Create Ledger

```go
resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
    Requests: []*servicepb.Request{{
        IdempotencyKey: "create-ledger-123",
        Type: &servicepb.Request_CreateLedger{
            CreateLedger: &servicepb.CreateLedgerRequest{
                Name: "my-ledger",
                Metadata: &commonpb.MetadataSet{
                    Metadata: []*commonpb.Metadata{
                        {Key: "environment", Value: &commonpb.MetadataValue{Value: "production"}},
                    },
                },
            },
        },
    }},
})
```

### Delete Ledger

```go
resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
    Requests: []*servicepb.Request{{
        Type: &servicepb.Request_DeleteLedger{
            DeleteLedger: &servicepb.DeleteLedgerRequest{
                Name: "my-ledger",
            },
        },
    }},
})
```

### Create Transaction

#### With Postings

```go
resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
    Requests: []*servicepb.Request{{
        IdempotencyKey: "tx-123",
        Type: &servicepb.Request_Apply{
            Apply: &servicepb.LedgerApplyRequest{
                Ledger: "my-ledger",
                Data: &servicepb.LedgerApplyRequest_CreateTransaction{
                    CreateTransaction: &servicepb.CreateTransactionPayload{
                        Postings: []*commonpb.Posting{{
                            Source:      "world",
                            Destination: "users:alice",
                            Amount:      bigIntToProto(big.NewInt(1000)),
                            Asset:       "USD/2",
                        }},
                        Reference: "payment-123",
                        Metadata: &commonpb.MetadataSet{
                            Metadata: []*commonpb.Metadata{
                                {Key: "description", Value: &commonpb.MetadataValue{Value: "Initial deposit"}},
                            },
                        },
                    },
                },
            },
        },
    }},
})
```

#### With Numscript

```go
resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
    Requests: []*servicepb.Request{{
        Type: &servicepb.Request_Apply{
            Apply: &servicepb.LedgerApplyRequest{
                Ledger: "my-ledger",
                Data: &servicepb.LedgerApplyRequest_CreateTransaction{
                    CreateTransaction: &servicepb.CreateTransactionPayload{
                        Script: &commonpb.Script{
                            Plain: `
                                send [USD/2 100] (
                                    source = @users:alice
                                    destination = @users:bob
                                )
                            `,
                        },
                    },
                },
            },
        },
    }},
})
```

### Revert Transaction

```go
resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
    Requests: []*servicepb.Request{{
        IdempotencyKey: "revert-tx-1",
        Type: &servicepb.Request_Apply{
            Apply: &servicepb.LedgerApplyRequest{
                Ledger: "my-ledger",
                Data: &servicepb.LedgerApplyRequest_RevertTransaction{
                    RevertTransaction: &servicepb.RevertTransactionPayload{
                        TransactionId:   1,
                        Force:           false, // Set true to ignore insufficient balance
                        AtEffectiveDate: false, // Set true to use original timestamp
                    },
                },
            },
        },
    }},
})
```

### Save Metadata

#### Account Metadata

```go
resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
    Requests: []*servicepb.Request{{
        Type: &servicepb.Request_Apply{
            Apply: &servicepb.LedgerApplyRequest{
                Ledger: "my-ledger",
                Data: &servicepb.LedgerApplyRequest_AddMetadata{
                    AddMetadata: &commonpb.SaveMetadataCommand{
                        Target: &commonpb.Target{
                            Target: &commonpb.Target_Account{
                                Account: &commonpb.TargetAccount{Addr: "users:alice"},
                            },
                        },
                        Metadata: &commonpb.MetadataSet{
                            Metadata: []*commonpb.Metadata{
                                {Key: "kyc_status", Value: &commonpb.MetadataValue{Value: "verified"}},
                            },
                        },
                    },
                },
            },
        },
    }},
})
```

#### Transaction Metadata

```go
resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
    Requests: []*servicepb.Request{{
        Type: &servicepb.Request_Apply{
            Apply: &servicepb.LedgerApplyRequest{
                Ledger: "my-ledger",
                Data: &servicepb.LedgerApplyRequest_AddMetadata{
                    AddMetadata: &commonpb.SaveMetadataCommand{
                        Target: &commonpb.Target{
                            Target: &commonpb.Target_Transaction{
                                Transaction: &commonpb.TargetTransaction{Id: 1},
                            },
                        },
                        Metadata: &commonpb.MetadataSet{
                            Metadata: []*commonpb.Metadata{
                                {Key: "reconciled", Value: &commonpb.MetadataValue{Value: "true"}},
                            },
                        },
                    },
                },
            },
        },
    }},
})
```

### Delete Metadata

```go
resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
    Requests: []*servicepb.Request{{
        Type: &servicepb.Request_Apply{
            Apply: &servicepb.LedgerApplyRequest{
                Ledger: "my-ledger",
                Data: &servicepb.LedgerApplyRequest_DeleteMetadata{
                    DeleteMetadata: &commonpb.DeleteMetadataCommand{
                        Target: &commonpb.Target{
                            Target: &commonpb.Target_Account{
                                Account: &commonpb.TargetAccount{Addr: "users:alice"},
                            },
                        },
                        Key: "kyc_status",
                    },
                },
            },
        },
    }},
})
```

## Batch Operations

The `Apply` method accepts multiple requests, enabling batch operations:

```go
resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
    Requests: []*servicepb.Request{
        // Request 1: Create ledger
        {
            Type: &servicepb.Request_CreateLedger{
                CreateLedger: &servicepb.CreateLedgerRequest{Name: "ledger-a"},
            },
        },
        // Request 2: Create transaction in another ledger
        {
            IdempotencyKey: "tx-batch-1",
            Type: &servicepb.Request_Apply{
                Apply: &servicepb.LedgerApplyRequest{
                    Ledger: "ledger-b",
                    Data: &servicepb.LedgerApplyRequest_CreateTransaction{
                        CreateTransaction: &servicepb.CreateTransactionPayload{
                            Postings: []*commonpb.Posting{{
                                Source:      "world",
                                Destination: "bank",
                                Amount:      bigIntToProto(big.NewInt(1000)),
                                Asset:       "USD/2",
                            }},
                        },
                    },
                },
            },
        },
    },
})

// Each request produces one log entry
for i, log := range resp.Logs {
    fmt.Printf("Request %d: Log sequence %d\n", i, log.Sequence)
}
```

### Skip Response

For high-throughput ingestion scenarios where the client does not need the full response payload, set `skip_response: true`. The server returns one log per request containing only the `sequence` number, skipping receipt signing, response signing, and payload serialization:

```go
resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
    Requests:     requests,
    SkipResponse: true,
})

// Logs contain only the sequence number; all other fields are empty
for _, log := range resp.Logs {
    fmt.Printf("Committed: sequence %d\n", log.Sequence)
}
```

## Response Structure

### ApplyResponse

```protobuf
message ApplyResponse {
  repeated Log logs = 1;  // One log per request
}

message Log {
  fixed64 sequence = 1;     // Global log sequence number
  LogPayload payload = 2;   // Log content (varies by operation type)
  Idempotency idempotency = 3;
}
```

### Log Payload Types

| Operation | Payload Type | Content |
|-----------|--------------|---------|
| Create ledger | `CreateLedgerLog` | `LedgerInfo` |
| Delete ledger | `DeleteLedgerLog` | `LedgerInfo` (with `deleted_at`) |
| Create transaction | `ApplyLedgerLog` → `CreatedTransaction` | `Transaction` + account metadata |
| Revert transaction | `ApplyLedgerLog` → `RevertedTransaction` | Reverted ID + revert transaction |
| Save metadata | `ApplyLedgerLog` → `SavedMetadata` | Target + metadata |
| Delete metadata | `ApplyLedgerLog` → `DeletedMetadata` | Target + key |

## Idempotency

Include an `idempotency_key` in any request for safe retries:

```go
resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
    Requests: []*servicepb.Request{{
        IdempotencyKey: "unique-request-id-123",
        Type: &servicepb.Request_Apply{...},
    }},
})
```

**Behavior:**
- **New key**: Request is processed normally
- **Same key + same content**: Returns reference to original log
- **Same key + different content**: Returns `idempotency key conflict` error

See [Idempotency](../data-model/idempotency.md) for detailed documentation.

## Error Handling

### BusinessError Wrapper

Processing errors (insufficient funds, ledger not found, etc.) are wrapped in a `BusinessError` struct in the FSM layer. The gRPC interceptor converts `BusinessError` instances to proper gRPC status codes with structured `google.rpc.ErrorInfo` details. This allows clients to programmatically identify error types without parsing error messages.

Each business error response includes:
- A **gRPC status code** (e.g., `NOT_FOUND`, `ALREADY_EXISTS`, `FAILED_PRECONDITION`)
- A **human-readable message** (the original error string)
- An **`ErrorInfo` detail** with:
  - `reason`: Machine-readable constant (e.g., `LEDGER_ALREADY_EXISTS`)
  - `domain`: Always `"ledger"`
  - `metadata`: Error-specific key-value pairs with context

### gRPC Status Codes

| Code | Condition |
|------|-----------|
| `OK` | Request succeeded |
| `NOT_FOUND` | Ledger, account, or transaction not found |
| `ALREADY_EXISTS` | Ledger already exists, idempotency key conflict, reference conflict |
| `INVALID_ARGUMENT` | Invalid request parameters, Numscript parse error, validation error |
| `FAILED_PRECONDITION` | Insufficient balance, transaction already reverted, balance not found |
| `UNAVAILABLE` | No leader available (client should retry) |
| `INTERNAL` | Unrecognized business error |

### Error Reason Constants

All business errors carry an `ErrorInfo` detail with a machine-readable reason. The reason constants are defined in `internal/domain/processing/errors.go` and shared between server and client.

| Error | gRPC Code | Reason | Metadata |
|-------|-----------|--------|----------|
| Ledger already exists | `ALREADY_EXISTS` | `LEDGER_ALREADY_EXISTS` | `name` |
| Ledger not found | `NOT_FOUND` | `LEDGER_NOT_FOUND` | `name` |
| Idempotency key conflict | `ALREADY_EXISTS` | `IDEMPOTENCY_KEY_CONFLICT` | `key` |
| Transaction reference conflict | `ALREADY_EXISTS` | `TRANSACTION_REFERENCE_CONFLICT` | `ledgerId`, `reference` |
| Transaction not found | `NOT_FOUND` | `TRANSACTION_NOT_FOUND` | `transactionId` |
| Transaction already reverted | `FAILED_PRECONDITION` | `TRANSACTION_ALREADY_REVERTED` | `transactionId` |
| Insufficient funds | `FAILED_PRECONDITION` | `INSUFFICIENT_FUNDS` | `account`, `asset`, `amount`, `balance` |
| Balance not found | `FAILED_PRECONDITION` | `BALANCE_NOT_FOUND` | `account`, `asset` |
| Balance not preloaded | `FAILED_PRECONDITION` | `BALANCE_NOT_PRELOADED` | `account`, `asset` |
| Numscript parse error | `INVALID_ARGUMENT` | `NUMSCRIPT_PARSE_ERROR` | `details` |
| Validation error | `INVALID_ARGUMENT` | `VALIDATION` | *(none)* |

### Error Handling Example (with ErrorInfo)

```go
import (
    "google.golang.org/genproto/googleapis/rpc/errdetails"
    "google.golang.org/grpc/codes"
    "google.golang.org/grpc/status"
)

resp, err := client.Apply(ctx, req)
if err != nil {
    st, ok := status.FromError(err)
    if !ok {
        return fmt.Errorf("unknown error: %w", err)
    }

    // Extract structured error details
    for _, detail := range st.Details() {
        if info, ok := detail.(*errdetails.ErrorInfo); ok && info.Domain == "ledger" {
            switch info.Reason {
            case "INSUFFICIENT_FUNDS":
                fmt.Printf("Account %s has insufficient %s: needed %s, available %s\n",
                    info.Metadata["account"], info.Metadata["asset"],
                    info.Metadata["amount"], info.Metadata["balance"])
            case "LEDGER_NOT_FOUND":
                fmt.Printf("Ledger %s does not exist\n", info.Metadata["name"])
            case "LEDGER_ALREADY_EXISTS":
                fmt.Printf("Ledger %s already exists\n", info.Metadata["name"])
            case "TRANSACTION_ALREADY_REVERTED":
                fmt.Printf("Transaction %s is already reverted\n", info.Metadata["transactionId"])
            case "NUMSCRIPT_PARSE_ERROR":
                fmt.Printf("Script error: %s\n", info.Metadata["details"])
            default:
                fmt.Printf("Business error [%s]: %s\n", info.Reason, st.Message())
            }
            return
        }
    }

    // Fallback: handle by status code only
    switch st.Code() {
    case codes.Unavailable:
        // No leader — retry (handled automatically by gRPC retry policy)
        return fmt.Errorf("no leader available: %s", st.Message())
    default:
        return fmt.Errorf("gRPC error %s: %s", st.Code(), st.Message())
    }
}
```

### Architecture

The error flow is:

```
Processor (returns typed error)
  → Machine.applyProposal (wraps in BusinessError)
    → Future / Admission / Controller
      → gRPC interceptor (converts BusinessError → Status + ErrorInfo)
        → Client (extracts ErrorInfo, reconstructs typed error)
```

**Server side** (`internal/adapter/grpc/errors.go`):
- `businessErrorToGRPCStatus()` uses `errors.As` to safely match error types through the chain
- Attaches `errdetails.ErrorInfo` with reason, domain, and metadata

**Client side** (`cmd/ledgerctl/errors.go`):
- `businessErrorFromGRPC()` extracts `ErrorInfo` from gRPC status details
- Reconstructs the original typed error from reason + metadata

## Store Check

Verify store integrity (hash chain and derived data consistency):

```go
stream, err := client.CheckStore(ctx, &servicepb.CheckStoreRequest{})
if err != nil {
    return err
}

for {
    event, err := stream.Recv()
    if err == io.EOF {
        break
    }
    if err != nil {
        return err
    }
    if e := event.GetError(); e != nil {
        fmt.Printf("Error: %s - %s\n", e.ErrorType, e.Message)
    }
    if p := event.GetProgress(); p != nil {
        fmt.Printf("Progress: %d/%d\n", p.LogsChecked, p.TotalLogs)
    }
}
```

## Audit Entries

List audit trail entries (success and failure):

```go
stream, err := client.ListAuditEntries(ctx, &servicepb.ListAuditEntriesRequest{
    Ledger:       "my-ledger",      // Optional: filter by ledger
    FailuresOnly: true,              // Optional: only failures
})
if err != nil {
    return err
}

for {
    entry, err := stream.Recv()
    if err == io.EOF {
        break
    }
    if err != nil {
        return err
    }
    fmt.Printf("Audit #%d: proposal=%d\n", entry.Sequence, entry.ProposalId)
}
```

## Store Metrics

Retrieve Pebble storage metrics (useful for monitoring):

```go
metrics, err := client.GetPrimaryMetrics(ctx, &servicepb.GetPrimaryMetricsRequest{})
if err != nil {
    return err
}

if metrics.Available {
    m := metrics.Metrics
    fmt.Printf("Block Cache: hits=%d misses=%d\n", 
        m.BlockCache.Hits, m.BlockCache.Misses)
    fmt.Printf("MemTable: size=%d count=%d\n",
        m.MemTable.Size, m.MemTable.Count)
    fmt.Printf("Disk Usage: %d bytes\n", m.DiskSpaceUsage)
}
```

## CLI Usage

The `ledgerctl` CLI tool uses gRPC internally:

```bash
# List ledgers
ledgerctl ledgers list --addr localhost:8888

# Create a ledger
ledgerctl ledgers create my-ledger --addr localhost:8888

# Get an account
ledgerctl accounts get --ledger my-ledger --addr users:alice --addr localhost:8888

# Get a transaction
ledgerctl transactions get --ledger my-ledger --id 1 --addr localhost:8888

# Create a transaction
ledgerctl transactions create --ledger my-ledger \
  --posting "world,users:alice,100,USD/2" \
  --addr localhost:8888
```

## Request Forwarding

When a gRPC request reaches a follower node, it is automatically forwarded to the leader via the **ServiceConnectionPool** (connecting to the leader's service port 8888):

```
┌────────────┐   gRPC:8888   ┌────────────┐  Forward:8888  ┌────────────┐
│   Client   │───────────────▶  Follower  │────────────────▶   Leader   │
└────────────┘               └────────────┘                └────────────┘
       ▲                            │                            │
       │                            │                            │
       └────────────────────────────┴────────────────────────────┘
                           Response returned
```

This is transparent to the client - the client can connect to any node. The `RoutedController` handles leader detection and request forwarding.

## Proto Files

The proto definitions are located in `misc/proto/`:

| File | Description |
|------|-------------|
| `bucket.proto` | Main gRPC service definition (BucketService) |
| `common.proto` | Shared data types (Transaction, Account, Log, etc.) |
| `raft_transport.proto` | Raft inter-node communication |
| `raftcmd.proto` | Internal Raft command types |
| `snapshot.proto` | Snapshot transfer service |
| `cluster.proto` | Cluster state types |

### Regenerating Code

After modifying proto files:

```bash
just generate-proto
```

## Next Steps

- [HTTP API](./api.md) - REST API documentation
- [Idempotency](../data-model/idempotency.md) - Safe request retries
- [Architecture](../core/architecture.md) - System overview
- [Data Flows](../data-model/data-flows.md) - Request processing flow
