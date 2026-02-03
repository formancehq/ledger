# gRPC API

## Overview

The gRPC API provides a programmatic interface for interacting with the ledger cluster. It is used for:

1. **Client applications**: Direct programmatic access to ledger operations
2. **Inter-node communication**: Request forwarding from followers to the leader
3. **CLI tools**: The `ledgerctl` command-line tool uses gRPC

## Connection

### Default Port

The gRPC server listens on port `8888` by default (configurable via `--bind-addr`).

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

```protobuf
service BucketService {
  // Read operations
  rpc GetAllLedgersInfo(GetAllLedgersRequest) returns (stream LedgerInfo);
  rpc GetLedger(GetLedgerRequest) returns (LedgerInfo);
  rpc GetAccount(GetAccountRequest) returns (Account);
  rpc GetTransaction(GetTransactionRequest) returns (Transaction);
  
  // Write operations (unified Apply method)
  rpc Apply(ApplyRequest) returns (ApplyResponse);
  
  // Diagnostics
  rpc GetStoreMetrics(GetStoreMetricsRequest) returns (GetStoreMetricsResponse);
}
```

## Read Operations

### GetAllLedgersInfo

Streams all ledgers in the cluster.

```go
stream, err := client.GetAllLedgersInfo(ctx, &servicepb.GetAllLedgersRequest{})
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
tx, err := client.GetTransaction(ctx, &servicepb.GetTransactionRequest{
    Ledger:        "my-ledger",
    TransactionId: 1,
})
if err != nil {
    return err
}

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

## Response Structure

### ApplyResponse

```protobuf
message ApplyResponse {
  repeated Log logs = 1;  // One log per request
}

message Log {
  uint64 sequence = 1;      // Global log sequence number
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

See [Idempotency](./idempotency.md) for detailed documentation.

## Error Handling

### gRPC Status Codes

| Code | Condition |
|------|-----------|
| `OK` | Request succeeded |
| `NOT_FOUND` | Ledger, account, or transaction not found |
| `ALREADY_EXISTS` | Ledger already exists |
| `INVALID_ARGUMENT` | Invalid request parameters |
| `FAILED_PRECONDITION` | Insufficient balance, transaction already reverted |
| `UNAVAILABLE` | No leader available |
| `UNKNOWN` | Internal error |

### Error Handling Example

```go
resp, err := client.Apply(ctx, req)
if err != nil {
    st, ok := status.FromError(err)
    if !ok {
        return fmt.Errorf("unknown error: %w", err)
    }
    
    switch st.Code() {
    case codes.NotFound:
        return fmt.Errorf("resource not found: %s", st.Message())
    case codes.FailedPrecondition:
        return fmt.Errorf("precondition failed: %s", st.Message())
    case codes.Unavailable:
        // Retry after delay
        time.Sleep(time.Second)
        return retry(ctx, req)
    default:
        return fmt.Errorf("gRPC error %s: %s", st.Code(), st.Message())
    }
}
```

## Store Metrics

Retrieve Pebble storage metrics (useful for monitoring):

```go
metrics, err := client.GetStoreMetrics(ctx, &servicepb.GetStoreMetricsRequest{})
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

When a gRPC request reaches a follower node, it is automatically forwarded to the leader:

```
┌────────────┐     gRPC      ┌────────────┐    Forward    ┌────────────┐
│   Client   │───────────────▶  Follower  │───────────────▶   Leader   │
└────────────┘               └────────────┘               └────────────┘
       ▲                            │                           │
       │                            │                           │
       └────────────────────────────┴───────────────────────────┘
                          Response returned
```

This is transparent to the client - the client can connect to any node.

## Proto Files

The proto definitions are located in `misc/proto/`:

| File | Description |
|------|-------------|
| `service.proto` | Main gRPC service definition |
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
- [Idempotency](./idempotency.md) - Safe request retries
- [Architecture](./architecture.md) - System overview
- [Data Flows](./data-flows.md) - Request processing flow
