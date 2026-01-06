# API and Interfaces

## Overview

Ledger v3 POC exposes two types of APIs:

1. **HTTP REST API**: Public API for clients
2. **gRPC API**: Inter-node communication and programmatic API

## HTTP REST API

### Base URL

By default: `http://localhost:9000`

### API Versioning

The API supports an optional `/v2` prefix for all endpoints. All routes are available both with and without the prefix:

- **Without prefix**: `GET /` (backward compatible)
- **With prefix**: `GET /v2/` (optional)

Both paths are equivalent and point to the same handlers. This allows for:
- **Backward compatibility**: Existing clients continue to work without changes
- **Future versioning**: Easy migration path when introducing breaking changes
- **Gradual migration**: Clients can migrate to `/v2` at their own pace

### Authentication

Currently, no authentication is required. In production, it is recommended to add:
- Token authentication (JWT)
- TLS/HTTPS
- Rate limiting

### Response Format

#### Success

```json
{
  "data": { ... }
}
```

#### Error

```json
{
  "errorCode": "ERROR_CODE",
  "errorMessage": "Human readable error message"
}
```

### HTTP Status Codes

- `200 OK`: Request successful
- `201 Created`: Resource created
- `204 No Content`: Resource deleted successfully
- `400 Bad Request`: Invalid request
- `404 Not Found`: Resource not found
- `409 Conflict`: Conflict (ex: resource already exists)
- `503 Service Unavailable`: No Leader available (with `Retry-After` header)
- `500 Internal Server Error`: Server error

### Error Handling "No Leader"

When no leader is available, the API returns:

- **Code**: `503 Service Unavailable`
- **Header**: `Retry-After: 1` (seconds)
- **Body**:
```json
{
  "errorCode": "NO_LEADER",
  "errorMessage": "No Leader"
}
```

The generated client SDK by Speakeasy automatically respects this header and retries the request.

## Main Endpoints

### Ledgers

#### Create a Ledger

```http
POST /{ledgerName}
Content-Type: application/json

{
  "driver": "sqlite",
  "config": {},
  "metadata": {
    "key": "value"
  },
  "snapshotThreshold": 100
}
```

**Response**:
```json
{
  "data": {
    "id": 1,
    "name": "my-ledger",
    "driver": "sqlite",
    "config": {},
    "createdAt": "2024-01-01T00:00:00Z",
    "snapshotThreshold": 100
  }
}
```

#### Get a Ledger

```http
GET /{ledgerName}
```

**Response**:
```json
{
  "data": {
    "id": 1,
    "name": "my-ledger",
    "driver": "sqlite",
    "config": {},
    "createdAt": "2024-01-01T00:00:00Z",
    "snapshotThreshold": 100,
    "raftState": {
      "state": "Leader",
      "leader": 1,
      "nodes": [...]
    }
  }
}
```

#### List All Ledgers

```http
GET /
```

**Response**:
```json
{
  "data": [
    {
      "id": 1,
      "name": "ledger1",
      "driver": "sqlite",
      ...
    },
    {
      "id": 2,
      "name": "ledger2",
      "driver": "sqlite",
      ...
    }
  ]
}
```

#### Get Ledger Raft State

```http
GET /{ledgerName}/raft/state
```

**Response**:
```json
{
  "data": {
    "state": "Leader",
    "leader": 1,
    "localNode": 1,
    "nodes": [
      {
        "id": 1,
        "address": "127.0.0.1:8888",
        "suffrage": "Voter"
      }
    ],
    "raftStatus": {
      "term": 1,
      "applied": 100,
      "commit": 100,
      "lastIndex": 100
    }
  }
}
```

#### Delete a Ledger

```http
DELETE /{ledgerName}
```

**Response**: `204 No Content`

### Transactions

#### Create a Transaction

```http
POST /{ledgerName}/transactions
Content-Type: application/json
Idempotency-Key: optional-key

{
  "postings": [
    {
      "source": "world",
      "destination": "bank",
      "amount": 100,
      "asset": "USD"
    }
  ],
  "metadata": {
    "description": "Payment"
  },
  "reference": "optional-reference"
}
```

**Query Parameters**:
- `dryRun=true`: Validate without applying

**Response**:
```json
{
  "data": {
    "transaction": {
      "id": 1,
      "postings": [...],
      "timestamp": "2024-01-01T00:00:00Z",
      "metadata": {...},
      "reference": "optional-reference"
    },
    "accountMetadata": {...}
  }
}
```

#### Bulk Operations

```http
POST /{ledgerName}/bulk
Content-Type: application/json

[
  {
    "action": "CREATE_TRANSACTION",
    "ik": "idempotency-key",
    "data": {
      "postings": [...]
    }
  },
  {
    "action": "ADD_METADATA",
    "data": {
      "targetType": "TRANSACTION",
      "targetId": 1,
      "metadata": {...}
    }
  }
]
```

**Alternative endpoint**: `POST /{ledgerName}/_bulk` (for backward compatibility)

**Query Parameters**:
- `continueOnFailure=true`: Continue even if an error occurs
- `atomic=true`: Execute atomically (all or nothing)
- `parallel=true`: Execute in parallel

### Account Metadata

#### Save Account Metadata

```http
POST /{ledgerName}/accounts/{address}/metadata
Content-Type: application/json

{
  "key1": "value1",
  "key2": "value2"
}
```

**Response**:
```json
{
  "data": {
    "address": "account-address",
    "metadata": {
      "key1": "value1",
      "key2": "value2"
    }
  }
}
```

### Cluster

#### Cluster State

```http
GET /cluster/state
```

**Response**:
```json
{
  "data": {
    "state": "Leader",
    "leader": 1,
    "localNode": 1,
    "nodes": [
      {
        "id": 1,
        "address": "127.0.0.1:8888",
        "suffrage": "Voter"
      },
      ...
    ],
    "raftStatus": {
      "term": 1,
      "applied": 100,
      "commit": 100,
      "lastIndex": 100,
      "progress": {
        "1": {
          "match": 100,
          "next": 101,
          "state": "Replicate",
          "pendingSnapshot": 0,
          "recentActive": true,
          "isPaused": false
        }
      }
    },
    "innerState": {
      "nextLedgerId": 3,
      "ledgers": {
        "ledger1": {
          "id": 1,
          "driver": "sqlite"
        },
        "ledger2": {
          "id": 2,
          "driver": "sqlite"
        }
      }
    }
  }
}
```

#### Create a Snapshot

```http
POST /snapshot
```

**Note**: Leader-only operation

**Response**:
```json
{
  "data": {
    "message": "Snapshot created successfully"
  }
}
```

### Health

#### Health Check

```http
GET /health
```

**Response**: `200 OK` (no body)

### Debug Endpoints

The API exposes pprof endpoints for debugging and profiling:

- `GET /debug/pprof/` - Index page
- `GET /debug/pprof/profile` - CPU profile
- `GET /debug/pprof/heap` - Heap profile
- `GET /debug/pprof/goroutine` - Goroutine dump
- `GET /debug/pprof/trace` - Execution trace

These endpoints are also available under `/v2/debug/pprof/`.

## gRPC API

### Services

#### SystemService

Manages system operations (ledgers):

```protobuf
service SystemService {
  rpc CreateLedger(CreateLedgerRequest) returns (CreateLedgerResponse);
  rpc DeleteLedger(DeleteLedgerRequest) returns (DeleteLedgerResponse);
  rpc Snapshot(SnapshotRequest) returns (SnapshotResponse);
}
```

#### LedgerService

Manages ledger operations (transactions):

```protobuf
service LedgerService {
  rpc CreateTransaction(CreateTransactionRequest) returns (CreateTransactionResponse);
  rpc GetLedger(GetLedgerRequest) returns (GetLedgerResponse);
  rpc GetLedgers(GetLedgersRequest) returns (GetLedgersResponse);
  rpc StreamLogs(StreamLogsRequest) returns (stream StreamLogsResponse);
}
```

**StreamLogs** streams logs from a ledger with optional sequence range filtering:

```protobuf
message StreamLogsRequest {
  string ledger = 1;
  uint64 from_id = 2; // Optional: start streaming from this log ID (inclusive). If 0, streams from the beginning
  uint64 to_id = 3;   // Optional: stop streaming at this log ID (inclusive). If 0, streams until the end
}
```

**Parameters**:
- `from_id`: Starting log ID (0 = from beginning)
- `to_id`: Ending log ID (0 = until end, inclusive)

**Use Cases**:
- **Full log streaming**: `from_id=0, to_id=0` - Stream all logs
- **Range queries**: `from_id=100, to_id=200` - Stream logs from log ID 100 to 200
- **Catch-up from snapshot**: `from_id=snapshotId, to_id=targetId` - Stream logs needed for catch-up

#### RaftTransportService

Raft communication between nodes:

```protobuf
service RaftTransportService {
  rpc SendMessage(stream RaftMessage) returns (stream RaftMessage);
}
```

### Internal Usage

gRPC is primarily used for:

1. **Request forwarding**: When a follower receives a write request, it forwards it to the leader
2. **Raft replication**: Communication between nodes for the consensus
3. **Log catch-up**: Log synchronization from the leader

## Service Interfaces

### MasterCluster

Main interface to access the cluster:

```go
type MasterCluster interface {
    Cluster
    SystemWriter
    SystemReader
}
```

**Main methods**:
- `CreateLedger()`: Create a ledger
- `DeleteLedger()`: Delete a ledger
- `GetLedger()`: Get a ledger
- `GetClusterState()`: Get cluster state

### LedgerCluster

Interface to access a ledger-specific cluster:

```go
type LedgerCluster interface {
    Cluster
    LedgerReader
    LedgerWriter
}
```

**Main methods**:
- `GetLedger()`: Get a ledger
- `GetLedgers()`: List ledgers
- `CreateTransaction()`: Create a transaction

### Ledger

Interface for ledger operations:

```go
type Ledger interface {
    CreateTransaction(...) (*Log, *CreatedTransaction, error)
    RevertTransaction(...) (*Log, *RevertedTransaction, error)
    SaveTransactionMetadata(...) (*Log, error)
    SaveAccountMetadata(...) (*Log, error)
    DeleteTransactionMetadata(...) (*Log, error)
    DeleteAccountMetadata(...) (*Log, error)
    Import(...) error
    Export(...) error
}
```

## Request Forwarding

### Principle

When a node receives a write request but is not the leader:

1. The node checks if it is leader
2. If not leader, it identifies the leader
3. It forwards the request to the leader via gRPC
4. The leader processes and returns the response

### Implementation

```go
func (adapter *systemNodeAdapter) getMainCluster() (interface {
    service.Cluster
    service.SystemWriter
}, error) {
    if adapter.IsLeader() {
        return adapter.Node, nil
    }
    if adapter.GetLeader() == 0 {
        return nil, ledger.ErrNoLeader
    }
    
    // Forward to leader via gRPC
    grpcConn := adapter.connectionPool.GetConnection(adapter.GetLeader())
    return service.NewGrpcSystemClient(...), nil
}
```

## Client SDK

### Generation

The Client SDK is generated automatically from `openapi.yml` using Speakeasy.

### Retry Configuration

The SDK is configured to automatically retry requests that fail with:

- Code `503` (Service Unavailable)
- Network connection errors

**Configuration** (in `openapi.yml`):
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

The SDK automatically respects the header `Retry-After` when present.

## OpenAPI Documentation

The OpenAPI specification is available in `openapi.yml`. It can be used for:

- Generate client SDKs
- Generate interactive documentation
- Validate requests

### Visualization

Use a tool like Swagger UI or Redoc to visualize the API:

```bash
# With Swagger UI
docker run -p 8080:8080 -e SWAGGER_JSON=/openapi.yml -v $(pwd)/openapi.yml:/openapi.yml swaggerapi/swagger-ui

# With Redoc
npx @redocly/cli preview-docs openapi.yml
```

## Next Steps

To deepen your understanding:

1. [General Architecture](./architecture.md) - How the APIs integrate
2. [Data Flows](./data-flows.md) - Detailed flows of requests
3. [Development](./development.md) - Add new endpoints
