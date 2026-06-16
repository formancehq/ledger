# HTTP API

## Overview

Ledger v3 POC exposes two types of APIs:

1. **HTTP REST API**: Public API for clients (documented here)
2. **gRPC API**: Inter-node communication and programmatic API (see [gRPC API](./grpc-api.md))

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

The server supports optional JWT/OIDC authentication with scope-based authorization. When enabled via `--auth-enabled`, all API requests must carry a valid Bearer token in the `Authorization` header. See [Authentication Guide](../../../ops/authentication.md) for configuration details.

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

### Retry-After Header

The `Retry-After` header is used to indicate when a client should retry a request after receiving a `503 Service Unavailable` response.

#### When It's Returned

The `Retry-After` header is included in responses when no leader is available in the Raft cluster. This can occur in the following situations:

1. **Leader Election in Progress**
   - The previous leader has failed or stepped down
   - A new leader election is currently taking place
   - No leader has been elected yet

2. **Network Partition**
   - The cluster is split into multiple partitions
   - No partition has a majority of nodes
   - No leader can be elected without a majority

3. **Cluster Startup**
   - The cluster is initializing
   - Nodes are still discovering each other
   - Leader election has not completed yet

4. **Insufficient Nodes**
   - Not enough nodes are available to form a quorum
   - The cluster cannot elect a leader

#### Response Format

When no leader is available, the API returns:

- **HTTP Status Code**: `503 Service Unavailable`
- **Header**: `Retry-After: 1` (seconds)
- **Response Body**:
```json
{
  "errorCode": "NO_LEADER",
  "errorMessage": "No Leader"
}
```

#### Header Value

The `Retry-After` header value is set to `1` second, indicating that clients should wait at least 1 second before retrying the request.

**Note**: This is a conservative value. In practice, leader elections typically complete within a few hundred milliseconds, but the 1-second delay ensures the cluster has time to stabilize.

#### Client Behavior

Clients should:

1. **Respect the header**: Wait at least the specified duration before retrying
2. **Implement exponential backoff**: Increase wait time between retries to avoid overwhelming the cluster
3. **Set a maximum retry limit**: Avoid infinite retry loops
4. **Handle gracefully**: Show appropriate error messages to users during leader elections

#### Best Practices

- **Read operations**: Can be served by any node (if implemented), avoiding leader dependency
- **Write operations**: Must go through the leader, so will fail during leader elections
- **Idempotency**: Ensure write operations are idempotent to safely retry after leader election
- **Monitoring**: Track `503` responses to monitor cluster health and leader election frequency

## Main Endpoints

### Ledgers

#### Create a Ledger

```http
POST /{ledgerName}
Content-Type: application/json

{
  "metadata": {
    "key": "value"
  }
}
```

**Response**:
```json
{
  "data": {
    "name": "my-ledger",
    "id": 1,
    "metadata": {
      "key": "value"
    },
    "createdAt": "2024-01-01T00:00:00Z"
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
    "name": "my-ledger",
    "id": 1,
    "metadata": {},
    "createdAt": "2024-01-01T00:00:00Z"
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
      "name": "ledger1",
      "id": 1,
      "metadata": {},
      "createdAt": "2024-01-01T00:00:00Z"
    },
    {
      "name": "ledger2",
      "id": 2,
      "metadata": {},
      "createdAt": "2024-01-01T00:00:00Z"
    }
  ]
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

#### Save Transaction Metadata

```http
POST /{ledgerName}/transactions/{transactionId}/metadata
Content-Type: application/json

{
  "key1": "value1",
  "key2": "value2"
}
```

**Response**: `204 No Content`

#### Delete Transaction Metadata

```http
DELETE /{ledgerName}/transactions/{transactionId}/metadata/{key}
```

**Response**: `204 No Content`

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
- `atomic=true`: Execute atomically (all or nothing) - not yet supported

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

#### Delete Account Metadata

```http
DELETE /{ledgerName}/accounts/{address}/metadata/{key}
```

**Response**: `204 No Content`

### Cluster

Cluster operations are available via the `ClusterService` gRPC API (port 8888) and the `ledgerctl cluster` CLI commands.

**Available RPCs**:
- `GetClusterState`: Current Raft cluster state (leader, voters, learners)
- `GetDiskUsage`: Local node disk usage
- `GetNodeTime`: Node's physical clock time
- `TransferLeadership`: Transfer Raft leadership to another node
- `Backup`: Point-in-time backup as tar archive
- `AddLearner`: Add a non-voting node to the cluster
- `PromoteLearner`: Promote a learner to full voter

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

The gRPC API provides a programmatic interface for interacting with the ledger cluster. It uses a unified `BucketService` with the `Apply` method for all write operations.

**Key features:**
- Unified `Apply` method for all write operations (create ledger, transactions, metadata)
- Batch operations support
- Automatic request forwarding from followers to leader
- Idempotency key support

For detailed documentation, examples, and client code, see [gRPC API](./grpc-api.md).

## Service Interfaces

### Controller

The main interface for read and write operations:

```go
// internal/application/ctrl/controller.go
type Controller interface {
    Apply(ctx context.Context, requests ...*servicepb.Request) ([]*commonpb.Log, error)
    GetAccount(ctx context.Context, ledger string, address string) (*commonpb.Account, error)
    GetTransaction(ctx context.Context, ledger string, txID uint64) (*commonpb.Transaction, error)
    GetLedgerByName(ctx context.Context, name string) (*commonpb.LedgerInfo, error)
    ListLedgers(ctx context.Context) (data.Cursor[*commonpb.LedgerInfo], error)
    ListTransactions(ctx context.Context, ledger string, pageSize uint32, afterTxID uint64) (data.Cursor[*commonpb.Transaction], error)
}
```

### Routed Controller

The `RoutedController` wraps the `Controller` to handle leader forwarding:
- **Write operations** (`Apply`): Forwarded to the leader via gRPC if the node is a follower
- **Read operations** (`GetAccount`, `GetTransaction`, etc.): Served locally from the Pebble store

## Request Forwarding

### Principle

When a node receives a write request but is not the leader:

1. The node checks if it is leader
2. If not leader, it identifies the leader
3. It forwards the request to the leader via gRPC
4. The leader processes and returns the response

### Implementation

The `RoutedController` checks if the node is the leader. If not, it forwards the request to the leader's service port via gRPC:

```go
// Simplified from internal/bootstrap/controller_routed.go
func (r *RoutedController) Apply(ctx context.Context, requests ...*servicepb.Request) ([]*commonpb.Log, error) {
    if r.isLeader() {
        return r.localController.Apply(ctx, requests...)
    }
    // Forward to leader via ServiceConnectionPool
    leaderClient := r.getLeaderClient()
    resp, err := leaderClient.Apply(ctx, &servicepb.ApplyRequest{Envelopes: servicepb.UnsignedEnvelopes(requests...)})
    return resp.Logs, err
}
```

## OpenAPI Documentation

The OpenAPI specification is available in `openapi.yml`. It can be used for:

- Generate client SDKs (using tools like openapi-generator)
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

1. [gRPC API](./grpc-api.md) - Programmatic API for clients and CLI
2. [General Architecture](../core/architecture.md) - How the APIs integrate
3. [Data Flows](../data-model/data-flows.md) - Detailed flows of requests
4. [Development](../../contributing/development.md) - Add new endpoints
