# HTTP API

## Overview

Ledger v3 exposes two types of APIs:

1. **HTTP REST API**: Public API for clients (documented here)
2. **gRPC API**: Inter-node communication and programmatic API (see [gRPC API](grpc-api.md))

## HTTP REST API

### Base URL

By default: `http://localhost:9000`

### API Versioning

All business routes are served under the `/v3/` prefix. Ops routes (`/health`, `/livez`, `/readyz`, `/clusterz`, `/_info`, `/debug/pprof/`) are unversioned and served at the root.

### Authentication

The server supports optional JWT/OIDC authentication with scope-based authorization. When enabled via `--auth-enabled`, all API requests must carry a valid Bearer token in the `Authorization` header. See [Authentication Guide](../../../../ops/authentication.md) for configuration details.

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

### Internal Errors and Sanitization

Business errors (validation, not-found, conflict, etc.) map to specific status codes and carry a machine-readable `errorCode` and a descriptive `errorMessage`, mirroring the gRPC adapter's `Describable` contract.

Two paths, however, must never expose internal error text to clients — the raw value could contain filesystem paths, wrapped Pebble/storage errors, or internal invariant strings:

1. **Panic recovery** (`jsonRecoverer`) — a panic in any handler.
2. **Unmapped errors** (`handleError` fallthrough → `writeInternalServerError`) — any error that is not a domain `Describable` or a known sentinel.

Both paths are sanitized identically to the gRPC adapter: the raw cause is logged **server-side** with a `correlation_id` field (and, for a panic, additionally recorded on the OTel span together with the stack), while the client receives only a generic body:

```json
{
  "errorCode": "INTERNAL_ERROR",
  "errorMessage": "internal server error (correlation ID: <id>)"
}
```

The correlation ID is the request's `X-Request-Id` (Chi `RequestID`) so operators can grep the server logs for the exact ID a caller reports. Adding a new persisted error path that reaches `handleError`'s fallthrough inherits this sanitization automatically; do not add a branch that serializes a raw non-domain error into the response body.

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
POST /v3/{ledgerName}
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
GET /v3/{ledgerName}
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
GET /v3/
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
DELETE /v3/{ledgerName}
```

**Response**: `204 No Content`

### Transactions

#### Create a Transaction

```http
POST /v3/{ledgerName}/transactions
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
POST /v3/{ledgerName}/transactions/{transactionId}/metadata
Content-Type: application/json

{
  "key1": "value1",
  "key2": "value2"
}
```

**Response**: `204 No Content`

#### Delete Transaction Metadata

```http
DELETE /v3/{ledgerName}/transactions/{transactionId}/metadata/{key}
```

**Response**: `204 No Content`

#### Bulk Operations

```http
POST /v3/{ledgerName}/bulk
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

**Query Parameters**:
- `continueOnFailure=true`: When the request is accepted, keep processing subsequent elements after a per-element **business** failure (validation / not-found / conflict / precondition / permission) instead of aborting. Business failures surface as `errorCode` on each element and the overall status stays `200`. Request-level failures (malformed body, missing scope, oversized) and processing-time infra/retryable failures (`ErrNoLeader`, cache-horizon exceeded, `KindInternal`, `KindResourceExhausted`, `KindUnavailable`) still surface as non-2xx (`4xx`, `429`, `503`, `500`) regardless of this flag — see the `POST /v3/{ledgerName}/bulk` operation in `openapi.yml` for the full status matrix.
- `atomic=true`: Execute atomically (all or nothing) - not yet supported

### Account Metadata

#### Save Account Metadata

```http
POST /v3/{ledgerName}/accounts/{address}/metadata
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
DELETE /v3/{ledgerName}/accounts/{address}/metadata/{key}
```

**Response**: `204 No Content`

### Audit

Audit reads expose the tamper-evident audit trail over HTTP (mirroring the gRPC
`BucketService.ListAuditEntries` / `GetAuditEntry`). Audit reads are
**bucket-wide, not ledger-scoped** — a single proposal can touch several ledgers
— so these routes sit at the top level (no `{ledgerName}`) and ledger scope is
expressed as a filter condition. Both require the `ledger:AuditRead` scope.

#### List Audit Entries

```http
GET /v3/_/audit-entries?pageSize=100&after=42&reverse=false&filter=outcome%20%3D%3D%20failure
```

Query parameters:
- `pageSize`: max entries (default 100, capped at 1000)
- `after`: audit sequence to start after (exclusive, opaque cursor)
- `reverse`: `true` iterates newest-first
- `filter`: a filter expression restricted to bare audit fields (`outcome`,
  `ledger`, `seq`, `proposal_id`, `timestamp`, `log_seq`, `caller_subject`,
  `order_type`), using the same grammar as `ledgerctl audit list --filter` (e.g.
  `outcome == failure`, `ledger == main`,
  `order_type in (create_transaction, revert_transaction)`). The fields are
  written without any prefix and resolved against the audit query target
  (EN-1549 — this replaced the old `audit[...]` namespaced syntax, a breaking
  change with no backward compatibility); bare `timestamp` and `ledger` resolve
  to the audit condition only on the audit target, which is why audit fields are
  valid on this endpoint alone. This is the shared `filterexpr` DSL — **not** the
  JSON `QueryFilter` DSL used by prepared queries, which cannot represent audit
  conditions. Filtered reads are served by
  the asynchronous audit index; the consistency guarantee matches the gRPC
  surface.

**Response**: `{ "data": [ AuditEntry, ... ] }` (list omits per-order `items`).

#### Get Audit Entry

```http
GET /v3/_/audit-entries/{sequence}
```

Returns a single `AuditEntry` by its global sequence, with per-order `items`
populated. Missing sequences return `404`.
### Indexes

Per-ledger and bucket-wide index management. See `openapi.yml` for the exhaustive schema; the flow below covers the create → observe → drop cycle.

**Canonical form**: index identifiers are exchanged as opaque strings produced by `indexes.Canonical` (`internal/domain/indexes/id.go`) — e.g. `metadata:TARGET_TYPE_ACCOUNT:color`, `tx_builtin:TX_BUILTIN_INDEX_TIMESTAMP`, `log_builtin:LOG_BUILTIN_INDEX_DATE`. Only metadata indexes carry a target + key; builtin indexes name a proto enum value.

#### Create an index

```http
POST /v3/{ledgerName}/indexes
Content-Type: application/json

{
  "id": "metadata:TARGET_TYPE_ACCOUNT:color"
}
```

Returns `201 Created` once the FSM has queued the backfill. The index enters `BUILDING` on the registry; poll `GET /v3/{ledgerName}/indexes/{canonicalId}/status` and wait for `currentVersion > 0` before running queries that need it.

#### List indexes on a ledger

```http
GET /v3/{ledgerName}/indexes
```

Returns the `Index` registry entries owned by the ledger.

#### Get a single index

```http
GET /v3/{ledgerName}/indexes/{canonicalId}
```

Returns the `Index` registry entry (id, build_status, ledger, created_at, last_built_at, forward_encoding_version).

#### Get per-replica index status

```http
GET /v3/{ledgerName}/indexes/{canonicalId}/status
```

Returns the `IndexEntry` — the registry entry joined with the backfill cursor and the per-replica `IndexVersionState` (current + pending version). Use this to poll for backfill completion.

#### Inspect a metadata index

```http
GET /v3/{ledgerName}/indexes/{canonicalId}/inspect?mode=summary
```

Scans the index and returns distinct values, facets, or a summary (`mode=distinctValues|facets|summary`). Only metadata indexes are inspectable; builtin canonicals return `400`.

#### Drop an index

```http
DELETE /v3/{ledgerName}/indexes/{canonicalId}
```

Returns `204 No Content` once the FSM has committed the drop.

#### Bucket-wide index reads

Cluster-wide observability (registry entries whose owning ledger is empty, aggregated indexer progress):

```http
GET /v3/_/indexes                        # ?scope=all (default) | bucket
GET /v3/_/indexes/status?ledger=         # aggregate: LastIndexedSequence, Lag, IndexFileSize
GET /v3/_/indexes/{canonicalId}          # single bucket-scoped Index entry
GET /v3/_/indexes/{canonicalId}/status   # single bucket-scoped IndexEntry
```

The bucket-scoped single-index routes are a hook — no production write hits `SubAttrIndex` with an empty ledger today. The audit index (cross-ledger by nature) lives in a dedicated read-store keyspace and will be exposed on `GET /v3/audit-entries` per EN-1481, not through this hook.

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

## gRPC API

The gRPC API provides a programmatic interface for interacting with the ledger cluster. It uses a unified `BucketService` with the `Apply` method for all write operations.

**Key features:**
- Unified `Apply` method for all write operations (create ledger, transactions, metadata)
- Batch operations support
- Automatic request forwarding from followers to leader
- Idempotency key support

For detailed documentation, examples, and client code, see [gRPC API](grpc-api.md).

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

1. [gRPC API](grpc-api.md) - Programmatic API for clients and CLI
2. [General Architecture](../../overview.md) - How the APIs integrate
3. [Data Flows](../../data-flows.md) - Detailed flows of requests
4. [Development](../../../contributing/development.md) - Add new endpoints
