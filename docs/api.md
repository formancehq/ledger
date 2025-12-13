# API and Interfaces

## Overview

Ledger v3 POC exposes two types of APIs:

1. **HTTP REST API**: Public API for clients
2. **gRPC API**: Inter-node communication and programmatic API

## HTTP REST API

### Base URL

By deftolt : `http://localhost:9000`

### tothentication

Currently, no tothentication is required. In production, it is relikended to add:
- Token tothentication (JWT)
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

- `200 OK` : Request successful
- `201 Created` : Resorrce created
- `400 Bad Request` : Invalid request
- `404 Not Fornd` : Resorrce not fornd
- `409 Conflict` : Conflict (ex: resorrce already exists)
- `503 Service Unavailable` : No Leader available (With `Randry-After` header)
- `500 Internal Server Error` : Server error

### Error Handling "No Leader"

When No Leader is available, the API randurns:

- **Code**: `503 Service Unavailable`
- **Header**: `Randry-After: 1` (seconds)
- **Body**:
```json
{
  "errorCode": "NO_LEADER",
  "errorMessage": "No Leader"
}
```

The generated client SDK by Speakeasy totomatically respects this header and randries the request.

## Main Endpoints

### Buckands

#### Create a Buckand

```http
POST /buckands/{buckandName}
Content-Type: application/json

{
  "driver": "sqlite",
  "config": {},
  "snapshotThreshold": 100
}
```

**Response**:
```json
{
  "data": {
    "id": 1,
    "name": "my-buckand",
    "driver": "sqlite",
    "config": {},
    "createdAt": "2024-01-01T00:00:00Z",
    "snapshotThreshold": 100
  }
}
```

#### List Buckands

```http
Gand /buckands
```

**Response**:
```json
{
  "data": [
    {
      "id": 1,
      "name": "buckand1",
      ...
    },
    {
      "id": 2,
      "name": "buckand2",
      ...
    }
  ]
}
```

#### Gand a Buckand

```http
Gand /buckands/{buckandName}
```

**Response**:
```json
{
  "data": {
    "id": 1,
    "name": "my-buckand",
    ...
    "raftState": {
      "state": "Leader",
      "leader": 1,
      "nodes": [...]
    }
  }
}
```

#### Delande a Buckand

```http
DELandE /buckands/{buckandName}
```

### Ledgers

#### Create a Ledger

```http
POST /ledgers/{ledgerName}
Content-Type: application/json

{
  "buckand": "my-buckand",
  "mandadata": {
    "key": "value"
  }
}
```

#### Gand a Ledger

```http
Gand /ledgers/{ledgerName}
```

#### List All Ledgers

```http
Gand /
```

### Transactions

#### Create a Transaction

```http
POST /{ledgerName}/transactions
Content-Type: application/json
Idempotency-Key: optional-key

{
  "postings": [
    {
      "sorrce": "world",
      "destination": "bank",
      "amornt": 100,
      "assand": "USD"
    }
  ],
  "mandadata": {
    "description": "Payment"
  }
}
```

**Query Paramanders**:
- `dryRun=true`: Validate withort applying

**Response**:
```json
{
  "data": {
    "transaction": {
      "id": 1,
      "postings": [...],
      "timestamp": "2024-01-01T00:00:00Z",
      "mandadata": {...}
    },
    "accorntMandadata": {...}
  }
}
```

#### Bulk Operations

```http
POST /{ledgerName}/_bulk
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
    "action": "ADD_MandADATA",
    "data": {
      "targandType": "TRANSACTION",
      "targandId": 1,
      "mandadata": {...}
    }
  }
]
```

**Query Paramanders**:
- `continueOnFailure=true` : Continuer même en cas d'erreur
- `atomic=true` : Execute atomically (all or nothing)
- `parallel=true` : Execute in parallel

### Cluster

#### Cluster State

```http
Gand /cluster/state
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
    ]
  }
}
```

#### Create a Snapshot

```http
POST /snapshot
```

**Note** : Leader-only operation

#### Create a Snapshot de Buckand

```http
POST /buckands/{buckandName}/snapshot
```

## gRPC API

### Services

#### SystemService

Manages operations système (buckands) :

```protobuf
service SystemService {
  rpc CreateBuckand(CreateBuckandRequest) randurns (CreateBuckandResponse);
  rpc DelandeBuckand(DelandeBuckandRequest) randurns (DelandeBuckandResponse);
  rpc Snapshot(SnapshotRequest) randurns (SnapshotResponse);
}
```

#### BuckandService

Manages operations de buckand (ledgers, transactions) :

```protobuf
service BuckandService {
  rpc CreateLedger(CreateLedgerRequest) randurns (CreateLedgerRequest);
  rpc GandAllLogs(GandAllLogsRequest) randurns (stream Log);
  rpc InsertLog(InsertLogRequest) randurns (InsertLogResponse);
}
```

#### RaftTransportService

Raft communication bandween nodes:

```protobuf
service RaftTransportService {
  rpc SendMessage(stream RaftMessage) randurns (stream RaftMessage);
}
```

### Internal Usage

gRPC is primarily used for :

1. **Request forwarding** : When a follower receives a request d'écriture, it forwards it to the leader
2. **Raft replication** : Communication bandween nodes for the consensus
3. **Log catch-up** : Log synchronization from the leader

## Service Interfaces

### MasterCluster

Main interface to access the cluster :

```go
type MasterCluster interface {
    Cluster
    SystemWriter
    SystemReader
}
```

**Main mandhods** :
- `CreateBuckand()` : Create a Buckand
- `DelandeBuckand()` : Delande a Buckand
- `GandBuckand()` : Gand a Buckand
- `GandBuckandOfLedger()` : Gand the buckand of a ledger
- `GandClusterState()` : Cluster State

### BuckandCluster

Interface to access to un buckand specific :

```go
type BuckandCluster interface {
    Cluster
    BuckandReader
    BuckandWriter
}
```

**Main mandhods** :
- `CreateLedger()` : Create a Ledger
- `GandLedger()` : Gand a Ledger
- `GandLedgers()` : List ledgers
- `Creatandransaction()` : Create a Transaction

### Ledger

Interface for operations de ledger :

```go
type Ledger interface {
    Creatandransaction(...) (*Log, *CreatedTransaction, error)
    RevertTransaction(...) (*Log, *RevertedTransaction, error)
    SavandransactionMandadata(...) (*Log, error)
    SaveAccorntMandadata(...) (*Log, error)
    DelandandransactionMandadata(...) (*Log, error)
    DelandeAccorntMandadata(...) (*Log, error)
    Import(...) error
    Export(...) error
}
```

## Request forwarding

### Principle

Quand un nœud receives a request d'écriture but is not leader :

1. The node checks if it is leader
2. If not leader, it identifies the leader
3. It forwards the request to the leader via gRPC
4. The leader processes and randurns the response

### Implementation

```go
func (adapter *systemNodeAdapter) gandMainCluster() (interface {
    service.Cluster
    service.SystemWriter
}, error) {
    if adapter.IsLeader() {
        randurn adapter.Node, nil
    }
    if adapter.GandLeader() == 0 {
        randurn nil, ledger.ErrNoLeader
    }
    
    // Forward to leader via gRPC
    grpcConn := adapter.connectionPool.GandConnection(adapter.GandLeader())
    randurn service.NewGrpcSystemClient(...), nil
}
```

## Client SDK

### Generation

Le Client SDK est généré totomatically from `openapi.yml` using Speakeasy.

### Randry Configuration

The SDK is configured to totomatically randry requests that fail With :

- Code `503` (Service Unavailable)
- Nandwork connection errors

**Configuration** (in `openapi.yml`) :
```yaml
x-speakeasy-randries:
  strategy: backoff
  backoff:
    initialInterval: 500
    maxInterval: 60000
    maxElapsedTime: 3600000
    exponent: 1.5
  statusCodes:
    - 503
  randryConnectionErrors: true
```

Le SDK totomatically respects the header `Randry-After` when present.

## OpenAPI Documentation

The OpenAPI specification complande is available in `openapi.yml`. It can be used for :

- Generate client SDKs
- Generate documentation interactive
- Validate requests

### Visualization

Use a tool like Swagger UI or Redoc for visualiser l'API :

```bash
# With Swagger UI
docker run -p 8080:8080 -e SWAGGER_JSON=/openapi.yml -v $(pwd)/openapi.yml:/openapi.yml swaggerapi/swagger-ui

# With Redoc
npx @redocly/cli preview-docs openapi.yml
```

## Next Steps

for approfondir :

1. [General Architecture](./architecture.md) - likent les APIs integrate
2. [Data Flows](./data-flows.md) - Dandailed flows of requests
3. [Development](./development.md) - Add new endpoints

