# General Architecture

## Overview

Ledger v3 POC is a distributed accounting ledger system using the Raft consensus protocol to ensure data consistency across a cluster of nodes. The system is designed to be highly available, fault-tolerant, and scalable.

## High-Level Architecture

```mermaid
graph TB
    subgraph "Client Applications"
        HTTPClient[HTTP Client]
        GRPCClient[gRPC Client]
        CLIClient[CLI Client<br/>gRPC]
    end

    subgraph "Node 1"
        HTTPServer1[HTTP Server<br/>Port 9000]
        GRPCServer1[gRPC Service<br/>Port 8888]
        RaftServer1[Raft Transport<br/>Port 7777]
        RaftNode1[Raft Node]
        FSM1[FSM<br/>All Ledgers]
        Storage1[Store]
    end
    
    subgraph "Node 2"
        HTTPServer2[HTTP Server<br/>Port 9000]
        GRPCServer2[gRPC Service<br/>Port 8888]
        RaftServer2[Raft Transport<br/>Port 7777]
        RaftNode2[Raft Node]
        FSM2[FSM<br/>All Ledgers]
        Storage2[Store]
    end
    
    subgraph "Node 3"
        HTTPServer3[HTTP Server<br/>Port 9000]
        GRPCServer3[gRPC Service<br/>Port 8888]
        RaftServer3[Raft Transport<br/>Port 7777]
        RaftNode3[Raft Node]
        FSM3[FSM<br/>All Ledgers]
        Storage3[Store]
    end
    
    HTTPClient --> HTTPServer1
    GRPCClient --> GRPCServer1
    CLIClient --> GRPCServer1
    
    HTTPServer1 --> RaftNode1
    HTTPServer2 --> RaftNode2
    HTTPServer3 --> RaftNode3
    
    RaftNode1 --> FSM1
    RaftNode2 --> FSM2
    RaftNode3 --> FSM3
    
    FSM1 --> Storage1
    FSM2 --> Storage2
    FSM3 --> Storage3
    
    RaftNode1 -.Raft.-> RaftNode2
    RaftNode2 -.Raft.-> RaftNode3
    RaftNode3 -.Raft.-> RaftNode1
```

## Main Components

### 1. Cluster Nodes

Each node in the cluster runs the following components:

- **HTTP Server**: Public REST API (port 9000)
- **gRPC Service Server** (port 8888): External client-facing API
  - `BucketService`: Ledger operations, transactions, accounts
  - `ClusterService`: Cluster state and node information
  - `Health`: Health checks
- **gRPC Raft Server** (port 7777): Internal inter-node communication
  - `RaftTransport`: Raft message exchange between nodes
  - `SnapshotService`: Snapshot transfer for new nodes
- **Raft Node**: Single Raft group managing all ledgers and transactions
- **Routed Controller**: Routes requests to local controller or forwards to leader
- **Service Connection Pool**: Manages gRPC connections to peers' service ports for request forwarding
- **Finite State Machine (FSM)**: State machine for applying commands (ledger and log operations)
- **Store**: Persistent storage for logs, balances, and metadata

### 2. Abstraction Layers

```mermaid
graph TB
    subgraph "API Layer"
        HTTP[HTTP Handlers]
        ServiceGRPC[gRPC Service Server<br/>BucketService, ClusterService<br/>Port 8888]
        RaftGRPC[gRPC Raft Server<br/>RaftTransport, SnapshotService<br/>Port 7777]
    end
    
    subgraph "Service Layer"
        Controller[Default Controller<br/>Transaction Processing]
        RoutedCtrl[Routed Controller<br/>Leader Forwarding]
    end
    
    subgraph "Raft Layer"
        RaftNode[Raft Node]
        Transport[gRPC Transport]
        ServicePool[Service Connection Pool]
    end
    
    subgraph "Storage Layer"
        WAL[WAL Storage]
        Store[Store<br/>Logs + Balances]
        Snapshot[Snapshot Store]
    end
    
    HTTP --> RoutedCtrl
    ServiceGRPC --> RoutedCtrl
    RoutedCtrl --> Controller
    RoutedCtrl -.forward to leader.-> ServicePool
    Controller --> Store
    
    RaftGRPC --> RaftNode
    RaftNode --> Transport
    
    RaftNode --> WAL
    RaftNode --> Snapshot
```

## Single Raft Architecture

The system uses a **single Raft group** to manage all operations:

### Unified FSM

The FSM (Finite State Machine) handles all commands:

**Ledger Commands**:
- `CreateLedgerCommand`: Create a new ledger
- `DeleteLedgerCommand`: Delete an existing ledger

**Log Commands**:
- `CreateLogCommand`: Insert a log (transaction, metadata changes, reversions) into a ledger

### State Structure

The FSM maintains a unified state containing all ledgers. The actual in-memory state lives in the `Machine` struct (`internal/infra/state/machine.go`) and its `StateRegistry` (`internal/infra/state/registry.go`). FSM-level scalar fields — those recovered from Pebble at boot or follower sync — live in `Machine.State *FSMState` (`internal/infra/state/fsmstate.go`); sub-trackers stay on `Machine` directly. Key fields include:

- `State.NextSequenceID` -- next global log sequence number
- `State.LastAppliedIndex` / `State.LastAppliedTimestamp` -- last Raft entry applied
- `Registry.Boundaries` -- per-ledger `LedgerBoundaries` proto (next log ID, next transaction ID) stored as a KeyStore attribute
- `Registry.Ledgers` -- per-ledger `LedgerInfo` tracked via KeyStore
- `Registry.Reversions` -- per-ledger reversion bitsets
- `Registry.Volumes`, `Registry.AccountMetadata`, `Registry.References`, etc. -- other KeyStore-backed attributes

Per-ledger boundaries use the `LedgerBoundaries` protobuf message from `raft_cmd.proto`:

```protobuf
message LedgerBoundaries {
  fixed64 next_transaction_id = 1;
  fixed64 next_log_id = 2;
  fixed64 volume_count = 3;
  fixed64 metadata_count = 4;
  fixed64 reference_count = 5;
  fixed64 posting_count = 6;
  fixed64 ephemeral_evicted_count = 7;
  fixed64 transient_used_count = 8;
  fixed64 revert_count = 9;
  fixed64 numscript_execution_count = 10;
}
```

> **Note:** The `State` / `LedgerState` proto messages sometimes shown in older documentation are conceptual models, not actual proto definitions. The real FSM state is spread across the `Machine` struct fields and its `StateRegistry`.

### Benefits of Single Raft

1. **Simplicity**: One Raft group to manage instead of N+1 groups
2. **Consistent Operations**: All operations go through the same consensus layer
3. **Easier Recovery**: Single snapshot and WAL for the entire system
4. **Reduced Overhead**: No need to coordinate multiple Raft leaders
5. **Cross-Ledger Atomicity**: Enables atomic bulk operations spanning multiple ledgers

> **📋 Related**: See [Global Log Architecture](subsystems/consensus/global-log.md) for details on how the global log enables system-level atomic operations.

```mermaid
graph TB
    subgraph "Single Raft Group"
        N1[Node 1 Leader]
        N2[Node 2 Follower]
        N3[Node 3 Follower]
    end
    
    subgraph "FSM State"
        LedgerA[Ledger A State]
        LedgerB[Ledger B State]
        LedgerC[Ledger C State]
    end
    
    N1 -.Raft.-> N2
    N2 -.Raft.-> N3
    N3 -.Raft.-> N1
    
    N1 --> LedgerA
    N1 --> LedgerB
    N1 --> LedgerC
```

## Data Flows

### Ledger Creation

```mermaid
sequenceDiagram
    participant Client
    participant API as gRPC/HTTP Server
    participant Ctrl as Routed Controller
    participant Admission
    participant FSM as FSM

    Client->>API: Apply(CreateLedger)
    API->>Ctrl: Apply()

    alt Node is leader
        Ctrl->>Admission: Admit()
        Admission->>FSM: Propose via Raft
        FSM->>FSM: Validate & Create LedgerState
        FSM-->>Admission: LedgerInfo
    else Node is follower
        Ctrl->>Ctrl: Forward via gRPC (to leader)
        Note over Ctrl: Same as leader path
    end

    Ctrl-->>API: ApplyResponse
    API-->>Client: Response
```

### Transaction Creation

```mermaid
sequenceDiagram
    participant Client
    participant API as gRPC/HTTP Server
    participant Ctrl as Routed Controller
    participant Admission
    participant FSM as FSM
    participant Store as Pebble Store

    Client->>API: Apply(CreateTransaction)
    API->>Ctrl: Apply()

    alt Node is leader
        Ctrl->>Admission: Admit()
        Note over Admission: Preload volumes from cache/store
        Admission->>FSM: Propose via Raft
        FSM->>FSM: Process + Generate IDs
        FSM->>Store: Commit batch
        FSM-->>Admission: Log
    else Node is follower
        Ctrl->>Ctrl: Forward via gRPC (to leader)
        Note over Ctrl: Same as leader path
    end

    Ctrl-->>API: ApplyResponse
    API-->>Client: Response
```

## Leader Management

### Request Forwarding

When a node receives a write request but is not the leader:

1. The node detects it is not the leader via `RoutedController`
2. It identifies the current leader from the Raft state
3. It uses the `ServiceConnectionPool` to get a connection to the leader's **service port** (8888)
4. It forwards the request to the leader's `BucketService` via gRPC
5. The leader processes the request and returns the response

**Important**: Request forwarding uses the **service port** (8888), not the Raft transport port (7777). Each peer is configured with both addresses: `<id>/<raftAddress>/<serviceAddress>`.

```mermaid
sequenceDiagram
    participant Client
    participant FollowerService as Follower<br/>Service :8888
    participant RoutedCtrl as Routed Controller
    participant ServicePool as Service Pool
    participant LeaderService as Leader<br/>Service :8888
    participant FSM
    
    Client->>FollowerService: Write Request
    FollowerService->>RoutedCtrl: Process
    RoutedCtrl->>RoutedCtrl: Check IsLeader()
    RoutedCtrl->>ServicePool: GetConnection(leaderID)
    ServicePool-->>RoutedCtrl: gRPC conn to :8888
    RoutedCtrl->>LeaderService: Forward via BucketService
    LeaderService->>FSM: Apply Command
    FSM-->>LeaderService: Result
    LeaderService-->>RoutedCtrl: Response
    RoutedCtrl-->>FollowerService: Response
    FollowerService-->>Client: Response
```

### "No Leader" Error Handling

If no leader is available (e.g., during an election), the system returns a `503 Service Unavailable` error with the `Retry-After: 1` header to indicate the client should retry.

## Data Isolation

### Logical Isolation Between Ledgers

Although all ledgers share the same Raft group and storage:

- Each ledger has its own sequence numbers (log IDs and transaction IDs)
- Balances and metadata are stored with ledger prefixes
- Operations on one ledger do not affect the state of others

### Storage Organization

All ledgers share a single Pebble key-value store with byte-prefixed keys organized into six zones:

| Zone | Byte | Purpose | Lifecycle |
|------|------|---------|-----------|
| **Attributes** | `0x01` | Volumes, metadata, boundaries, tx state, references, sink configs, numscript, prepared queries | Hot storage, hashed during seal |
| **Cache** | `0x02` | Generation-based cache for fast restart | Rotated per generation |
| **Per-Ledger** | `0x03` | Reversions, pending cleanups, prepared queries, mirror source head, mirror cursor, mirror status | Per-ledger lifecycle |
| **Cold** | `0x04` | Logs + audit entries | Archived to cold storage then purged per chapter |
| **Idempotency** | `0x05` | Deduplication keys + time index | TTL-based eviction |
| **Global** | `0x06` | Applied index/timestamp, ledger info, signing, chapters, cluster config, bloom | Lives forever |

## Scalability

### Horizontal Scaling

The system can be scaled horizontally by adding nodes to the cluster:

- New nodes join the Raft group
- They automatically receive replicated data
- Load is distributed across all nodes for reads

**Note:** Horizontal scaling is currently under implementation.

### Limitations

- The number of nodes must be odd to avoid ties during voting
- A cluster of N nodes can tolerate (N-1)/2 failures
- Performance may be limited by the leader (all writes go through the leader)
- All ledgers share the same leader

## Observability

### Logging

The system uses structured logging with contextual fields:
- Node ID
- Ledger name
- Command ID
- Raft index

### Tracing

OpenTelemetry is integrated for distributed tracing:
- HTTP request traces
- gRPC call traces
- Raft operation traces

### Metrics

The following metrics are available:
- Cluster state (leader, followers)
- Number of ledgers
- Apply entries duration and batch size
- Raft state transitions

## Next Steps

To deepen your understanding:

1. [Global Log Architecture](subsystems/consensus/global-log.md) - Two-level log architecture and atomic bulk operations
2. [Raft Consensus](subsystems/consensus/raft-consensus.md) - Details on Raft implementation
3. [Ledgers](data-model.md) - Data organization
4. [API and Interfaces](subsystems/api/http-api.md) - API documentation
5. [Storage and Persistence](subsystems/storage/storage.md) - Storage management
6. [gRPC Connections](subsystems/api/grpc-connections.md) - Inter-node communication and reconnection strategies
