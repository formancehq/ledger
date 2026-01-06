# General Architecture

## Overview

Ledger v3 POC is a distributed accounting ledger system using the Raft consensus protocol to ensure data consistency across a cluster of nodes. The system is designed to be highly available, fault-tolerant, and scalable.

## High-Level Architecture

```mermaid
graph TB
    subgraph "Cluster Raft"
        Node1[Node 1<br/>Leader]
        Node2[Node 2<br/>Follower]
        Node3[Node 3<br/>Follower]
    end
    
    subgraph "Client Applications"
        HTTPClient[HTTP Client]
        GRPCClient[gRPC Client]
        CLIClient[CLI Client]
    end
    
    subgraph "Node Components"
        HTTPServer[HTTP Server<br/>Port 9000]
        GRPCServer[gRPC Server<br/>Port 8888]
        SystemRaft[System Raft Group]
        LedgerRaft1[Ledger Raft Group 1]
        LedgerRaft2[Ledger Raft Group 2]
        FSM[Finite State Machine]
        Storage[(Storage)]
    end
    
    HTTPClient --> HTTPServer
    GRPCClient --> GRPCServer
    CLIClient --> HTTPServer
    
    HTTPServer --> SystemRaft
    GRPCServer --> SystemRaft
    GRPCServer --> LedgerRaft1
    GRPCServer --> LedgerRaft2
    
    SystemRaft --> FSM
    LedgerRaft1 --> FSM
    LedgerRaft2 --> FSM
    
    FSM --> Storage
    
    Node1 -.Raft Protocol.-> Node2
    Node2 -.Raft Protocol.-> Node3
    Node3 -.Raft Protocol.-> Node1
```

## Main Components

### 1. Cluster Nodes

Each node in the cluster runs the following components:

- **HTTP Server**: Public REST API (port 9000)
- **gRPC Server**: Inter-node communication and gRPC API (port 8888)
- **System Raft Group**: Main Raft group managing ledgers
- **Ledger Raft Groups**: One Raft group per ledger to manage transactions
- **Finite State Machine (FSM)**: State machine for applying commands
- **Storage**: Persistent storage (WAL, snapshots, logs)

### 2. Abstraction Layers

```mermaid
graph LR
    subgraph "API Layer"
        HTTP[HTTP Handlers]
        GRPC[gRPC Services]
    end
    
    subgraph "Service Layer"
        MasterCluster[MasterCluster]
        LedgerCluster[LedgerCluster]
        LedgerService[Ledger Service]
    end
    
    subgraph "Raft Layer"
        SystemNode[System Node]
        LedgerNode[Ledger Node]
        Transport[Transport]
    end
    
    subgraph "Storage Layer"
        WAL[WAL Storage]
        LogStore[Log Store]
        Snapshot[Snapshot Store]
    end
    
    HTTP --> MasterCluster
    GRPC --> MasterCluster
    MasterCluster --> SystemNode
    MasterCluster --> LedgerNode
    LedgerCluster --> LedgerNode
    LedgerService --> LedgerNode
    SystemNode --> Transport
    LedgerNode --> Transport
    SystemNode --> WAL
    LedgerNode --> WAL
    LedgerNode --> LogStore
    SystemNode --> Snapshot
    LedgerNode --> Snapshot
```

## Multi-Level Raft Architecture

The system uses a two-level Raft group architecture:

### Level 1: System Raft Group

The system Raft group manages ledger creation and deletion. Every node participates in this group.

**Responsibilities**:
- Create/delete ledgers
- Manage the ledger list
- Coordinate ledger Raft groups

### Level 2: Ledger Raft Groups

Each ledger has its own independent Raft group to manage:
- Insert logs (transactions)
- Synchronize ledger data
- Manage ledger state

**Isolation**: Ledger Raft groups are completely isolated from each other. A problem in one ledger does not affect others.

```mermaid
graph TB
    subgraph "System Raft Group"
        S1[Node 1 Leader]
        S2[Node 2 Follower]
        S3[Node 3 Follower]
    end
    
    subgraph "Ledger A Raft Group"
        LA1[Node 1 Leader]
        LA2[Node 2 Follower]
        LA3[Node 3 Follower]
    end
    
    subgraph "Ledger B Raft Group"
        LB1[Node 1 Follower]
        LB2[Node 2 Leader]
        LB3[Node 3 Follower]
    end
    
    S1 -.Manages.-> LA1
    S1 -.Manages.-> LB1
    S2 -.Manages.-> LA2
    S2 -.Manages.-> LB2
    S3 -.Manages.-> LA3
    S3 -.Manages.-> LB3
```

## Data Flows

### Ledger Creation

```mermaid
sequenceDiagram
    participant Client
    participant HTTP
    participant SystemNode
    participant SystemFSM
    participant LedgerNode
    participant Storage
    
    Client->>HTTP: POST /ledgers/{name}
    HTTP->>SystemNode: CreateLedger()
    SystemNode->>SystemFSM: Propose Command
    SystemFSM->>SystemFSM: Validate & Assign ID
    SystemFSM->>LedgerNode: Start Ledger Raft Group
    LedgerNode->>Storage: Initialize Storage
    SystemFSM->>Storage: Persist Ledger Info
    SystemNode-->>HTTP: LedgerInfo
    HTTP-->>Client: 201 Created
```

### Transaction Creation

```mermaid
sequenceDiagram
    participant Client
    participant HTTP
    participant LedgerNode
    participant LedgerFSM
    participant LogStore
    participant Storage
    
    Client->>HTTP: POST /ledgers/{name}/transactions
    HTTP->>LedgerNode: CreateTransaction()
    LedgerNode->>LedgerFSM: Propose InsertLog Command (via Raft)
    LedgerFSM->>LedgerFSM: Generate Sequence & Store in memory
    LedgerFSM->>RuntimeStore: InsertLogs() - Update balances
    Note over LedgerFSM,LogStore: Logs written to LogStore during snapshot
    LedgerNode-->>HTTP: CreatedTransaction
    HTTP-->>Client: 201 Created
```

## Leader Management

### Request Forwarding

When a node receives a write request but is not the leader:

1. The node detects it is not the leader
2. It identifies the current leader
3. It forwards the request to the leader via gRPC
4. The leader processes the request and returns the response

```mermaid
sequenceDiagram
    participant Client
    participant Follower
    participant Leader
    participant FSM
    
    Client->>Follower: Write Request
    Follower->>Follower: Check IsLeader()
    Follower->>Leader: Forward via gRPC
    Leader->>FSM: Apply Command
    FSM-->>Leader: Result
    Leader-->>Follower: Response
    Follower-->>Client: Response
```

### "No Leader" Error Handling

If no leader is available (e.g., during an election), the system returns a `503 Service Unavailable` error with the `Retry-After: 1` header to indicate the client should retry.

## Isolation and Security

### Ledger Isolation

- Each ledger has its own Raft group
- Ledger data is stored separately
- A ledger can use a different storage driver (configurable)
- Problems in one ledger do not affect others

### Data Isolation

- Transaction logs are stored in the ledger-specific LogStore
- Snapshots are created per ledger
- Recovery is done ledger by ledger

## Scalability

### Horizontal Scaling

The system can be scaled horizontally by adding nodes to the cluster:

- New nodes join the system Raft group
- They automatically participate in existing ledger Raft groups
- Load is distributed across all nodes

**Note:** Horizontal scaling is currently under implementation.

### Limitations

- The number of nodes must be odd to avoid ties during voting
- A cluster of N nodes can tolerate (N-1)/2 failures
- Performance may be limited by the leader (all writes go through the leader)

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
- Number of transactions per ledger

## Next Steps

To deepen your understanding:

1. [Raft Consensus](./raft-consensus.md) - Details on Raft implementation
2. [Ledgers](./buckets-ledgers.md) - Data organization
3. [API and Interfaces](./api.md) - API documentation
4. [Storage and Persistence](./storage.md) - Storage management
