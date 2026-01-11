# General Architecture

## Overview

Ledger v3 POC is a distributed accounting ledger system using the Raft consensus protocol to ensure data consistency across a cluster of nodes. The system is designed to be highly available, fault-tolerant, and scalable.

## High-Level Architecture

```mermaid
graph TB
    subgraph "Client Applications"
        HTTPClient[HTTP Client]
        GRPCClient[gRPC Client]
        CLIClient[CLI Client]
    end
    
    subgraph "Node 1"
        HTTPServer1[HTTP Server<br/>Port 9000]
        GRPCServer1[gRPC Server<br/>Port 8888]
        MasterCluster1[MasterCluster<br/>Adapter]
        SystemNode1[System Raft Node]
        LedgerNode1A[Ledger Raft Node A]
        LedgerNode1B[Ledger Raft Node B]
    end
    
    subgraph "Node 2"
        HTTPServer2[HTTP Server<br/>Port 9000]
        GRPCServer2[gRPC Server<br/>Port 8888]
        MasterCluster2[MasterCluster<br/>Adapter]
        SystemNode2[System Raft Node]
        LedgerNode2A[Ledger Raft Node A]
        LedgerNode2B[Ledger Raft Node B]
    end
    
    subgraph "Node 3"
        HTTPServer3[HTTP Server<br/>Port 9000]
        GRPCServer3[gRPC Server<br/>Port 8888]
        MasterCluster3[MasterCluster<br/>Adapter]
        SystemNode3[System Raft Node]
        LedgerNode3A[Ledger Raft Node A]
        LedgerNode3B[Ledger Raft Node B]
    end
    
    HTTPClient --> HTTPServer1
    GRPCClient --> GRPCServer1
    CLIClient --> HTTPServer1
    
    HTTPServer1 --> MasterCluster1
    HTTPServer2 --> MasterCluster2
    HTTPServer3 --> MasterCluster3
    
    MasterCluster1 --> SystemNode1
    MasterCluster1 --> LedgerNode1A
    MasterCluster1 --> LedgerNode1B
    
    GRPCServer1 --> SystemNode1
    GRPCServer1 --> LedgerNode1A
    GRPCServer1 --> LedgerNode1B
    
    SystemNode1 -.Raft.-> SystemNode2
    SystemNode2 -.Raft.-> SystemNode3
    SystemNode3 -.Raft.-> SystemNode1
    
    LedgerNode1A -.Raft.-> LedgerNode2A
    LedgerNode2A -.Raft.-> LedgerNode3A
    LedgerNode3A -.Raft.-> LedgerNode1A
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
graph TB
    subgraph "API Layer"
        HTTP[HTTP Handlers]
        GRPC[gRPC Server<br/>SystemService + LedgerService + RaftTransport]
    end
    
    subgraph "Service Layer"
        MasterCluster[MasterCluster<br/>Adapter]
        LedgerCluster[LedgerCluster<br/>Adapter]
    end
    
    subgraph "Raft Layer"
        SystemNode[System Raft Node]
        LedgerNode[Ledger Raft Node]
        Transport[gRPC Transport<br/>Multiplexed]
    end
    
    subgraph "Storage Layer"
        WAL[WAL Storage]
        RuntimeStore[Runtime Store<br/>Logs + Balances]
        Snapshot[Snapshot Store]
    end
    
    HTTP --> MasterCluster
    GRPC --> MasterCluster
    MasterCluster --> SystemNode
    MasterCluster --> LedgerCluster
    LedgerCluster --> LedgerNode
    
    SystemNode --> Transport
    LedgerNode --> Transport
    
    SystemNode --> WAL
    LedgerNode --> WAL
    LedgerNode --> RuntimeStore
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
    
    S1 -.Creates.-> LA1
    S1 -.Creates.-> LB1
    Note over S1,LB1: System FSM creates ledger groups<br/>Groups then operate independently
    
    LA1 -.Raft.-> LA2
    LA2 -.Raft.-> LA3
    LA3 -.Raft.-> LA1
    
    LB1 -.Raft.-> LB2
    LB2 -.Raft.-> LB3
    LB3 -.Raft.-> LB1
```

## Data Flows

### Ledger Creation

```mermaid
sequenceDiagram
    participant Client
    participant HTTP as HTTP Handler
    participant MasterCluster as MasterCluster Adapter
    participant SystemNode as System Raft Node
    participant SystemFSM as System FSM
    participant LedgerNode as Ledger Raft Node
    participant Storage as Storage
    
    Client->>HTTP: POST /ledgers/{name}
    HTTP->>MasterCluster: CreateLedger()
    
    alt Node is leader
        MasterCluster->>SystemNode: CreateLedger()
        SystemNode->>SystemFSM: Propose CreateLedgerCommand (via Raft)
        SystemFSM->>SystemFSM: Validate & Assign ID
        SystemFSM->>LedgerNode: Start Ledger Raft Group
        LedgerNode->>Storage: Initialize Storage
        SystemFSM->>SystemFSM: Store Ledger Info
        SystemNode-->>MasterCluster: LedgerInfo
    else Node is follower
        MasterCluster->>MasterCluster: Find leader
        MasterCluster->>SystemNode: Forward via gRPC (to leader)
        Note over SystemNode,Storage: Same as leader path
        SystemNode-->>MasterCluster: LedgerInfo
    end
    
    MasterCluster-->>HTTP: LedgerInfo
    HTTP-->>Client: 201 Created
```

### Transaction Creation

```mermaid
sequenceDiagram
    participant Client
    participant HTTP as HTTP Handler
    participant MasterCluster as MasterCluster Adapter
    participant LedgerCluster as LedgerCluster Adapter
    participant LedgerNode as Ledger Raft Node
    participant LedgerFSM as Ledger FSM
    participant RuntimeStore as Runtime Store
    
    Client->>HTTP: POST /ledgers/{name}/transactions
    HTTP->>MasterCluster: GetLedgerCluster(name)
    MasterCluster->>LedgerCluster: Get Ledger Cluster
    HTTP->>LedgerCluster: CreateTransaction()
    
    alt Node is leader
        LedgerCluster->>LedgerNode: CreateTransaction()
        LedgerNode->>LedgerFSM: Propose InsertLogCommand (via Raft)
        LedgerFSM->>LedgerFSM: Generate Sequence
        LedgerFSM->>RuntimeStore: InsertLogs() - Persist log and update balances
        Note over LedgerFSM: Logs persisted during apply
        LedgerNode-->>LedgerCluster: CreatedTransaction
    else Node is follower
        LedgerCluster->>LedgerCluster: Find leader
        LedgerCluster->>LedgerNode: Forward via gRPC (to leader)
        Note over LedgerNode,RuntimeStore: Same as leader path
        LedgerNode-->>LedgerCluster: CreatedTransaction
    end
    
    LedgerCluster-->>HTTP: CreatedTransaction
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

- Transaction logs are stored in the ledger-specific RuntimeStore
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
