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
        BucketRaft1[Bucket Raft Group 1]
        BucketRaft2[Bucket Raft Group 2]
        FSM[Finite State Machine]
        Storage[(Storage)]
    end
    
    HTTPClient --> HTTPServer
    GRPCClient --> GRPCServer
    CLIClient --> HTTPServer
    
    HTTPServer --> SystemRaft
    GRPCServer --> SystemRaft
    GRPCServer --> BucketRaft1
    GRPCServer --> BucketRaft2
    
    SystemRaft --> FSM
    BucketRaft1 --> FSM
    BucketRaft2 --> FSM
    
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
- **System Raft Group**: Main Raft group managing buckets
- **Bucket Raft Groups**: One Raft group per bucket to manage ledgers
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
        BucketCluster[BucketCluster]
        LedgerService[Ledger Service]
    end
    
    subgraph "Raft Layer"
        SystemNode[System Node]
        BucketNode[Bucket Node]
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
    MasterCluster --> BucketNode
    BucketCluster --> BucketNode
    LedgerService --> BucketNode
    SystemNode --> Transport
    BucketNode --> Transport
    SystemNode --> WAL
    BucketNode --> WAL
    BucketNode --> LogStore
    SystemNode --> Snapshot
    BucketNode --> Snapshot
```

## Multi-Level Raft Architecture

The system uses a two-level Raft group architecture:

### Level 1: System Raft Group

The system Raft group manages bucket creation and deletion. Every node participates in this group.

**Responsibilities**:
- Create/delete buckets
- Manage the bucket list
- Coordinate bucket Raft groups

### Level 2: Bucket Raft Groups

Each bucket has its own independent Raft group to manage:
- Create/delete ledgers in the bucket
- Insert logs (transactions)
- Synchronize bucket data

**Isolation**: Bucket Raft groups are completely isolated from each other. A problem in one bucket does not affect others.

```mermaid
graph TB
    subgraph "System Raft Group"
        S1[Node 1 Leader]
        S2[Node 2 Follower]
        S3[Node 3 Follower]
    end
    
    subgraph "Bucket A Raft Group"
        BA1[Node 1 Leader]
        BA2[Node 2 Follower]
        BA3[Node 3 Follower]
    end
    
    subgraph "Bucket B Raft Group"
        BB1[Node 1 Follower]
        BB2[Node 2 Leader]
        BB3[Node 3 Follower]
    end
    
    S1 -.Manages.-> BA1
    S1 -.Manages.-> BB1
    S2 -.Manages.-> BA2
    S2 -.Manages.-> BB2
    S3 -.Manages.-> BA3
    S3 -.Manages.-> BB3
```

## Data Flows

### Bucket Creation

```mermaid
sequenceDiagram
    participant Client
    participant HTTP
    participant SystemNode
    participant SystemFSM
    participant BucketNode
    participant Storage
    
    Client->>HTTP: POST /buckets/{name}
    HTTP->>SystemNode: CreateBucket()
    SystemNode->>SystemFSM: Propose Command
    SystemFSM->>SystemFSM: Validate & Assign ID
    SystemFSM->>BucketNode: Start Bucket Raft Group
    BucketNode->>Storage: Initialize Storage
    SystemFSM->>Storage: Persist Bucket Info
    SystemNode-->>HTTP: BucketInfo
    HTTP-->>Client: 201 Created
```

### Transaction Creation

```mermaid
sequenceDiagram
    participant Client
    participant HTTP
    participant BucketNode
    participant BucketFSM
    participant LogStore
    participant Storage
    
    Client->>HTTP: POST /{ledger}/transactions
    HTTP->>BucketNode: CreateTransaction()
    BucketNode->>BucketFSM: Propose InsertLog Command
    BucketFSM->>BucketFSM: Generate Sequence
    BucketFSM->>LogStore: Write Log
    BucketFSM->>Storage: Persist Log
    BucketNode-->>HTTP: CreatedTransaction
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

### Bucket Isolation

- Each bucket has its own Raft group
- Bucket data is stored separately
- A bucket can use a different storage driver (SQLite)
- Problems in one bucket do not affect others

### Data Isolation

- Transaction logs are stored in the bucket-specific LogStore
- Snapshots are created per bucket
- Recovery is done bucket by bucket

## Scalability

### Horizontal Scaling

The system can be scaled horizontally by adding nodes to the cluster:

- New nodes join the system Raft group
- They automatically participate in existing bucket Raft groups
- Load is distributed across all nodes

### Limitations

- The number of nodes must be odd to avoid ties during voting
- A cluster of N nodes can tolerate (N-1)/2 failures
- Performance may be limited by the leader (all writes go through the leader)

## Observability

### Logging

The system uses structured logging with contextual fields:
- Node ID
- Bucket name
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
- Number of buckets
- Number of ledgers per bucket
- Number of transactions per ledger

## Next Steps

To deepen your understanding:

1. [Raft Consensus](./raft-consensus.md) - Details on Raft implementation
2. [Buckets and Ledgers](./buckets-ledgers.md) - Data organization
3. [API and Interfaces](./api.md) - API documentation
4. [Storage and Persistence](./storage.md) - Storage management

