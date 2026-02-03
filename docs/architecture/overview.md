# Architecture Overview

This document provides a high-level overview of the Ledger v3 POC architecture, including component interactions and package dependencies.

## System Architecture

The Ledger v3 POC is a distributed ledger system built on Raft consensus. It provides strong consistency guarantees for financial transactions across a cluster of nodes.

```mermaid
flowchart TB
    subgraph Clients
        HTTP[HTTP Client]
        GRPC[gRPC Client]
        CLI[CLI ledgerctl]
    end

    subgraph API["API Layer"]
        HTTPServer[HTTP Server<br/>internal/compat/http]
        GRPCServer[gRPC Server<br/>internal/application]
    end

    subgraph Control["Control Layer"]
        Controller[Controller<br/>internal/ctrl]
        RoutedCtrl[Routed Controller<br/>internal/application]
        Admission[Admission<br/>internal/service/admission]
    end

    subgraph Consensus["Consensus Layer"]
        Node[Raft Node<br/>internal/service/node]
        Transport[Transport<br/>internal/service/transport]
    end

    subgraph State["State Management"]
        FSM[State Machine<br/>internal/service/state]
        Processing[Request Processor<br/>internal/service/processing]
        Cache[Cache<br/>internal/service/cache]
        Attributes[Attributes<br/>internal/service/attributes]
    end

    subgraph Storage["Storage Layer"]
        Store[Pebble Store<br/>internal/storage/data]
        WAL[Write-Ahead Log<br/>internal/storage/wal]
        Spool[Spool<br/>internal/storage/spool]
    end

    HTTP --> HTTPServer
    GRPC --> GRPCServer
    CLI --> GRPCServer

    HTTPServer --> Controller
    GRPCServer --> Controller

    Controller --> RoutedCtrl
    RoutedCtrl -->|Writes| Admission
    RoutedCtrl -->|Reads| Store

    Admission --> Node
    Node <--> Transport
    Transport <-->|Raft Messages| OtherNodes[Other Nodes]

    Node --> FSM
    FSM --> Processing
    FSM --> Cache
    FSM --> Attributes
    FSM --> Store

    Processing --> Attributes

    Node --> WAL
    Node --> Spool
    Spool --> Store

    Cache --> Attributes
```

## Component Interactions

### Request Flow (Write Path)

```mermaid
sequenceDiagram
    participant C as Client
    participant API as gRPC/HTTP Server
    participant Ctrl as Controller
    participant Adm as Admission
    participant Node as Raft Node
    participant FSM as State Machine
    participant Store as Pebble Store

    C->>API: Apply Request
    API->>Ctrl: Apply()
    Ctrl->>Adm: Admit()
    
    Note over Adm: Preload volumes from cache/store
    
    Adm->>Node: Propose()
    Node->>Node: Replicate via Raft
    
    Note over Node: Wait for majority consensus
    
    Node->>FSM: Apply(entries)
    FSM->>FSM: Process command
    FSM->>Store: Commit batch
    
    FSM-->>Node: Result
    Node-->>Adm: Future resolved
    Adm-->>Ctrl: Log entries
    Ctrl-->>API: Response
    API-->>C: Success
```

### Request Flow (Read Path)

```mermaid
sequenceDiagram
    participant C as Client
    participant API as gRPC/HTTP Server
    participant Ctrl as Controller
    participant Store as Pebble Store
    participant Attrs as Attributes

    C->>API: GetAccount
    API->>Ctrl: GetAccount()
    Ctrl->>Store: Read from store
    Store->>Attrs: List mapping entries
    Attrs->>Store: Compute values
    Store-->>Ctrl: Account data
    Ctrl-->>API: Response
    API-->>C: Account
```

## Package Dependencies

```mermaid
graph TB
    subgraph External["External Dependencies"]
        etcd[etcd/raft]
        pebble[cockroachdb/pebble]
        proto[protobuf/grpc]
        fx[uber/fx]
    end

    subgraph Application["internal/application"]
        app_module[module.go]
        app_grpc[grpc_*_server.go]
        app_ctrl[controller_routed.go]
    end

    subgraph Compat["internal/compat"]
        http_handler[http/handler.go]
        http_server[http/server.go]
    end

    subgraph Ctrl["internal/ctrl"]
        ctrl_iface[controller.go]
        ctrl_default[controller_default.go]
        ctrl_store[store.go]
    end

    subgraph Node["internal/service/node"]
        node_main[node.go]
        node_transport[transport.go]
        node_queue[queue.go]
    end

    subgraph State["internal/service/state"]
        state_machine[machine.go]
        state_buffer[buffer.go]
        state_snapshot[snapshot.go]
    end

    subgraph Processing["internal/service/processing"]
        proc_main[processor.go]
        proc_cache[numscript_cache.go]
    end

    subgraph Admission["internal/service/admission"]
        adm_main[admission.go]
        adm_loader[loader.go]
    end

    subgraph Cache["internal/service/cache"]
        cache_main[cache.go]
        cache_gen[generation.go]
    end

    subgraph Attributes["internal/service/attributes"]
        attr_main[attributes.go]
        attr_keystore[key_store.go]
        attr_u128[u128.go]
    end

    subgraph Storage["internal/storage"]
        data_store[data/store.go]
        data_batch[data/batch.go]
        wal_main[wal/wal.go]
        spool_main[spool/spool.go]
    end

    subgraph Proto["internal/proto"]
        commonpb[commonpb/]
        servicepb[servicepb/]
        raftcmdpb[raftcmdpb/]
        clusterpb[clusterpb/]
    end

    %% External dependencies
    node_main --> etcd
    data_store --> pebble
    app_grpc --> proto
    app_module --> fx

    %% Application layer
    app_module --> app_grpc
    app_module --> app_ctrl
    app_module --> http_handler
    app_ctrl --> ctrl_iface

    %% HTTP layer
    http_handler --> ctrl_iface
    http_server --> http_handler

    %% Controller layer
    ctrl_default --> ctrl_store
    ctrl_store --> attr_main
    ctrl_default --> data_store

    %% Node layer
    node_main --> state_machine
    node_main --> node_transport
    node_main --> wal_main
    node_main --> spool_main
    node_main --> cache_main

    %% State layer
    state_machine --> state_buffer
    state_machine --> proc_main
    state_machine --> cache_main
    state_machine --> attr_keystore
    state_buffer --> attr_main

    %% Processing layer
    proc_main --> attr_keystore
    proc_main --> proc_cache

    %% Admission layer
    adm_main --> adm_loader
    adm_main --> cache_main
    adm_main --> node_main

    %% Cache layer
    cache_main --> cache_gen
    cache_main --> attr_keystore

    %% Attributes layer
    attr_main --> attr_keystore
    attr_keystore --> attr_u128

    %% Storage layer
    data_store --> data_batch
    wal_main --> data_store
    spool_main --> data_store

    %% Proto dependencies
    state_machine --> commonpb
    state_machine --> raftcmdpb
    app_grpc --> servicepb
    app_grpc --> clusterpb
```

## Entity Relationship Diagram

```mermaid
erDiagram
    LEDGER ||--o{ TRANSACTION : contains
    LEDGER ||--o{ ACCOUNT : has
    LEDGER {
        uint32 id PK
        string name UK
        timestamp created_at
        metadata config
    }

    TRANSACTION ||--|{ POSTING : contains
    TRANSACTION {
        uint64 id PK
        uint64 ledger_id FK
        timestamp timestamp
        string reference
        metadata metadata
        bool reverted
    }

    POSTING {
        string source FK
        string destination FK
        string asset
        bigint amount
    }

    ACCOUNT ||--o{ VOLUME : has
    ACCOUNT {
        string address PK
        string ledger_name FK
        metadata metadata
    }

    VOLUME {
        string account_address FK
        string asset
        bigint input
        bigint output
        bigint balance
    }

    LOG ||--|| TRANSACTION : creates
    LOG ||--|| ACCOUNT : modifies
    LOG {
        uint64 id PK
        uint64 ledger_id FK
        timestamp date
        logtype type
        bytes data
    }

    RAFT_ENTRY ||--o{ LOG : contains
    RAFT_ENTRY {
        uint64 index PK
        uint64 term
        bytes data
    }
```

## Key Components

### 1. API Layer (`internal/application`, `internal/compat/http`)

- **gRPC Server**: Primary API for client interactions, supports Apply, GetLedger, GetAccount, GetTransaction
- **HTTP Server**: REST compatibility layer for legacy clients
- **Routed Controller**: Routes requests to leader node for writes, serves reads locally

### 2. Control Layer (`internal/ctrl`, `internal/service/admission`)

- **Controller**: Interface defining read and write operations
- **DefaultController**: Local implementation reading from Pebble store
- **Admission**: Handles write request admission, preloads volumes, coordinates with Raft

### 3. Consensus Layer (`internal/service/node`, `internal/service/transport`)

- **Raft Node**: Wraps etcd/raft, manages consensus, applies committed entries
- **Transport**: gRPC-based message transport between cluster nodes
- **Connection Pool**: Manages persistent gRPC connections to peers

### 4. State Management (`internal/service/state`, `internal/service/processing`)

- **State Machine (FSM)**: Deterministic state machine, processes Raft log entries
- **Request Processor**: Business logic for transactions, Numscript execution
- **Buffer**: Accumulates changes during command processing before commit

### 5. Caching Layer (`internal/service/cache`, `internal/service/attributes`)

- **Cache**: Dual-generation cache for hot data, rotates based on Raft index
- **Attributes**: Generic attribute system for volumes, metadata, reversions
- **KeyStore**: Hash-based key mapping with collision detection

### 6. Storage Layer (`internal/storage/data`, `internal/storage/wal`, `internal/storage/spool`)

- **Pebble Store**: Persistent key-value storage for all state
- **WAL (Write-Ahead Log)**: Durability for Raft entries before FSM apply
- **Spool**: Committed entry buffer during FSM synchronization

## Data Flow Summary

| Operation | Path | Consensus Required |
|-----------|------|-------------------|
| Create Ledger | Client → API → Admission → Raft → FSM → Store | Yes |
| Create Transaction | Client → API → Admission → Raft → FSM → Store | Yes |
| Get Account | Client → API → Controller → Store | No (local read) |
| Get Transaction | Client → API → Controller → Store | No (local read) |
| Revert Transaction | Client → API → Admission → Raft → FSM → Store | Yes |
| Save Metadata | Client → API → Admission → Raft → FSM → Store | Yes |

## Deployment Topology

```mermaid
graph TB
    subgraph Cluster["Raft Cluster (3+ nodes)"]
        subgraph Node1["Node 1 (Leader)"]
            API1[gRPC/HTTP API]
            Raft1[Raft]
            Store1[Pebble]
        end
        
        subgraph Node2["Node 2 (Follower)"]
            API2[gRPC/HTTP API]
            Raft2[Raft]
            Store2[Pebble]
        end
        
        subgraph Node3["Node 3 (Follower)"]
            API3[gRPC/HTTP API]
            Raft3[Raft]
            Store3[Pebble]
        end
    end

    LB[Load Balancer]
    
    Client[Client] --> LB
    LB --> API1
    LB --> API2
    LB --> API3

    Raft1 <--> Raft2
    Raft1 <--> Raft3
    Raft2 <--> Raft3
```

## See Also

- [Raft Consensus](./raft-consensus.md) - Detailed Raft implementation
- [Deterministic FSM](./deterministic-fsm.md) - State machine design
- [Attributes](./attributes.md) - Attribute storage system
- [Storage](./storage.md) - Persistence architecture
- [gRPC API](./grpc-api.md) - API documentation
