# Global Log vs Ledger Log

This document explains the two-level log architecture in Ledger v3 and how it differs from v2, enabling system-level atomic operations.

## Overview

Ledger v3 introduces a **two-level log architecture**:

1. **Global Log (System Log)**: A single, ordered log across the entire system with a global `sequence` number
2. **Ledger Log (Per-Ledger Log)**: A log specific to each ledger with a ledger-scoped `id` number

This architecture is a fundamental departure from v2 and enables powerful features like **system-level atomic bulk operations**.

## Log Structure

### Global Log (System Log)

The global log captures all system-wide events in a single ordered sequence:

```protobuf
message Log {
  fixed64 sequence = 1;                   // Global sequence number (system-wide)
  LogPayload payload = 2;                 // Payload containing the log data
  Idempotency idempotency = 3;            // Idempotency information
}

message LogPayload {
  oneof type {
    CreatedLedgerLog create_ledger = 1;    // Ledger creation
    DeletedLedgerLog delete_ledger = 2;    // Ledger deletion
    ApplyLedgerLog apply = 3;                   // Ledger-level log entry
  }
}
```

The `sequence` is a **monotonically increasing counter** that spans the entire system, regardless of which ledger the operation affects.

### Ledger Log (Per-Ledger Log)

Each ledger maintains its own log with entries specific to that ledger:

```protobuf
message LedgerLog {
  LedgerLogPayload data = 1;                    // Log payload (transaction, metadata, etc.)
  Timestamp date = 2;                     // Log date
  fixed64 id = 3;                         // Per-ledger log ID
}

message LedgerLogPayload {
  oneof payload {
    CreatedTransaction created_transaction = 1;
    RevertedTransaction reverted_transaction = 2;
    SavedMetadata saved_metadata = 3;
    DeletedMetadata deleted_metadata = 4;
  }
}
```

The `id` is scoped to the ledger and represents the chronological order of operations within that specific ledger.

### Relationship

The `ApplyLedgerLog` message links the two levels:

```protobuf
message ApplyLedgerLog {
  string ledger_name = 1;                  // Target ledger name
  LedgerLog log = 2;                       // Ledger-level log entry
}
```

## Visual Representation

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          GLOBAL LOG (System-wide)                           │
├─────────────────────────────────────────────────────────────────────────────┤
│ seq=1  │ CreateLedger(ledger_a)                                             │
│ seq=2  │ Apply(ledger_a, id=1, CreateTransaction)                           │
│ seq=3  │ CreateLedger(ledger_b)                                             │
│ seq=4  │ Apply(ledger_a, id=2, CreateTransaction)                           │
│ seq=5  │ Apply(ledger_b, id=1, CreateTransaction)     ← Same global seq     │
│ seq=6  │ Apply(ledger_a, id=3, SaveMetadata)              different ledgers │
│ seq=7  │ Apply(ledger_b, id=2, CreateTransaction)                           │
│ seq=8  │ DeleteLedger(ledger_a)                                             │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────┐    ┌─────────────────────────────────┐
│     LEDGER A LOG (id scope)     │    │     LEDGER B LOG (id scope)     │
├─────────────────────────────────┤    ├─────────────────────────────────┤
│ id=1 │ CreateTransaction (seq=2)│    │ id=1 │ CreateTransaction (seq=5)│
│ id=2 │ CreateTransaction (seq=4)│    │ id=2 │ CreateTransaction (seq=7)│
│ id=3 │ SaveMetadata (seq=6)     │    │                                 │
└─────────────────────────────────┘    └─────────────────────────────────┘
```

## Comparison with v2

### v2 Architecture: Per-Ledger Logs Only

In Ledger v2, each ledger had its own independent log stored in PostgreSQL:

```
┌─────────────────────────────────┐    ┌─────────────────────────────────┐
│     LEDGER A LOG (PostgreSQL)   │    │     LEDGER B LOG (PostgreSQL)   │
├─────────────────────────────────┤    ├─────────────────────────────────┤
│ id=1 │ CreateTransaction        │    │ id=1 │ CreateTransaction        │
│ id=2 │ CreateTransaction        │    │ id=2 │ SaveMetadata             │
│ id=3 │ SaveMetadata             │    │ id=3 │ CreateTransaction        │
└─────────────────────────────────┘    └─────────────────────────────────┘
         Independent                            Independent
         No global ordering                     No global ordering
```

**v2 Limitations**:

| Aspect | v2 | v3 |
|--------|----|----|
| **Log scope** | Per-ledger only | Global + Per-ledger |
| **Global ordering** | None | Yes (sequence number) |
| **Cross-ledger atomicity (API)** | Not exposed | Supported via bulk |
| **System-wide consistency** | Per-ledger only | System-wide |
| **Bulk operations** | Per-ledger only | System-level |
| **Replay/Recovery** | Per-ledger | Unified system replay |

> **Note**: PostgreSQL supports cross-schema atomic transactions, but the v2 API only exposed bulk operations at the ledger level. The bulk endpoint (`POST /{ledger}/_bulk`) could only operate on a single ledger per request.

### v3 Architecture: Single Raft Group with Global Log

In v3, a **single Raft group** manages all operations:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           SINGLE RAFT GROUP                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌────────────────┐                                                         │
│  │  Raft Leader   │  ← All writes go through a single consensus             │
│  └────────────────┘                                                         │
│           │                                                                  │
│           ▼                                                                  │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                         GLOBAL LOG                                    │   │
│  │  [seq=1: CreateLedger A] [seq=2: Tx A] [seq=3: CreateLedger B] ...  │   │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

## System-Level Atomic Bulk Operations

The global log architecture enables a powerful feature: **atomic bulk operations across multiple ledgers**.

### How It Works

In v3, a single **Raft proposal** can contain multiple **orders**:

```protobuf
message Proposal {
  fixed64 id = 1;                   // Random proposal ID
  repeated Order orders = 2;        // List of orders to execute atomically
  Timestamp date = 3;               // Creation date in UTC
  ExecutionPlan preload = 4;           // Preloaded attributes for deterministic execution
  // ... other fields ...
  repeated MetadataConversionBatch metadata_conversion_batches = 10;   // Background metadata conversion (no log entry)
  repeated MetadataConversionCompletion metadata_conversions_complete = 11; // Conversion complete signals (no log entry)
  repeated IndexReadyUpdate index_ready_updates = 12;                  // Index ready signals (no log entry)
}

message Order {
  Idempotency idempotency = 1;
  oneof type {
    LedgerApplyOrder apply = 2;         // Ledger operation (transaction, metadata)
    CreateLedgerOrder create_ledger = 3;
    DeleteLedgerOrder delete_ledger = 4;
  }
}
```

> **Note**: Technical operations like metadata conversion and index readiness are modeled as direct `Proposal` fields rather than orders. They are processed by the FSM but do not produce log entries.

Each order can target a different ledger:

```go
// A single proposal with orders on multiple ledgers
proposal := &raftcmdpb.Proposal{
    Orders: []*raftcmdpb.Order{
        createLedgerOrder("ledger-a"),        // Order on ledger A
        createTransactionOrder("ledger-b"),   // Order on ledger B
        createTransactionOrder("ledger-a"),   // Another order on ledger A
    },
}
```

### Atomic Execution

Because all orders in a proposal are applied atomically in the FSM:

1. **All-or-nothing**: Either all orders succeed, or none are applied
2. **Single Raft entry**: The entire proposal is a single Raft log entry
3. **Single sequence range**: All resulting logs get consecutive global sequences
4. **Consistent state**: The system state is always consistent

### Bulk via gRPC (Atomic Mode)

The HTTP bulk endpoint and gRPC service support atomic bulk operations:

```go
// In handlers_bulk.go
func (s *Server) runBulkAtomic(ctx context.Context, requests []*servicepb.Request) []bulkResult {
    results := make([]bulkResult, len(requests))

    // All requests are sent in a single Apply() call → single Raft proposal
    resp, err := s.backend.Apply(ctx, &servicepb.ApplyRequest{Envelopes: servicepb.UnsignedEnvelopes(requests...)})
    if err != nil {
        // In atomic mode, if any order fails, all fail with the same error
        for i := range results {
            results[i] = bulkResult{err: err}
        }
        return results
    }

    for i, log := range resp.Logs {
        results[i] = bulkResult{log: log}
    }
    return results
}
```

### Example: Cross-Ledger Transfer

Consider a bulk operation that transfers money between accounts in different ledgers:

```json
POST /ledger-a/_bulk?atomic=true

[
  {"action": "CREATE_TRANSACTION", "data": {"postings": [{"source": "bank", "destination": "user:123", "amount": 100, "asset": "USD"}]}},
  {"action": "CREATE_TRANSACTION", "ledger": "ledger-b", "data": {"postings": [{"source": "user:123", "destination": "merchant", "amount": 100, "asset": "USD"}]}}
]
```

In v2, this would require two separate operations with no atomicity guarantee. In v3, both transactions are applied atomically as a single Raft entry.

### Consistency Guarantees

The global log provides:

| Guarantee | Description |
|-----------|-------------|
| **Atomicity** | All operations in a bulk succeed or fail together |
| **Ordering** | Operations have a deterministic global order |
| **Durability** | Once committed via Raft, operations are persisted |
| **Consistency** | System state is always consistent across all ledgers |

## Benefits Summary

### For Operations

1. **Unified Recovery**: Single point of recovery for the entire system
2. **Consistent Snapshots**: Snapshots capture the entire system state
3. **Simpler Replication**: One Raft group to manage instead of N+1

### For Applications

1. **Cross-Ledger Atomicity**: Atomic operations spanning multiple ledgers
2. **Global Ordering**: Deterministic ordering of all system events
3. **System-Wide Consistency**: All ledgers are always in a consistent state

### For Bulk Operations

| Mode | v2 Behavior | v3 Behavior |
|------|-------------|-------------|
| **Sequential** | Per-ledger, no global order | Global sequence order |
| **Atomic** | Per-ledger only | System-level atomicity |
| **Cross-ledger** | Not supported | Fully supported |

## Implementation Details

### FSM Processing

The FSM processes all orders in a proposal atomically:

```go
// In internal/infra/state/machine.go
func (fsm *Machine) applyProposal(ctx context.Context, raftIndex uint64, batch *data.Batch, proposal *raftcmdpb.Proposal) (*ApplyResult, error) {
    var logs []*commonpb.Log

    // Process ALL orders atomically
    for _, order := range proposal.Orders {
        log, err := fsm.applyOrder(ctx, batch, order, proposal.Date)
        if err != nil {
            return nil, err  // Entire proposal fails if any order fails
        }
        if log != nil {
            logs = append(logs, log)
        }
    }

    return &ApplyResult{
        Logs:       logs,
        ProposalID: proposal.Id,
    }, nil
}
```

### Global Sequence Assignment

Each log entry gets a unique global sequence:

```go
func (fsm *FSM) getNextSequence() uint64 {
    seq := fsm.state.NextSequence
    fsm.state.NextSequence++
    return seq
}
```

This sequence is assigned at apply time, ensuring strict ordering.

## Next Steps

- [Architecture](./architecture.md) - Overall system architecture
- [Data Flows](../data-model/data-flows.md) - Detailed data flow diagrams
- [Raft Consensus](./raft-consensus.md) - How Raft ensures consistency
- [v2 Problems Solved](../../../sales/v2-vs-v3.md) - Other improvements over v2
