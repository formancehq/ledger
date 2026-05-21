# Problems Solved from Ledger v2

This document summarizes the key problems and limitations encountered in Ledger v2 (`github.com/formancehq/ledger`) that have been addressed in this v3 POC.

## Summary

| Category | v2 Problem | v3 Solution |
|----------|-----------|-------------|
| **Database** | PostgreSQL dependency | Embedded storage (Pebble) |
| **Replication** | PostgreSQL replication complexity | Raft consensus protocol |
| **Deployment** | External database management | Self-contained binary |
| **Build** | External database dependency | Self-contained with pure Go options |
| **Performance** | Network latency to external DB | Local disk writes |
| **Recovery** | Complex database recovery | Unified snapshot + WAL recovery |
| **Observability** | Limited metrics | Full OpenTelemetry + Pyroscope |
| **Contention** | Row-level locks on hot accounts | Append-only balance diffs |

---

## 1. PostgreSQL Dependency Eliminated

### v2 Problem

Ledger v2 required an external PostgreSQL database:

- **Operational complexity**: Needed to provision, configure, and maintain PostgreSQL
- **Network latency**: Every write required a network roundtrip to the database
- **Scaling challenges**: PostgreSQL scaling (read replicas, connection pooling) added complexity
- **Cost**: Managed PostgreSQL services (RDS, Cloud SQL) have significant costs
- **Backup complexity**: Required PostgreSQL-specific backup strategies (pg_dump, WAL archiving)

### v3 Solution

Ledger v3 uses **embedded storage** with no external dependencies. Where v2 required a PostgreSQL connection string, v3 simply points to a local data directory and manages its own storage. See [Architecture Overview](../technical/architecture-overview.md) for implementation details.

**Benefits**:
- Zero external dependencies
- Single binary deployment
- Data stored locally with the application
- Simplified backup (just copy the data directory)

---

## 2. Distributed Consensus with Raft

### v2 Problem

v2 relied on PostgreSQL for data consistency:

- **Single point of failure**: PostgreSQL was the only source of truth
- **Replication complexity**: Setting up PostgreSQL streaming replication or logical replication was complex
- **Failover challenges**: Automatic failover required additional tools (Patroni, pgpool)
- **Split-brain risk**: Without proper configuration, split-brain scenarios were possible

### v3 Solution

v3 implements the **Raft consensus protocol** for distributed consistency:

```mermaid
graph TB
    subgraph "v3 Raft Cluster"
        N1[Node 1 Leader]
        N2[Node 2 Follower]
        N3[Node 3 Follower]
        N1 -.Raft.-> N2
        N2 -.Raft.-> N3
        N3 -.Raft.-> N1
    end
```

**Benefits**:
- **Strong consistency**: All nodes see the same data in the same order
- **Automatic leader election**: No manual failover required
- **Partition tolerance**: Cluster continues with majority of nodes
- **Built-in replication**: No external replication tools needed

---

## 3. Single Raft Group Architecture

### v2 Problem

Managing multiple ledgers in v2 could lead to:

- **Inconsistent operations**: No atomic operations across ledgers
- **Complex sharding**: If sharding was needed, it added significant complexity
- **Multiple replication streams**: Each shard might have its own replication

### v3 Solution

v3 uses a **single Raft group** for all ledgers. The FSM state holds every ledger in one unified structure, so all operations are ordered by a single consensus group. See [Architecture Overview](../technical/architecture-overview.md) for implementation details.

**Benefits**:
- **Simplified operations**: One Raft group to manage
- **Unified snapshots**: Single snapshot contains all ledger states
- **Consistent ordering**: All operations ordered by Raft
- **Reduced overhead**: No coordination between multiple leaders

---

## 4. Pure Go Build Options

### v2 Limitation

v2 with PostgreSQL required an external database server, but the Go binary itself could be built without CGO. However, the overall system still depended on PostgreSQL infrastructure.

### v3 Solution

v3 uses **Pebble** as its embedded storage engine, a high-performance LSM-tree based key-value store from CockroachDB that requires no CGO. The binary can be built with `CGO_ENABLED=0` and runs from a minimal scratch or distroless Docker image.

**Benefits**:
- Easy cross-compilation (Linux, macOS, Windows, ARM)
- Minimal Docker images (scratch/distroless)
- Simplified CI/CD pipelines
- No C compiler dependencies

---

## 5. Improved Write Performance

### v2 Problem

Every write in v2 required:

1. Network roundtrip to PostgreSQL
2. PostgreSQL transaction processing
3. WAL write on PostgreSQL server
4. Network roundtrip for response

**Typical latency**: 1-10ms per write (depending on network)

### v3 Solution

v3 writes directly to local storage:

```
Client → Raft Leader → Local WAL (NVMe/SSD) → Response
```

**Benefits**:
- **Sub-millisecond writes**: Direct disk I/O, no network latency
- **Predictable performance**: No shared database contention
- **Better throughput**: Local storage can handle higher IOPS

> **Recommendation**: Place the WAL directory on fast storage (NVMe/SSD) for optimal performance.

---

## 6. Simplified Recovery and Synchronization

### v2 Problem

Recovery in v2 involved:

- **PostgreSQL point-in-time recovery**: Complex WAL archiving setup
- **Replica synchronization**: Waiting for replicas to catch up
- **Backup restoration**: pg_restore with large databases was slow
- **No application-level visibility**: Recovery was handled entirely by PostgreSQL

### v3 Solution

v3 has **unified application-level recovery**:

```
┌─────────────────────────────────────────────────────────────────┐
│                     Recovery Process                              │
├─────────────────────────────────────────────────────────────────┤
│  1. Load snapshot (FSM state)                                    │
│  2. Replay WAL entries after snapshot                            │
│  3. If behind: sync business logs from leader via gRPC           │
│  4. Replay spooled commands                                      │
│  5. Resume normal operation                                      │
└─────────────────────────────────────────────────────────────────┘
```

**Key mechanisms**:
- **Spool**: Buffers commands during synchronization (no data loss)
- **gRPC streaming**: Efficient log transfer from leader
- **Two-level sync**: Raft snapshots + business log streaming

**Benefits**:
- Fast recovery (snapshot + incremental replay)
- No external tooling required
- Application-level visibility into recovery progress
- Automatic follower catch-up

---

## 7. Enhanced Observability

### v2 Problem

v2 had limited observability:

- **Separate database metrics**: PostgreSQL metrics were separate from application metrics
- **No unified tracing**: Difficult to trace requests through the database
- **Limited profiling**: No continuous profiling built-in

### v3 Solution

v3 has **comprehensive observability**. OpenTelemetry is built in for traces, metrics, and logs -- configure the standard OTLP environment variables to send telemetry to your backend of choice. Optional Pyroscope continuous profiling is available via build tag for CPU, allocation, and goroutine profiling. Pebble storage metrics (compaction, write stalls, cache hit rates, level statistics) are exposed alongside application metrics. See [Architecture Overview](../technical/architecture-overview.md) for implementation details.

**Benefits**:
- End-to-end request tracing
- Unified metrics (application + storage)
- Continuous profiling for performance analysis
- Better debugging and troubleshooting

---

## 8. Optimized Storage Backend

### v2 Problem

v2 was tied to PostgreSQL:

- **No choice**: PostgreSQL was the only option
- **Workload mismatch**: PostgreSQL is general-purpose, not optimized for ledger workloads
- **Overhead**: Full SQL engine for what is essentially key-value storage

### v3 Solution

v3 uses **Pebble**, an LSM-tree storage engine optimized for high-throughput workloads:

**Benefits**:
- High-throughput write performance (LSM-tree optimized for writes)
- No CGO required (pure Go)
- Efficient range scans for balance reconstruction
- Built-in compression and compaction

---

## 9. Simplified API (Pre/Post Commit Volumes Removed)

### v2 Problem

v2 transaction responses included pre- and post-commit volume calculations for every affected account, adding four extra nested objects to every response.

**Issues**:
- **Performance overhead**: Required additional reads to compute volumes
- **Complexity**: Complicated the response structure
- **Raft incompatibility**: Volumes computed at apply time, not propose time

### v3 Solution

v3 **removes pre/post commit volumes** from transaction creation responses. The response contains only the transaction itself. Volumes are available via dedicated read endpoints when needed.

**Benefits**:
- Simpler, faster transaction creation
- No extra database reads during writes
- Cleaner API
- Volumes available via dedicated read endpoints if needed

---

## 10. Better Handling of UUID-based Account Addresses

### v2 Problem

When using UUID v4 as account addresses with PostgreSQL:

- **B-tree fragmentation**: Random UUIDs cause poor B-tree performance
- **Index bloat**: Indexes grow larger than necessary
- **Slow inserts**: Each insert goes to a random position

### v3 Solution

v3 offers **Pebble as an alternative**:

```
LSM-tree (Pebble):
┌─────────────────────────────────────────┐
│ Memtable (RAM) ← All writes go here    │
│      ↓ (background flush)               │
│ SST Files (sorted, compacted)           │
└─────────────────────────────────────────┘
```

**Benefits**:
- Pebble's LSM-tree handles random inserts efficiently
- Writes always go to in-memory memtable (fast)
- Background compaction sorts data
- No B-tree page splits during writes

Pebble's LSM-tree architecture handles UUID v4 account addresses efficiently.

---

## 11. Source Account Contention Eliminated

### v2 Problem

In v2 with PostgreSQL, updating an account balance required a **read-modify-write** cycle: begin a transaction, select the current balance with a row lock, compute the new balance, update the row, and commit. Every concurrent transaction targeting the same account had to wait for the lock.

**Contention issues**:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     v2: Row-Level Lock Contention                            │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Transaction A ──┬── SELECT ... FOR UPDATE (lock acquired) ─── UPDATE ───┐  │
│                  │                                                        │  │
│  Transaction B ──┴── SELECT ... FOR UPDATE (BLOCKED) ────────────────────┘  │
│                                            ↑                                 │
│                                       Waiting for                            │
│                                       lock release                           │
│                                                                              │
│  Transaction C ────────────────────── (BLOCKED) ─────────────────────────   │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Problems**:
- **Lock contention**: Multiple transactions debiting the same account wait in queue
- **Throughput bottleneck**: Hot accounts (e.g., `bank`, `fees`, `treasury`) become bottlenecks
- **Increased latency**: Transactions wait for locks instead of processing
- **Deadlock risk**: Complex transaction patterns could cause deadlocks
- **Connection pool exhaustion**: Waiting transactions hold database connections

This was particularly problematic for:
- **High-volume accounts**: Central bank accounts receiving many payments
- **Fee collection**: Fee accounts debited on every transaction
- **Treasury operations**: Accounts involved in many concurrent operations

### v3 Solution

v3 with Pebble uses **balance diffs** instead of absolute balances, eliminating read-modify-write:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    v3: Append-Only Balance Diffs                             │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Transaction A ──── Append diff: bank/USD/log_1 = -100 ─────────────────┐   │
│                                                                          │   │
│  Transaction B ──── Append diff: bank/USD/log_2 = -50 ──────────────────┤   │
│                                                                          │   │
│  Transaction C ──── Append diff: bank/USD/log_3 = -75 ──────────────────┘   │
│                                                                              │
│                     ↓ All writes happen in parallel ↓                        │
│                                                                              │
│  No locks! Each transaction appends its own diff entry.                      │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

#### How It Works

On the **write path**, each transaction appends a new balance diff entry keyed by ledger, account, asset, and log ID. Multiple transactions debiting the same account write in parallel with no locking -- each simply appends its own diff.

On the **read path**, the current balance is reconstructed by iterating over all diffs for a given account/asset and summing them. Pebble's efficient range scans make this fast even with many entries.

See [Architecture Overview](../technical/architecture-overview.md) for implementation details on the storage key format and balance reconstruction.

#### Performance Comparison

| Metric | v2 (PostgreSQL) | v3 (Pebble diffs) |
|--------|-----------------|-------------------|
| **Write contention** | High (row locks) | None (append-only) |
| **Concurrent writes to same account** | Sequential | Parallel |
| **Write complexity** | O(1) with lock wait | O(1) no wait |
| **Read complexity** | O(1) | O(n) where n = number of diffs |
| **Hot account throughput** | Limited by lock | Unlimited |

#### Trade-offs

**Advantages**:
- **No write contention**: All writes are appends
- **Parallel processing**: Multiple transactions can update the same account simultaneously
- **No deadlocks**: No locks means no deadlock possibility
- **Higher throughput**: Hot accounts don't become bottlenecks

**Considerations**:
- **Read cost**: Balance reads require summing all diffs (mitigated by Pebble's efficient range scans)
- **Storage growth**: Each transaction creates new diff entries (mitigated by compaction)

---

## Migration Considerations

When migrating from v2 to v3, consider:

1. **Data Migration**: Export logs from v2 and import to v3 (import/export feature planned)
2. **API Changes**: Update clients to handle removed `preCommitVolumes`/`postCommitVolumes` fields
3. **Infrastructure**: Remove PostgreSQL from your infrastructure
4. **Cluster Setup**: Configure Raft cluster (odd number of nodes for quorum)
5. **Storage Selection**: Choose appropriate storage backend for your workload

---

## 12. System-Level Atomic Bulk Operations

### v2 Problem

In v2, the bulk API was limited to a single ledger per request:

- **Per-ledger scope**: Bulk operations could only affect one ledger at a time
- **No cross-ledger atomicity in API**: Operations on multiple ledgers required separate requests with no atomicity guarantee
- **Partial failures**: In a multi-ledger scenario, some operations could succeed while others failed

> **Note**: While PostgreSQL supports cross-schema atomic transactions, the v2 API did not expose this capability. The bulk endpoint (`POST /{ledger}/_bulk`) was designed to operate on a single ledger only.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     v2: Per-Ledger Bulk Operations                           │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Request 1: POST /ledger-a/_bulk ──── [Op1, Op2] ──► Ledger A               │
│                                                                              │
│  Request 2: POST /ledger-b/_bulk ──── [Op3, Op4] ──► Ledger B               │
│                                                                              │
│  ⚠️  No atomicity guarantee between Request 1 and Request 2                 │
│  ⚠️  Request 1 could succeed, Request 2 could fail                          │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### v3 Solution

v3 introduces **system-level atomic bulk operations** thanks to the global log architecture:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    v3: System-Level Atomic Bulk                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Single Raft Command:                                                       │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │ [Op1: Ledger A] [Op2: Ledger B] [Op3: Ledger A] [Op4: Ledger C]    │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                         ↓                                                    │
│                  Single Raft Entry                                           │
│                         ↓                                                    │
│                  All-or-Nothing                                              │
│                                                                              │
│  ✅  Either ALL operations succeed, or NONE are applied                     │
│  ✅  Cross-ledger atomicity guaranteed                                      │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

#### How It Works

1. **Single Raft Group**: All ledgers are managed by a single Raft group
2. **Global Log**: All operations are ordered in a global sequence
3. **Multi-Action Commands**: A single Raft command can contain actions on multiple ledgers
4. **Atomic FSM Application**: The FSM applies all actions atomically

#### Usage

Via **gRPC**, the `Apply` call accepts multiple actions targeting different ledgers in a single request -- all actions succeed or all fail together. Via **HTTP**, the bulk endpoint supports an `atomic=true` query parameter that wraps all operations in the request into a single Raft command, even when individual operations target different ledgers. See [Architecture Overview](../technical/architecture-overview.md) for implementation details.

**Benefits**:
- **Cross-ledger transfers**: Atomic transfers between accounts in different ledgers
- **Consistent state**: System is always in a consistent state
- **Simplified error handling**: No need to handle partial failures across ledgers
- **Audit trail**: Single global sequence for all operations

See [Global Log Architecture](../technical/architecture/core/global-log.md) for detailed documentation on the two-level log architecture that enables this feature.

---

## Conclusion

Ledger v3 addresses the fundamental architectural challenges of v2 by:

1. **Eliminating external dependencies** (PostgreSQL → embedded storage)
2. **Implementing native distributed consensus** (PostgreSQL replication → Raft)
3. **Providing deployment flexibility** (pure Go builds, Pebble storage)
4. **Improving performance** (local writes, optimized storage engines)
5. **Enhancing observability** (OpenTelemetry, Pyroscope)
6. **Eliminating write contention** (row locks → append-only balance diffs)

The result is a simpler, more performant, and more operationally friendly ledger system that scales better under high concurrency.
