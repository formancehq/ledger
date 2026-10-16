# Ledger v3 — Product Overview

| Metric | Value |
|--------|-------|
| **Throughput** | ~106,000 tx/s average¹ |
| **Latency p99** | < 75 ms¹ |
| **High Availability** | 3-node Raft cluster |
| **Deployment** | Single binary |
| **Implementation** | Pure Go, zero CGO |

¹ Staging load test of 2026-01-29: 3 nodes (8 cores, 4 GiB each), k6 bulk requests of 50 transactions in atomic mode, unbounded overdraft, 7-minute run. See [benchmarks.md](./benchmarks.md) for the full conditions.

## Why v3

Ledger v2 depended on PostgreSQL for storage, which meant every deployment needed a managed database, connection pooling, replication configuration, and a DBA to keep it all running. Scaling required read replicas, and failover required external tools like Patroni. The database was the bottleneck, the single point of failure, and the largest line item on the infrastructure bill.

Ledger v3 eliminates PostgreSQL entirely. Storage is handled by Pebble, an embedded LSM-tree engine from CockroachDB, running inside the same process as the ledger. There is no network hop between the application and its data and no connection pool to tune. Data lives on local NVMe or SSD storage. Backup and restore are built in: a full backup uploads a Pebble checkpoint to S3 or Azure Blob Storage, frequent incremental backups export new log and audit entries, and an online restore is validated (integrity checker) and previewed before it is finalized. See [Backup and Restore](../ops/backup-restore.md).

For distributed consistency, v3 implements the Raft consensus protocol. A 3-node cluster elects a leader automatically, replicates every write to a majority before acknowledging it, and handles node failures without operator intervention. There is no split-brain risk, no manual failover procedure, and no external coordination service. The entire system ships as a single binary with zero external dependencies.

## Performance

In a staging load test run on 2026-01-29, Ledger v3 averaged about 106,000 transactions per second on modest hardware: a 3-node cluster where each node has 8 CPU cores and 4 GiB of RAM. The p99 latency was about 72–74 ms (below 75 ms) during the 7-minute run. The load was HTTP bulk requests of 50 transactions each, in atomic mode, with unbounded overdraft, so no balance checks rejected transactions. Other workloads, such as single-transaction requests or Numscript with balance checks, will give different numbers.

This performance comes from three design choices. There is no network roundtrip to an external database. Data is written directly to local storage. There are no row locks on hot accounts: Raft orders every write, and the state machine (FSM) applies them one after another from in-memory state. Admission (validation and preparation) runs in parallel; apply is sequential. See [benchmarks.md](./benchmarks.md) for full methodology and analysis, and [v2-vs-v3.md](./v2-vs-v3.md#8-current-balance-storage) for how current balances are stored.

## Key Features

- **Atomic batches** -- HTTP bulk requests in atomic mode commit all-or-nothing within one ledger; the gRPC `Apply` call can span multiple ledgers in one atomic commit
- **Numscript scripting** -- expressive transaction DSL with overdraft control, multi-source/destination routing, and fee calculation
- **Ed25519 request signing** -- cryptographic authenticity, integrity, and non-repudiation for signed writes, with optional mandatory enforcement
- **Immutable audit log** -- global log of all operations, ordered by Raft consensus
- **Maintenance mode** -- graceful read-only mode for planned operations
- **Event sinks** -- HTTP webhooks built in; Kafka, NATS, ClickHouse, and Databricks integrations via build tags
- **Full OpenTelemetry** -- traces, metrics, and logs with optional Pyroscope continuous profiling
- **Prepared queries** -- pre-compiled read queries for low-latency reporting

## Detail Pages

| Document | Description |
|----------|-------------|
| [features.md](./features.md) | Feature matrix with implementation status |
| [benchmarks.md](./benchmarks.md) | Performance benchmarks and analysis |
| [v2-vs-v3.md](./v2-vs-v3.md) | Problems solved from Ledger v2 |
| [limitations.md](./limitations.md) | System limitations and capacity planning |
