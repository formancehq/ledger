# Ledger v3 — Product Overview

| Metric | Value |
|--------|-------|
| **Throughput** | 106,000 tx/s sustained |
| **Latency p99** | < 75 ms under full load |
| **High Availability** | 3-node Raft cluster |
| **Deployment** | Single binary |
| **Implementation** | Pure Go, zero CGO |

## Why v3

Ledger v2 depended on PostgreSQL for storage, which meant every deployment needed a managed database, connection pooling, replication configuration, and a DBA to keep it all running. Scaling required read replicas, and failover required external tools like Patroni. The database was the bottleneck, the single point of failure, and the largest line item on the infrastructure bill.

Ledger v3 eliminates PostgreSQL entirely. Storage is handled by Pebble, an embedded LSM-tree engine from CockroachDB, running inside the same process as the ledger. There is no network hop between the application and its data, no connection pool to tune, and no separate backup strategy to maintain. Data lives on local NVMe or SSD storage, and backups are as simple as copying a directory.

For distributed consistency, v3 implements the Raft consensus protocol. A 3-node cluster elects a leader automatically, replicates every write to a majority before acknowledging it, and handles node failures without operator intervention. There is no split-brain risk, no manual failover procedure, and no external coordination service. The entire system ships as a single binary with zero external dependencies.

## Performance

Ledger v3 sustains 106,000 transactions per second on modest hardware: a 3-node cluster where each node has 8 CPU cores and 4 GiB of RAM. Tail latency stays below 75 ms at the 99th percentile under full load. This performance comes from eliminating network roundtrips to an external database, writing directly to local storage, and using append-only balance diffs that remove lock contention on hot accounts. See [benchmarks.md](./benchmarks.md) for full methodology and analysis.

## Key Features

- **Cross-ledger atomic operations** -- bulk operations spanning multiple ledgers in a single all-or-nothing commit
- **Numscript scripting** -- expressive transaction DSL with overdraft control, multi-source/destination routing, and fee calculation
- **Ed25519 request signing** -- cryptographic authenticity, integrity, and non-repudiation for every write
- **Immutable audit log** -- global log of all operations, ordered by Raft consensus
- **Maintenance mode** -- graceful read-only mode for planned operations
- **Event sinks** -- Kafka, NATS, ClickHouse, and Databricks integrations via build tags
- **Full OpenTelemetry** -- traces, metrics, and logs with optional Pyroscope continuous profiling
- **Chapters and archiving** -- time-based partitioning and cold storage offloading
- **Prepared queries** -- pre-compiled read queries for low-latency reporting

## Detail Pages

| Document | Description |
|----------|-------------|
| [features.md](./features.md) | Feature matrix with implementation status |
| [benchmarks.md](./benchmarks.md) | Performance benchmarks and analysis |
| [v2-vs-v3.md](./v2-vs-v3.md) | Problems solved from Ledger v2 |
| [limitations.md](./limitations.md) | System limitations and capacity planning |
