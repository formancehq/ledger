# Ledger v3 - Product Overview

## Headline Numbers

| Metric | Value |
|--------|-------|
| **Throughput** | 106,000 tx/s sustained (3-node cluster) |
| **Latency p99** | < 75 ms under full load |
| **Architecture** | Raft consensus, no external database |
| **Deployment** | Single binary, pure Go, minimal Docker image |

## Contents

| Document | Description |
|----------|-------------|
| [features.md](./features.md) | Feature matrix with implementation status |
| [benchmarks.md](./benchmarks.md) | Performance benchmarks and analysis |
| [v2-vs-v3.md](./v2-vs-v3.md) | Problems solved from Ledger v2 |
| [limitations.md](./limitations.md) | System limitations and capacity planning |

## Key Differentiators

- **Zero external dependencies**: No PostgreSQL, no Redis - single binary with embedded Pebble storage
- **Raft consensus**: Automatic leader election, partition tolerance, built-in replication
- **Cross-ledger atomicity**: Bulk operations spanning multiple ledgers in a single atomic commit
- **Request signing**: Ed25519 signatures for authenticity, integrity, and non-repudiation
- **Full observability**: OpenTelemetry (traces, metrics, logs) + Pyroscope continuous profiling
- **Numscript**: Expressive transaction scripting with overdraft control, multi-source/destination, fee calculation
