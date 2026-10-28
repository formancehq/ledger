# Feature Matrix

## Write Operations

| Feature | Status | Notes |
|---------|--------|-------|
| Create transaction (postings) | ✅ | |
| Create transaction (Numscript) | ✅ | |
| Revert transaction | ✅ | Standard, `force`, `atEffectiveDate` |
| Force transaction (bypass balance check) | ✅ | |
| Save/delete account metadata | ✅ | |
| Save/delete transaction metadata | ✅ | |
| Bulk operations | ✅ | CREATE_TRANSACTION, ADD_METADATA, REVERT_TRANSACTION, DELETE_METADATA |
| Bulk atomic mode | ✅ | All-or-nothing within the route's ledger (HTTP bulk is ledger-scoped); cross-ledger atomic batches use the gRPC `Apply` call |
| Bulk continueOnFailure | ✅ | |
| Create/delete/get/list ledgers | ✅ | |
| Idempotency key | ✅ | BLAKE3 hash-based conflict detection; a retry with the same key and body returns the original outcome while the key is retained (default 24h) |
| Unique transaction reference | ✅ | Per-ledger uniqueness, HTTP 409 on conflict |

## Read Operations

| Feature | Status | Notes |
|---------|--------|-------|
| Get transaction by ID | ✅ | From any node |
| List transactions | ✅ | HTTP and gRPC stream, cursor pagination |
| Get account (with volumes) | ✅ | Input/output/balance per asset |
| List accounts | ✅ | Address and metadata filters, cursor pagination |
| Get/list ledgers | ✅ | |
| List logs | ✅ | System-wide and per-ledger listing |
| Balance aggregation | ✅ | Per-asset aggregation for filtered accounts |
| Ledger statistics | ✅ | Account and transaction counts |
| Two-way pagination | ✅ | Transaction, account, log, audit-entry and ledger lists return `next` and `previous` page tokens |
| Read consistency selector | ✅ | Linearizable by default; `X-Consistency: stale` (HTTP) or `x-consistency: stale` (gRPC) serves local state without the Raft barrier |

## Numscript Experimental Features (available, opt-in per script)

The server does not restrict these features, but each script must opt in with a `#![feature("...")]` pragma. See [Numscript](../technical/contributing/numscript.md).

| Feature | Description |
|---------|-------------|
| Account interpolation | Dynamic addresses (e.g., `@escrow:$order_id`) |
| Asset colors | Fund origin tracking |
| `get_amount()` / `get_asset()` | Extract components from monetary values |
| Mid-script function calls | Balance queries during execution |
| `oneof` selector | Conditional routing |
| `overdraft()` function | Dynamic overdraft calculation |

## Security

| Feature | Status | Notes |
|---------|--------|-------|
| Ed25519 request signing | ✅ | Envelope pattern with signed_payload |
| Dynamic key management | ✅ | Register/revoke keys via API |
| Mandatory signature enforcement | ✅ | Optional, toggled via API |
| Audit log (success + failure) | ✅ | Raft-replicated, hash-chained (keyed BLAKE3 by default), stored in Pebble |

## Operations

| Feature | Status | Notes |
|---------|--------|-------|
| Maintenance mode | ✅ | Cluster-wide write blocking, dual enforcement |
| Store metrics (Pebble) | ✅ | |
| Store integrity check | ✅ | Hash chain + derived data verification |
| Full + incremental backup | ✅ | Full backup uploads a Pebble checkpoint as content-addressed objects plus a manifest (S3 or Azure); incremental backups export new log and audit entries. One backend + `bucket-id` holds one restorable chain, and the recovery point is the last completed backup (full or incremental). No arbitrary point-in-time restore; see [long-term retention](../ops/backup-restore.md#long-term-retention) |
| Restore pipeline | ✅ | Download, validate, preview, finalize; or offline `store bootstrap` (integrity check with `--validate`) |
| Cluster management | ✅ | Transfer leader, add/promote learner, remove node |
| Disk space monitoring | ✅ | Rejects writes when WAL or data volume usage exceeds its threshold |

## Event Sinks

| Sink | Status | Notes |
|------|--------|-------|
| HTTP webhook | ✅ | Built in; optional HMAC signature |
| NATS JetStream | ✅ | `nats` build tag |
| Kafka | ✅ | `kafka` build tag |
| ClickHouse | ✅ | `clickhouse` build tag |
| Databricks | ✅ | `databricks` build tag |

Sinks are added and removed at runtime through the HTTP API (`POST /v3/_/events-sinks`, `DELETE /v3/_/events-sinks/{sinkName}`), the gRPC `Apply` call, or `ledgerctl events add-sink` / `remove-sink`. The configuration is replicated through Raft.

## Intentionally Removed from v2

| Feature | Reason |
|---------|--------|
| `preCommitVolumes` in responses | Volumes are available through dedicated read endpoints |

`postCommitVolumes` is part of every transaction and is returned on all transaction responses (create, revert, get, list, prepared-query) as an immutable per-transaction snapshot.

---

For the full technical comparison, see [api-comparison.md](../technical/contributing/api-comparison.md).
