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
| Bulk atomic mode | ✅ | System-level atomicity (cross-ledger) |
| Bulk continueOnFailure | ✅ | |
| Create/delete/get/list ledgers | ✅ | |
| Idempotency key | ✅ | BLAKE3 hash-based conflict detection |
| Unique transaction reference | ✅ | Per-ledger uniqueness, HTTP 409 on conflict |

## Read Operations

| Feature | Status | Notes |
|---------|--------|-------|
| Get transaction by ID | ✅ | From any node |
| List transactions | ✅ | gRPC stream with pagination |
| Get account (with volumes) | ✅ | Input/output/balance per asset |
| List accounts | ✅ | Prefix filter, cursor pagination |
| Get/list ledgers | ✅ | |
| List logs | ✅ | System-wide and per-ledger listing |
| Balance aggregation | ✅ | Per-asset aggregation for filtered accounts |
| Ledger statistics | ✅ | Account and transaction counts |

## Numscript Features (all enabled by default)

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
| Audit log (success + failure) | ✅ | Raft-replicated, stored in Pebble |

## Operations

| Feature | Status | Notes |
|---------|--------|-------|
| Maintenance mode | ✅ | Cluster-wide write blocking, dual enforcement |
| Store metrics (Pebble) | ✅ | |
| Store integrity check | ✅ | Hash chain + derived data verification |
| Point-in-time backup | ✅ | Pebble snapshot as tar archive |
| Restore pipeline | ✅ | Upload, validate, preview, finalize |
| Chapters (close, seal, archive) | ✅ | BLAKE3 sealing hash, JWT receipts |
| Cluster management | ✅ | Transfer leader, add/promote learner |
| Disk space monitoring | ✅ | Auto-reject writes when storage full |

## Event Sinks

| Sink | Status |
|------|--------|
| NATS | ✅ |
| Kafka | ✅ |
| ClickHouse | ✅ |
| HTTP webhook | ✅ |

## Intentionally Removed from v2

| Feature | Reason |
|---------|--------|
| `preCommitVolumes` in responses | Same - volumes available via dedicated read endpoints |

`postCommitVolumes` is still available as an opt-in response field for transaction creation and reverts when `expandVolumes` is enabled.

---

For the full technical comparison, see [api-comparison.md](../technical/contributing/api-comparison.md).
