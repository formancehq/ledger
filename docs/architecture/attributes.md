# System Attributes

## Overview

Attributes are key-value pairs that track the state of the ledger system. They use a unified storage and caching model based on **generation-based caching** and **preloading** to ensure deterministic FSM execution across all Raft nodes.

All attributes share:
- **U128 hash-based keys** for efficient storage and lookup
- **Tag-based collision detection** (64-bit secondary hash)
- **Generation cache** (gen0/gen1) for fast access
- **AttributeLoader** for coordinated concurrent loads
- **Base + diff storage** for efficient updates

See [Deterministic FSM](./deterministic-fsm.md) for details on the caching and preloading mechanisms.

## Attribute Types

| Attribute | Key | Value | Scope | Behavior |
|-----------|-----|-------|-------|----------|
| **Input Volumes** | ledger/account/asset | `BigInt` | Per-ledger | Additive (base + latest cumulative diff) |
| **Output Volumes** | ledger/account/asset | `BigInt` | Per-ledger | Additive (base + latest cumulative diff) |
| **Account Metadata** | ledger/account/key | `MetadataValue` | Per-ledger | Last-write-wins |
| **Ledger Metadata** | ledger/key | `MetadataValue` | Per-ledger | Last-write-wins |
| **Reversions** | ledger/txID | `bool` | Per-ledger | Last-write-wins |
| **Idempotency Keys** | key string | `IdempotencyKeyValue` | System-wide | Immutable once set |

## Volumes (Input/Output)

Track funds flow for each account and asset combination.

| Property | Description |
|----------|-------------|
| **Key** | `VolumeKey` = ledger name + account address + asset |
| **Value** | `BigInt` (arbitrary precision integer) |
| **Computation** | Base + latest cumulative diff |
| **Balance** | `Input - Output` |

**Example:**
```
Key: ledger="main", account="users:alice", asset="USD/2"
Input: 150000 (funds received)
Output: 50000 (funds sent)
Balance: 100000 ($1000.00 with 2 decimals)
```

**Usage:**
- Balance verification before transactions
- Account balance queries
- Insufficient funds detection

## Account Metadata

Key-value metadata attached to accounts.

| Property | Description |
|----------|-------------|
| **Key** | `MetadataKey` = ledger name + account address + metadata key |
| **Value** | `MetadataValue` (string) |
| **Computation** | Last diff wins (or base if no diffs) |

**Example:**
```
Key: ledger="main", account="users:alice", key="kyc_status"
Value: "verified"
```

**Usage:**
- Store arbitrary data on accounts
- Numscript can read account metadata via `meta()`
- Queryable via API

## Ledger Metadata

Key-value metadata attached to ledgers.

| Property | Description |
|----------|-------------|
| **Key** | `LedgerMetadataKey` = ledger name + metadata key |
| **Value** | `MetadataValue` (string) |
| **Computation** | Last diff wins (or base if no diffs) |

**Example:**
```
Key: ledger="main", key="environment"
Value: "production"
```

**Usage:**
- Store ledger-level configuration
- Set at ledger creation or via metadata API

## Reversions

Track whether a transaction has been reverted.

| Property | Description |
|----------|-------------|
| **Key** | `TransactionKey` = ledger name + transaction ID |
| **Value** | `bool` (reverted = true/false) |
| **Computation** | Last diff wins (default false) |

**Example:**
```
Key: ledger="main", txID=42
Value: true (transaction 42 has been reverted)
```

**Usage:**
- Prevent double reversions
- Query transaction reversion status

## Idempotency Keys

Map idempotency keys to log sequences for request deduplication.

| Property | Description |
|----------|-------------|
| **Key** | `IdempotencyKey` = key string (user-provided) |
| **Value** | `IdempotencyKeyValue` = log sequence + content hash |
| **Computation** | Immutable (first value wins) |
| **Scope** | System-wide (not per-ledger) |

**Example:**
```
Key: "payment-123"
Value: { logSequence: 456, hash: <blake3 hash of request content> }
```

**Usage:**
- Safe request retries
- Duplicate detection
- Conflict detection (same key, different content)

See [Idempotency](./idempotency.md) for detailed documentation.

## Storage Format

### Key Structure

All attributes use a unified key format:

```
[KeyPrefixAttributes][U128 hash][attribute prefix][raft index][entry type]
```

| Component | Size | Description |
|-----------|------|-------------|
| `KeyPrefixAttributes` | 1 byte | Constant prefix for all attributes |
| `U128 hash` | 16 bytes | BLAKE3 hash of canonical key |
| `attribute prefix` | 1 byte | Identifies attribute type |
| `raft index` | 8 bytes | Raft log index (big-endian) |
| `entry type` | 1 byte | 0 = base, 1 = diff |

### Attribute Prefixes

| Attribute | Prefix |
|-----------|--------|
| Input Volumes | `0x01` |
| Output Volumes | `0x02` |
| Account Metadata | `0x03` |
| Ledger Metadata | `0x04` |
| Reversions | `0x05` |
| Idempotency Keys | `0x06` |

### Value Computation

During read, the system:
1. Finds the most recent base with index ≤ target index
2. Finds the latest diff with index > base index and ≤ target index
3. Applies the computation function (varies by attribute type)

For volumes (Input/Output), diffs are stored as cumulative values since the base, so only the latest diff is needed:
```
Final Value = computeFn(base, [latest_cumulative_diff])
```

## Mapping Index

Each attribute maintains a mapping index for listing all keys:

```
[KeyPrefixAttributesMapping][attribute prefix][canonical key bytes]
  → [U128 hash (16 bytes)][tag64 (8 bytes)]
```

This enables:
- Listing all accounts with volumes
- Listing all metadata keys
- Iterating over all idempotency keys

## Cache Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    AttributeLoader                       │
│         (Coordinates concurrent loads from store)        │
│                                                          │
│   loading: map[U128]chan    loaded: map[U128]entry      │
└─────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────┐
│                  Generation Cache                        │
│                                                          │
│   ┌─────────────┐    ┌─────────────┐                    │
│   │    Gen0     │    │    Gen1     │                    │
│   │  (current)  │    │  (previous) │                    │
│   │             │    │             │                    │
│   │  U128 →     │    │  U128 →     │                    │
│   │  Entry[T]   │    │  Entry[T]   │                    │
│   └─────────────┘    └─────────────┘                    │
│                                                          │
│   One cache per attribute type:                         │
│   - Input, Output, AccountMetadata, LedgerMetadata,     │
│     Reversions, IdempotencyKeys                         │
└─────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────┐
│                      Pebble Store                        │
│            (Persisted bases and diffs)                   │
└─────────────────────────────────────────────────────────┘
```

## Related Documentation

- [Deterministic FSM](./deterministic-fsm.md) - Generation-based caching and preloading
- [Idempotency](./idempotency.md) - Idempotency keys in detail
- [Storage](./storage.md) - Pebble storage architecture
