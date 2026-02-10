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

All attributes use a unified key format in PebbleDB:

```
[KeyPrefixAttributes][attribute prefix][canonical key bytes][raft index][entry type]
```

| Component | Size | Description |
|-----------|------|-------------|
| `KeyPrefixAttributes` | 1 byte | Constant prefix (`0x09`) for all attributes |
| `attribute prefix` | 1 byte | Identifies attribute type (see table below) |
| `canonical key bytes` | variable | Domain-specific key (e.g., ledgerID + account + asset for volumes) |
| `raft index` | 8 bytes | Raft log index (big-endian) |
| `entry type` | 1 byte | `0` = base, `1` = diff |

This layout groups all entries for the same canonical key together, enabling efficient range scans for `ComputeValue`, `DeleteOldest`, and `List`.

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

During read (`ComputeValue`), the system:
1. Scans all entries for the canonical key up to the target raft index
2. Finds the most recent base entry
3. Finds the latest diff entry after that base
4. Applies the computation function (varies by attribute type)

For volumes (Input/Output), diffs are cumulative (each stores the total delta since the base), so only the latest diff is needed:
```
Final Value = base + latest_cumulative_diff
```

For metadata, the latest diff wins:
```
Final Value = latest_diff ?? base
```

## Volume Compaction

Volume diffs accumulate in PebbleDB over time. Three mechanisms limit growth and keep the entry count bounded:

### 1. Known-Path Base Consolidation (per Merge)

When a volume value is preloaded from the store (cache hit via admission), the `VolumeHolder.Known` field contains the absolute value. During `Buffered.Merge`, this is written as a `SetBase` entry in PebbleDB, effectively consolidating all prior state into a single base value.

This is the primary compaction path for **hot accounts** (frequently accessed, kept in cache).

**Trigger:** Every `Buffered.Merge` where `Known != nil`.

### 2. Generation-Rotation Diff Pruning (periodic)

When a cache generation rotation occurs (every K entries), `compactVolumeDiffs` is called to prune old superseded diffs. For each volume attribute key in PebbleDB, it calls `DeleteOldest(compactionIndex)` which removes all entries with raft index strictly less than the compaction threshold.

The compaction index is the old Gen1 base index, captured just before rotation. This is safe because:
- All entries below this index were part of Gen1 (now being discarded)
- The latest cumulative diff above this index still represents the correct total delta

This is a **prune-only** strategy: it removes old diffs but does NOT create a new base. This is critical because cumulative diffs are always relative to the original base (or implicit base 0). Creating a new base would make subsequent diffs inconsistent.

**Trigger:** `CheckRotationNeeded` detects a generation change during `ApplyEntries`.

**Effect:** Keeps the number of entries per key bounded to approximately `2*K` (two generations' worth).

### 3. DeleteOldest for Non-Volume Attributes (per Merge)

For non-cumulative attributes (ledgers, boundaries, reversions, idempotency keys), `DeleteOldest` is called during `Buffered.Merge` after writing a new base. Since these attributes use last-write-wins semantics, the old base is simply superseded.

### Compaction Flow

```
Entry applied at index i
        │
        ▼
  ┌─────────────────────────────────┐
  │ CheckRotationNeeded(i)          │
  │   gen(i) != currentGeneration?  │
  └───────────┬─────────────────────┘
              │ Yes (rotation)
              ▼
  ┌─────────────────────────────────┐
  │ compactVolumeDiffs(oldGen1Base) │
  │   For each Input/Output key:   │
  │     DeleteOldest(oldGen1Base)   │
  │   (prune diffs < oldGen1Base)  │
  └─────────────────────────────────┘
              │
              ▼
  ┌─────────────────────────────────┐
  │ Process entry → Buffered.Merge  │
  │   Known != nil → SetBase (hot)  │
  │   Known == nil → AddDiff (cold) │
  └─────────────────────────────────┘
```

## Listing Attribute Keys

The `List` method iterates over actual attribute entries in PebbleDB (prefix scan) and extracts unique canonical keys by stripping the prefix (2 bytes) and suffix (9 bytes: index + type) from each Pebble key.

This enables:
- Listing all accounts with volumes (for compaction)
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
