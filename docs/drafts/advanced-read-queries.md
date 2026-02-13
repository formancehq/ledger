# Draft — Advanced Read Queries

**Status**: Draft for team review
**Author**: Geoffrey + Claude
**Date**: 2026-02-13

---

## 1. Problem Statement

The ledger currently provides only basic read capabilities:

| Operation | What it does |
|-----------|-------------|
| `GetAccount(ledger, address)` | Volumes + metadata for a single account |
| `GetTransaction(ledger, txID)` | Single transaction by ID |
| `ListTransactions(ledger, pageSize, afterTxID)` | Paginated list, newest-first, no filter |
| `GetAllLedgersInfo` / `GetLedger` | Ledger listing |
| `ListAuditEntries` | Audit trail with ledger + failures-only filter |

This is insufficient for real-world use cases:

- **"Show me all accounts starting with `users:`"** — impossible without scanning every transaction
- **"What is the total balance across all merchant accounts?"** — requires N individual GetAccount calls
- **"List all transactions involving Alice"** — requires scanning all transactions
- **"How many transactions does this ledger have?"** — no stats endpoint

The original `github.com/formancehq/ledger` supports ListAccounts, aggregate balances, ListLogs, and account stats — all missing from the POC.

## 2. Goals

> **Note**: **ListAccounts** has been implemented (see `ListAccountAddresses` in `store.go`, `ListAccounts` across all layers). It uses forward Pebble iteration over Input attribute keys with prefix filtering and cursor pagination.

1. **AggregateBalances**: sum volumes across accounts with optional filters
2. **ListLogs**: list ledger logs (per-ledger)
3. **Ledger stats**: transaction count, account count
4. **Transactions by account**: list transactions involving a specific account

## 3. Scope

### In scope
- AggregateBalances with address prefix filter, per-asset results
- ListLogs per ledger (requires lightweight secondary index)
- Ledger stats (transaction count, account count)
- Transactions-by-account secondary index

### Out of scope
- Full-text search on metadata values
- Cross-ledger aggregation queries (can be built on top later)
- SQL-like query language
- Transaction filtering by metadata key/value (would require inverted index — future work)
- Transaction filtering by date range (requires secondary index — future work, same pattern as by-account)

## 4. Design Overview — Leveraging Pebble's Key Layout

Pebble is an LSM-tree (sorted key-value store). Its core strength is **ordered iteration over key prefixes**. The current key layout already groups data by type, then by ledger, then by account:

```
Volume attributes:
[0x09]['I'][ledgerID (4)][account\x00][asset][raftIndex (8)][entryType (1)]
[0x09]['O'][ledgerID (4)][account\x00][asset][raftIndex (8)][entryType (1)]

Metadata attributes:
[0x09]['M'][ledgerID (4)][account\x01][key][raftIndex (8)][entryType (1)]

Transaction updates:
[ledgerID (4)][0x08][txID (8)][byLog (8)]
```

Because keys are sorted lexicographically, **all entries for the same ledger are physically adjacent** in the LSM tree. Within a ledger, accounts are sorted alphabetically. This means:

- Iterating all accounts of a ledger = single range scan on prefix `[0x09]['I'][ledgerID]`
- Filtering by account prefix (e.g., `users:`) = narrowed range scan with adjusted LowerBound
- Aggregating volumes = same scan, accumulating values instead of emitting them

**No new indexes are needed** for AggregateBalances. The data is already there, we just need new iterators.

## 5. Feature Designs

### 5.1 AggregateBalances

#### Approach

Same range scan as ListAccounts, but instead of emitting accounts, accumulate volumes per asset across all matching accounts.

```
For each (account, asset) in range:
    input  = ComputeValue(Input, canonicalKey)
    output = ComputeValue(Output, canonicalKey)
    totals[asset].input  += input
    totals[asset].output += output
    totals[asset].balance += (input - output)
```

#### Account Prefix Filtering

Prefix-based range narrowing. This enables queries like:
- "Total balance of all `merchants:*` accounts" — narrowed range scan
- "Total balance of all accounts" — full ledger scan
- "Total USD/2 across `users:*`" — range scan + post-filter on asset

#### Proto Definition

```protobuf
message AggregateBalancesRequest {
  string ledger = 1;
  string address_prefix = 2;    // Optional: filter by account prefix
}

message AggregateBalancesResponse {
  // Per-asset aggregated volumes across all matching accounts
  map<string, common.VolumesWithBalance> aggregated = 1;
}

rpc AggregateBalances(AggregateBalancesRequest) returns (AggregateBalancesResponse);
```

#### Implementation Strategy

New method on `DefaultController`:

```go
func (ctrl *DefaultController) AggregateBalances(ctx context.Context, ledgerName string, addressPrefix string) (map[string]*commonpb.VolumesWithBalance, error) {
    // 1. Resolve ledger name → ID
    // 2. Range scan Input attributes for ledgerID + addressPrefix
    //    For each unique (account, asset): ComputeValue → accumulate
    // 3. Range scan Output attributes for same range
    //    For each unique (account, asset): ComputeValue → accumulate
    // 4. Compute balance = totalInput - totalOutput per asset
}
```

This reuses `Attribute.List` and `Attribute.ComputeValue` from the existing attributes package. The aggregation loop is similar to `GetAccountVolumes` in `ctrl/store.go` but operates across all accounts instead of one.

#### Complexity

O(total_volume_entries_in_range). Each entry requires a `ComputeValue` call, but Pebble iterates sequentially through physically adjacent blocks — excellent cache locality.

### 5.3 ListLogs per Ledger

#### Problem

Logs are stored globally: `[0x01][sequence] → Log`. A per-ledger log list requires either:
- **Full scan + filter**: iterate all logs, check ledger name in payload — O(total_logs)
- **Secondary index**: `[prefix][ledgerID][ledgerLogID] → sequence` — O(ledger_logs)

#### Design: Secondary Index

Add a new key prefix for per-ledger log indexing:

```
Key:   [0x05][ledgerID (4)][ledgerLogID (8)]
Value: [globalSequence (8)]
```

This index is populated during FSM log application (in `handleCreateLog`) — the ledgerID and ledgerLogID are already known at that point. The value is the global sequence number, which can be used to fetch the full log via `GetLogBySequence`.

#### Index Population

In the FSM's `handleCreateLog` method (or equivalent in `Buffered.Merge`), add one Pebble `Set` per log applied:

```go
// During log application
kb.PutByte(keyPrefixLedgerLog).
    PutLedgerPrefix(ledgerID).
    PutUInt64(ledgerLogID)
batch.Set(kb.Build(), sequenceBytes, pebble.NoSync)
```

Cost: 12 bytes key + 8 bytes value per log. Negligible compared to the log itself.

#### Iteration

Reverse iteration (newest first) with cursor-based pagination, same pattern as `ListTransactionIDs`:

```go
func (s *Store) ListLedgerLogSequences(ledgerID uint32, pageSize uint32, afterLogID uint64) (Cursor[uint64], error) {
    // Range: [0x05][ledgerID] ... [0x05][ledgerID][afterLogID or 0xFF*8]
    // Reverse iteration: iter.Last() then iter.Prev()
    // Returns global sequences that can be used with GetLogBySequence
}
```

#### Proto Definition

```protobuf
message ListLogsRequest {
  string ledger = 1;
  uint32 page_size = 2;
  uint64 after_log_id = 3;     // Cursor: start after this ledger log ID (exclusive)
}

rpc ListLogs(ListLogsRequest) returns (stream common.Log);
```

### 5.4 Ledger Stats

#### Approach

The `LedgerBoundaries` attribute already tracks `nextTransactionId` and `nextLogId` per ledger. These are effectively counters:

- **Transaction count** = `nextTransactionId - 1` (IDs start at 1)
- **Log count** = `nextLogId - 1`

For **account count**, add a counter to `LedgerBoundaries`:

```protobuf
message LedgerBoundaries {
  uint64 next_log_id = 1;
  uint64 next_transaction_id = 2;
  uint64 account_count = 3;       // NEW: number of distinct accounts
}
```

The account count is incremented during FSM log application when a new account is first seen (first volume entry for an account).

#### Detection of New Accounts

During transaction processing in the FSM, when setting a volume diff for an account:
- Check if this account already has any Input or Output entry (in cache or store)
- If not, increment `account_count` in the ledger boundaries

This is lightweight because the volume entries are already being read/written during transaction processing.

#### Proto Definition

```protobuf
message GetLedgerStatsRequest {
  string ledger = 1;
}

message GetLedgerStatsResponse {
  uint64 transaction_count = 1;
  uint64 log_count = 2;
  uint64 account_count = 3;
}

rpc GetLedgerStats(GetLedgerStatsRequest) returns (GetLedgerStatsResponse);
```

### 5.4 Point-in-Time Reads — Why They Don't Work (and alternatives)

#### The Problem: Compaction Destroys History

At first glance, the attribute system looks like it supports point-in-time reads: `ComputeValue(store, maxIndex, canonicalKey)` takes a `maxIndex` parameter. In theory, passing a past raft index would return the state at that point.

In practice, **three compaction mechanisms destroy historical data**:

1. **Generation-rotation pruning** (`compactVolumeDiffs`): on every generation rotation, `DeleteOldest(oldGen1BaseIndex)` removes all diffs with raft index < threshold. Historical diffs disappear.

2. **Known-path base consolidation** (hot accounts): during `Buffered.Merge`, when a volume is preloaded from cache, `SetBase` writes a new base at the **current** raft index. The base advances forward, erasing the previous base.

3. **Background compactor Phase 2** (cold keys): `Delete` all entries + `SetBase(latestIndex, consolidated)`. A single entry remains at the latest index.

**Consequence**: if you call `ComputeValue(store, pastIndex, key)`, the upperBound is `pastIndex + 1`. But:
- The base may have been consolidated at a raft index **after** `pastIndex` → not found (outside range)
- Diffs at indices before `pastIndex` may have been pruned → nothing left to compute
- Result: zero/nil value, or incomplete data — **silently wrong**

This is by design: compaction trades historical accuracy for bounded storage and write performance. The attribute system maintains the **current** value, not a full history.

#### Alternative Approaches

If point-in-time reads become a requirement, several approaches exist, each with different tradeoffs:

**Option A — Pebble Checkpoints (aligns with TODO item)**

Pebble checkpoints are cheap copy-on-write snapshots (hard links). The system already creates them for Raft snapshots. Exposing an API to read from a past checkpoint would give PIT at checkpoint granularity.

- **Pro**: correct data, no change to compaction strategy
- **Con**: coarse granularity (checkpoint frequency, not per-log), disk space for retained checkpoints
- **Best for**: periodic snapshots (daily, per-period), not arbitrary point queries

**Option B — Log Replay from Period Boundary**

If the [data retention draft](./data-retention-cold-storage.md) is implemented, balance snapshots at period boundaries provide known-good starting points. PIT at sequence N = load period boundary snapshot + replay logs from boundary to N.

- **Pro**: exact PIT at any sequence, correct by construction
- **Con**: replay cost is O(logs between boundary and target), requires period infrastructure
- **Best for**: audit queries, debugging, rare historical lookups

**Option C — Separate Non-Compacted Read Replica**

Fork a Pebble instance that receives the same writes but never runs compaction. Use it exclusively for PIT reads.

- **Pro**: full history preserved, exact PIT
- **Con**: unbounded storage growth (defeats the purpose of compaction), operational complexity
- **Best for**: environments where storage is cheap and PIT is critical

**Recommendation**: Point-in-time reads are **out of scope** for this draft. Option B (log replay from period boundary) is the most promising long-term approach and naturally builds on the data retention infrastructure. Option A (checkpoints) can be a quick win for coarse-granularity PIT.

### 5.5 Transactions by Account

#### Problem

Currently, listing transactions involving a specific account requires scanning all transactions in the ledger and checking postings. This is O(total_transactions).

#### Design: Secondary Index

Add a per-account transaction index:

```
Key:   [0x0B][ledgerID (4)][account (variable)]\x00[txID (8)]
Value: (empty — presence is sufficient)
```

The `\x00` separator after account distinguishes it from account names containing the txID bytes. The key is sorted by `(ledgerID, account, txID)`, which enables:

- List all transactions for an account: range scan on `[0x0B][ledgerID][account\x00]`
- Pagination: use afterTxID to narrow the UpperBound (same pattern as `ListTransactionIDs`)
- Reverse iteration for newest-first ordering

#### Index Population

During FSM log application, when a transaction is created, insert one entry per unique account in the postings:

```go
accounts := uniqueAccountsFromPostings(tx.Postings)
for _, account := range accounts {
    kb.PutByte(keyPrefixAccountTransaction).
        PutLedgerPrefix(ledgerID).
        PutString(account).
        PutByte(0x00).
        PutUInt64(txID)
    batch.Set(kb.Build(), nil, pebble.NoSync)
}
```

Cost per transaction: ~20 bytes per account involved. A typical transaction with 2-3 postings involves 2-4 accounts, so ~60-80 bytes of index data per transaction.

#### Proto Definition

```protobuf
message ListAccountTransactionsRequest {
  string ledger = 1;
  string address = 2;
  uint32 page_size = 3;
  uint64 after_tx_id = 4;      // Cursor (exclusive, for pagination)
}

rpc ListAccountTransactions(ListAccountTransactionsRequest) returns (stream common.Transaction);
```

## 6. New Key Prefixes

Summary of all key prefix additions:

| Prefix | Key | Value | Feature |
|--------|-----|-------|---------|
| `0x05` | `[ledgerID (4)][ledgerLogID (8)]` | `globalSequence (8)` | ListLogs per ledger |
| `0x0B` | `[ledgerID (4)][account]\x00[txID (8)]` | (empty) | Transactions by account |

Note: `0x06` and `0x07` are available if needed (gap between `0x05` and `0x08`). The `0x05` prefix was chosen because it sits logically between the ledger info prefix (`0x03`) and the transaction update prefix (`0x08`).

## 7. gRPC Service Additions

```protobuf
service BucketService {
  // ... existing RPCs ...

  // New read RPCs
  rpc AggregateBalances(AggregateBalancesRequest) returns (AggregateBalancesResponse);
  rpc ListLogs(ListLogsRequest) returns (stream common.Log);
  rpc GetLedgerStats(GetLedgerStatsRequest) returns (GetLedgerStatsResponse);
  rpc ListAccountTransactions(ListAccountTransactionsRequest) returns (stream common.Transaction);
}
```

## 8. HTTP Compatibility Layer

| HTTP Endpoint | gRPC Method | Notes |
|---------------|-------------|-------|
| `GET /{ledger}/aggregate/balances` | `AggregateBalances` | Query param: `address_prefix` |
| `GET /{ledger}/logs` | `ListLogs` | Query params: `page_size`, `cursor` |
| `GET /{ledger}/stats` | `GetLedgerStats` | |
| `GET /{ledger}/accounts/{address}/transactions` | `ListAccountTransactions` | Query params: `page_size`, `cursor` |

## 9. Implementation Plan

### Phase 1 — No New Index Required (quick wins)

These features use only the existing Pebble data layout:

1. **AggregateBalances**: range scan on volume attributes, accumulate per asset
2. **Ledger stats** (partial): transaction count and log count from existing `LedgerBoundaries`

Estimated changes:
- `internal/service/ctrl/controller.go` — add methods to Controller interface
- `internal/service/ctrl/controller_default.go` — implement methods
- `internal/service/ctrl/store.go` — add `AggregateVolumes` function
- `misc/proto/service.proto` — add RPCs and messages
- `internal/application/grpc_ledger_server.go` — add gRPC handlers
- `internal/compat/http/` — add HTTP handlers (one per file)
- `cmd/ledgerctl/` — add CLI commands

### Phase 2 — Lightweight Secondary Index

Requires adding writes during FSM log application:

4. **ListLogs per ledger**: add `[0x05][ledgerID][ledgerLogID] → sequence` index
5. **Ledger stats** (account count): add counter to `LedgerBoundaries`

Estimated changes:
- `internal/storage/data/store.go` — add `keyPrefixLedgerLog` and `ListLedgerLogSequences`
- `internal/service/state/machine.go` (or `buffered.go`) — populate index during log application
- `misc/proto/common.proto` — add `account_count` to `LedgerBoundaries`

### Phase 3 — Write-Path Secondary Index

Requires adding writes during transaction processing:

6. **Transactions by account**: add `[0x0B][ledgerID][account\x00][txID]` index

Estimated changes:
- `internal/storage/data/store.go` — add `keyPrefixAccountTransaction` and iteration methods
- `internal/service/state/machine.go` — populate index during transaction application
- Proto + gRPC + HTTP + CLI additions

### Data Migration Note

For Phase 2 and Phase 3, existing data will not have the secondary indexes populated. Two options:
- **Backfill**: scan existing logs and populate indexes (one-time migration, can be a CLI command)
- **Progressive**: indexes are only populated for new data; old data falls back to full scan

Backfill is recommended for correctness and can be implemented as a `store rebuild-indexes` CLI command that reads all logs and populates the missing indexes.

## 10. Performance Considerations

### Pebble Range Scan Characteristics

| Operation | I/O Pattern | Cache Behavior |
|-----------|-------------|----------------|
| AggregateBalances | Sequential scan + ComputeValue per entry | Excellent — all data under same prefix |
| ListLogs | Index scan + random GetLogBySequence | Two-level lookup, log data may not be cached |
| Transactions by account | Index scan + random buildTransaction | Index is small and cached; tx reconstruction is heavier |

### Write Overhead of Secondary Indexes

| Index | Extra Writes per Transaction | Key Size | Value Size |
|-------|------------------------------|----------|------------|
| Ledger log index (`0x05`) | 1 per log | 13 bytes | 8 bytes |
| Account-tx index (`0x0B`) | 2-4 per transaction (one per account) | ~25 bytes | 0 bytes |

Total overhead: ~100-200 bytes per transaction. Negligible compared to the log itself (~500-2000 bytes) and the attribute updates.

### Compaction Impact

Secondary indexes are append-only (no updates, no deletes in normal operation). This is ideal for LSM trees — no write amplification from compaction merges. The indexes will naturally sort into the lowest levels of the LSM tree over time.

## 11. Decisions Record

| Topic | Decision | Rationale |
|---|---|---|
| **Pagination model** | Cursor-based (last address/txID) | Consistent with existing ListTransactions pattern. Offset-based is unreliable with concurrent writes. |
| **Ledger log index** | New `0x05` prefix | O(1) per log write, enables O(ledger_logs) per-ledger iteration instead of O(total_logs) |
| **Account-tx index** | New `0x0B` prefix | Enables account-centric transaction views without full scan |
| **Point-in-time reads** | Out of scope | Compaction destroys historical diffs — `ComputeValue(pastIndex)` returns incorrect results. See Section 5.5 for alternatives. |
| **Account count** | Counter in LedgerBoundaries | Avoids expensive count-by-scan; incremented during normal write path |

## 12. Open Questions for Team

1. **AggregateBalances performance**: for ledgers with millions of accounts, a full aggregation scan could take seconds. Should we add a time limit or streaming aggregation? Or is this acceptable given it's a read-only operation on a follower?
2. **Index backfill**: should `store rebuild-indexes` be a blocking CLI command, or a background operation with progress streaming (like `store check`)?
3. **Interaction with data retention**: when periods are archived and purged, secondary indexes for purged data should also be cleaned up. The per-ledger log index (`0x05`) can use range delete on `[0x05][ledgerID][startLogID]...[endLogID]`. The account-tx index (`0x0B`) requires scanning to find entries in the purged range — should we store txID in big-endian so range delete works?
4. **Point-in-time reads**: the compaction strategy destroys historical data (see Section 5.4). If PIT becomes a requirement, the most promising approach is log replay from period boundary snapshots (builds on the data retention draft). Should we plan for this, or is current-state-only sufficient?
