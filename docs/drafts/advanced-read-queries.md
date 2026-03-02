# Draft — Advanced Read Queries

**Status**: Outdated — superseded by [Prepared Queries](./prepared-queries.md)
**Author**: Geoffrey + Claude
**Date**: 2026-02-13, updated 2026-02-24

> **Note**: This document is an early exploration and is outdated relative to the [Prepared Queries](./prepared-queries.md) draft, which contains the current design. Some sections here (storage engine options, key layout) have been reworked or replaced. Do not take this document at face value — refer to Prepared Queries for the up-to-date design.

---

## 1. Problem Statement

The ledger currently provides these read capabilities:

| Operation | What it does |
|-----------|-------------|
| `GetAccount(ledger, address)` | Volumes + metadata for a single account |
| `GetTransaction(ledger, txID)` | Single transaction by ID |
| `ListTransactions(ledger, pageSize, afterTxID)` | Paginated list, newest-first, no filter |
| `ListAccounts(ledger, pageSize, afterAddr, prefix)` | Paginated list with optional address prefix filter |
| `ListLedgers` / `GetLedger` | Ledger listing |
| `ListLogs(afterSequence, pageSize)` | Global log listing by sequence (not per-ledger) |
| `ListAuditEntries` | Audit trail with ledger + failures-only filter |
| `ListPeriods` / `ListSigningKeys` | Period and signing key listing |
| `GetMetadataSchemaStatus` | Metadata schema validation status per ledger |

This is insufficient for real-world use cases:

- **"What is the total balance across all merchant accounts?"** — requires N individual GetAccount calls
- **"List all transactions involving Alice"** — requires scanning all transactions
- **"How many transactions does this ledger have?"** — no stats endpoint
- **"List all accounts where category=premium"** — no metadata filtering at all
- **"List accounts where category=premium AND region=eu"** — no multi-criteria filtering

The original `github.com/formancehq/ledger` supports aggregate balances, ListLogs, and account stats — all missing from the POC. More importantly, the reference implementation supports metadata-based filtering via SQL queries — a capability that requires a fundamentally different approach in an embedded key-value store.

## 2. Goals

1. **AggregateBalances**: sum volumes across accounts with optional filters
2. **ListLogs**: list ledger logs (per-ledger)
3. **Ledger stats**: transaction count, account count
4. **Transactions by account**: list transactions involving a specific account
5. **Metadata filtering**: list accounts/transactions matching metadata criteria (key/value)
6. **Prepared queries**: client-defined query templates with inverted indexes for efficient multi-criteria filtering

## 3. Scope

### In scope
- AggregateBalances with address prefix filter, per-asset results
- ListLogs per ledger (requires lightweight secondary index)
- Ledger stats (transaction count, account count)
- Transactions-by-account secondary index

### Out of scope
- Full-text search on metadata values (fuzzy matching, substring search)
- Cross-ledger aggregation queries (can be built on top later)
- SQL-like query language
- Transaction filtering by date range (requires secondary index — future work, same pattern as by-account)

## 4. Design Overview — Leveraging Pebble's Key Layout

Pebble is an LSM-tree (sorted key-value store). Its core strength is **ordered iteration over key prefixes**. The current key layout groups data into three zones:

| Zone | Range | Purpose |
|------|-------|---------|
| **Cold-storable** | `[0x01, 0xF1)` | Logs, audit, tx updates — archivable to cold storage |
| **Attributes** | `[0xF1, 0xF2)` | Volumes, metadata, reversions — stays in hot storage |
| **System** | `[0xF2, 0xFF]` | Config, signing keys, periods — lives forever |

Within the attributes zone, keys follow this structure:

```
Attribute key format:
[0xF1][canonicalKey][attrType (1B)][raftIndex (8B)][entryType (1B)]

Volume canonical key:    [ledgerName\x00][account\x00][asset]
Metadata canonical key:  [ledgerName\x00][account\x01][metadataKey]

Full volume attribute key:
[0xF1][ledgerName\x00][account\x00][asset]['V'][raftIndex (8B)][entryType (1B)]

Full metadata attribute key:
[0xF1][ledgerName\x00][account\x01][metadataKey]['M'][raftIndex (8B)][entryType (1B)]

Transaction update key:
[0x03][ledgerName\x00][txID (8B)][byLog (8B)]
```

Attribute types: `'V'` = Volume, `'M'` = Metadata, `'R'` = Reverted, `'K'` = IdempotencyKey, `'F'` = Reference, `'G'` = Ledger, `'B'` = Boundary.

Entry types: `0x00` = Base, `0x01` = Diff (used by the base+diff compaction model).

Because keys are sorted lexicographically, **all entries for the same ledger are physically adjacent** in the LSM tree. Within a ledger, accounts are sorted alphabetically. This means:

- Iterating all accounts of a ledger = single range scan on prefix `[0xF1][ledgerName\x00]`
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
    // 1. Range scan volume attributes: [0xF1][ledgerName\x00][addressPrefix...] with attrType 'V'
    //    For each unique (account, asset): ComputeValue → accumulate
    // 2. Compute balance = totalInput - totalOutput per asset
}
```

This reuses `Attribute.List` and `Attribute.ComputeValue` from the existing attributes package. The aggregation loop is similar to `GetAccountVolumes` in `ctrl/store.go` but operates across all accounts instead of one.

#### Complexity

O(total_volume_entries_in_range). Each entry requires a `ComputeValue` call, but Pebble iterates sequentially through physically adjacent blocks — excellent cache locality.

### 5.3 ListLogs per Ledger

> **Current state**: `ListLogs(afterSequence, pageSize)` is already implemented as a **global** log listing (iterates `[0x01][sequence]` keys). It returns all logs across all ledgers, paginated by global sequence. See `ctrl.DefaultController.ListLogs()` and `state.ReadLogsSince()`.

#### Problem

The existing `ListLogs` iterates **all** logs globally. A **per-ledger** log list requires either:
- **Full scan + filter**: iterate all logs, check ledger name in payload — O(total_logs)
- **Secondary index**: `[prefix][ledgerName\x00][ledgerLogID] → sequence` — O(ledger_logs)

#### Design: Secondary Index

Add a new key prefix for per-ledger log indexing:

```
Key:   [0x05][ledgerName\x00][ledgerLogID (8B)]
Value: [globalSequence (8B)]
```

This index is populated during FSM log application (in `handleCreateLog`) — the ledger name and ledgerLogID are already known at that point. The value is the global sequence number, which can be used to fetch the full log via `GetLogBySequence`.

#### Index Population

In the FSM's `handleCreateLog` method (or equivalent in `Buffered.Merge`), add one Pebble `Set` per log applied:

```go
// During log application
kb.PutByte(keyPrefixLedgerLog).
    PutLedgerName(ledgerName).
    PutUInt64(ledgerLogID)
batch.Set(kb.Build(), sequenceBytes, pebble.NoSync)
```

Cost: `1 + len(ledgerName) + 1 + 8` bytes key + 8 bytes value per log. Negligible compared to the log itself.

#### Iteration

Reverse iteration (newest first) with cursor-based pagination, same pattern as `ListTransactionIDs`:

```go
func (s *Store) ListLedgerLogSequences(ledgerName string, pageSize uint32, afterLogID uint64) (Cursor[uint64], error) {
    // Range: [0x05][ledgerName\x00] ... [0x05][ledgerName\x00][afterLogID or 0xFF*8]
    // Reverse iteration: iter.Last() then iter.Prev()
    // Returns global sequences that can be used with GetLogBySequence
}
```

#### Proto Definition

The existing `ListLogsRequest` (global listing) is already implemented:

```protobuf
// Existing — global listing by sequence
message ListLogsRequest {
  optional uint64 after_sequence = 1;
  uint32 page_size = 2;
}
rpc ListLogs(ListLogsRequest) returns (stream common.Log);
```

For per-ledger listing, extend or add a new RPC:

```protobuf
// New — per-ledger listing by ledger log ID (requires 0x05 index)
message ListLedgerLogsRequest {
  string ledger = 1;
  uint32 page_size = 2;
  uint64 after_log_id = 3;     // Cursor: start after this ledger log ID (exclusive)
}
rpc ListLedgerLogs(ListLedgerLogsRequest) returns (stream common.Log);
```

### 5.4 Ledger Stats

#### Approach

The `LedgerBoundaries` attribute already tracks `next_transaction_id` and `next_log_id` per ledger. These are effectively counters:

- **Transaction count** = `next_transaction_id - 1` (IDs start at 1)
- **Log count** = `next_log_id - 1`

Current proto definition (field 3 is reserved from the old `ledger_id` removal):

```protobuf
// Current state in raft_cmd.proto
message LedgerBoundaries {
  uint64 next_transaction_id = 1;
  uint64 next_log_id = 2;
  reserved 3;  // Was: uint32 ledger_id (removed: ledger name used as key)
}
```

For **account count**, add a new field (use field 4 since 3 is reserved):

```protobuf
message LedgerBoundaries {
  uint64 next_transaction_id = 1;
  uint64 next_log_id = 2;
  reserved 3;
  uint64 account_count = 4;       // NEW: number of distinct accounts
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

3. **Inline compaction** (at rotation): `DeleteOldest` removes all diffs with raft index < compaction threshold for tracked dirty keys.

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
Key:   [0x0B][ledgerName\x00][account\x00][txID (8B)]
Value: (empty — presence is sufficient)
```

The `\x00` separator after account distinguishes it from account names containing the txID bytes. The key is sorted by `(ledgerName, account, txID)`, which enables:

- List all transactions for an account: range scan on `[0x0B][ledgerName\x00][account\x00]`
- Pagination: use afterTxID to narrow the UpperBound (same pattern as `ListTransactionIDs`)
- Reverse iteration for newest-first ordering

#### Index Population

During FSM log application, when a transaction is created, insert one entry per unique account in the postings:

```go
accounts := uniqueAccountsFromPostings(tx.Postings)
for _, account := range accounts {
    kb.PutByte(keyPrefixAccountTransaction).
        PutLedgerName(ledgerName).
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

### 5.6 Generic Metadata Inverted Index

#### Problem

Currently, listing accounts with specific metadata (e.g., "all accounts where `category=premium`") requires a full scan of all accounts in the ledger, deserializing each account's metadata and filtering in-memory. This is O(total_accounts) regardless of how many accounts match.

With a key-value store like Pebble, the SQL `WHERE metadata->>'category' = 'premium'` pattern doesn't exist. We need **inverted indexes**: given a metadata key/value pair, directly look up which accounts match.

#### Design: Always-On Metadata Inverted Index

Rather than creating indexes on-demand per prepared query, we index **all** metadata key/value pairs systematically. This avoids the complexity of per-query index lifecycle management and backfill orchestration.

```
Key:   [0x0C][ledgerName\x00][metadataKey\x00][metadataValue\x00][accountAddress]
Value: (empty — presence is sufficient)
```

The keys are sorted lexicographically, which means:
- All accounts with `category=premium` in ledger `default` are adjacent under prefix `[0x0C]default\x00category\x00premium\x00`
- Range scan on that prefix returns matching account addresses **in sorted order**

#### Index Maintenance — The Old-Value Problem

When metadata changes from `category=premium` to `category=standard`, we need to:
1. **Delete** the old entry: `[0x0C][ledger\x00][category\x00][premium\x00][account]`
2. **Insert** the new entry: `[0x0C][ledger\x00][category\x00][standard\x00][account]`

This is the core challenge: **FSM apply doesn't read Pebble**. If the account isn't in the gen0/gen1 cache, the old metadata value is unknown during apply.

**Solution: Extend preload to cover metadata.**

During the **admission phase** (leader-side, before Raft proposal), preload current metadata values for any account whose metadata is being modified. Attach the old values to the Raft entry alongside the existing volume preloads.

```go
// During admission, for metadata-modifying commands:
type MetadataPreload struct {
    Account     string
    OldMetadata map[string]string  // current metadata state, for index cleanup
}
```

During FSM apply:
- Old values come from the preload (for accounts not in cache) or from the gen0/gen1 cache (for recently-touched accounts)
- For each changed key: delete old index entry, insert new one
- For new keys (no old value): insert only

This mirrors the existing volume preload pattern — the admission phase already reads from Pebble to prepare data for deterministic apply.

#### Multi-Criteria Filtering via Merge-Join on Sorted Iterators

When a query combines multiple metadata filters (e.g., `category=premium AND region=eu`), we use the fact that each inverted index returns account addresses **in sorted order** to perform a **streaming merge-join intersection**:

```
Iterator 1: accounts where category=premium  → [acc_A, acc_C, acc_F, acc_Z]
Iterator 2: accounts where region=eu          → [acc_A, acc_B, acc_F, acc_M]
Merge-join intersection                       → [acc_A, acc_F]
```

This is the equivalent of bitmap AND operations, but leveraging Pebble's natural key ordering instead of explicit bitmap structures. The algorithm:

```go
func IntersectSorted(iters ...pebble.Iterator) []string {
    // Advance all iterators to their first entry
    // At each step, find the max address across all iterators
    // Advance all iterators that are behind the max
    // When all iterators point to the same address → emit match
    // Complexity: O(N × min_result_set) where N = number of filters
}
```

**Advantages over explicit bitmaps (Roaring Bitmaps, etc.):**
- Accounts are identified by **strings** (addresses), not integers — no mapping layer needed
- No in-memory bitmap structure to maintain — purely iterator-based, constant memory
- Pebble's block cache handles hot data naturally
- Streaming: results can be paginated without materializing the full intersection

**For OR (union) operations**, the same merge-join pattern works but emits when **any** iterator matches (merge-union instead of merge-intersect).

#### Write Overhead

Each metadata `Set` operation costs:
- 1 Pebble `Delete` for the old index entry (if old value exists)
- 1 Pebble `Set` for the new index entry
- Key size: ~30-80 bytes (depends on ledger name, metadata key, value, account address lengths)

Metadata writes are infrequent compared to volume updates (volumes change on every transaction; metadata changes are explicit user operations). The overhead is negligible.

### 5.7 Account Existence Index

#### Problem

Several features need to enumerate accounts efficiently without scanning the full attribute zone (`[0xF1]`) and deduplicating across assets:

- **`AddressPrefix` filter** in prepared queries
- **`NOT` operator** (needs the "universe" of all accounts)
- **`account_count`** in ledger stats (alternative to the counter in `LedgerBoundaries`)

#### Design

A lightweight index that records the existence of each account:

```
Key:   [0x0D][ledgerName\x00][accountAddress]
Value: (empty — presence is sufficient)
```

Populated once per account, when the account is first seen (first volume write). The index is append-only (accounts are never deleted).

This enables:
- `AddressPrefix("merchants:")` → range scan `[0x0D][ledger\x00][merchants:]...[merchants:\xFF]`
- `AddressExact("merchants:alice")` → point lookup on `[0x0D][ledger\x00][merchants:alice]`
- Full account universe for `NOT` → range scan on `[0x0D][ledger\x00]`
- Account count → count entries under `[0x0D][ledger\x00]` (or maintain a counter)

### 5.8 Prepared Queries

#### Motivation

The generic metadata index (Section 5.6) handles exact-match filtering on individual metadata keys. Prepared queries go further by allowing **complex boolean expressions** combining metadata filters and address filters with AND, OR, and NOT operators.

#### Design: Recursive Filter Model

A prepared query filter is a **tree of boolean operators** where leaves are either metadata matches or address matches. Each node in the tree produces a **sorted iterator of account addresses**, and operators compose iterators:

```
AND(a, b)  →  merge-intersect of sorted iterators
OR(a, b)   →  merge-union of sorted iterators
NOT(a)     →  merge-difference (universe \ a), where universe = account existence index [0x0D]
```

This composes to arbitrary depth:

```
AND(
  OR(
    MetadataEquals("category", "premium"),
    MetadataEquals("category", "gold")
  ),
  AddressPrefix("merchants:"),
  NOT(MetadataEquals("status", "suspended"))
)
```

Each leaf opens a Pebble iterator, each intermediate node consumes its children and produces a sorted stream. Memory remains constant (no intermediate materialization).

#### Leaf Nodes

**MetadataMatch(key, value)** — uses the inverted index (Section 5.6):
```
Scan [0x0C][ledger\x00][key\x00][value\x00] → sorted account addresses
```

**AddressPrefix(prefix)** — uses the account existence index (Section 5.7):
```
Scan [0x0D][ledger\x00][prefix]...[prefix\xFF] → sorted account addresses
```

**AddressExact(address)** — point lookup on account existence index:
```
Lookup [0x0D][ledger\x00][address] → iterator with 0 or 1 element
```

Values can be hardcoded at query creation time, or parameterized (resolved at execution time).

#### NOT Operator

`NOT` requires the "universe" of all accounts to compute the complement:

```
NOT(MetadataEquals("status", "suspended"))
= all accounts in ledger EXCEPT those with status=suspended
```

Implementation: merge-difference between the full account existence index `[0x0D][ledger\x00]` and the child filter's iterator. Streaming, O(total_accounts).

**Performance caveat**: `NOT` as a top-level filter is expensive (scans all accounts). Under an `AND`, the intersection with a more selective filter reduces the scan early. The query planner should warn or reject queries where `NOT` is the outermost operator on a large ledger.

#### Proto Definition

```protobuf
message QueryFilter {
  oneof filter {
    MetadataMatch metadata = 1;
    AddressMatch address = 2;
    AndFilter and = 3;
    OrFilter or = 4;
    NotFilter not = 5;
  }
}

message MetadataMatch {
  string key = 1;
  oneof value {
    string hardcoded_value = 2;   // fixed at query creation time
    string parameter_name = 3;    // resolved at execution time
  }
}

message AddressMatch {
  oneof match {
    string hardcoded_prefix = 1;
    string hardcoded_exact = 2;
    string param_prefix = 3;      // parameter: prefix resolved at execution
    string param_exact = 4;       // parameter: exact resolved at execution
  }
}

message AndFilter {
  repeated QueryFilter filters = 1;
}

message OrFilter {
  repeated QueryFilter filters = 1;
}

message NotFilter {
  QueryFilter filter = 1;
}

message PreparedQuery {
  string name = 1;                           // unique within ledger
  string ledger = 2;
  QueryFilter filter = 3;                    // recursive filter tree
}

message CreatePreparedQueryRequest {
  PreparedQuery query = 1;
}

message ExecutePreparedQueryRequest {
  string ledger = 1;
  string query_name = 2;
  map<string, string> parameters = 3;        // values for parameter_name placeholders
  uint32 page_size = 4;
  string after_address = 5;                  // cursor for pagination
}
```

#### Lifecycle

1. **Create**: client sends `CreatePreparedQueryRequest` → Raft command → persisted in system zone
2. **Execute**: client sends `ExecutePreparedQueryRequest` → read-only, builds iterator tree from filter, streams results
3. **Delete**: client deletes prepared query → Raft command → definition cleaned up

The prepared query definition is stored in the system zone:

```
Key:   [0xE0][ledgerName\x00][queryName]
Value: PreparedQuery protobuf
```

#### Execution Example

```
Prepared query "active_premium_merchants":
  filter: AND(
    OR(
      MetadataEquals("category", hardcoded: "premium"),
      MetadataEquals("category", hardcoded: "gold")
    ),
    AddressPrefix(param: "prefix"),
    NOT(MetadataEquals("status", hardcoded: "suspended"))
  )

Execution with parameters: { "prefix": "merchants:" }

  iter1: scan [0x0C][ledger\x00][category\x00][premium\x00]    → [acc_A, acc_C, acc_F]
  iter2: scan [0x0C][ledger\x00][category\x00][gold\x00]       → [acc_B, acc_F, acc_G]
  iter3: OR(iter1, iter2)                                        → [acc_A, acc_B, acc_C, acc_F, acc_G]
  iter4: scan [0x0D][ledger\x00][merchants:]...[merchants:\xFF] → [acc_A, acc_B, acc_C, acc_F, acc_H]
  iter5: scan [0x0C][ledger\x00][status\x00][suspended\x00]    → [acc_B]
  iter6: NOT(iter5) via universe [0x0D][ledger\x00]              → [acc_A, acc_C, acc_F, acc_G, acc_H, ...]
  iter7: AND(iter3, iter4, iter6)                                → [acc_A, acc_C, acc_F]
```

All iterators are lazy and streaming — no intermediate array is materialized.

#### Iterator Resource Limits

Each leaf node opens one Pebble iterator. Complex queries with many leaves consume proportionally more file handles and block cache entries. Recommended limits:

| Constraint | Suggested limit | Rationale |
|-----------|----------------|-----------|
| Max leaf nodes per query | 20 | Pebble iterator overhead (~4KB each) |
| Max tree depth | 10 | Prevents pathological nesting |
| Max NOT without parent AND | Rejected | Prevents full-universe scans on large ledgers |

These limits are validated at **query creation time**, not execution time.

#### Why Prepared Queries vs. Ad-Hoc Filters?

1. **Validation at creation time**: verify well-formedness, check metadata keys against schema, enforce resource limits
2. **Named and discoverable**: clients can list available queries for a ledger
3. **Future optimizations**: materialized views, result caching, query plan caching
4. **API ergonomics**: a single `ExecutePreparedQuery` call with parameters instead of sending a complex filter tree on every request

### 5.9 Dedicated Read Store

#### Problem: I/O Contention

The inverted indexes (Section 5.6) and secondary indexes (Sections 5.5, 5.7) share the same Pebble instance and physical disk as the primary write path. On write-heavy workloads, Pebble's LSM compaction and WAL writes compete with read-side index scans for disk I/O bandwidth.

This is particularly concerning because:
- The Raft FSM write path is latency-sensitive (consensus critical path)
- Index scans for complex multi-criteria queries (AND/OR/NOT with 5+ iterators) may trigger large sequential reads
- LSM compaction already generates significant background I/O

#### Architecture

The index builder is an asynchronous worker, decoupled from the FSM. The read store is **self-contained** — it never reads from the primary store.

```
                    ┌──────────────────┐
                    │   Raft FSM       │
                    │  (primary store)  │
                    └────────┬─────────┘
                             │ committed entries (via channel or log tailing)
                             ▼
                    ┌──────────────────┐
                    │  Index Builder   │  ← single writer, processes entries in order
                    │  (async worker)  │
                    └────────┬─────────┘
                             │ index writes
                             ▼
                    ┌──────────────────┐
                    │  Read Index Store │  ← optionally on dedicated disk
                    └──────────────────┘
                             ▲
                             │ concurrent read queries
                    ┌──────────────────┐
                    │  Query Executor  │
                    └──────────────────┘
```

The **Index Builder**:
1. Tails the committed Raft log (or receives notifications from the FSM after apply)
2. For each applied entry, computes and writes index updates to the read store
3. Tracks its own progress (`lastIndexedRaftIndex`) persisted in the read store
4. Runs on each node independently — deterministic because it processes the same log in order

#### Read Store Autonomy — Reverse Metadata Map

The index builder must handle the old-value problem (Section 5.6) when updating metadata inverted indexes. Rather than reading from the primary store (which would create cross-store I/O dependency), the read store maintains its own **reverse metadata map**:

```
Reverse map:  [0x0E][ledgerName\x00][account\x00][metadataKey] → currentValue
Forward index: [0x0C][ledgerName\x00][metadataKey\x00][metadataValue\x00][accountAddress] → (empty)
```

When the index builder processes a `SetMetadata(account, key, newValue)` entry:

1. **Read** the old value from reverse map `[0x0E][ledger\x00][account\x00][key]` (local to read store)
2. **Delete** old forward index entry `[0x0C][ledger\x00][key\x00][oldValue\x00][account]`
3. **Insert** new forward index entry `[0x0C][ledger\x00][key\x00][newValue\x00][account]`
4. **Update** reverse map `[0x0E][ledger\x00][account\x00][key] = newValue`

This makes the two stores **fully independent** — no cross-disk I/O. The primary store can be on disk A, the read store on disk B, and neither ever reads from the other.

#### Storage Engine Choice

The read store has a very different I/O profile from the primary store:

- **Single writer** (index builder), low write throughput
- **Many concurrent readers** (query executors), heavy scan workload
- **Append-mostly** data (inverted indexes rarely update)
- **Analytical queries**: filtering, aggregation, multi-criteria intersection

Two fundamentally different approaches exist:

**Option A — Key-value store (bbolt) with custom query execution**

Use a B+ tree store and implement inverted indexes, merge-join iterators, and the AND/OR/NOT operator tree manually (as described in Sections 5.6-5.8).

bbolt (pure Go, B+ tree, used by etcd/Consul) is well-suited: single-writer model matches the index builder, sorted scans are native to B+ trees, and MVCC enables many concurrent readers.

Key prefixes in bbolt would mirror the Pebble design: `0x0C` (inverted index), `0x0D` (account existence), `0x0E` (reverse metadata map).

**Option B — Embedded analytical database (DuckDB) with SQL execution**

Use DuckDB as the read store and replace the entire custom indexing layer with relational tables and SQL queries.

```sql
CREATE TABLE accounts (
  ledger VARCHAR NOT NULL, account VARCHAR NOT NULL,
  PRIMARY KEY (ledger, account)
);
CREATE TABLE account_metadata (
  ledger VARCHAR NOT NULL, account VARCHAR NOT NULL,
  key VARCHAR NOT NULL, value VARCHAR NOT NULL,
  PRIMARY KEY (ledger, account, key)
);
CREATE TABLE account_transactions (
  ledger VARCHAR NOT NULL, account VARCHAR NOT NULL,
  tx_id BIGINT NOT NULL,
  PRIMARY KEY (ledger, account, tx_id)
);
CREATE TABLE ledger_logs (
  ledger VARCHAR NOT NULL, log_id BIGINT NOT NULL,
  global_sequence BIGINT NOT NULL,
  PRIMARY KEY (ledger, log_id)
);
```

The prepared query filter tree (AND/OR/NOT) compiles directly to a SQL `WHERE` clause. No custom merge-join, no inverted index key design, no iterator tree implementation.

The old-value problem disappears — `UPDATE account_metadata SET value = $1 WHERE ...` handles it natively.

**Comparison:**

| Criteria | bbolt + custom indexes | DuckDB |
|----------|----------------------|--------|
| Custom code to write | Significant (indexes, merge-join, iterator tree, NOT) | Minimal (SQL generation, schema) |
| Scan performance | Good (B+ tree) | Excellent (columnar, vectorized, SIMD) |
| Aggregations | Manual implementation | Native `SUM`, `COUNT`, `GROUP BY` |
| Query optimizer | None (hand-coded execution) | Sophisticated (join reordering, predicate pushdown) |
| Pure Go | Yes | No (CGo via `github.com/marcboeker/go-duckdb`) |
| Binary size impact | Negligible | +30-50 MB |
| Future query flexibility | Each new query = new code | SQL covers nearly everything |
| Operational maturity in Go | Very mature (etcd) | Younger Go bindings |

**Recommendation: DuckDB if CGo is acceptable, bbolt otherwise.**

The read store is fundamentally an analytical problem (scan, filter, aggregate), and DuckDB is purpose-built for it. The code simplification is dramatic — thousands of lines of custom indexing replaced by SQL. It also opens the door to queries not yet anticipated.

If the CGo boundary (cross-compilation complexity, potential memory bugs at the CGo boundary, build time) is a deal-breaker, bbolt with the custom iterator tree design is the fallback. See the [prepared queries draft](./prepared-queries.md) for the detailed design of both approaches.

#### Consistency Model

The read index store is **eventually consistent** with a bounded lag:

- After a write is committed via Raft and applied by the FSM, the index builder processes it asynchronously
- Typical lag: milliseconds (bounded by disk I/O of the read store)
- The `lastIndexedRaftIndex` is exposed via API so clients can check freshness
- For queries requiring strong consistency, the query executor can wait until `lastIndexedRaftIndex >= targetIndex`

```protobuf
message ExecutePreparedQueryRequest {
  // ...existing fields...
  uint64 min_raft_index = 6;  // Optional: wait until indexes catch up to this index
}
```

#### Is a Dedicated Disk Necessary?

**No, but it helps.** Analysis:

The index builder is asynchronous, so it **never blocks the FSM**. Whether both stores share a disk or not, the consensus critical path is unaffected.

The real question is: does **read query I/O** interfere with **primary store compaction I/O**?

| Disk type | Contention risk | Recommendation |
|-----------|----------------|----------------|
| **NVMe SSD** (~500K IOPS, 3-7 GB/s) | Low — bandwidth is sufficient for both stores | Same disk is fine |
| **SATA SSD** (~50K IOPS, 500 MB/s) | Medium — complex queries + compaction can saturate | Dedicated disk recommended |
| **HDD** | High — sequential scans compete with random I/O | Dedicated disk required |

Since the two stores are fully independent (no cross-disk reads thanks to the reverse map), a dedicated disk provides **complete I/O isolation** with no architectural overhead.

Note: the Raft WAL is already recommended on a separate disk (`--wal-dir`). Adding a third disk for the read store follows the same pattern:

```
Disk 1 (fast, small):  --wal-dir       /mnt/nvme-wal/        ← Raft WAL (latency-critical)
Disk 2 (large):        --data-dir      /mnt/ssd-data/        ← Primary Pebble store
Disk 3 (large):        --read-index-dir /mnt/ssd-read/       ← Read index store (bbolt)
```

For simpler deployments, disk 2 and disk 3 can be the same. The async index builder ensures the FSM is never impacted regardless.

#### Configuration

```
--read-index-dir    /mnt/read-ssd/indexes    (separate disk mount point)
--read-index-enable true                       (opt-in, disabled by default)
```

If `--read-index-dir` is not specified, the read index store defaults to `{data-dir}/read-indexes/` (same disk, still benefits from async writes keeping the FSM fast).

#### What Lives Where

| Store | Engine | Data | Populated by |
|-------|--------|------|-------------|
| **Primary** (existing) | Pebble (LSM) | Volumes, metadata, logs, tx updates, system config, periods, prepared query definitions (`0xE0`) | FSM apply (synchronous) |
| **Read index** (new) | bbolt (B+ tree) | Inverted metadata index (`0x0C`), reverse metadata map (`0x0E`), account existence index (`0x0D`), per-ledger log index (`0x05`), account-tx index (`0x0B`), `lastIndexedRaftIndex` | Index builder (asynchronous) |

#### Crash Recovery

If the read store is lost or corrupted:
1. The primary store is unaffected (it contains all authoritative data)
2. `store rebuild-indexes` replays all logs from the primary store and reconstructs the read store
3. During rebuild, prepared queries return an error indicating indexes are not ready
4. No data loss — the read store is entirely derived data

### 5.10 Scatter-Gather for Parallel Query Execution

#### Concept

In a multi-node Raft cluster, read queries can be executed on **any node** (leader or follower). For large datasets, a query can be split across nodes:

1. **Scatter**: the coordinator node splits the key range into N partitions (one per node)
2. **Gather**: each node scans its partition locally and returns partial results
3. **Merge**: the coordinator merges partial results into the final response

This enables near-linear horizontal scaling of read throughput.

#### Applicability

| Query | Scatter-Gather benefit |
|-------|----------------------|
| AggregateBalances | High — partition accounts by prefix ranges, aggregate per node, merge sums |
| ListAccounts with metadata filter | Medium — partition account ranges, merge-join per node, merge sorted results |
| ListTransactions by account | Low — data is already scoped to one account |
| Ledger stats | None — already O(1) from boundaries |

#### Design Sketch

```
Coordinator (any node):
  1. Get account address range for ledger [min_addr, max_addr]
  2. Split into N roughly equal ranges based on known account distribution
  3. Send sub-query to each node with its range
  4. Merge results (union for list, sum for aggregation)
```

This is a future optimization — the single-node inverted index approach (Sections 5.6-5.8) should be implemented first. Scatter-gather adds value primarily at scale (millions of accounts per ledger).

## 6. New Key Prefixes

Summary of all key prefix additions:

| Prefix | Key | Value | Feature |
|--------|-----|-------|---------|
| `0x05` | `[ledgerName\x00][ledgerLogID (8B)]` | `globalSequence (8B)` | ListLogs per ledger |
| `0x0B` | `[ledgerName\x00][account\x00][txID (8B)]` | (empty) | Transactions by account |
| `0x0C` | `[ledgerName\x00][metadataKey\x00][metadataValue\x00][accountAddress]` | (empty) | Metadata inverted index |
| `0x0D` | `[ledgerName\x00][accountAddress]` | (empty) | Account existence index |
| `0x0E` | `[ledgerName\x00][account\x00][metadataKey]` | `currentValue (string)` | Reverse metadata map (read store only) |
| `0xE0` | `[ledgerName\x00][queryName]` | `PreparedQuery protobuf` | Prepared query definitions |

Note: `0x05`, `0x0B`, `0x0C`, `0x0D` are in the cold-storable range (can be rebuilt from logs). `0xE0` is in the system range (below the attribute zone, above the cold-storable zone — existing system prefixes use `0xEE`+, `0xF2`+). If the dedicated read store (Section 5.9) is enabled, `0x0C`, `0x0D`, `0x05`, and `0x0B` live in the read index store; `0xE0` stays in the primary store.

## 7. gRPC Service Additions

```protobuf
service BucketService {
  // ... existing RPCs (ListLedgers, GetLedger, GetAccount, GetTransaction,
  //     ListTransactions, ListAccounts, ListLogs, ListAuditEntries,
  //     GetAuditEntry, ListPeriods, ListSigningKeys, ...) ...

  // New read RPCs
  rpc AggregateBalances(AggregateBalancesRequest) returns (AggregateBalancesResponse);
  rpc ListLedgerLogs(ListLedgerLogsRequest) returns (stream common.Log);  // per-ledger (ListLogs global already exists)
  rpc GetLedgerStats(GetLedgerStatsRequest) returns (GetLedgerStatsResponse);
  rpc ListAccountTransactions(ListAccountTransactionsRequest) returns (stream common.Transaction);

  // Prepared queries
  rpc CreatePreparedQuery(CreatePreparedQueryRequest) returns (CreatePreparedQueryResponse);
  rpc DeletePreparedQuery(DeletePreparedQueryRequest) returns (DeletePreparedQueryResponse);
  rpc ListPreparedQueries(ListPreparedQueriesRequest) returns (ListPreparedQueriesResponse);
  rpc ExecutePreparedQuery(ExecutePreparedQueryRequest) returns (stream common.Account);
}
```

## 8. HTTP Compatibility Layer

| HTTP Endpoint | gRPC Method | Notes |
|---------------|-------------|-------|
| `GET /{ledger}/aggregate/balances` | `AggregateBalances` | Query param: `address_prefix` |
| `GET /{ledger}/logs` | `ListLedgerLogs` | Query params: `page_size`, `cursor` (per-ledger; global `ListLogs` already exists) |
| `GET /{ledger}/stats` | `GetLedgerStats` | |
| `GET /{ledger}/accounts/{address}/transactions` | `ListAccountTransactions` | Query params: `page_size`, `cursor` |
| `POST /{ledger}/prepared-queries` | `CreatePreparedQuery` | Body: query definition |
| `DELETE /{ledger}/prepared-queries/{name}` | `DeletePreparedQuery` | |
| `GET /{ledger}/prepared-queries` | `ListPreparedQueries` | |
| `POST /{ledger}/prepared-queries/{name}/execute` | `ExecutePreparedQuery` | Body: parameters; Query params: `page_size`, `cursor` |

## 9. Implementation Plan

### Phase 1 — No New Index Required (quick wins)

These features use only the existing Pebble data layout:

1. **AggregateBalances**: range scan on volume attributes, accumulate per asset
2. **Ledger stats** (partial): transaction count and log count from existing `LedgerBoundaries`

Estimated changes:
- `internal/application/ctrl/controller.go` — add methods to Controller interface
- `internal/application/ctrl/controller_default.go` — implement methods
- `internal/application/ctrl/store.go` — add `AggregateVolumes` function
- `misc/proto/service.proto` — add RPCs and messages
- `internal/adapter/grpc/server_bucket.go` — add gRPC handlers
- `internal/adapter/http/` — add HTTP handlers (one per file)
- `cmd/ledgerctl/` — add CLI commands

### Phase 2 — Lightweight Secondary Index

Requires adding writes during FSM log application:

4. **ListLedgerLogs** (per-ledger): add `[0x05][ledgerName\x00][ledgerLogID] → sequence` index. Note: global `ListLogs(afterSequence, pageSize)` already exists in `bucket.proto` and `DefaultController`.
5. **Ledger stats** (account count): add `account_count` field to `LedgerBoundaries` (field 4, since field 3 is reserved)

Estimated changes:
- `internal/storage/dal/store.go` — add `keyPrefixLedgerLog` and `ListLedgerLogSequences`
- `internal/infra/state/machine.go` (or `buffered.go`) — populate index during log application
- `misc/proto/common.proto` — add `account_count` to `LedgerBoundaries`
- `misc/proto/bucket.proto` — add `ListLedgerLogs` RPC and `ListLedgerLogsRequest` message

### Phase 3 — Write-Path Secondary Index

Requires adding writes during transaction processing:

6. **Transactions by account**: add `[0x0B][ledgerName\x00][account\x00][txID]` index

Estimated changes:
- `internal/storage/dal/store.go` — add `keyPrefixAccountTransaction` and iteration methods
- `internal/infra/state/machine.go` — populate index during transaction application
- Proto + gRPC + HTTP + CLI additions

### Phase 4 — Metadata Inverted Index & Prepared Queries

Requires extending the preload mechanism and adding async index building:

7. **Generic metadata inverted index**: `[0x0C]` prefix, populated for all metadata writes
8. **Account existence index**: `[0x0D]` prefix, populated on first account volume write
9. **Prepared query CRUD**: Raft commands for creating/deleting prepared queries, stored in `[0xE0]`
10. **Iterator tree executor**: merge-intersect, merge-union, merge-difference on sorted iterators for AND/OR/NOT
11. **Dedicated read store** (optional): separate Pebble instance for read indexes

Estimated changes:
- `internal/storage/dal/read_store.go` — new Pebble instance for read indexes (optional)
- `internal/storage/dal/index_builder.go` — async worker that tails Raft log and writes indexes
- `internal/service/state/machine.go` — extend preload to include old metadata values
- `internal/service/ctrl/controller_default.go` — merge-join executor, prepared query CRUD
- `internal/service/ctrl/merge_join.go` — streaming sorted iterator intersection
- `misc/proto/service.proto` — PreparedQuery messages and RPCs
- `internal/application/grpc_ledger_server.go` — gRPC handlers
- `internal/compat/http/` — HTTP handlers (one per endpoint)
- `cmd/server/server.go` — `--read-index-dir`, `--read-index-enable` flags

### Phase 5 — Scatter-Gather (Future)

11. **Query partitioning**: split key ranges across nodes for parallel execution
12. **Result merging**: merge partial results from multiple nodes

This phase depends on having a multi-node cluster with read replicas.

### Data Migration Note

For Phases 2, 3, and 4, existing data will not have the secondary indexes populated. Two options:
- **Backfill**: scan existing logs and populate indexes (one-time migration, can be a CLI command)
- **Progressive**: indexes are only populated for new data; old data falls back to full scan

Backfill is recommended for correctness and can be implemented as a `store rebuild-indexes` CLI command that reads all logs and populates the missing indexes. This command also serves as the recovery mechanism for the dedicated read store — if the read store is lost or corrupted, `store rebuild-indexes` reconstructs it from the primary store.

## 10. Performance Considerations

### Pebble Range Scan Characteristics

| Operation | I/O Pattern | Cache Behavior |
|-----------|-------------|----------------|
| AggregateBalances | Sequential scan + ComputeValue per entry | Excellent — all data under same prefix |
| ListLogs | Index scan + random GetLogBySequence | Two-level lookup, log data may not be cached |
| Transactions by account | Index scan + random buildTransaction | Index is small and cached; tx reconstruction is heavier |
| Metadata filter (single) | Sequential scan on inverted index prefix | Excellent — sorted, adjacent keys |
| Metadata filter (multi) | N parallel prefix scans + merge-join | Good — each scan is sequential; merge is CPU-bound |
| Prepared query execution | Same as multi-filter + optional post-filter | Depends on selectivity of filters |

### Write Overhead of Secondary Indexes

| Index | Extra Writes per Operation | Key Size | Value Size |
|-------|------------------------------|----------|------------|
| Ledger log index (`0x05`) | 1 per log | 13 bytes | 8 bytes |
| Account-tx index (`0x0B`) | 2-4 per transaction (one per account) | ~25 bytes | 0 bytes |
| Metadata inverted index (`0x0C`) | 1 delete + 1 set per metadata change | ~40-80 bytes | 0 bytes |
| Account existence index (`0x0D`) | 1 per new account (append-only) | ~20-50 bytes | 0 bytes |

For the metadata index: metadata writes are infrequent compared to volume updates. A typical workload might have 1 metadata change per 100 transactions. The overhead is negligible.

If using the dedicated read store (Section 5.8), the metadata index writes don't touch the primary store at all — zero impact on the FSM write path.

### Compaction Impact

Secondary indexes are append-only (no updates, no deletes in normal operation). This is ideal for LSM trees — no write amplification from compaction merges. The indexes will naturally sort into the lowest levels of the LSM tree over time.

The metadata inverted index is an exception: metadata updates cause delete+insert pairs. However, metadata is rarely updated after initial account setup, so in practice the index is nearly append-only.

### Merge-Join Performance

The merge-join intersection of N sorted iterators has complexity O(N × S) where S is the size of the smallest result set. In practice:

- **High-selectivity filters** (few matches): the merge-join terminates quickly because the smallest iterator is short
- **Low-selectivity filters** (many matches): more scanning, but the result set is also larger (amortized cost per result is constant)
- **Worst case**: all filters match all accounts → degrades to N full scans. This is inherent to any filtering approach.

For typical use cases (10-20% selectivity), a 2-filter merge-join on 1M accounts scans ~200K entries per iterator — sub-second on SSD.

## 11. Decisions Record

| Topic | Decision | Rationale |
|---|---|---|
| **Pagination model** | Cursor-based (last address/txID) | Consistent with existing ListTransactions pattern. Offset-based is unreliable with concurrent writes. |
| **Ledger log index** | New `0x05` prefix | O(1) per log write, enables O(ledger_logs) per-ledger iteration instead of O(total_logs) |
| **Account-tx index** | New `0x0B` prefix | Enables account-centric transaction views without full scan |
| **Point-in-time reads** | Out of scope | Compaction destroys historical diffs — `ComputeValue(pastIndex)` returns incorrect results. See Section 5.5 for alternatives. |
| **Account count** | Counter in LedgerBoundaries | Avoids expensive count-by-scan; incremented during normal write path |
| **Metadata index** | Generic (all metadata), not per-query | Avoids per-query backfill, simpler lifecycle, metadata writes are rare enough that indexing everything is cheap |
| **Multi-filter strategy** | Recursive iterator tree (AND/OR/NOT) on sorted iterators | Leverages Pebble's natural key ordering, constant memory, composable to arbitrary depth |
| **Account existence index** | New `0x0D` prefix | Enables address prefix/exact filters, NOT operator universe, account counting |
| **Filter model** | Recursive boolean tree, not flat AND-only | Supports AND, OR, NOT on metadata and address filters; validated at creation time |
| **Index population** | Asynchronous via index builder | Keeps FSM apply fast (RAM-only), no I/O contention on write path |
| **Read store engine** | bbolt (B+ tree) instead of Pebble (LSM) | Read-heavy workload: B+ tree avoids LSM multi-level merge on scans; single-writer model matches index builder; pure Go |
| **Read store autonomy** | Reverse metadata map (`0x0E`) for old-value resolution | Eliminates cross-store I/O; read store and primary store are fully independent; clean dedicated disk story |
| **Dedicated disk** | Optional, not required | Async index builder already decouples FSM; NVMe can handle both stores; dedicated disk recommended for SATA SSD |
| **Consistency model** | Eventually consistent reads with optional freshness wait | Bounded lag (milliseconds), opt-in strong consistency via `min_raft_index` parameter |

## 12. Open Questions for Team

1. **AggregateBalances performance**: for ledgers with millions of accounts, a full aggregation scan could take seconds. Should we add a time limit or streaming aggregation? Or is this acceptable given it's a read-only operation on a follower?
2. **Index backfill**: should `store rebuild-indexes` be a blocking CLI command, or a background operation with progress streaming (like `store check`)?
3. **Interaction with data retention**: when periods are archived and purged, secondary indexes for purged data should also be cleaned up. The per-ledger log index (`0x05`) can use range delete on `[0x05][ledgerName\x00][startLogID]...[endLogID]`. The account-tx index (`0x0B`) requires scanning to find entries in the purged range — should we store txID in big-endian so range delete works? (Note: txID is already stored big-endian via `PutUInt64`.)
4. **Point-in-time reads**: the compaction strategy destroys historical data (see Section 5.4). If PIT becomes a requirement, the most promising approach is log replay from period boundary snapshots (builds on the data retention draft). Should we plan for this, or is current-state-only sufficient?
5. **Metadata preload cost**: extending the admission preload to include old metadata values adds Pebble reads on the leader during admission. For high-throughput metadata updates, this could become a bottleneck. Should we batch metadata preloads? Or is the async index builder (Section 5.8) sufficient to avoid this entirely?
6. **Eventually consistent reads**: is the eventual consistency model (Section 5.8) acceptable for all use cases? Some clients may expect read-after-write consistency for metadata filtering. The `min_raft_index` opt-in mechanism adds latency.
7. **Metadata value representation in index keys**: metadata values can be arbitrary strings. Should we hash long values to keep index keys bounded? Risk: hash collisions require post-filtering. Alternative: truncate to N bytes + hash suffix.
8. **Prepared query limits**: should there be a maximum number of prepared queries per ledger? Each prepared query doesn't create a separate index (they all use the generic metadata index), but query definitions still consume storage and must be replicated via Raft.
9. **Inverted index for transaction metadata**: the current design indexes account metadata only. Should transaction metadata also be indexed? Same pattern (`[0x0D][ledger][key][value][txID]`), but transactions are immutable so no old-value problem.
