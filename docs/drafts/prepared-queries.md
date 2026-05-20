# Draft — Prepared Queries

**Status**: Draft for team review
**Author**: Geoffrey + Claude
**Date**: 2026-02-24
---

## 1. Problem Statement

The ledger's storage is write-oriented. Read capabilities are limited to primary-key lookups and sequential scans. There is no support for:

- **Account metadata filtering**: "List accounts where `category=premium`"
- **Transaction metadata filtering**: "List transactions where `amount >= 5000`"
- **Multi-criteria filtering**: "List accounts where `category=premium AND region=eu`"
- **Boolean combinations**: "List accounts where `(category=premium OR category=gold) AND NOT status=suspended`"
- **Typed range queries**: "List transactions where `amount >= 10000`" (leveraging the typed metadata system)
- **Address-based filtering combined with metadata**: "List transactions involving merchants with amount >= 5000"

The reference implementation (`github.com/formancehq/ledger`) supports metadata filtering via SQL `WHERE` clauses — a capability that requires a fundamentally different approach in an embedded key-value store.

**Prepared queries** allow clients to define named, parameterized query templates. The system pre-validates them at creation time and executes them efficiently using secondary indexes built asynchronously from the Raft log.

## 2. Goals

The read store serves **three types of queries**, all with advanced filtering:

1. **List accounts** — filter accounts by metadata and/or address, return matching account addresses
2. **List transactions** — filter transactions by metadata and/or account involvement, return matching transaction IDs
3. **Aggregate volumes** — filter accounts by metadata and/or address, then retrieve and aggregate volumes (per-asset input/output/balance) for matching accounts

Design goals:

1. Allow clients to filter **accounts** or **transactions** by metadata key/value pairs with boolean operators (AND, OR, NOT)
2. Allow clients to filter by **account address** (prefix or exact match) — for account queries directly, for transaction queries via posting involvement
3. Allow **mixing** metadata and address filters in a single query
4. Support **parameterized** filters (values resolved at execution time)
5. Execute queries efficiently via **secondary indexes**, not full scans
6. Keep the **FSM apply fast** — index maintenance happens asynchronously
7. Leverage the **typed metadata system** — support range/comparison operators on integer types, boolean checks, not just string equality
8. Support **volume aggregation** on filtered accounts — filter then aggregate, reusing the same prepared query filter model

## 3. Scope

### In scope

- **Three query modes**: list accounts, list transactions, aggregate volumes — all with advanced filtering
- Recursive boolean filter model (AND/OR/NOT) on metadata and addresses
- **Typed metadata conditions**: string equality, integer ranges (>, <, >=, <=, BETWEEN), boolean checks, existence checks — leveraging the existing typed metadata system (`MetadataType` enum: `STRING`, `INT64`, `BOOL`, `UINT64`, sub-64-bit integer types)
- **Address filters on transactions**: filter transactions by accounts involved in their postings (source or destination)
- **Volume aggregation**: filter accounts via prepared query, then retrieve volumes from primary store and aggregate per asset
- Prepared query CRUD (create, update, list, delete) via Raft commands
- Prepared query execution (read-only) with parameter substitution
- Asynchronous index building from Raft log
- Dedicated read store (bbolt B+ tree, optionally on separate disk)

### Out of scope

- Full-text search / fuzzy matching on metadata values
- Volume-based filters (e.g., "balance > 1000") — future work, could be added as a `FieldRef` source
- **Cross-entity conditions**: e.g., `AccountMetadata` in a transaction query or `TransactionMetadata` in an account query — future work, requires cross-table joins
- Cross-ledger queries
- Real-time (synchronous) index updates in FSM apply

## 4. Architecture Overview

```
                    ┌──────────────────┐
                    │   Raft FSM       │
                    │  (primary store)  │  ← Pebble: volumes, metadata, logs
                    └────────┬─────────┘
                             │ accepted logs only (see Section 7.1)
                             ▼
                    ┌──────────────────┐
                    │  Index Builder   │  ← single writer, processes entries in order
                    │  (async worker)  │
                    └────────┬─────────┘
                             │ index writes
                             ▼
                    ┌──────────────────┐
                    │  Read Index Store │  ← bbolt (B+ tree), optionally on dedicated disk
                    └──────────────────┘
                             ▲
                             │ filter queries (list accounts, list transactions)
                    ┌──────────────────┐
                    │  Query Executor  │  ← builds iterator tree from filter
                    └──────┬───────────┘
                           │ for volume aggregation: sorted account list
                           ▼
                    ┌──────────────────┐
                    │  Primary Store   │  ← Pebble: volume prefix scan for matching accounts
                    │  (read-only)     │
                    └──────────────────┘
```

**Three query modes:**

| Mode | Filter in | Data from | Returns |
|------|----------|-----------|---------|
| **List accounts** | bbolt (metadata + address) | bbolt | Paginated account addresses |
| **List transactions** | bbolt (metadata + address) | bbolt | Paginated transaction IDs |
| **Aggregate volumes** | bbolt (metadata + address) | bbolt (filter) + Pebble (volumes) | Per-asset aggregated volumes |

**Key design decisions:**

- The **index builder** runs asynchronously, outside the FSM hot path. The FSM apply remains RAM-only.
- The index builder consumes **only accepted operations** — not raw Raft entries. Proposals can be rejected by the FSM (insufficient funds, idempotency conflict, already reverted, etc.) and must not be indexed (see Section 7.1).
- The **read store** (bbolt) is self-contained for metadata/address filtering — it never performs random reads against the primary Pebble store (see Section 7.2: Reverse Metadata Map).
- **Volume aggregation** is the exception: after filtering accounts in bbolt, volumes are read from the primary Pebble store via sequential prefix scans (see Section 9).
- **Prepared query definitions** are stored in the primary store via Raft commands (`[0xE0]`). The read store contains only derived index data.
- The read store is a **pure derived index** — it holds no authoritative data. Visibility lag is bounded (sub-millisecond on NVMe). Clients can opt into freshness guarantees via `min_log_sequence`.
- **bbolt** is read-optimized (B+ tree, single-level, zero read amplification) with native MVCC — concurrent readers never block the index builder writer. This matches our I/O profile: single writer (index builder), many concurrent readers (query executors).

## 5. Filter Model

### 5.1 Recursive Filter Tree

Each prepared query declares a **target type** — `ACCOUNTS` or `TRANSACTIONS` — which determines what entity is returned and how filters are interpreted.

A prepared query filter is a **tree of boolean operators** where leaves are either field conditions or address matches. Each node in the tree produces a **sorted iterator of entity IDs** (account addresses or transaction IDs). Operators compose iterators:

```
AND(a, b)  →  merge-intersect of sorted iterators
OR(a, b)   →  merge-union of sorted iterators
NOT(a)     →  merge-difference against existence index
```

A **field condition** pairs a **field reference** (where to look) with a **condition** (what to match):

```
FieldCondition = FieldRef × Condition
```

Examples:

```
-- String equality + boolean combination on account metadata
AND(
  OR(
    StringEquals(AccountMetadata("category"), "premium"),
    StringEquals(AccountMetadata("category"), "gold")
  ),
  AddressPrefix("merchants:"),
  NOT(StringEquals(AccountMetadata("status"), "suspended"))
)

-- Typed range on integer metadata
AND(
  IntRange(AccountMetadata("credit_limit"), min: 10000),
  BoolEquals(AccountMetadata("active"), true),
  AddressPrefix("merchants:")
)

-- Mixed: account address + transaction metadata
AND(
  AddressPrefix("merchants:"),
  IntRange(TransactionMetadata("amount"), min: 5000)
)
```

### 5.2 Leaf Nodes

Leaf nodes are either **field conditions** (FieldRef + Condition) or **address matches**. Each value can be **hardcoded** (fixed at query creation) or **parameterized** (resolved at execution time via `$param_name`).

#### Field references

A `FieldRef` identifies **where** to look for the value. The field source must match the query's target type:

| FieldRef | Description | bbolt index prefix | Valid target |
|----------|-------------|-------------------|--------------|
| `AccountMetadata("key")` | Metadata on accounts | `[0x0C][ledger\x00][a:]` | `ACCOUNTS` only |
| `TransactionMetadata("key")` | Metadata on transactions | `[0x0C][ledger\x00][t:]` | `TRANSACTIONS` only |

Cross-entity conditions (e.g., `AccountMetadata` in a `TRANSACTIONS` query) are rejected at creation time — see Section 3 (Out of scope).

The condition type must be compatible with the declared `MetadataType` for the key. If the ledger has a schema declaring `credit_limit` as `INT64`, only `IntCondition` is valid for that key. For untyped keys (no schema), only `StringCondition` is allowed.

This design is extensible — future `FieldRef` sources (e.g., `Volume("USD/2")`) can reuse the same conditions without new message types.

#### Conditions (typed)

| Condition | Matches | Types | Example |
|-----------|---------|-------|---------|
| **StringCondition** | Exact equality on string value | `STRING` (or untyped) | `StringEquals(AccountMetadata("category"), "premium")` |
| **IntCondition** | Range/equality on signed integer | `INT8`..`INT64` | `IntRange(AccountMetadata("credit_limit"), min: 10000)` |
| **UintCondition** | Range/equality on unsigned integer | `UINT8`..`UINT64` | `UintRange(AccountMetadata("retry_count"), max: 5)` |
| **BoolCondition** | Equality on boolean | `BOOL` | `BoolEquals(AccountMetadata("active"), true)` |
| **ExistsCondition** | Key exists on target | Any | `Exists(AccountMetadata("email"))` |

**IntCondition / UintCondition** support flexible ranges:

| `min` | `max` | Behavior | bbolt scan |
|-------|-------|----------|------------|
| set | omitted | `>= min` (or `> min` if exclusive) | `Seek(encode(min))` → scan to end of key prefix |
| omitted | set | `<= max` (or `< max` if exclusive) | `Seek(typeTag)` → scan until `encode(max+1)` |
| set | set | BETWEEN | `Seek(encode(min))` → scan until `encode(max+1)` |
| `min = max` | (same) | Equality | Point scan on `encode(value)` |

#### Address conditions

`AddressMatch` is valid for both target types, but its semantics differ:

| Target | AddressPrefix("merchants:") | AddressExact("merchants:acme_corp") |
|--------|----------------------------|-------------------------------------|
| `ACCOUNTS` | Accounts whose address starts with `merchants:` | The single account `merchants:acme_corp` |
| `TRANSACTIONS` | Transactions with at least one posting involving an account matching `merchants:*` | Transactions with at least one posting involving `merchants:acme_corp` |

For transaction queries, `AddressMatch` scans the account-transaction mapping (`[0x0F]`) to find transactions linked to matching accounts.

### 5.3 NOT Operator

`NOT` requires the "universe" of all entities to compute the complement. This means:

```
-- ACCOUNTS target
NOT(StringEquals(AccountMetadata("status"), "suspended"))
= all accounts in ledger EXCEPT those with status=suspended

-- TRANSACTIONS target
NOT(IntRange(TransactionMetadata("amount"), min: 10000))
= all transactions in ledger EXCEPT those with amount >= 10000
```

**Performance caveat**: `NOT` as a top-level filter is expensive (scans all entities). Under an `AND`, the intersection with a more selective filter reduces the scan early. The query validator should **reject** queries where `NOT` is the outermost operator on a large ledger.

### 5.4 Proto Definition

```protobuf
// ============================================================================
// Filter tree
// ============================================================================

message QueryFilter {
  oneof filter {
    FieldCondition field = 1;       // field reference + typed condition
    AddressMatch address = 2;       // account address match
    AndFilter and = 3;
    OrFilter or = 4;
    NotFilter not = 5;
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

// ============================================================================
// Field reference — identifies WHERE to look
// ============================================================================

// FieldRef identifies the data source for a condition.
// Extensible: future sources (e.g., volumes) can be added as new oneof fields.
message FieldRef {
  oneof source {
    string account_metadata = 1;        // account metadata key name
    string transaction_metadata = 2;    // transaction metadata key name
    // future: string volume_asset = 3;
  }
}

// ============================================================================
// Field condition — pairs FieldRef with a typed condition
// ============================================================================

// FieldCondition = FieldRef × Condition.
// The condition type must match the declared MetadataType for the referenced key
// (enforced at query creation time and re-validated at execution time).
message FieldCondition {
  FieldRef field = 1;
  oneof condition {
    StringCondition string_cond = 2;
    IntCondition int_cond = 3;
    UintCondition uint_cond = 4;
    BoolCondition bool_cond = 5;
    ExistsCondition exists_cond = 6;
  }
}

// ============================================================================
// Conditions — reusable operators, independent of the data source
// ============================================================================

// StringCondition: exact equality on string-typed field.
message StringCondition {
  oneof value {
    string hardcoded = 1;               // fixed at query creation
    string param = 2;                   // resolved at execution ($param_name)
  }
}

// IntCondition: range/equality on signed integer field (int8 through int64).
// Omit min for no lower bound, omit max for no upper bound.
// Both min and max omitted = matches any integer value for this field.
message IntCondition {
  optional int64 min = 1;               // lower bound (hardcoded)
  optional int64 max = 2;               // upper bound (hardcoded)
  bool min_exclusive = 3;               // true: >, false: >= (default: >=)
  bool max_exclusive = 4;               // true: <, false: <= (default: <=)
  string param_min = 5;                 // if set, overrides min with named parameter
  string param_max = 6;                 // if set, overrides max with named parameter
}

// UintCondition: range/equality on unsigned integer field (uint8 through uint64).
message UintCondition {
  optional uint64 min = 1;
  optional uint64 max = 2;
  bool min_exclusive = 3;
  bool max_exclusive = 4;
  string param_min = 5;
  string param_max = 6;
}

// BoolCondition: equality on boolean field.
message BoolCondition {
  oneof value {
    bool hardcoded = 1;
    string param = 2;
  }
}

// ExistsCondition: checks if the referenced field exists on the target.
message ExistsCondition {
  bool include_null = 1;                // if true, matches NullValue entries too (default: false)
}

// ============================================================================
// Address match — standalone leaf (not a FieldCondition, addresses are not metadata)
// ============================================================================

message AddressMatch {
  oneof match {
    string hardcoded_prefix = 1;
    string hardcoded_exact = 2;
    string param_prefix = 3;            // parameter: prefix resolved at execution
    string param_exact = 4;             // parameter: exact resolved at execution
  }
}

// ============================================================================
// Query target — determines what entity type is returned
// ============================================================================

enum QueryTarget {
  ACCOUNTS = 0;                         // returns account addresses
  TRANSACTIONS = 1;                     // returns transaction IDs
}
```

### 5.5 Resource Limits

Each leaf node opens a bbolt cursor. Recommended limits, validated at **query creation time**:

| Constraint | Suggested limit | Rationale |
|-----------|----------------|-----------|
| Max leaf nodes per query | 20 | Cursor overhead (~4KB each) |
| Max tree depth | 10 | Prevents pathological nesting |
| NOT as outermost operator | Rejected | Prevents full-universe scans on large ledgers |

## 6. Prepared Query Lifecycle

### 6.1 CRUD

A prepared query is a **named resource** scoped to a ledger.

```protobuf
message PreparedQuery {
  string name = 1;                           // unique within ledger
  string ledger = 2;
  QueryFilter filter = 3;                    // recursive filter tree
  QueryTarget target = 4;                    // ACCOUNTS or TRANSACTIONS
}

message CreatePreparedQueryRequest {
  PreparedQuery query = 1;
}

message ExecutePreparedQueryRequest {
  string ledger = 1;
  string query_name = 2;
  map<string, string> parameters = 3;        // parameter values as strings, parsed to target type at execution
                                              // e.g., "50000" → int64(50000) for IntCondition.param_min
  uint32 page_size = 4;                      // max results per page (default 15, max 1000); ignored for AGGREGATE_VOLUMES
  string cursor = 5;                         // opaque cursor from previous response (if set, other fields are ignored); ignored for AGGREGATE_VOLUMES
  uint64 min_log_sequence = 6;               // optional: wait until read store has indexed up to this log sequence
  QueryMode mode = 7;                        // LIST (default) or AGGREGATE_VOLUMES
}

enum QueryMode {
  LIST = 0;                                   // list entities (paginated) — default
  AGGREGATE_VOLUMES = 1;                      // aggregate volumes for matching accounts (ACCOUNTS target only)
}

message ExecutePreparedQueryResponse {
  oneof result {
    Cursor cursor = 1;                        // for LIST mode
    AggregateResult aggregate = 2;            // for AGGREGATE_VOLUMES mode
  }
}

// AggregatedVolume represents the total input/output for a single asset
// across all accounts matching the filter.
message AggregatedVolume {
  string asset = 1;
  common.Uint256 input = 2;
  common.Uint256 output = 3;
}

message AggregateResult {
  repeated AggregatedVolume volumes = 1;
}

// Cursor follows the reference implementation pattern (bunpaginate).
// The cursor string is a base64-encoded JSON containing the full pagination state.
// The data field depends on the query's target type:
//   ACCOUNTS     → account_data is populated (list of account addresses)
//   TRANSACTIONS → transaction_data is populated (list of transaction IDs)
message Cursor {
  uint32 page_size = 1;
  bool has_more = 2;
  string previous = 3;                       // opaque cursor to previous page (empty if first page)
  string next = 4;                           // opaque cursor to next page (empty if last page)
  repeated string account_data = 5;          // for ACCOUNTS target
  repeated uint64 transaction_data = 6;      // for TRANSACTIONS target
}
```

- **Create**: `CreatePreparedQueryRequest` → Raft command → validated (resource limits, condition/schema compatibility, FieldRef/target compatibility) → persisted at `[0xE0][ledgerName\x00][queryName]` in primary store. FieldRef sources must match the target type (e.g., `AccountMetadata` only on `ACCOUNTS` target, `TransactionMetadata` only on `TRANSACTIONS` target).
- **Update**: `UpdatePreparedQueryRequest` → Raft command → re-validated → overwrites filter at same key. Target type cannot be changed (delete + recreate if needed). Designed to be batched with `SetMetadataFieldType` in the same proposal for atomic schema + query updates.
- **Execute**: `ExecutePreparedQueryRequest` → read-only against the read index store → `LIST` mode returns matching accounts or transaction IDs (paginated), `AGGREGATE_VOLUMES` mode returns per-asset aggregated volumes (ACCOUNTS target only)
- **List**: returns all prepared queries for a ledger
- **Delete**: Raft command → definition removed from primary store

### 6.2 gRPC Service

```protobuf
service BucketService {
  // ...existing RPCs...
  rpc CreatePreparedQuery(CreatePreparedQueryRequest) returns (CreatePreparedQueryResponse);
  rpc UpdatePreparedQuery(UpdatePreparedQueryRequest) returns (UpdatePreparedQueryResponse);
  rpc DeletePreparedQuery(DeletePreparedQueryRequest) returns (DeletePreparedQueryResponse);
  rpc ListPreparedQueries(ListPreparedQueriesRequest) returns (ListPreparedQueriesResponse);
  rpc ExecutePreparedQuery(ExecutePreparedQueryRequest) returns (ExecutePreparedQueryResponse);
}
```

### 6.3 HTTP Endpoints

| HTTP Endpoint | gRPC Method | Notes |
|---------------|-------------|-------|
| `POST /{ledger}/prepared-queries` | `CreatePreparedQuery` | Body: query definition |
| `PUT /{ledger}/prepared-queries/{name}` | `UpdatePreparedQuery` | Body: new filter; replaces existing query filter |
| `GET /{ledger}/prepared-queries` | `ListPreparedQueries` | |
| `DELETE /{ledger}/prepared-queries/{name}` | `DeletePreparedQuery` | |
| `POST /{ledger}/prepared-queries/{name}/execute` | `ExecutePreparedQuery` | Body: `{ parameters }`. Query params: `page_size`, `cursor`, `mode` (`list` or `aggregate_volumes`). When `cursor` is set, all other params are ignored. Response: `{ cursor: { ... } }` for LIST mode, `{ aggregate: { volumes: [...] } }` for AGGREGATE_VOLUMES mode. |

## 7. Index Builder & Read Store

### 7.1 Index Builder — Feeding Strategy

**Problem**: the index builder must only process **accepted** operations. The Raft log contains both accepted and rejected proposals — the FSM may reject a command (insufficient funds, idempotency conflict, transaction already reverted, etc.) and in that case no system log is produced. Only the Raft applied index advances. Indexing raw Raft entries would include phantom data from rejected commands.

**Two approaches:**

#### Option A — Tail system logs in Pebble (recommended)

Follow the same pattern as the existing event emitter (`internal/service/events/emitter.go`):

1. Subscribe to `NotifyLogsCommitted()` from the FSM event notifier
2. On notification, read new system logs from Pebble (`[0x01][sequence]`)
3. System logs contain only accepted operations — rejected proposals never produce log entries
4. Extract metadata changes, new accounts, transactions from each log
5. Write index updates to bbolt
6. Persist progress (`lastIndexedLogSequence`) in bbolt

**Pros**: proven pattern (event emitter already works this way), simple, no FSM coupling.

**Cons**: reads from the primary Pebble store (sequential reads only — scanning log entries in order, not random I/O on metadata/volumes).

#### Option B — FSM pushes accepted logs via channel

The FSM already resolves per-proposal futures with `ApplyResult.Logs`. A dedicated channel could receive the same data:

1. FSM emits `ApplyResult.Logs` to a buffered channel after `batch.Commit()`
2. Index builder consumes from this channel
3. Zero Pebble reads

**Pros**: no primary store reads at all, lower latency (no notification + read round-trip).

**Cons**: tighter coupling between FSM and index builder, channel back-pressure must be handled, crash recovery requires fallback to log tailing anyway (to replay logs the channel missed).

#### Recommendation

**Option A** (tail system logs). It reuses the existing `NotifyLogsCommitted()` pattern, is simpler to implement, and crash recovery is straightforward (resume from `lastIndexedLogSequence`). The Pebble reads are sequential scans on prefix `0x01` — negligible I/O compared to random reads.

### 7.2 Read Store Autonomy — Reverse Metadata Map

The index builder already reads system logs from the primary store (Section 7.1). However, for **random-access metadata lookups** (e.g., "what is the current value of key X for account Y?"), we avoid reading from Pebble. This would require cross-store random I/O on the volume/metadata key space, which is expensive and creates contention with the FSM's write path.

When updating the metadata inverted index, the index builder must know the **old** metadata value to delete the stale index entry. The read store maintains its own reverse mapping in bbolt:

```
Reverse map:   [0x0E][ledgerName\x00][a:][account\x00][metadataKey] → MetadataValue protobuf
Forward index: [0x0C][ledgerName\x00][a:][metadataKey\x00][typeTag][sortableValue][accountAddress] → (empty)
```

When the index builder processes a `SetMetadata(account, key, newValue)`:

1. Read old value from reverse map `[0x0E][ledger\x00][a:][account\x00][key]`
2. Delete old forward index entry `[0x0C][ledger\x00][a:][key\x00][oldTypeTag][oldEncodedValue][account]`
3. Insert new forward index entry `[0x0C][ledger\x00][a:][key\x00][newTypeTag][newEncodedValue][account]`
4. Update reverse map `[0x0E][ledger\x00][a:][account\x00][key]` = newValue

All four operations happen in a single bbolt write transaction — atomic, no partial state.

This makes the primary store and read store **fully independent** for random metadata lookups — no cross-disk I/O.

**Transaction metadata** can be modified after creation (same as account metadata) and its types can change via `SetMetadataFieldType`. The same old-value problem applies — the index builder needs the previous value to delete stale index entries. The reverse metadata map pattern applies identically:

```
Reverse map:   [0x0E][ledgerName\x00][t:][txId(8B BE)][metadataKey] → MetadataValue protobuf
Forward index: [0x0C][ledgerName\x00][t:][metadataKey\x00][typeTag][sortableValue][txId(8B BE)] → (empty)
```

### 7.3 Consistency Model

The bbolt read store is a **pure derived index** — it contains no authoritative data. All authoritative data lives in the primary Pebble store, which is always consistent. If the read store disappeared entirely, the system would continue to function (minus advanced queries) and the index can be rebuilt from scratch by replaying system logs.

The only effect of the index builder's lag is **visibility**: a recently created account or metadata change may not yet appear in query results. But the data is never inconsistent — there is no stale data, no contradictory state. The index simply hasn't seen the latest changes yet.

- Typical lag: sub-millisecond on NVMe (bounded by bbolt write throughput)
- `lastIndexedLogSequence` is exposed via API for freshness checks
- Queries can specify `min_log_sequence` to wait until the read store catches up (opt-in freshness guarantee, at the cost of latency)
- The log sequence (global, monotonically increasing) is a better progress marker than the Raft index because it only counts accepted operations

### 7.4 Crash Recovery

If the read store is lost or corrupted:

1. The primary store is unaffected (all authoritative data is there)
2. `store rebuild-indexes` replays all system logs (`[0x01]`) and reconstructs the read store from scratch
3. During rebuild, prepared query execution returns an error ("indexes not ready")
4. No data loss — the read store is entirely derived from system logs

On normal restart, the index builder resumes from `lastIndexedLogSequence` stored in bbolt and catches up by tailing the remaining system logs.

### 7.5 Schema Change Handling

When a metadata key's type changes (`SetMetadataFieldType`), both the read store indexes and any prepared queries referencing that key must be updated.

#### Index rebuild (automatic)

When the index builder processes a `SetMetadataFieldType` log, it re-encodes all inverted index entries for that key using the new type. Uses the existing `ConvertMetadataValue` matrix (`internal/proto/commonpb/metadata_convert.go`) to convert each value:

1. Scan all entries matching `[0x0C][ledger\x00][a:][key\x00]` (or `[t:]` for transaction metadata)
2. For each entry: decode old value, convert via `ConvertMetadataValue`, re-encode with new type tag
3. Delete old entry, insert new entry
4. Update reverse map `[0x0E]` with the converted value

Inconvertible values become `NullValue` in the index (invisible to typed conditions unless `ExistsCondition(include_null: true)`).

#### Prepared query update (user responsibility)

Prepared query definitions are **not auto-converted**. It is the **user's responsibility** to update prepared queries atomically with schema changes, by submitting both operations in the same Raft proposal batch:

```
Proposal {
  orders: [
    SetMetadataFieldType(target: ACCOUNT, key: "amount", type: INT64),
    UpdatePreparedQuery(ledger: "default", name: "amount_query",
      filter: IntRange(AccountMetadata("amount"), min: 100)),
  ]
}
```

Since a `Proposal` contains `repeated Order orders` executed atomically, this guarantees no window of inconsistency between schema and query definitions. `UpdatePreparedQuery` replaces the filter of an existing query in-place — no need to delete + recreate (which would lose the name temporarily and require two orders instead of one).

**Validation at execution time**: if a prepared query's condition type does not match the current schema for a key (e.g., `StringCondition` on a key now declared as `INT64`), execution returns a clear error:

```
error: condition type mismatch for field AccountMetadata("amount"):
  query uses StringCondition but schema declares INT64.
  Update the prepared query to use IntCondition.
```

This is simple, explicit, and avoids all runtime conversion complexity.

## 8. Storage Engine

### 8.1 I/O Profile

The read store has a very different I/O profile from the primary store:

- **Single writer** (index builder), low write throughput
- **Many concurrent readers** (query executors), heavy scan workload
- **Analytical queries**: filtering, range scans, multi-criteria intersection

Two candidates: **bbolt** (B+ tree) and **Pebble** (LSM-tree, already used for the primary store).

### 8.2 Comparison

| Criteria | bbolt (B+ tree) | Pebble (LSM-tree) |
|----------|----------------|-------------------|
| **Range scan I/O** | Single-level — sequential page reads, zero read amplification | Multi-level — a scan may touch L0 through L6, read amplification proportional to LSM depth |
| **Concurrent readers** | Native MVCC — `db.View()` opens a snapshot automatically, readers never block writer | Explicit `db.NewSnapshot()` needed for consistent multi-cursor reads; without snapshot, cursors may see different states mid-query |
| **Background I/O** | None — no compaction, no WAL flush, no surprises | Compaction goroutines run within the read store instance; compete with its own read scans for disk bandwidth (not with the primary store — separate instance) |
| **Write performance** | Page rewrites (B+ tree) — slower for random writes | Append-only WAL + memtable — faster writes, but irrelevant here (index builder writes are sequential and low throughput) |
| **Compression** | None — data stored uncompressed | Snappy/Zstd per SSTable block — smaller on-disk footprint |
| **File size management** | Freed pages reused but file never shrinks; `bbolt.Compact()` can rewrite the file | SSTable files created/deleted by compaction — space reclaimed naturally |
| **Tuning** | Zero config — page size, that's it | Memtable size, L0 compaction threshold, bloom filters, block cache size, compression level |
| **Code reuse** | New dependency (`go.etcd.io/bbolt`), different cursor API | Same dependency as primary store, same key builders, same iterator patterns |
| **Multi-cursor consistency** | Automatic — all cursors in a `View` tx see the same snapshot | Must explicitly create a `Snapshot` and open all iterators from it; if forgotten, a 5-cursor AND query may return incorrect results |
| **Maturity** | 10+ years (etcd, Consul) | 5+ years (CockroachDB) |
| **Pure Go** | Yes | Yes |

### 8.3 Analysis

**Read amplification** is the key differentiator. Our prepared queries open 2-10 cursors simultaneously (one per filter leaf) and merge-join them. Each cursor does a range scan on a prefix.

- **bbolt**: each cursor reads sequential pages from a single B+ tree level. A range scan touching N entries reads ~N/fanout pages. Predictable, no variability.
- **Pebble**: each cursor must merge data across multiple LSM levels (typically 4-6 after steady-state compaction). A range scan touching N entries may read 4-6× more data from disk. Bloom filters don't help — they optimize point lookups, not range scans.

With 5 concurrent cursors, the total read amplification in Pebble is 5 × (LSM depth) — potentially 20-30× more I/O than bbolt for the same query.

**Background I/O**: the read store receives steady writes from the index builder. With Pebble, compaction runs within the read store instance — it doesn't interfere with the primary store (separate Pebble instance), but it does compete with the read store's own query scans for disk bandwidth. With bbolt, there is no background activity at all — every byte of I/O is either a write from the index builder or a read from a query. This makes performance more predictable, though the difference is less critical than the read amplification factor.

**Snapshot consistency**: a prepared query with multiple filter leaves opens multiple cursors that must all see the same data state. bbolt provides this automatically — `db.View()` captures a snapshot for the entire transaction. With Pebble, you must explicitly create a `Snapshot` and derive all iterators from it. Forgetting this (or using `db.NewIter()` directly) means each cursor may see a different state, producing incorrect AND/OR results. This is a correctness footgun.

**Code reuse** is Pebble's main advantage: same dependency, same key builder functions, same iterator API patterns. The key layout (Section 8.5) works identically in both engines. With bbolt, we need to adapt key builders and cursor handling for a different API.

**Compression**: Pebble compresses SSTables (Snappy or Zstd). For an inverted index with many short keys and empty values, compression can reduce disk usage by 2-3×. bbolt stores data uncompressed. On NVMe this doesn't matter much (space is cheap); on constrained disks it could.

### 8.4 Recommendation

**bbolt** for the read store.

The workload is read-heavy with multiple concurrent range scans — the exact scenario where B+ trees outperform LSM-trees. Zero read amplification, automatic MVCC snapshots (no correctness footgun), and no background compaction interference make bbolt the better fit.

Pebble's advantages (code reuse, compression) don't outweigh the I/O disadvantage for this workload. The index builder writes are low-throughput and sequential — Pebble's write-optimized design is wasted here.

### 8.5 Key Layout

| Prefix | Key | Value | Purpose |
|--------|-----|-------|---------|
| `0x0C` | `[ledgerName\x00][a:][metadataKey\x00][typeTag(1B)][sortableValue][accountAddress]` | (empty) | Account metadata inverted index |
| `0x0C` | `[ledgerName\x00][t:][metadataKey\x00][typeTag(1B)][sortableValue][txId(8B BE)]` | (empty) | Transaction metadata inverted index |
| `0x0D` | `[ledgerName\x00][a:][accountAddress]` | (empty) | Account existence index |
| `0x0D` | `[ledgerName\x00][t:][txId(8B BE)]` | (empty) | Transaction existence index |
| `0x0E` | `[ledgerName\x00][a:][account\x00][metadataKey]` | `MetadataValue protobuf` | Account reverse metadata map |
| `0x0E` | `[ledgerName\x00][t:][txId(8B BE)][metadataKey]` | `MetadataValue protobuf` | Transaction reverse metadata map |
| `0x0F` | `[ledgerName\x00][accountAddress\x00][txId(8B BE)]` | (empty) | Account → transactions mapping |
| `0xE0` | `[ledgerName\x00][queryName]` | `PreparedQuery protobuf` | Query definitions (primary store) |

`a:` / `t:` namespace tags distinguish account and transaction entries under the same prefix, keeping the sorted key layout clean.

`0x0C`, `0x0D`, `0x0E`, `0x0F` live in the read store (bbolt). `0xE0` lives in the primary Pebble store.

### 8.6 Sortable Value Encoding

The `typeTag` byte and `sortableValue` encoding depend on the metadata type:

| MetadataType | typeTag | sortableValue encoding | Rationale |
|-------------|---------|----------------------|-----------|
| STRING | `'S'` | Raw UTF-8 bytes + `\x00` separator | Byte-lexicographic = string sort order |
| INT8..INT64 | `'I'` | 8 bytes big-endian, sign bit XOR'd (`v ^ 0x8000000000000000`) | XOR flips sign bit → negative sorts before positive in unsigned byte order |
| UINT8..UINT64 | `'U'` | 8 bytes big-endian | Natural byte order = numeric order |
| BOOL | `'B'` | 1 byte (`0x00` = false, `0x01` = true) | |
| NullValue | `'N'` | Original raw string + `\x00` separator | Allows ExistsCondition to scan null entries |

This encoding ensures that bbolt's B+ tree natural ordering corresponds to the logical value ordering — range scans on encoded values produce results in correct numeric/lexicographic order.

### 8.7 Query Execution — Iterator Tree

The filter tree compiles into an **iterator tree**. Each leaf opens a bbolt cursor (range scan or point lookup). Operators compose cursors:

- `AND` → merge-intersect of N sorted cursors
- `OR` → merge-union of N sorted cursors
- `NOT` → merge-difference against the existence index (`[0x0D][ledger\x00][a:]` for accounts, `[0x0D][ledger\x00][t:]` for transactions)

All cursors are lazy and streaming — no intermediate materialization. Memory usage is constant regardless of result set size.

#### Leaf operations

**String equality** — point scan:
```
scan [0x0C][ledger\x00][a:][key\x00]['S'][value\x00] → sorted account addresses
```

**Integer range** — range scan:
```
lower: [0x0C][ledger\x00][a:][key\x00]['I'][encode(min)]
upper: [0x0C][ledger\x00][a:][key\x00]['I'][encode(max+1)]
→ all accounts with key in [min, max]
```

**Boolean equality** — point scan:
```
scan [0x0C][ledger\x00][a:][key\x00]['B'][\x01] → accounts where key = true
```

**Exists** — prefix scan:
```
scan [0x0C][ledger\x00][a:][key\x00] → all accounts that have this key
(optionally exclude typeTag 'N' for non-null only)
```

**Address prefix** — range scan on existence index:
```
scan [0x0D][ledger\x00][a:][prefix]...[prefix\xFF] → accounts matching prefix
```

**Transaction queries** use the `t:` namespace (`[0x0C][ledger\x00][t:][key\x00]...`) and return transaction IDs (8-byte big-endian). For `AddressMatch` on transactions:
1. Scan `[0x0D][ledger\x00][a:][prefix]...[prefix\xFF]` → matching account addresses
2. For each account, scan `[0x0F][ledger\x00][account\x00]` → linked transaction IDs
3. Merge-union all transaction ID sets → sorted iterator of transaction IDs

#### Merge-join performance

O(N × S) where N = number of filters, S = smallest result set. High-selectivity filters terminate quickly.

- A 2-filter AND on 1M accounts with 10-20% selectivity scans ~200K entries per cursor — sub-second on SSD
- The key layout `[0x0C][ledger][a:][key\x00]` naturally scopes to a specific metadata key — equivalent to a partial index, no wasted scans on unrelated keys

## 9. Volume Aggregation

### 9.1 Problem

Query mode 3 ("Aggregate volumes") requires filtering accounts by metadata/address, then retrieving their volumes from Pebble and aggregating per asset. Volumes are stored in the primary Pebble store as attribute entries — they are not metadata and follow a different storage model (base + diff with `ComputeValue`).

The question: **where do volumes come from** during aggregation?

### 9.2 Two Approaches

#### Option A — Replicate volumes in bbolt

Duplicate volume data into the read store so everything is self-contained.

**Why this doesn't work:**

- Every transaction touches volumes for all its postings — 2× accounts per posting (source + destination). A transaction with 3 postings updates 6 account/asset volume pairs.
- The index builder would need to write to bbolt for every posting in every transaction — drastically increasing bbolt write volume.
- Volumes in Pebble use a base+diff model (`ComputeValue`) with multiple entries per canonical key (one per Raft index). Replicating this in bbolt either requires the same multi-entry model (complex) or pre-computing the final value (stale the moment the next transaction lands).
- Volume data is large: `Uint256` input + `Uint256` output per (account, asset) pair, vs. the current bbolt entries which are mostly empty values with data encoded in the key.

This approach would transform the read store from a low-write index into a high-write data store — undermining the I/O profile that made bbolt the right choice (Section 8).

#### Option B — Cross-store filter→aggregate (recommended)

Keep volumes in Pebble only. The query executor filters accounts in bbolt, then reads volumes from Pebble for matching accounts:

```
1. Execute filter in bbolt     → sorted iterator of account addresses
2. For each matching account   → SeekGE in Pebble to account's volume prefix
3. Scan all (account, asset)   → ComputeValue per canonical key
4. Accumulate per asset        → sum input/output across all matching accounts
5. Return aggregated volumes
```

This is efficient because:

- Both data sources are sorted by account address — the bbolt cursor emits accounts in order, and Pebble volume keys are sorted by `[ledger\x00][account\x00][asset]`. This enables a merge-scan pattern (no random I/O).
- Pebble reads are sequential prefix scans — the same pattern as `ForEachInPrefix` already used elsewhere.
- The number of Pebble seeks = number of matching accounts (not total accounts in ledger).
- No data duplication, no extra bbolt writes.

### 9.3 Algorithm Detail

**Pebble volume key layout:**

```
[0xF1][ledger\x00][account\x00][asset]['V'][raftIndex(8B)][entryType(1B)]
```

All volumes for a given `(ledger, account)` share the prefix `[0xF1][ledger\x00][account\x00]`. `ForEachInPrefix` scans this prefix and computes the final value (base + diff) for each `(account, asset)` canonical key.

**Merge-scan algorithm:**

```go
// Pseudocode
accountIterator := executeFilter(bboltTx, filter)  // sorted account addresses from bbolt
aggregator := map[string]AggregatedVolume{}         // asset → {totalInput, totalOutput}

for accountIterator.Next() {
    account := accountIterator.Current()
    prefix := canonicalPrefix(ledger, account)       // [ledger\x00][account\x00]

    volumes.ForEachInPrefix(pebbleReader, maxIndex, prefix, func(entry ComputedEntry[VolumePair]) {
        var vk VolumeKey
        vk.Unmarshal(entry.CanonicalKey)             // extract asset from key
        agg := aggregator[vk.Asset]
        agg.Input.Add(entry.Value.Input)             // Uint256 addition
        agg.Output.Add(entry.Value.Output)
        aggregator[vk.Asset] = agg
    })
}
```

**I/O profile per query:**

| Step | Store | I/O pattern | Volume |
|------|-------|-------------|--------|
| Filter accounts | bbolt | Range scans (lazy cursors) | Same as list accounts query |
| Seek to each account's volumes | Pebble | One SeekGE per matching account | N seeks (N = matching accounts) |
| Scan account volumes | Pebble | Sequential prefix scan | Proportional to assets per account |
| ComputeValue | CPU | base+diff computation | Negligible |

For a query matching 100 accounts with ~5 assets each: ~100 Pebble seeks + ~500 sequential reads. On NVMe, this completes in low single-digit milliseconds.

### 9.4 Consistency

The authoritative data lives entirely in the primary Pebble store — it is always consistent, always correct. The bbolt read store is purely a **derived index**: it does not hold any data that doesn't already exist in Pebble. If bbolt disappeared, the system would still function (minus advanced queries), and it can be rebuilt from scratch at any time by replaying system logs.

The only consequence of the index lag is **visibility**: a newly created account or a metadata change may not appear in query results for a few milliseconds until the index builder catches up. But the data itself is never inconsistent — there is no split-brain, no stale volume values, no contradictory state between stores. Volumes are always read from Pebble (the source of truth), not from the index.

In practice, the lag is bounded by the index builder's throughput (typically sub-millisecond on NVMe). For use cases that need strict freshness, `min_log_sequence` lets the client wait until the index has processed a specific log entry before executing the query.

### 9.5 Pagination

Volume aggregation does **not** use pagination — it returns a single response with all per-asset aggregated volumes. The result set is bounded by the number of distinct assets (typically small: 1-20), not by the number of matching accounts.

If the matching account set is very large (millions), the query may take longer but memory usage remains bounded: the accumulator map has one entry per asset, not per account.

### 9.6 Proto Definition

```protobuf
// AggregatedVolume represents the total input/output for a single asset
// across all accounts matching the filter.
message AggregatedVolume {
  string asset = 1;
  common.Uint256 input = 2;        // sum of all matching accounts' input
  common.Uint256 output = 3;       // sum of all matching accounts' output
}
```

The `ExecutePreparedQueryResponse` is updated to carry aggregate results:

```protobuf
message ExecutePreparedQueryResponse {
  oneof result {
    Cursor cursor = 1;                              // for list accounts / list transactions
    AggregateResult aggregate = 2;                  // for volume aggregation
  }
}

message AggregateResult {
  repeated AggregatedVolume volumes = 1;
}
```

The `ExecutePreparedQueryRequest` gains a mode field:

```protobuf
message ExecutePreparedQueryRequest {
  string ledger = 1;
  string query_name = 2;
  map<string, string> parameters = 3;
  uint32 page_size = 4;                            // ignored for aggregate mode
  string cursor = 5;                               // ignored for aggregate mode
  uint64 min_log_sequence = 6;
  QueryMode mode = 7;                              // LIST (default) or AGGREGATE_VOLUMES
}

enum QueryMode {
  LIST = 0;                                         // list entities (paginated)
  AGGREGATE_VOLUMES = 1;                            // aggregate volumes for matching accounts
}
```

**Validation**: `AGGREGATE_VOLUMES` is only valid on queries with `target: ACCOUNTS`. Using it on a `TRANSACTIONS` query returns an error.

## 10. Dedicated Disk — Is It Necessary?

**No, but it helps.**

The index builder is asynchronous, so it **never blocks the FSM**. Whether both stores share a disk or not, the consensus critical path is unaffected. The two stores are fully independent (no cross-disk I/O thanks to the reverse metadata map).

The real question: does read query I/O interfere with primary store compaction?

| Disk type | Contention risk | Recommendation |
|-----------|----------------|----------------|
| **NVMe SSD** (~500K IOPS, 3-7 GB/s) | Low | Same disk is fine |
| **SATA SSD** (~50K IOPS, 500 MB/s) | Medium | Dedicated disk recommended |
| **HDD** | High | Dedicated disk required |

The Raft WAL is already recommended on a separate disk (`--wal-dir`). Adding a third disk follows the same pattern:

```
Disk 1 (fast, small):  --wal-dir        /mnt/nvme-wal/   ← Raft WAL (latency-critical)
Disk 2 (large):        --data-dir       /mnt/ssd-data/   ← Primary Pebble store
Disk 3 (large):        --read-index-dir /mnt/ssd-read/   ← Read index store (bbolt)
```

For simpler deployments, disk 2 and 3 can be the same.

**Configuration:**

```
--read-index-dir    /mnt/read-ssd/indexes    (separate disk mount point)
--read-index-enable true                       (opt-in, disabled by default)
```

Default: `{data-dir}/read-indexes/` (same disk).

## 11. Execution Examples

### 11.1 String equality + boolean combination

**Use case**: "List active premium/gold merchants, excluding suspended ones"

```
Prepared query "active_premium_merchants":
  target: ACCOUNTS
  filter: AND(
    OR(
      StringEquals(AccountMetadata("category"), hardcoded: "premium"),
      StringEquals(AccountMetadata("category"), hardcoded: "gold")
    ),
    AddressPrefix(param: "prefix"),
    NOT(StringEquals(AccountMetadata("status"), hardcoded: "suspended"))
  )

Client: { "prefix": "merchants:" }
```

```
  cursor1: scan [0x0C][ledger\x00][a:][category\x00]['S'][premium\x00]  → [acc_A, acc_C, acc_F]
  cursor2: scan [0x0C][ledger\x00][a:][category\x00]['S'][gold\x00]     → [acc_B, acc_F, acc_G]
  cursor3: OR(cursor1, cursor2)                                           → [acc_A, acc_B, acc_C, acc_F, acc_G]
  cursor4: scan [0x0D][ledger\x00][a:][merchants:]...[merchants:\xFF]    → [acc_A, acc_B, acc_C, acc_F, acc_H]
  cursor5: scan [0x0C][ledger\x00][a:][status\x00]['S'][suspended\x00]  → [acc_B]
  cursor6: NOT(cursor5) via universe [0x0D][ledger\x00][a:]              → [acc_A, acc_C, acc_F, acc_G, acc_H, ...]
  cursor7: AND(cursor3, cursor4, cursor6)                                 → [acc_A, acc_C, acc_F]
```

### 11.2 Integer range query

**Use case**: "List high-value merchants with credit limit between 10K and 50K"

```
Prepared query "high_value_merchants":
  target: ACCOUNTS
  filter: AND(
    IntRange(AccountMetadata("credit_limit"), min: 10000, max: 50000),
    AddressPrefix(hardcoded: "merchants:")
  )

Client: {} (no parameters — all values hardcoded)
```

```
  cursor1: range [0x0C][ledger\x00][a:][credit_limit\x00]['I'][encode(10000)]
             ... [0x0C][ledger\x00][a:][credit_limit\x00]['I'][encode(50001)]
           → [acc_A(15000), acc_C(30000), acc_F(10000), acc_H(45000)]
  cursor2: scan [0x0D][ledger\x00][a:][merchants:]...[merchants:\xFF]
           → [acc_A, acc_C, acc_F, acc_H, acc_J]
  cursor3: AND(cursor1, cursor2)
           → [acc_A, acc_C, acc_F, acc_H]
```

### 11.3 Parameterized range with boolean filter

**Use case**: "List active accounts with volume above a threshold (threshold provided at execution)"

```
Prepared query "active_above_threshold":
  target: ACCOUNTS
  filter: AND(
    IntRange(AccountMetadata("monthly_volume"), param_min: "min_volume"),
    BoolEquals(AccountMetadata("active"), hardcoded: true),
    AddressPrefix(param: "prefix")
  )

Client: { "min_volume": "50000", "prefix": "merchants:" }
```

```
  cursor1: range [0x0C][ledger\x00][a:][monthly_volume\x00]['I'][encode(50000)]
             ... [0x0C][ledger\x00][a:][monthly_volume\x00]['I'][\xFF x 8]
           → [acc_A(75000), acc_F(50000), acc_H(120000)]
  cursor2: scan [0x0C][ledger\x00][a:][active\x00]['B'][\x01]
           → [acc_A, acc_C, acc_F, acc_H]
  cursor3: scan [0x0D][ledger\x00][a:][merchants:]...[merchants:\xFF]
           → [acc_A, acc_C, acc_F, acc_H, acc_J]
  cursor4: AND(cursor1, cursor2, cursor3)
           → [acc_A, acc_F, acc_H]
```

### 11.4 Transaction query — metadata range + address involvement

**Use case**: "List transactions involving merchants with amount >= 5000"

```
Prepared query "large_merchant_transactions":
  target: TRANSACTIONS
  filter: AND(
    IntRange(TransactionMetadata("amount"), param_min: "min_amount"),
    AddressPrefix(param: "prefix")
  )

Client: { "min_amount": "5000", "prefix": "merchants:" }
```

```
  cursor1: range [0x0C][ledger\x00][t:][amount\x00]['I'][encode(5000)]
                 ...[0x0C][ledger\x00][t:][amount\x00]['I'][\xFF x 8]
           → [tx_1(10000), tx_3(7500), tx_5(5000), tx_8(25000)]
  cursor2: scan [0x0D][ledger\x00][a:][merchants:]...[merchants:\xFF]
           → [merchants:acme, merchants:beta]
  cursor3: for each account, scan [0x0F][ledger\x00][account\x00]
           → merchants:acme → [tx_1, tx_5, tx_9], merchants:beta → [tx_3, tx_5]
  cursor4: merge-union(cursor3 results) → [tx_1, tx_3, tx_5, tx_9]
  cursor5: AND(cursor1, cursor4) → [tx_1, tx_3, tx_5]
```

### 11.5 Complex mixed query

**Use case**: "List EU or US merchants with credit limit >= 5000, category premium or gold, that are not suspended"

```
Prepared query "qualified_regional_merchants":
  target: ACCOUNTS
  filter: AND(
    OR(
      StringEquals(AccountMetadata("region"), hardcoded: "eu"),
      StringEquals(AccountMetadata("region"), hardcoded: "us")
    ),
    OR(
      StringEquals(AccountMetadata("category"), hardcoded: "premium"),
      StringEquals(AccountMetadata("category"), hardcoded: "gold")
    ),
    IntRange(AccountMetadata("credit_limit"), param_min: "min_credit"),
    NOT(BoolEquals(AccountMetadata("suspended"), hardcoded: true)),
    AddressPrefix(param: "prefix")
  )

Client: { "min_credit": "5000", "prefix": "merchants:" }
```

```
  cursor1: scan [0x0C][ledger\x00][a:][region\x00]['S'][eu\x00]          → [acc_A, acc_C, acc_F]
  cursor2: scan [0x0C][ledger\x00][a:][region\x00]['S'][us\x00]          → [acc_B, acc_H, acc_J]
  cursor3: OR(cursor1, cursor2)                                             → [acc_A, acc_B, acc_C, acc_F, acc_H, acc_J]
  cursor4: scan [0x0C][ledger\x00][a:][category\x00]['S'][premium\x00]   → [acc_A, acc_C, acc_H]
  cursor5: scan [0x0C][ledger\x00][a:][category\x00]['S'][gold\x00]      → [acc_B, acc_F]
  cursor6: OR(cursor4, cursor5)                                             → [acc_A, acc_B, acc_C, acc_F, acc_H]
  cursor7: range [0x0C][ledger\x00][a:][credit_limit\x00]['I'][encode(5000)]
             ... [0x0C][ledger\x00][a:][credit_limit\x00]['I'][\xFF x 8]  → [acc_A, acc_B, acc_C, acc_F, acc_H, acc_J]
  cursor8: scan [0x0C][ledger\x00][a:][suspended\x00]['B'][\x01]         → [acc_J]
  cursor9: NOT(cursor8) via universe [0x0D][ledger\x00][a:]               → [all except acc_J]
  cursor10: scan [0x0D][ledger\x00][a:][merchants:]...[merchants:\xFF]   → [acc_A, acc_B, acc_C, acc_F, acc_H, acc_J]
  cursor11: AND(cursor3, cursor6, cursor7, cursor9, cursor10)              → [acc_A, acc_B, acc_C, acc_F, acc_H]
```

All cursors are lazy — the AND short-circuits as soon as one cursor is exhausted. With 5 filters, the merge-intersect advances the smallest cursor at each step, skipping large ranges in the other cursors.

### 11.6 Volume aggregation — filtered accounts

**Use case**: "Aggregate volumes for all active premium merchants"

```
Prepared query "premium_merchant_volumes":
  target: ACCOUNTS
  filter: AND(
    StringEquals(AccountMetadata("category"), hardcoded: "premium"),
    BoolEquals(AccountMetadata("active"), hardcoded: true),
    AddressPrefix(hardcoded: "merchants:")
  )

Client: mode=AGGREGATE_VOLUMES, parameters: {}
```

```
  Step 1 — Filter in bbolt:
  cursor1: scan [0x0C][ledger\x00][a:][category\x00]['S'][premium\x00]  → [acc_A, acc_C, acc_F, acc_H]
  cursor2: scan [0x0C][ledger\x00][a:][active\x00]['B'][\x01]           → [acc_A, acc_C, acc_F, acc_H]
  cursor3: scan [0x0D][ledger\x00][a:][merchants:]...[merchants:\xFF]    → [acc_A, acc_C, acc_F, acc_H, acc_J]
  cursor4: AND(cursor1, cursor2, cursor3)                                 → [acc_A, acc_C, acc_F, acc_H]

  Step 2 — Merge-scan Pebble volumes for matching accounts:
  acc_A → ForEachInPrefix([ledger\x00][acc_A\x00])
        → (acc_A, USD): input=1000, output=500
        → (acc_A, EUR): input=200,  output=100
  acc_C → ForEachInPrefix([ledger\x00][acc_C\x00])
        → (acc_C, USD): input=3000, output=1500
  acc_F → ForEachInPrefix([ledger\x00][acc_F\x00])
        → (acc_F, USD): input=500,  output=200
        → (acc_F, EUR): input=800,  output=400
  acc_H → ForEachInPrefix([ledger\x00][acc_H\x00])
        → (acc_H, USD): input=2000, output=1000

  Step 3 — Aggregate per asset:
  USD: input=6500, output=3200
  EUR: input=1000, output=500
```

Response:
```json
{
  "aggregate": {
    "volumes": [
      { "asset": "EUR", "input": "1000", "output": "500" },
      { "asset": "USD", "input": "6500", "output": "3200" }
    ]
  }
}
```

## 12. Implementation Plan

1. **Read store infrastructure**: index builder (tails system logs via `NotifyLogsCommitted()`), bbolt read store, configuration flags (`--read-index-dir`, `--read-index-enable`)
2. **Sortable encoding**: `encode(int64)`, `encode(uint64)`, type tag dispatch, key builders for all prefixes
3. **Account existence tracking**: populate `[0x0D][a:]` index from system logs
4. **Account metadata index**: populate `[0x0C][a:]` + `[0x0E][a:]` indexes, typed value encoding
5. **Transaction existence tracking**: populate `[0x0D][t:]` index from system logs
6. **Transaction metadata index**: populate `[0x0C][t:]` + `[0x0E][t:]` indexes, same reverse map pattern
7. **Account-transaction mapping**: populate `[0x0F]` index from transaction postings (for AddressMatch on TRANSACTIONS target)
8. **Schema change handling**: index builder detects `SetMetadataFieldType` logs → re-encodes all entries for the affected key via `ConvertMetadataValue`
9. **Iterator tree**: merge-intersect, merge-union, merge-difference operators on sorted bbolt cursors
10. **Prepared query CRUD**: Raft commands for create/update/delete, storage in `[0xE0]`, gRPC + HTTP handlers, condition/schema validation + FieldRef/target compatibility at creation time
11. **Query executor**: filter tree compiler (recursive tree → iterator tree), condition/schema re-validation at execution time, target-dependent result type (accounts or transaction IDs), keyset pagination
12. **Volume aggregation**: cross-store merge-scan (bbolt filter → Pebble `ForEachInPrefix`) with per-asset accumulator, `AGGREGATE_VOLUMES` mode validation (ACCOUNTS target only)
13. **Backfill**: `store rebuild-indexes` CLI command to populate read store from existing logs

## 13. Decisions Record

| Topic | Decision | Rationale |
|---|---|---|
| **Query targets** | ACCOUNTS and TRANSACTIONS | Each prepared query declares a target type. ACCOUNTS returns account addresses, TRANSACTIONS returns transaction IDs. FieldRef sources must match the target type (e.g., `AccountMetadata` only on ACCOUNTS, `TransactionMetadata` only on TRANSACTIONS). Cross-entity conditions are out of scope (future work). |
| **Filter model** | Recursive boolean tree (AND/OR/NOT) | Supports complex queries; composable; compiles naturally to iterator trees on sorted cursors |
| **Leaf types** | FieldCondition (FieldRef × Condition) + AddressMatch | Separates "where to look" (FieldRef: AccountMetadata, TransactionMetadata) from "what to match" (Condition: string/int/uint/bool/exists). Conditions are reusable across data sources. Extensible to future sources (e.g., volumes) without new condition types. AddressMatch semantics depend on target type: direct match for ACCOUNTS, posting involvement for TRANSACTIONS. |
| **Index population** | Asynchronous via index builder | Keeps FSM apply fast (RAM-only); no I/O contention on write path |
| **Feeding strategy** | Tail system logs (0x01) in Pebble, not raw Raft entries | Raw Raft entries include rejected proposals (insufficient funds, idempotency conflicts, etc.); system logs contain only accepted operations; reuses existing `NotifyLogsCommitted()` pattern from event emitter |
| **Read store autonomy** | Reverse metadata map in bbolt | Eliminates cross-store random I/O for metadata lookups; all four operations (read old, delete old index, insert new index, update reverse map) are atomic in a single bbolt write tx |
| **Storage engine** | bbolt (B+ tree) over Pebble (LSM-tree) | Read-optimized workload: zero read amplification (vs multi-level LSM merges), automatic MVCC snapshots for multi-cursor consistency (vs explicit Snapshot management), no background compaction interference. Pebble's write optimization and compression are wasted on the low-throughput index builder. See Section 8. |
| **Key-scoped indexes** | Key layout `[0x0C][ledger][a:][key\x00]` naturally scopes to a specific metadata key | Equivalent to a partial index — only entries for the queried key are scanned. No wasted I/O on unrelated metadata keys. Zero overhead for unqueried keys. |
| **Dedicated disk** | Optional, not required | Async builder already decouples FSM; beneficial on SATA SSD, not needed on NVMe |
| **Consistency** | Pure derived index with opt-in freshness wait | bbolt is a derived index, not a source of truth — the authoritative data is always consistent in Pebble. Only visibility is delayed (sub-ms). `min_log_sequence` for freshness guarantee when needed. |
| **Validation** | At creation time + execution time | Creation: enforce resource limits, condition/schema type compatibility. Execution: re-validate condition/schema match (catches schema changes not batched with query updates) |
| **Condition/type matching** | Condition type must match declared `MetadataType` for the key | `IntCondition` only on `INT8`..`INT64` keys, `UintCondition` only on `UINT8`..`UINT64`, `BoolCondition` only on `BOOL`, `StringCondition` only on `STRING` or untyped keys. `ExistsCondition` on any type. Enforced at creation and execution. |
| **NullValue visibility** | NullValue entries are invisible to typed conditions | `IntRange` won't match a NullValue — this is acceptable. `ExistsCondition(include_null: true)` can explicitly detect inconvertible values. Consistent with the typed metadata system's semantics. |
| **Pagination** | Cursor-based (keyset) following the reference implementation pattern | Opaque base64-encoded cursor with `next`/`previous` pointers. Keyset pagination (`account > $cursor` / `tx_id > $cursor`) instead of OFFSET — stable under concurrent inserts, O(1) seek. |
| **Transaction metadata** | Indexed in the read store (same pattern as account metadata) | Transaction metadata is mutable (can be modified via SaveMetadata) and types can change; same reverse map pattern as account metadata; enables filtering transactions by metadata |
| **Schema change handling** | Auto-rebuild indexes + user-managed query updates | Index builder re-encodes entries automatically via `ConvertMetadataValue` matrix. Prepared queries are the user's responsibility — schema change + `UpdatePreparedQuery` batched in the same `Proposal` (atomic). Execution-time validation rejects condition/schema mismatches with a clear error. No runtime conversion magic. |
| **Volume aggregation** | Cross-store merge-scan (bbolt → Pebble), not volume replication | Volumes change on every transaction (high write volume) — replicating them in bbolt would undermine its low-write I/O profile. Instead: filter accounts in bbolt, read volumes from Pebble via sequential `ForEachInPrefix`. Both stores are sorted by account → merge-scan pattern, no random I/O. See Section 9. |
| **ExistsCondition on untyped keys** | Allowed | `ExistsCondition` checks for key presence, not the value type — it works regardless of whether the key has a declared schema. Some clients store large amounts of untyped metadata; `ExistsCondition` lets them query key presence without requiring a schema declaration first. |
| **Volume-based filters** | Out of scope — not viable as index-backed filters | A condition like `balance(USD/2) > 1000` cannot be backed by an inverted index in bbolt because volumes change on every transaction — the index would need to be rewritten for every posting. This is the same write amplification problem that ruled out replicating volumes in bbolt (Section 9.2). The only viable approach is post-filtering: run the metadata filter first, then load volumes from Pebble for each candidate and apply the balance condition. This breaks the streaming/lazy cursor model — the executor must materialize candidates, perform N Pebble reads, and re-filter. For a metadata filter matching 10K accounts, that's 10K `ComputeValue` calls before the balance condition even applies. If needed in the future, it should be implemented as a separate post-filter step with a hard limit on candidate set size, not as a `FieldRef` in the filter tree. |
| **Point-in-time queries** | Not supported (arbitrary PIT); [period](../technical/architecture/data-model/periods.md)-boundary snapshots recommended instead | v2 proved that storing all diffs indefinitely causes unbounded storage growth and progressive performance degradation. v3's generational compaction and period archival are designed to bound the hot dataset — PIT negates both. Period-boundary snapshots (file copy at period close) provide auditable historical queries without fighting the architecture. See Section 14. |

## 14. Point-in-Time Queries

### 14.1 Context — Ledger v2

Ledger v2 supports querying the ledger at any arbitrary date via a `pit` parameter. The implementation stores every change in a `moves` table in PostgreSQL, then reconstructs state with `SUM(amount) WHERE date <= PIT`. This provides full bi-temporality (insertion date + effective date).

**Known problems in v2:**

- **Storage**: the `moves` table grows linearly with every transaction, indefinitely. No aggregation or cleanup is possible because any row could be needed for a future PIT query.
- **Performance**: rebuilding state at a given date requires aggregating all moves up to that date — full table scans with `SUM` on large datasets. Complex `JOIN` + `DISTINCT ON` + `ORDER BY` for metadata history. Performance degrades proportionally to ledger age.
- **Complexity**: feature flags (`FeatureMovesHistory`, `FeatureMovesHistoryPostCommitEffectiveVolumes`) control which historical data is stored, adding configuration surface and failure modes.

### 14.2 What PIT Would Mean for v3

The v3 architecture is fundamentally designed around **compaction**: old entries are consolidated and cleaned up over time (generational compaction in Pebble, [period](../technical/architecture/data-model/periods.md) archival to cold storage). PIT fights directly against this design.

#### What would be needed

To support arbitrary PIT in the prepared query read store:

1. **Versioned inverted index**: every metadata change creates a new bbolt entry instead of replacing the old one. Key becomes `[0x0C][ledger][a:][key\x00][typeTag][value][account][timestamp]`. Every query must filter `WHERE timestamp <= PIT` within each prefix scan.

2. **No deletion of old entries**: the reverse metadata map (`[0x0E]`) must keep all historical values, not just the latest. Storage grows linearly with metadata changes.

3. **Timestamp→Raft index mapping**: PIT queries use a calendar timestamp, but the read store is indexed by Raft index. Need a secondary index to translate `timestamp → raftIndex`, adding another lookup per query.

4. **Volume reconstruction**: `ComputeValue(reader, maxIndex, key)` can reconstruct volumes at a specific Raft index — but only if the base+diff entries still exist in Pebble. After generational compaction, old entries are deleted. PIT beyond the compaction window is impossible.

5. **[Period](../technical/architecture/data-model/periods.md) archival breaks PIT**: once a period is archived and logs purged from Pebble, the base+diff entries for that period's transactions are gone. PIT queries into archived periods would require retrieving data from cold storage — fundamentally changing the query latency model.

#### Cost analysis

| Aspect | Current (no PIT) | With full PIT |
|--------|-----------------|---------------|
| **bbolt storage** | O(accounts × keys) | O(accounts × keys × changes) — unbounded growth |
| **Index builder writes** | 1 delete + 1 insert per metadata change | 1 insert only (no delete — keep history) but storage never shrinks |
| **Query I/O** | Range scan on current entries | Range scan + timestamp filter on all historical entries — proportional to churn |
| **Volume aggregation** | `ForEachInPrefix` at latest index | `ComputeValue` at historical index — only works within compaction window |
| **Complexity** | Single-version index, simple cursors | Multi-version index, timestamp filtering, Raft index translation |
| **Compaction compatibility** | Fully compatible | Incompatible — PIT requires retaining everything compaction is designed to remove |

### 14.3 Recommendation — No Arbitrary PIT

**Arbitrary point-in-time queries are not supported.** The v2 experience demonstrated that storing all diffs indefinitely creates unbounded storage growth and progressive performance degradation. The v3 architecture is explicitly designed to avoid this: generational compaction, [period](../technical/architecture/data-model/periods.md) archival, and cold storage are all mechanisms to bound the hot dataset size.

Adding PIT to the prepared query system would:

- **Negate compaction**: the index can never discard old entries if any PIT query might need them
- **Re-create v2's storage problem**: in bbolt instead of PostgreSQL, with worse tooling for managing growth
- **Break the I/O profile**: versioned indexes multiply range scan I/O by the average churn rate per key
- **Create a false promise**: PIT on metadata works until compaction clears the Pebble base+diff entries; then it silently becomes unavailable for older dates, which is worse than not offering it at all

### 14.4 Alternatives for Historical Queries

Instead of arbitrary PIT, three lighter-weight alternatives provide historical query capabilities without fighting the architecture:

#### Option A — Period-boundary snapshots

When a [period](../technical/architecture/data-model/periods.md) closes, capture a snapshot of the bbolt read store state. This gives exact answers to "what was the ledger state at the end of period N?" without any ongoing storage cost between periods.

- **Storage**: one frozen bbolt file per closed period (can be compressed and moved to cold storage)
- **Query**: open the period's bbolt snapshot as read-only, execute the same prepared query against it
- **Granularity**: period boundaries only — not arbitrary dates, but predictable and auditable
- **Implementation**: `bbolt.View()` + file copy at period close; or Pebble checkpoint-style approach
- **Natural fit**: periods already exist for archival (see [period lifecycle](../technical/architecture/data-model/periods.md)); snapshots extend them to read queries

#### Option B — On-demand reconstruction from logs

For rare historical queries, replay system logs up to a specific sequence into a temporary bbolt, then execute the prepared query against it.

- **Storage**: zero ongoing cost — temporary bbolt is built on demand and discarded after
- **Query latency**: proportional to the number of logs to replay (seconds to minutes for large ledgers)
- **Granularity**: any log sequence (mapped to any point in time)
- **Use case**: audit, compliance investigations, dispute resolution — infrequent but needs precision
- **Implementation**: CLI command `ledgerctl query-at --date 2024-10-20 --query "premium_merchants"` replays logs from cold storage if needed

#### Option C — Periodic materialized snapshots

At a configurable interval (e.g., daily, weekly), the index builder captures a full snapshot of the current bbolt state and stores it with its `lastIndexedLogSequence` timestamp.

- **Storage**: one snapshot per interval (bounded, configurable retention)
- **Query**: "what was the state at the most recent snapshot before date X?"
- **Granularity**: configurable — daily gives "end of day" queries, hourly gives finer resolution
- **Trade-off**: more snapshots = more storage, but always bounded by retention policy
- **Implementation**: cron-like scheduler creates `bbolt.Compact()` copies, tagged with timestamp

#### Comparison

| | Arbitrary PIT | Period snapshots (A) | On-demand replay (B) | Periodic snapshots (C) |
|---|---|---|---|---|
| **Granularity** | Any timestamp | Period boundaries | Any log sequence | Configurable interval |
| **Ongoing storage** | Unbounded growth | One file per period | Zero | Bounded by retention |
| **Query latency** | Same as live | Same as live | Seconds to minutes | Same as live |
| **Complexity** | High (versioned index) | Low (file copy) | Medium (log replay) | Low (scheduled copy) |
| **Compaction compatible** | No | Yes | Yes | Yes |
| **Cold storage compatible** | No | Yes (snapshot in cold) | Yes (replays from cold) | Yes (snapshot in cold) |

**Recommendation**: start with **Option A** (period-boundary snapshots) — it has the best cost/value ratio and aligns naturally with the existing [period lifecycle](../technical/architecture/data-model/periods.md). Option B can be added later for ad-hoc audit queries.

## 15. Open Questions

1. **String value length**: string metadata values can be arbitrarily long. For the inverted index key, should long values be truncated + hashed to keep keys bounded? Risk: hash collisions require post-filtering. With typed metadata, most filterable keys will have short strings or numeric values.
2. **Prepared query limits per ledger**: should there be a maximum number? Query definitions are small but replicated via Raft.
3. **bbolt file growth**: bbolt reuses freed pages but never shrinks the file. For ledgers with heavy metadata churn, should we add periodic compaction (`bbolt.Compact()`) or file rotation?
4. **Historical query strategy**: which alternative(s) from Section 14.4 should we implement? Period snapshots (A) are recommended as the starting point. On-demand replay (B) and periodic snapshots (C) can be added incrementally.
