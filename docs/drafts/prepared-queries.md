# Draft — Prepared Queries

**Status**: Draft for team review
**Author**: Geoffrey + Claude
**Date**: 2026-02-24
**Related**: [Advanced Read Queries](./advanced-read-queries.md)

---

## 1. Problem Statement

The ledger's storage is write-oriented. Read capabilities are limited to primary-key lookups and sequential scans. There is no support for:

- **Metadata filtering**: "List accounts where `category=premium`"
- **Multi-criteria filtering**: "List accounts where `category=premium AND region=eu`"
- **Boolean combinations**: "List accounts where `(category=premium OR category=gold) AND NOT status=suspended`"
- **Typed range queries**: "List accounts where `credit_limit >= 10000`" (leveraging the typed metadata system)
- **Address-based filtering combined with metadata**: "List merchants with credit_limit >= 5000 AND active=true"

The reference implementation (`github.com/formancehq/ledger`) supports metadata filtering via SQL `WHERE` clauses — a capability that requires a fundamentally different approach in an embedded key-value store.

**Prepared queries** allow clients to define named, parameterized query templates. The system pre-validates them at creation time and executes them efficiently using secondary indexes built asynchronously from the Raft log.

## 2. Goals

1. Allow clients to filter accounts by **metadata key/value pairs** with boolean operators (AND, OR, NOT)
2. Allow clients to filter by **account address** (prefix or exact match)
3. Allow **mixing** metadata and address filters in a single query
4. Support **parameterized** filters (values resolved at execution time)
5. Execute queries efficiently via **secondary indexes**, not full scans
6. Keep the **FSM apply fast** — index maintenance happens asynchronously
7. Leverage the **typed metadata system** — support range/comparison operators on integer types, boolean checks, not just string equality

## 3. Scope

### In scope

- Recursive boolean filter model (AND/OR/NOT) on metadata and addresses
- **Typed metadata conditions**: string equality, integer ranges (>, <, >=, <=, BETWEEN), boolean checks, existence checks — leveraging the existing typed metadata system (`MetadataType` enum: `STRING`, `INT64`, `BOOL`, `UINT64`, sub-64-bit integer types)
- Prepared query CRUD (create, list, delete) via Raft commands
- Prepared query execution (read-only) with parameter substitution
- Asynchronous index building from Raft log
- Dedicated read store (separate engine, optionally on separate disk)
- Two storage engine options: bbolt (pure Go) or DuckDB (SQL, CGo)

### Out of scope

- Full-text search / fuzzy matching on metadata values
- Volume-based filters (e.g., "balance > 1000") — future work, could be added as a post-filter
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
                    │  Read Index Store │  ← bbolt or DuckDB, optionally on dedicated disk
                    └──────────────────┘
                             ▲
                             │ concurrent read queries
                    ┌──────────────────┐
                    │  Query Executor  │  ← builds iterator tree (bbolt) or SQL (DuckDB)
                    └──────────────────┘
```

**Key design decisions:**

- The **index builder** runs asynchronously, outside the FSM hot path. The FSM apply remains RAM-only.
- The index builder consumes **only accepted operations** — not raw Raft entries. Proposals can be rejected by the FSM (insufficient funds, idempotency conflict, already reverted, etc.) and must not be indexed (see Section 7.1).
- The **read store** is self-contained for metadata lookups — it never performs random reads against the primary Pebble store (see Section 7.2: Reverse Metadata Map). However, it does consume system logs from the primary store or from the FSM directly (see Section 7.1).
- **Prepared query definitions** are stored in the primary store via Raft commands (`[0xE0]`). The read store contains only derived index data.
- The read store is **eventually consistent** with bounded lag (milliseconds). Clients can opt into freshness guarantees via `min_log_sequence`.

## 5. Filter Model

### 5.1 Recursive Filter Tree

A prepared query filter is a **tree of boolean operators** where leaves are either field conditions or address matches. Each node in the tree produces a **sorted iterator of account addresses** (bbolt approach) or a SQL subquery (DuckDB approach).

```
AND(a, b)  →  intersection of results
OR(a, b)   →  union of results
NOT(a)     →  complement (all accounts EXCEPT matches)
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

A `FieldRef` identifies **where** to look for the value:

| FieldRef | Targets | Index table |
|----------|---------|-------------|
| `AccountMetadata("key")` | Metadata on accounts | `account_metadata` / `[0x0C]` |
| `TransactionMetadata("key")` | Metadata on transactions | `transaction_metadata` / `[0x0C]` |

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

| `min` | `max` | Behavior | SQL equivalent |
|-------|-------|----------|----------------|
| set | omitted | `>= min` (or `> min` if exclusive) | `int_value >= $1` |
| omitted | set | `<= max` (or `< max` if exclusive) | `int_value <= $1` |
| set | set | BETWEEN | `int_value >= $1 AND int_value <= $2` |
| `min = max` | (same) | Equality | `int_value = $1` |

#### Address conditions

| Condition | Matches | Example |
|-----------|---------|---------|
| **AddressPrefix** | All accounts whose address starts with prefix | `AddressPrefix("merchants:")` |
| **AddressExact** | Single account by exact address | `AddressExact("merchants:acme_corp")` |

### 5.3 NOT Operator

`NOT` requires the "universe" of all accounts to compute the complement. This means:

```
NOT(StringEquals(AccountMetadata("status"), "suspended"))
= all accounts in ledger EXCEPT those with status=suspended
```

**Performance caveat**: `NOT` as a top-level filter is expensive (scans all accounts). Under an `AND`, the intersection with a more selective filter reduces the scan early. The query validator should **reject** queries where `NOT` is the outermost operator on a large ledger.

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
```

### 5.5 Resource Limits

Each leaf node consumes resources (iterator or SQL subquery). Recommended limits, validated at **query creation time**:

| Constraint | Suggested limit | Rationale |
|-----------|----------------|-----------|
| Max leaf nodes per query | 20 | Iterator/subquery overhead |
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
}

message CreatePreparedQueryRequest {
  PreparedQuery query = 1;
}

message ExecutePreparedQueryRequest {
  string ledger = 1;
  string query_name = 2;
  map<string, string> parameters = 3;        // parameter values as strings, parsed to target type at execution
                                              // e.g., "50000" → int64(50000) for IntCondition.param_min
  uint32 page_size = 4;                      // max results per page (default 15, max 1000)
  string cursor = 5;                         // opaque cursor from previous response (if set, other fields are ignored)
  uint64 min_log_sequence = 6;               // optional: wait until read store has indexed up to this log sequence
}

message ExecutePreparedQueryResponse {
  Cursor cursor = 1;
}

// Cursor follows the reference implementation pattern (bunpaginate).
// The cursor string is a base64-encoded JSON containing the full pagination state.
message Cursor {
  uint32 page_size = 1;
  bool has_more = 2;
  string previous = 3;                       // opaque cursor to previous page (empty if first page)
  string next = 4;                           // opaque cursor to next page (empty if last page)
  repeated common.Account data = 5;
}
```

- **Create**: `CreatePreparedQueryRequest` → Raft command → validated (resource limits, condition/schema compatibility) → persisted at `[0xE0][ledgerName\x00][queryName]` in primary store
- **Update**: `UpdatePreparedQueryRequest` → Raft command → re-validated → overwrites filter at same key. Designed to be batched with `SetMetadataFieldType` in the same proposal for atomic schema + query updates.
- **Execute**: `ExecutePreparedQueryRequest` → read-only against the read index store → streams matching accounts
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
| `POST /{ledger}/prepared-queries/{name}/execute` | `ExecutePreparedQuery` | Body: `{ parameters }`. Query params: `page_size`, `cursor`. When `cursor` is set, all other params are ignored (state is encoded in the cursor). Response: `{ cursor: { pageSize, hasMore, previous, next, data } }` |

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
5. Write index updates to the read store
6. Persist progress (`lastIndexedLogSequence`) in the read store

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

When updating the metadata inverted index, the index builder must know the **old** metadata value to delete the stale index entry. The read store maintains its own reverse mapping:

**In bbolt (key-value approach):**

```
Reverse map:   [0x0E][ledgerName\x00][account\x00][metadataKey] → currentValue
Forward index: [0x0C][ledgerName\x00][metadataKey\x00][metadataValue\x00][accountAddress] → (empty)
```

**In DuckDB:** the `account_metadata` table naturally stores the current value. An `UPDATE` handles both old-value cleanup and new-value insertion.

When the index builder processes a `SetMetadata(account, key, newValue)`:

1. Read old value (from reverse map or `SELECT` on `account_metadata`)
2. Delete old forward index entry (or let the `UPDATE` handle it)
3. Insert new forward index entry
4. Update reverse map

This makes the primary store and read store **fully independent** for random metadata lookups — no cross-disk I/O.

**Transaction metadata** can be modified after creation (same as account metadata) and its types can change via `SetMetadataFieldType`. The same old-value problem applies — the index builder needs the previous value to delete stale index entries. The reverse metadata map pattern applies identically:

- **bbolt**: `[0x0E][ledgerName\x00][tx:{txId}\x00][metadataKey]` → `MetadataValue protobuf`
- **DuckDB**: the `transaction_metadata` table stores the current value natively (`INSERT OR REPLACE`)

### 7.3 Consistency Model

The read store is **eventually consistent** with bounded lag:

- Typical lag: milliseconds (bounded by read store write throughput)
- `lastIndexedLogSequence` is exposed via API for freshness checks
- Queries can specify `min_log_sequence` to wait until the read store catches up (opt-in strong consistency, at the cost of latency)
- The log sequence (global, monotonically increasing) is a better progress marker than the Raft index because it only counts accepted operations

### 7.4 Crash Recovery

If the read store is lost or corrupted:

1. The primary store is unaffected (all authoritative data is there)
2. `store rebuild-indexes` replays all system logs (`[0x01]`) and reconstructs the read store from scratch
3. During rebuild, prepared query execution returns an error ("indexes not ready")
4. No data loss — the read store is entirely derived from system logs

On normal restart, the index builder resumes from `lastIndexedLogSequence` stored in the read store and catches up by tailing the remaining system logs.

### 7.5 Schema Change Handling

When a metadata key's type changes (`SetMetadataFieldType`), both the read store indexes and any prepared queries referencing that key must be updated.

#### Index rebuild (automatic)

When the index builder processes a `SetMetadataFieldType` log, it re-encodes all inverted index entries for that key using the new type. Uses the existing `ConvertMetadataValue` matrix (`internal/proto/commonpb/metadata_convert.go`) to convert each value:

- **bbolt**: scan and delete all `[0x0C][ledger\x00][key\x00][oldTypeTag]...` entries, re-insert with `[newTypeTag][newEncodedValue]`
- **DuckDB**: `UPDATE account_metadata SET value_type=$1, int_value=$2, ... WHERE ledger=$3 AND key=$4` (same for `transaction_metadata`)

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

## 8. Storage Engine Options

### 8.1 Option A — bbolt (B+ Tree, Pure Go)

Use bbolt as the read store engine. Implement inverted indexes, merge-join iterators, and the AND/OR/NOT operator tree manually.

**Key layout:**

| Prefix | Key | Value | Purpose |
|--------|-----|-------|---------|
| `0x0C` | `[ledgerName\x00][metadataKey\x00][typeTag(1B)][sortableValue][accountAddress]` | (empty) | Metadata inverted index |
| `0x0D` | `[ledgerName\x00][accountAddress]` | (empty) | Account existence index |
| `0x0E` | `[ledgerName\x00][account\x00][metadataKey]` | `MetadataValue protobuf` | Reverse metadata map |

The `typeTag` byte and `sortableValue` encoding depend on the metadata type:

| MetadataType | typeTag | sortableValue encoding | Rationale |
|-------------|---------|----------------------|-----------|
| STRING | `'S'` | Raw UTF-8 bytes + `\x00` separator | Byte-lexicographic = string sort order |
| INT8..INT64 | `'I'` | 8 bytes big-endian, sign bit XOR'd (`v ^ 0x8000000000000000`) | XOR flips sign bit → negative sorts before positive in unsigned byte order |
| UINT8..UINT64 | `'U'` | 8 bytes big-endian | Natural byte order = numeric order |
| BOOL | `'B'` | 1 byte (`0x00` = false, `0x01` = true) | |
| NullValue | `'N'` | Original raw string + `\x00` separator | Allows ExistsCondition to scan null entries |

**String equality** — point scan on `[0x0C][ledger\x00][key\x00]['S'][value\x00]`.

**Integer range** — range scan:
```
lower: [0x0C][ledger\x00][key\x00]['I'][encode(min)]
upper: [0x0C][ledger\x00][key\x00]['I'][encode(max+1)]
```

This exploits bbolt's B+ tree sorted iteration — the encoded values sort in numeric order.

**Query execution:** the filter tree is compiled into an **iterator tree**. Each leaf opens a bbolt cursor (range scan or point lookup). Operators compose cursors:

- `AND` → merge-intersect of N sorted cursors
- `OR` → merge-union of N sorted cursors
- `NOT` → merge-difference against the account existence index (`0x0D`)

All cursors are lazy and streaming — no intermediate materialization. Memory usage is constant regardless of result set size.

```
Execution example — string equality:
  AND(OR(StringEquals("category","premium"), StringEquals("category","gold")), AddressPrefix("merchants:"))

  cursor1: scan [0x0C][ledger\x00][category\x00]['S'][premium\x00]  → [acc_A, acc_C, acc_F]
  cursor2: scan [0x0C][ledger\x00][category\x00]['S'][gold\x00]     → [acc_B, acc_F, acc_G]
  cursor3: OR(cursor1, cursor2)                                       → [acc_A, acc_B, acc_C, acc_F, acc_G]
  cursor4: scan [0x0D][ledger\x00][merchants:]...[merchants:\xFF]    → [acc_A, acc_B, acc_C, acc_F, acc_H]
  cursor5: AND(cursor3, cursor4)                                      → [acc_A, acc_B, acc_C, acc_F]

Execution example — integer range (credit_limit >= 10000):
  cursor1: range [0x0C][ledger\x00][credit_limit\x00]['I'][encode(10000)]
                 ...[0x0C][ledger\x00][credit_limit\x00]['I'][\xFF x 8]
           → all accounts with credit_limit >= 10000
```

**Merge-join performance:** O(N x S) where N = number of filters, S = smallest result set. High-selectivity filters terminate quickly. Sub-second on SSD for typical use cases (10-20% selectivity on 1M accounts).

**Pros:** pure Go, no CGo, battle-tested (etcd, Consul), full control over storage layout.

**Cons:** significant custom code (iterator tree, merge-join, merge-union, merge-difference, sortable encoding per type), each new query type requires new code, no query optimizer. Typed range queries require careful binary encoding — more complex than string-only indexes.

### 8.2 Option B — DuckDB (Columnar SQL, CGo)

Use DuckDB as the read store engine. Replace the entire custom indexing layer with relational tables and SQL.

**Schema (typed metadata):**

```sql
CREATE TABLE accounts (
  ledger VARCHAR NOT NULL,
  account VARCHAR NOT NULL,
  PRIMARY KEY (ledger, account)
);

CREATE TABLE account_metadata (
  ledger VARCHAR NOT NULL,
  account VARCHAR NOT NULL,
  key VARCHAR NOT NULL,
  value_type TINYINT NOT NULL,     -- MetadataType enum (0=STRING, 1=INT64, 2=BOOL, 3=UINT64, ...)
  string_value VARCHAR,            -- populated when value_type = STRING
  int_value BIGINT,                -- populated for INT8/INT16/INT32/INT64
  uint_value UBIGINT,              -- populated for UINT8/UINT16/UINT32/UINT64
  bool_value BOOLEAN,              -- populated when value_type = BOOL
  null_original VARCHAR,           -- raw value preserved for NullValue (inconvertible)
  PRIMARY KEY (ledger, account, key)
);

-- No generic indexes here — indexes are created dynamically per prepared query
-- (see Section 8.3: Query-Driven Indexes)

CREATE TABLE transaction_metadata (
  ledger VARCHAR NOT NULL,
  tx_id BIGINT NOT NULL,
  key VARCHAR NOT NULL,
  value_type TINYINT NOT NULL,     -- same MetadataType enum as account_metadata
  string_value VARCHAR,
  int_value BIGINT,
  uint_value UBIGINT,
  bool_value BOOLEAN,
  null_original VARCHAR,
  PRIMARY KEY (ledger, tx_id, key)
);

-- Transaction metadata can be modified (SaveMetadata on transaction target)
-- Same old-value handling as account_metadata (INSERT OR REPLACE)
-- Indexes created dynamically per prepared query (see Section 8.3)

CREATE TABLE account_transactions (
  ledger VARCHAR NOT NULL,
  account VARCHAR NOT NULL,
  tx_id BIGINT NOT NULL,
  PRIMARY KEY (ledger, account, tx_id)
);

CREATE TABLE ledger_logs (
  ledger VARCHAR NOT NULL,
  log_id BIGINT NOT NULL,
  global_sequence BIGINT NOT NULL,
  PRIMARY KEY (ledger, log_id)
);
```

**Query execution:** the filter tree compiles to a **SQL prepared statement**. Each condition type maps to the appropriate column:

```sql
-- StringCondition: exact match on string_value
WHERE key = 'category' AND string_value = 'premium'

-- IntCondition: range on int_value
WHERE key = 'credit_limit' AND int_value >= 10000

-- IntCondition: BETWEEN
WHERE key = 'credit_limit' AND int_value >= 10000 AND int_value <= 50000

-- BoolCondition: equality on bool_value
WHERE key = 'active' AND bool_value = true

-- ExistsCondition (without null): key exists and is not NullValue
WHERE key = 'email' AND value_type != 3
```

**Full example — string + range + boolean:**

```sql
-- Filter: AND(StringEquals("category", "premium"), IntRange("credit_limit", min: 10000), BoolEquals("active", true))
SELECT DISTINCT a.account
FROM accounts a
JOIN account_metadata m1
  ON a.ledger = m1.ledger AND a.account = m1.account
  AND m1.key = 'category' AND m1.string_value = 'premium'
JOIN account_metadata m2
  ON a.ledger = m2.ledger AND a.account = m2.account
  AND m2.key = 'credit_limit' AND m2.int_value >= 10000
JOIN account_metadata m3
  ON a.ledger = m3.ledger AND a.account = m3.account
  AND m3.key = 'active' AND m3.bool_value = true
WHERE a.ledger = $1
  AND a.account > $3                        -- keyset pagination
ORDER BY a.account
LIMIT $2
```

The old-value problem disappears — `INSERT OR REPLACE` / `UPDATE` on `account_metadata` handles it natively. The typed columns are updated together:

```sql
INSERT OR REPLACE INTO account_metadata
  (ledger, account, key, value_type, string_value, int_value, uint_value, bool_value, null_original)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9);
```

DuckDB also enables typed queries beyond metadata filtering with zero additional code:

```sql
-- AggregateBalances (if volumes are also stored in DuckDB)
SELECT asset, SUM(input) as total_input, SUM(output) as total_output
FROM account_volumes
WHERE ledger = $1 AND account LIKE $2 || '%'
GROUP BY asset;

-- Account count per ledger
SELECT COUNT(*) FROM accounts WHERE ledger = $1;

-- Average credit limit of premium accounts
SELECT AVG(m2.int_value) as avg_credit_limit
FROM account_metadata m1
JOIN account_metadata m2 ON m1.ledger = m2.ledger AND m1.account = m2.account
WHERE m1.ledger = $1
  AND m1.key = 'category' AND m1.string_value = 'premium'
  AND m2.key = 'credit_limit';
```

**Pros:** dramatically less custom code, built-in query optimizer, columnar + vectorized scan, SQL covers future queries, native aggregations, query-driven indexes (see Section 8.3).

**Cons:** CGo required (`github.com/marcboeker/go-duckdb`), +30-50 MB binary size, younger Go bindings, cross-compilation complexity.

### 8.3 Query-Driven Indexes (DuckDB)

Prepared queries tell us **at creation time** exactly which fields and operators the client will use. Instead of maintaining generic indexes on all metadata keys, we create **partial indexes targeted to each prepared query's fields**.

#### Generic vs. query-driven

```sql
-- GENERIC (indexes ALL keys of a given type — wasteful)
CREATE INDEX idx_meta_int ON account_metadata (ledger, key, int_value)
  WHERE value_type IN (1, 4, 5, 6);
-- Contains entries for credit_limit, monthly_volume, retry_count, age, ...

-- QUERY-DRIVEN (indexes only the keys actually referenced by prepared queries)
CREATE INDEX idx_pq_credit_limit
  ON account_metadata (ledger, int_value, account)
  WHERE key = 'credit_limit';
-- Contains only credit_limit entries — much smaller, much faster
```

**Why this is better:**

| Aspect | Generic index | Query-driven partial index |
|--------|--------------|---------------------------|
| **Size** | All keys of that type | Only the referenced key |
| **Selectivity** | `key = 'credit_limit'` filtered at runtime | `WHERE key = 'credit_limit'` baked into the index — already filtered |
| **Covering** | Needs lookup in main table | `(ledger, int_value, account)` covers the query → index-only scan |
| **Scan** | Traverses irrelevant entries | Only relevant entries |

#### Index naming convention

Indexes are named deterministically from the field reference:

```
idx_pq_{table}_{key}_{condition_type}
```

Examples:
```sql
-- AccountMetadata("category") + StringCondition
CREATE INDEX idx_pq_am_category_string
  ON account_metadata (ledger, string_value, account)
  WHERE key = 'category';

-- AccountMetadata("credit_limit") + IntCondition
CREATE INDEX idx_pq_am_credit_limit_int
  ON account_metadata (ledger, int_value, account)
  WHERE key = 'credit_limit';

-- AccountMetadata("active") + BoolCondition
CREATE INDEX idx_pq_am_active_bool
  ON account_metadata (ledger, bool_value, account)
  WHERE key = 'active';

-- TransactionMetadata("amount") + IntCondition
CREATE INDEX idx_pq_tm_amount_int
  ON transaction_metadata (ledger, int_value, tx_id)
  WHERE key = 'amount';
```

#### Lifecycle

| Event | Action |
|-------|--------|
| `CreatePreparedQuery` | Extract `(field, condition_type)` pairs → `CREATE INDEX IF NOT EXISTS` for each |
| `DeletePreparedQuery` | Check if any other query references each index → `DROP INDEX` if orphan |
| `UpdatePreparedQuery` | Diff old vs new fields → create missing indexes, drop orphaned ones |
| `store rebuild-indexes` | Drop all `idx_pq_*` indexes, scan all query definitions, recreate |

The index builder maintains a **reference count** per index in a metadata table:

```sql
CREATE TABLE index_registry (
  index_name VARCHAR PRIMARY KEY,
  field_key VARCHAR NOT NULL,         -- e.g., "credit_limit"
  field_source VARCHAR NOT NULL,      -- "account_metadata" or "transaction_metadata"
  condition_type VARCHAR NOT NULL,    -- "string", "int", "uint", "bool", "exists"
  ref_count INTEGER NOT NULL DEFAULT 1
);
```

Multiple prepared queries referencing the same `(field, condition_type)` share the same index — `ref_count` tracks the number of references.

#### Impact on write path

Each metadata update in the index builder touches only the partial indexes for that specific key. A key not referenced by any prepared query has **zero index overhead** — no wasted writes on unqueried metadata.

### 8.4 Comparison

| Criteria | bbolt + custom | DuckDB |
|----------|---------------|--------|
| **Custom code** | ~2000-3000 LOC (indexes, iterators, merge ops, sortable encoding) | ~500 LOC (SQL generation, schema migration, index lifecycle) |
| **Typed metadata** | Custom binary encoding per type (sign-bit XOR for int, big-endian for uint) | Native typed columns (`BIGINT`, `UBIGINT`, `BOOLEAN`) — zero custom encoding |
| **Range queries** | Range scan on sortable-encoded values | `int_value >= $1 AND int_value <= $2` — native |
| **Query-driven indexes** | Already key-scoped by design (`[0x0C][ledger\x00][key\x00]...`) | Partial indexes per prepared query — covering, index-only scans, zero overhead on unqueried keys |
| **Scan performance** | Good (B+ tree, single-level) | Excellent (columnar, vectorized, SIMD) |
| **Aggregations** | Must implement manually | `SUM`, `COUNT`, `AVG`, `GROUP BY` native |
| **Query optimizer** | None | Sophisticated (join reordering, predicate pushdown, partial index selection) |
| **Pure Go** | Yes | No (CGo) |
| **Binary size** | Negligible | +30-50 MB |
| **Future flexibility** | Each query type = new code | SQL covers nearly everything |
| **Maturity in Go** | Very mature (etcd) | Younger bindings |

### 8.5 Recommendation

**DuckDB if CGo is acceptable, bbolt otherwise.**

The read store is fundamentally an analytical problem (scan, filter, aggregate). DuckDB is purpose-built for this. The code simplification is dramatic and it opens the door to queries not yet anticipated.

If CGo is a deal-breaker (cross-compilation, build complexity, memory safety at boundary), bbolt with the custom iterator tree is the fallback — more code but fully self-contained in pure Go.

## 9. Dedicated Disk — Is It Necessary?

**No, but it helps.**

The index builder is asynchronous, so it **never blocks the FSM**. Whether both stores share a disk or not, the consensus critical path is unaffected. The two stores are fully independent (no cross-disk I/O thanks to the reverse metadata map / DuckDB's self-contained tables).

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
Disk 3 (large):        --read-index-dir /mnt/ssd-read/   ← Read index store
```

For simpler deployments, disk 2 and 3 can be the same.

**Configuration:**

```
--read-index-dir    /mnt/read-ssd/indexes    (separate disk mount point)
--read-index-enable true                       (opt-in, disabled by default)
```

Default: `{data-dir}/read-indexes/` (same disk).

## 10. Key Prefixes (bbolt approach)

These prefixes apply when using bbolt. With DuckDB, they are replaced by table schemas.

| Prefix | Key | Value | Purpose |
|--------|-----|-------|---------|
| `0x0C` | `[ledgerName\x00][metadataKey\x00][typeTag(1B)][sortableValue][accountAddress]` | (empty) | Typed metadata inverted index |
| `0x0D` | `[ledgerName\x00][accountAddress]` | (empty) | Account existence index |
| `0x0E` | `[ledgerName\x00][account\x00][metadataKey]` | `MetadataValue protobuf` | Reverse metadata map (current typed value) |
| `0xE0` | `[ledgerName\x00][queryName]` | `PreparedQuery protobuf` | Query definitions (primary store) |

`typeTag` values: `'S'` (string), `'I'` (signed int), `'U'` (unsigned int), `'B'` (bool), `'N'` (null). See Section 8.1 for encoding details.

`0x0C`, `0x0D`, `0x0E` live in the read store. `0xE0` lives in the primary Pebble store.

## 11. Execution Examples

### 11.1 String equality + boolean combination

**Use case**: "List active premium/gold merchants, excluding suspended ones"

```
Prepared query "active_premium_merchants":
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

**DuckDB:**

```sql
SELECT DISTINCT a.account
FROM accounts a
JOIN account_metadata m1
  ON a.ledger = m1.ledger AND a.account = m1.account
  AND m1.key = 'category' AND m1.string_value IN ('premium', 'gold')
WHERE a.ledger = $1
  AND a.account LIKE 'merchants:%'
  AND NOT EXISTS (
    SELECT 1 FROM account_metadata m2
    WHERE m2.ledger = a.ledger AND m2.account = a.account
    AND m2.key = 'status' AND m2.string_value = 'suspended'
  )
  AND a.account > $3                        -- keyset pagination
ORDER BY a.account
LIMIT $2
```

**bbolt:**

```
  cursor1: scan [0x0C][ledger\x00][category\x00]['S'][premium\x00]  → [acc_A, acc_C, acc_F]
  cursor2: scan [0x0C][ledger\x00][category\x00]['S'][gold\x00]     → [acc_B, acc_F, acc_G]
  cursor3: OR(cursor1, cursor2)                                       → [acc_A, acc_B, acc_C, acc_F, acc_G]
  cursor4: scan [0x0D][ledger\x00][merchants:]...[merchants:\xFF]    → [acc_A, acc_B, acc_C, acc_F, acc_H]
  cursor5: scan [0x0C][ledger\x00][status\x00]['S'][suspended\x00]  → [acc_B]
  cursor6: NOT(cursor5) via universe [0x0D][ledger\x00]              → [acc_A, acc_C, acc_F, acc_G, acc_H, ...]
  cursor7: AND(cursor3, cursor4, cursor6)                             → [acc_A, acc_C, acc_F]
```

### 11.2 Integer range query

**Use case**: "List high-value merchants with credit limit between 10K and 50K"

```
Prepared query "high_value_merchants":
  filter: AND(
    IntRange(AccountMetadata("credit_limit"), min: 10000, max: 50000),
    AddressPrefix(hardcoded: "merchants:")
  )

Client: {} (no parameters — all values hardcoded)
```

**DuckDB:**

```sql
SELECT DISTINCT a.account
FROM accounts a
JOIN account_metadata m1
  ON a.ledger = m1.ledger AND a.account = m1.account
  AND m1.key = 'credit_limit' AND m1.int_value >= 10000 AND m1.int_value <= 50000
WHERE a.ledger = $1
  AND a.account LIKE 'merchants:%'
  AND a.account > $3                        -- keyset pagination
ORDER BY a.account
LIMIT $2
```

**bbolt:**

```
  cursor1: range [0x0C][ledger\x00][credit_limit\x00]['I'][encode(10000)]
             ... [0x0C][ledger\x00][credit_limit\x00]['I'][encode(50001)]
           → [acc_A(15000), acc_C(30000), acc_F(10000), acc_H(45000)]
  cursor2: scan [0x0D][ledger\x00][merchants:]...[merchants:\xFF]
           → [acc_A, acc_C, acc_F, acc_H, acc_J]
  cursor3: AND(cursor1, cursor2)
           → [acc_A, acc_C, acc_F, acc_H]
```

### 11.3 Parameterized range with boolean filter

**Use case**: "List active accounts with balance above a threshold (threshold provided at execution)"

```
Prepared query "active_above_threshold":
  filter: AND(
    IntRange(AccountMetadata("monthly_volume"), param_min: "min_volume"),
    BoolEquals(AccountMetadata("active"), hardcoded: true),
    AddressPrefix(param: "prefix")
  )

Client: { "min_volume": "50000", "prefix": "merchants:" }
```

**DuckDB:**

```sql
SELECT DISTINCT a.account
FROM accounts a
JOIN account_metadata m1
  ON a.ledger = m1.ledger AND a.account = m1.account
  AND m1.key = 'monthly_volume' AND m1.int_value >= $3
JOIN account_metadata m2
  ON a.ledger = m2.ledger AND a.account = m2.account
  AND m2.key = 'active' AND m2.bool_value = true
WHERE a.ledger = $1
  AND a.account LIKE $4 || '%'
  AND a.account > $5                        -- keyset pagination
ORDER BY a.account
LIMIT $2
-- $1='default', $2=100, $3=50000, $4='merchants:', $5='' (cursor)
```

**bbolt:**

```
  cursor1: range [0x0C][ledger\x00][monthly_volume\x00]['I'][encode(50000)]
             ... [0x0C][ledger\x00][monthly_volume\x00]['I'][\xFF x 8]
           → [acc_A(75000), acc_F(50000), acc_H(120000)]
  cursor2: scan [0x0C][ledger\x00][active\x00]['B'][\x01]
           → [acc_A, acc_C, acc_F, acc_H]
  cursor3: scan [0x0D][ledger\x00][merchants:]...[merchants:\xFF]
           → [acc_A, acc_C, acc_F, acc_H, acc_J]
  cursor4: AND(cursor1, cursor2, cursor3)
           → [acc_A, acc_F, acc_H]
```

### 11.4 Complex mixed query

**Use case**: "List EU or US merchants with credit limit >= 5000, category premium or gold, that are not suspended"

```
Prepared query "qualified_regional_merchants":
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

**DuckDB:**

```sql
SELECT DISTINCT a.account
FROM accounts a
JOIN account_metadata m1
  ON a.ledger = m1.ledger AND a.account = m1.account
  AND m1.key = 'region' AND m1.string_value IN ('eu', 'us')
JOIN account_metadata m2
  ON a.ledger = m2.ledger AND a.account = m2.account
  AND m2.key = 'category' AND m2.string_value IN ('premium', 'gold')
JOIN account_metadata m3
  ON a.ledger = m3.ledger AND a.account = m3.account
  AND m3.key = 'credit_limit' AND m3.int_value >= $3
WHERE a.ledger = $1
  AND a.account LIKE $4 || '%'
  AND NOT EXISTS (
    SELECT 1 FROM account_metadata m4
    WHERE m4.ledger = a.ledger AND m4.account = a.account
    AND m4.key = 'suspended' AND m4.bool_value = true
  )
  AND a.account > $5                        -- keyset pagination
ORDER BY a.account
LIMIT $2
-- $1='default', $2=100, $3=5000, $4='merchants:', $5='' (cursor)
```

Note: DuckDB's optimizer can rewrite `OR(StringEquals, StringEquals)` on the same key as `IN (...)`, simplifying the JOIN pattern automatically.

## 12. Implementation Plan

1. **Read store infrastructure**: index builder (tails system logs via `NotifyLogsCommitted()`), read store interface (abstract over bbolt/DuckDB), configuration flags (`--read-index-dir`, `--read-index-enable`)
2. **Account existence tracking**: populate `accounts` table / `0x0D` index from Raft entries
3. **Account metadata index**: populate `account_metadata` table / `0x0C` + `0x0E` indexes, typed value encoding
4. **Transaction metadata index**: populate `transaction_metadata` table / same reverse map pattern, same typed encoding
5. **Schema change handling**: index builder detects `SetMetadataFieldType` logs → re-encodes all entries for the affected key via `ConvertMetadataValue`
6. **Prepared query CRUD**: Raft commands for create/delete, storage in `[0xE0]`, gRPC + HTTP handlers, condition/schema validation at creation time
7. **Query executor**: filter tree → iterator tree (bbolt) or SQL (DuckDB), condition/schema re-validation at execution time, pagination, streaming
8. **Backfill**: `store rebuild-indexes` CLI command to populate read store from existing logs

## 13. Decisions Record

| Topic | Decision | Rationale |
|---|---|---|
| **Filter model** | Recursive boolean tree (AND/OR/NOT) | Supports complex queries; composable; compiles naturally to both iterator trees and SQL |
| **Leaf types** | FieldCondition (FieldRef × Condition) + AddressMatch | Separates "where to look" (FieldRef: AccountMetadata, TransactionMetadata) from "what to match" (Condition: string/int/uint/bool/exists). Conditions are reusable across data sources. Extensible to future sources (e.g., volumes) without new condition types. |
| **Index population** | Asynchronous via index builder | Keeps FSM apply fast (RAM-only); no I/O contention on write path |
| **Feeding strategy** | Tail system logs (0x01) in Pebble, not raw Raft entries | Raw Raft entries include rejected proposals (insufficient funds, idempotency conflicts, etc.); system logs contain only accepted operations; reuses existing `NotifyLogsCommitted()` pattern from event emitter |
| **Read store autonomy** | Reverse metadata map (bbolt) or native UPDATE (DuckDB) | Eliminates cross-store random I/O for metadata lookups; sequential log reads from Pebble are acceptable |
| **Storage engine** | DuckDB (preferred) or bbolt (fallback) | DuckDB: minimal code, built-in optimizer, columnar perf, query-driven partial indexes. bbolt: pure Go, no CGo |
| **Query-driven indexes** | Partial indexes created per prepared query, not generic | Prepared queries declare their fields at creation → `CREATE INDEX ... WHERE key = 'X'`. Smaller indexes, covering scans, zero write overhead on unqueried keys. Reference-counted, dropped when no query references them. |
| **Dedicated disk** | Optional, not required | Async builder already decouples FSM; beneficial on SATA SSD, not needed on NVMe |
| **Consistency** | Eventually consistent with opt-in freshness wait | Bounded lag (ms); `min_log_sequence` for strong consistency when needed |
| **Validation** | At creation time + execution time | Creation: enforce resource limits, condition/schema type compatibility. Execution: re-validate condition/schema match (catches schema changes not batched with query updates) |
| **Condition/type matching** | Condition type must match declared `MetadataType` for the key | `IntCondition` only on `INT8`..`INT64` keys, `UintCondition` only on `UINT8`..`UINT64`, `BoolCondition` only on `BOOL`, `StringCondition` only on `STRING` or untyped keys. `ExistsCondition` on any type. Enforced at creation and execution. |
| **NullValue visibility** | NullValue entries are invisible to typed conditions | `IntRange` won't match a NullValue — this is acceptable. `ExistsCondition(include_null: true)` can explicitly detect inconvertible values. Consistent with the typed metadata system's semantics. |
| **Pagination** | Cursor-based (keyset) following the reference implementation pattern | Opaque base64-encoded cursor with `next`/`previous` pointers. Keyset pagination (`account > $cursor`) instead of OFFSET — stable under concurrent inserts, O(1) seek. |
| **Transaction metadata** | Indexed in the read store (same pattern as account metadata) | Transaction metadata is mutable (can be modified via SaveMetadata) and types can change; same reverse map / INSERT OR REPLACE pattern as account metadata; enables filtering transactions by metadata |
| **Schema change handling** | Auto-rebuild indexes + user-managed query updates | Index builder re-encodes entries automatically via `ConvertMetadataValue` matrix. Prepared queries are the user's responsibility — schema change + `UpdatePreparedQuery` batched in the same `Proposal` (atomic). Execution-time validation rejects condition/schema mismatches with a clear error. No runtime conversion magic. |

## 14. Open Questions

1. **CGo decision**: is CGo acceptable for the project? This is the key factor for DuckDB vs bbolt. Impacts cross-compilation, build time, binary size.
2. **Volume-based post-filters**: should prepared queries support conditions like `balance(USD/2) > 1000`? This would require loading account volumes for each candidate after metadata filtering. Potentially expensive.
3. **String value length in bbolt**: string metadata values can be arbitrarily long. For the bbolt inverted index key, should long values be truncated + hashed to keep keys bounded? Risk: hash collisions require post-filtering. Less of an issue with DuckDB (native VARCHAR indexing). With typed metadata, most filterable keys will have short strings or numeric values.
4. **Prepared query limits per ledger**: should there be a maximum number? Query definitions are small but replicated via Raft.
5. **Untyped keys**: for metadata keys with no declared schema, only `StringCondition` is allowed. Should `ExistsCondition` also be allowed on untyped keys? (Probably yes — it doesn't depend on the value type.)
