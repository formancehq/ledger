# Typed Metadata

## Overview

Metadata values support multiple types beyond plain strings. The `MetadataValue` proto uses a `oneof` discriminated union supporting `string`, `int64`, `uint64`, `bool`, and `NullValue` (for inconvertible values). An explicit **metadata schema** per ledger declares the expected type for each key, enabling automatic type enforcement on writes and lazy conversion on reads.

## Supported Types

| Type | Proto field | Go type | Use case |
|------|------------|---------|----------|
| String | `string_value` | `string` | Labels, categories, free-text |
| Signed Integer | `int_value` | `int64` | Timestamps, counters, thresholds, deltas |
| Unsigned Integer | `uint_value` | `uint64` | IDs, sequence numbers, non-negative counters |
| Boolean | `bool_value` | `bool` | Flags (kyc_verified, is_active, etc.) |
| Null | `null_value` | `*NullValue` | Inconvertible value placeholder (preserves original) |

The `MetadataType` enum also supports sub-64-bit integer types (`INT8`, `INT16`, `INT32`, `UINT8`, `UINT16`, `UINT32`) with range validation. These are stored using the same `int_value`/`uint_value` proto fields but enforce bounds at read/write time.

**Intentionally excluded:** `float64` (dangerous in financial software), `[]byte`, nested objects/arrays.

### Wire Compatibility

Field 1 (`string_value`) has the same wire type and field number as the previous `string value = 1` field. Existing serialized data decodes correctly into the `string_value` branch with zero migration.

## Proto Schema

### common.proto

```protobuf
message NullValue {
  string original = 1;  // raw value before failed conversion
}

message MetadataValue {
  oneof type {
    string string_value = 1;
    int64 int_value = 2;
    bool bool_value = 3;
    NullValue null_value = 4;
    uint64 uint_value = 5;
  }
}

enum MetadataType {
  METADATA_TYPE_STRING = 0;
  METADATA_TYPE_INT64 = 1;
  METADATA_TYPE_BOOL = 2;
  METADATA_TYPE_UINT64 = 3;
  METADATA_TYPE_INT8 = 4;
  METADATA_TYPE_INT16 = 5;
  METADATA_TYPE_INT32 = 6;
  METADATA_TYPE_UINT8 = 7;
  METADATA_TYPE_UINT16 = 8;
  METADATA_TYPE_UINT32 = 9;
}

message MetadataFieldSchema {
  MetadataType type = 1;
  MetadataConversionStatus status = 2;
}

message MetadataSchema {
  map<string, MetadataFieldSchema> account_fields = 1;
  map<string, MetadataFieldSchema> transaction_fields = 2;
  map<string, MetadataFieldSchema> ledger_fields = 3;
}
```

The aggregate schema is stored per-ledger in `LedgerInfo.metadata_schema` (field 5).

## Metadata Schema Declaration

### Per-Key Operations

Schema is managed **key by key**, not as a monolithic block. Each operation targets a single metadata key:

- **`SetMetadataFieldTypeRequest`** — declares or changes the type of a single key. Triggers background conversion for that key.
- **`RemoveMetadataFieldTypeRequest`** — removes the type declaration. Existing values are converted back to string (always succeeds).

This means changing the type of one key does not affect other keys, and multiple conversions for different keys can run in parallel.

### Initial Schema at Ledger Creation

`CreateLedgerRequest` accepts an `initial_schema` field — a list of `SetMetadataFieldTypeCommand` entries applied at creation time with status `COMPLETE` (no conversion needed for a new ledger).

### Behavior Rules

1. **No type declared** — metadata stored as-is (type from proto oneof on gRPC, inferred from JSON on HTTP).
2. **Type declared** — on write, the value is converted to the declared type via the conversion matrix. On read, the value is guaranteed to be the declared type (or `NullValue`).
3. **Type changed** — triggers background conversion for that key only.
4. **Type removed** — values converted back to `string` (always succeeds).

## Type Conversion Matrix

Conversions **never produce errors at the storage level**. Inconvertible values become `NullValue` (preserving the original raw value). The client is responsible for ensuring metadata values match the expected format.

### From String

| Target | Rule | Failure |
|--------|------|---------|
| `int64` | `strconv.ParseInt(s, 10, 64)` | `NullValue` |
| `uint64` | `strconv.ParseUint(s, 10, 64)` | `NullValue` |
| `bool` | `"true"`/`"1"` -> `true`, `"false"`/`"0"` -> `false` | `NullValue` |
| `string` | identity | — |

### From Int64

| Target | Rule | Failure |
|--------|------|---------|
| `string` | `strconv.FormatInt(n, 10)` | never fails |
| `uint64` | direct cast if `n >= 0` | `NullValue` |
| `bool` | `0` -> `false`, non-zero -> `true` | never fails |

### From Uint64

| Target | Rule | Failure |
|--------|------|---------|
| `string` | `strconv.FormatUint(n, 10)` | never fails |
| `int64` | direct cast if `n <= math.MaxInt64` | `NullValue` |
| `bool` | `0` -> `false`, non-zero -> `true` | never fails |

### From Bool

| Target | Rule | Failure |
|--------|------|---------|
| `string` | `"true"` / `"false"` | never fails |
| `int64` | `true` -> `1`, `false` -> `0` | never fails |
| `uint64` | `true` -> `1`, `false` -> `0` | never fails |

### From NullValue

| Target | Rule | Failure |
|--------|------|---------|
| `string` | returns `original` field | never fails |
| `int64` | attempt parse of `original` | stays `NullValue` |
| `uint64` | attempt parse of `original` | stays `NullValue` |
| `bool` | attempt parse of `original` | stays `NullValue` |

**Design principle:** converting to `string` never fails. Converting from `string` to a narrower type may produce `NullValue`. Converting between `int64` and `uint64` may fail on overflow/sign.

Sub-64-bit types (`INT8`, `INT16`, etc.) follow the same rules with additional range checking. For example, converting `int64(200)` to `INT8` produces `NullValue` because 200 exceeds the `[-128, 127]` range.

## Hybrid Conversion Strategy

When a key's declared type changes, existing stored values for **that key only** must be converted. Two layers ensure correctness and eventual convergence.

### Layer 1: Lazy Read-Time Conversion

Any read that encounters a value whose stored type doesn't match the declared schema type applies the conversion matrix on-the-fly before returning. This check is cheap (one type comparison per read, branch-predicted hot path when types match).

This layer guarantees that **reads always return the declared type** (or `NullValue`) regardless of the store's current state.

**Implementation:**
- Account metadata: `enforceAccountSchema()` in `internal/application/ctrl/store.go`
- Transaction metadata: `enforceTransactionSchema()` in `internal/application/ctrl/controller_default.go`, applied during `assembleTransaction` which replays append-only update logs on every read

Both call `commonpb.TypeMatches()` and `commonpb.ConvertMetadataValue()`.

### Layer 2: Automatic Batched Conversion

When the FSM applies a `SetMetadataFieldTypeOrder`, the type declaration is updated and a background conversion is **automatically started** for that key on the leader (following the existing `Sealer`/`Archiver` pattern):

1. Type declaration Raft entry applied -> `LedgerInfo.metadata_schema` updated, status set to `CONVERTING`
2. Background goroutine on the leader receives the conversion task via channel
3. Goroutine scans Pebble for metadata entries matching (ledger, key) that don't match the declared type
4. Groups entries into batches and proposes each as a `ConvertMetadataBatchOrder` Raft entry
5. After all batches, proposes `MetadataConversionCompleteOrder`
6. FSM marks that key's status as `COMPLETE`

Concurrent conversions are bounded by a configurable pool size (default: 2). Excess conversions are queued.

**Implementation:** `MetadataConverter` in `internal/infra/state/metadata_converter.go`.

### Writes During Conversion

Writes during an ongoing conversion are safe: the schema is already declared, so new writes are converted immediately. If the background batch later encounters an already-converted entry, it skips it (no-op).

### Concurrent Schema Changes

If a new type change arrives for a key that is already converting, the current conversion is abandoned and restarted with the new type. Conversions for different keys proceed independently.

### Leader Changes

If the leader changes during conversion, the new leader detects `CONVERTING` status and restarts the scan. Already-converted entries are skipped (idempotent).

### Conversion Status

The `GetMetadataSchemaStatus` RPC returns per-key status (`CONVERTING` or `COMPLETE`).

## Raft Commands

### Orders in `LedgerApplyOrder` oneof

| Order | Purpose |
|-------|---------|
| `SetMetadataFieldTypeOrder` | Declares/changes type for a key, starts conversion |
| `RemoveMetadataFieldTypeOrder` | Removes type declaration, converts values back to string |
| `ConvertMetadataBatchOrder` | Batch of converted entries (proposed by background worker) |
| `MetadataConversionCompleteOrder` | Marks conversion complete for a key |

`ConvertMetadataBatchOrder` and `MetadataConversionCompleteOrder` include an `expected_type` field for staleness validation — if the declared type has changed since the batch was prepared, the batch is silently dropped.

## gRPC API

### Request Variants

| Request | Description |
|---------|-------------|
| `SetMetadataFieldTypeRequest` | Sets/changes the type of a metadata key on a ledger |
| `RemoveMetadataFieldTypeRequest` | Removes the type declaration for a metadata key |

Both are added to the `Request.type` oneof (fields 18-19).

### RPCs

| RPC | Description |
|-----|-------------|
| `GetMetadataSchemaStatus` | Returns per-key field status (type + conversion state) |

The aggregate schema is also available on `LedgerInfo.metadata_schema` (returned by `GetLedger`).

## HTTP API

### REST Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/{ledgerName}/metadata-schema` | Get schema status (field types + conversion progress) |
| `PUT` | `/{ledgerName}/metadata-schema/{targetType}/{key}` | Set/change metadata field type |
| `DELETE` | `/{ledgerName}/metadata-schema/{targetType}/{key}` | Remove metadata field type declaration |

The `targetType` path parameter accepts `account`, `transaction`, or `ledger`. The PUT body is `{ "type": "<metadataType>" }` where `metadataType` is one of: `string`, `int64`, `bool`, `uint64`, `int8`, `int16`, `int32`, `uint8`, `uint16`, `uint32`.

### JSON Type Inference

When no schema is declared for a key, the HTTP layer infers the type from JSON:

| JSON value | MetadataValue type |
|------------|-------------------|
| `true` / `false` | `bool_value` |
| Positive integer (fits `uint64`) | `uint_value` |
| Negative integer (fits `int64`) | `int_value` |
| String | `string_value` |
| Number with decimal point | **rejected** (floats not supported) |
| `null` | **deletes** the key |
| Object / array | **rejected** |

`NullValue` is serialized as JSON `null`.

## CLI Commands

| Command | Description |
|---------|-------------|
| `ledgerctl ledgers create --schema target:key:type,...` | Create ledger with initial schema |
| `ledgerctl ledgers set-metadata-type` | Set/change type for a metadata key |
| `ledgerctl ledgers remove-metadata-type` | Remove type declaration |
| `ledgerctl ledgers get-schema` | Display schema with conversion status |

## Numscript Integration

Numscript remains **string-only**. The typed metadata system bridges via conversion at the boundary:

- **Write path:** Numscript `set_tx_meta("key", "1000")` produces a string. If the schema declares `key` as `int64`, the conversion matrix is applied before storage.
- **Read path:** The store returns a typed value (e.g., `int_value: 1000`). The Numscript bridge converts to string (`"1000"`) for Numscript consumption.

## Key Files

| File | Purpose |
|------|---------|
| `misc/proto/common.proto` | MetadataValue oneof, NullValue, MetadataSchema, MetadataType |
| `misc/proto/raft_cmd.proto` | Schema/conversion Raft orders |
| `misc/proto/bucket.proto` | gRPC requests, GetMetadataSchemaStatus RPC |
| `internal/proto/commonpb/metadata_convert.go` | Conversion matrix implementation |
| `internal/proto/commonpb/metadata_convert_test.go` | Conversion matrix tests (98 cases) |
| `internal/proto/commonpb/metadata.go` | MetadataFromMap, MetadataToMap, JSON helpers |
| `internal/domain/processing/processor_metadata_schema.go` | Schema enforcement, set/remove field type |
| `internal/domain/processing/processor_convert_metadata.go` | Batch conversion, conversion complete |
| `internal/infra/state/metadata_converter.go` | Background conversion worker (Layer 2) |
| `internal/application/ctrl/store.go` | Lazy read-time conversion (Layer 1) |
| `internal/adapter/http/handlers_get_metadata_schema.go` | HTTP: GET metadata schema status |
| `internal/adapter/http/handlers_set_metadata_type.go` | HTTP: PUT set metadata field type |
| `internal/adapter/http/handlers_remove_metadata_type.go` | HTTP: DELETE remove metadata field type |
| `cmd/ledgerctl/ledgers/set_metadata_type.go` | CLI: set-metadata-type |
| `cmd/ledgerctl/ledgers/remove_metadata_type.go` | CLI: remove-metadata-type |
| `cmd/ledgerctl/ledgers/get_schema.go` | CLI: get-schema |

### Transaction Metadata

Transaction metadata is stored as append-only `TransactionUpdate` entries replayed on every read (`assembleTransaction`). Unlike account metadata (standalone Pebble attributes), background conversion would add new entries but old ones are still read — providing no performance benefit.

Therefore transaction metadata uses **read-time enforcement only** (Layer 1). The conversion lifecycle still runs for status consistency: when `SetMetadataFieldType` is called for a transaction field, the converter immediately proposes completion (CONVERTING → COMPLETE) without scanning.

## Future Work

- **Generation rotation optimization:** Opportunistic conversion during `rotateLocked()` to reduce lazy conversion overhead.
- **Numscript typed literals:** Extend Numscript parser for typed literals natively (coordinated with "typed variables" work).
- **ClickHouse evolution:** Evolve from `Map(String, String)` to `Map(String, Variant(...))` when advanced read queries land.
