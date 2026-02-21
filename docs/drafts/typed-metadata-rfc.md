# RFC: Typed Metadata Values

- **Status:** Draft
- **Target:** Ledger v3
- **Audience:** Ledger core implementers
- **Scope:** Replace string-only metadata values with a typed value system supporting `int64`, `uint64`, `bool`, and `string` — with an explicit metadata schema declaration mechanism, deterministic conversion rules, and a `NullValue` fallback for inconvertible data.

---

## 1. Motivation

### 1.1 Current State

Metadata in v3 is strictly `map[string]string`. The proto definition is:

```protobuf
message MetadataValue {
  string value = 1;  // always a string
}
```

This flows through the entire stack:
- **gRPC API** sends/receives `MetadataValue` with a single string field
- **HTTP API** accepts `{"key": "string_value"}` (OpenAPI: `additionalProperties: type: string`)
- **Pebble storage** stores the serialized `MetadataValue` proto (string payload)
- **ClickHouse events** flatten to `Map(String, String)`
- **Numscript** `set_tx_meta` / `set_account_meta` always pass string literals
- **go-libs `metadata.Metadata`** is `map[string]string`

### 1.2 Performance: GC Pressure from String Allocations

Every metadata value is a Go `string`, which means:
- Each value is a `(ptr, len)` pair pointing to heap-allocated bytes
- Deserialization from protobuf allocates a new string on the heap every time
- High-throughput workloads (e.g. bulk transaction creation with metadata) generate large volumes of short-lived string objects
- The GC must trace and collect all of them

For numeric metadata (timestamps, counters, amounts, IDs), an `int64` is **8 bytes on the stack** with **zero GC overhead**, vs a string like `"1708387200"` which is 10 bytes on the heap + 16 bytes header = 26 bytes + GC tracing cost.

For booleans, `true`/`false` strings are 4-5 bytes heap-allocated vs 1 byte inline.

In the attribute `KeyStore`, metadata values are stored in `kv.KV[U128, Entry[*commonpb.MetadataValue]]`. Each `*commonpb.MetadataValue` is a pointer to a struct containing a string. With typed values, many entries could become value types (no pointer indirection, no separate heap object).

### 1.3 Query Performance and Inverted Indexes

String-only values prevent meaningful indexing:
- **No range queries**: "find accounts where `balance_threshold` > 1000" requires parsing every string to a number at query time
- **No boolean filtering**: "find accounts where `kyc_verified` is true" requires string comparison against `"true"`
- **No numeric sorting**: sorting by a numeric metadata field requires runtime parsing

With typed values, inverted indexes become viable:
- Integer values can use B-tree indexes with native comparison
- Boolean values can use bitmap indexes (extremely compact)
- String values remain indexed as today

This is particularly relevant for the planned **read query** features (see `docs/drafts/advanced-read-queries.md`).

---

## 2. Proposed Design

### 2.1 Proto Schema Change

Extend `MetadataValue` in `misc/proto/common.proto` using a `oneof`:

```protobuf
message MetadataValue {
  oneof type {
    string string_value = 1;
    int64 int_value = 2;
    bool bool_value = 3;
    NullValue null_value = 4;
    uint64 uint_value = 5;
  }
}

// NullValue represents a metadata value that could not be converted to the
// declared type. It preserves the original raw value for debugging, but
// signals to consumers that the value is unusable in its expected type.
message NullValue {
  string original = 1;  // the raw value before failed conversion
}
```

#### Why `oneof`?

- **Wire-compatible**: field 1 (`string_value`) has the same wire type and field number as the current `string value = 1`. Existing serialized data decodes correctly into the `string_value` branch.
- **Zero-cost discrimination**: protobuf `oneof` stores the type tag inline (no extra field)
- **vtprotobuf friendly**: generates efficient marshal/unmarshal code per variant

#### Why not `any` / JSON?

- `google.protobuf.Any` adds a type URL string per value — too heavy for hot-path metadata
- Free-form JSON (`string json_value`) defers parsing to every consumer — defeats the purpose
- A bounded `oneof` gives us exhaustive switch statements in Go, compile-time type safety, and zero ambiguity

#### Supported Types

| Type | Proto field | Go type | Use case |
|------|------------|---------|----------|
| String | `string_value` | `string` | Labels, categories, free-text |
| Signed Integer | `int_value` | `int64` | Timestamps, counters, thresholds, deltas |
| Unsigned Integer | `uint_value` | `uint64` | IDs, sequence numbers, non-negative counters |
| Boolean | `bool_value` | `bool` | Flags (kyc_verified, is_active, etc.) |
| Null | `null_value` | `*NullValue` | Inconvertible value placeholder |

**Intentionally excluded:**
- `float64`: floating point in financial software is dangerous; use integer + scale convention. No float support, period.
- `[]byte`: no clear use case; can be base64-encoded string
- Nested objects/arrays: complexity explosion for marginal benefit; keep metadata flat

### 2.2 Metadata Schema Declaration

Metadata is free-form by default: a user can write a string, then later overwrite the same key with an integer. To enable typed storage, indexing, and conversion, we introduce an explicit **metadata schema** per ledger.

#### 2.2.1 Schema Proto

```protobuf
enum MetadataType {
  METADATA_TYPE_STRING = 0;
  METADATA_TYPE_INT64 = 1;
  METADATA_TYPE_BOOL = 2;
  METADATA_TYPE_UINT64 = 3;
}

// MetadataSchema is the aggregate view per-ledger.
// Stored in LedgerInfo, built from individual key declarations.
message MetadataSchema {
  map<string, MetadataType> account_fields = 1;
  map<string, MetadataType> transaction_fields = 2;
}
```

#### 2.2.2 Per-Key Operations

Schema is managed **key by key**, not as a monolithic block. Each operation targets a single metadata key:

```protobuf
// Declares or changes the type of a single metadata key.
message SetMetadataFieldTypeCommand {
  Target target_type = 1;         // ACCOUNT or TRANSACTION
  string key = 2;                 // metadata key name (e.g. "kyc_verified")
  MetadataType type = 3;          // declared type
}

// Removes the type declaration for a single metadata key.
// Existing values are converted back to string (always succeeds).
message RemoveMetadataFieldTypeCommand {
  Target target_type = 1;
  string key = 2;
}
```

This design means:
- Changing the type of `kyc_verified` does **not** affect an ongoing conversion of `balance_threshold`
- Multiple conversions for different keys can run in parallel
- Raft entries are lightweight (one key per command)

#### 2.2.3 Schema Attachment

The aggregate schema is stored **per-ledger** in `LedgerInfo`:

```protobuf
message LedgerInfo {
  string name = 1;
  Timestamp created_at = 2;
  uint32 id = 3;
  Timestamp deleted_at = 4;
  MetadataSchema metadata_schema = 5;  // NEW: aggregate view
}
```

The schema can be:
- Populated at ledger creation time (via `CreateLedgerRequest`, providing multiple key declarations at once)
- Modified later via individual `SetMetadataFieldType` / `RemoveMetadataFieldType` commands

#### 2.2.4 Behavior Rules

1. **No type declared for a key** → metadata is stored as-is (type from proto `oneof` on gRPC, inferred from JSON on HTTP). No conversion, no validation. This is the default.

2. **Type declared for a key** → on write, the value is converted to the declared type using the conversion matrix (section 2.3). On read, the value is guaranteed to be the declared type (or `NullValue` if inconvertible).

3. **Type changed for a key** → triggers background conversion of existing values for that key only (see section 2.4).

4. **Type removed for a key** → triggers conversion of existing values back to `string` (always succeeds). The key returns to free-form mode.

### 2.3 Type Conversion Matrix

Conversions **never produce errors at the storage level**. If a conversion is not possible, the result is `NullValue` (preserving the original raw value for debugging). The client is responsible for ensuring metadata values match the expected format.

#### Write Path: gRPC vs HTTP

- **gRPC** (primary interface): the client provides the value already typed via the proto `oneof`. If the type matches the schema, there is **zero conversion cost** — the value is stored directly. If the type doesn't match, the conversion matrix is applied.
- **HTTP (JSON)**: the JSON value is inferred (string/number/boolean) then converted to the declared schema type if one exists.

#### From String

| Target | Rule | Example | Failure |
|--------|------|---------|---------|
| `int64` | `strconv.ParseInt(s, 10, 64)` | `"42"` → `42` | `"hello"` → `NullValue{"hello"}` |
| `uint64` | `strconv.ParseUint(s, 10, 64)` | `"42"` → `42` | `"-1"` → `NullValue{"-1"}` |
| `bool` | `"true"`/`"1"` → `true`, `"false"`/`"0"` → `false` | `"true"` → `true` | `"maybe"` → `NullValue{"maybe"}` |
| `string` | identity | — | — |

#### From Int64

| Target | Rule | Example | Failure |
|--------|------|---------|---------|
| `string` | `strconv.FormatInt(n, 10)` | `42` → `"42"` | never fails |
| `uint64` | direct cast if `n >= 0` | `42` → `uint64(42)` | `-1` → `NullValue{"-1"}` |
| `bool` | `0` → `false`, non-zero → `true` | `1` → `true` | never fails |
| `int64` | identity | — | — |

#### From Uint64

| Target | Rule | Example | Failure |
|--------|------|---------|---------|
| `string` | `strconv.FormatUint(n, 10)` | `42` → `"42"` | never fails |
| `int64` | direct cast if `n <= math.MaxInt64` | `42` → `int64(42)` | `2^63` → `NullValue{"9223372036854775808"}` |
| `bool` | `0` → `false`, non-zero → `true` | `1` → `true` | never fails |
| `uint64` | identity | — | — |

#### From Bool

| Target | Rule | Example | Failure |
|--------|------|---------|---------|
| `string` | `"true"` / `"false"` | `true` → `"true"` | never fails |
| `int64` | `true` → `1`, `false` → `0` | `true` → `1` | never fails |
| `uint64` | `true` → `1`, `false` → `0` | `true` → `1` | never fails |
| `bool` | identity | — | — |

#### From NullValue

| Target | Rule | Example | Failure |
|--------|------|---------|---------|
| `string` | returns `original` field | `NullValue{"hello"}` → `"hello"` | never fails |
| `int64` | attempt parse of `original` | `NullValue{"42"}` → `42` | `NullValue{"x"}` → `NullValue{"x"}` |
| `uint64` | attempt parse of `original` | `NullValue{"42"}` → `42` | `NullValue{"x"}` → `NullValue{"x"}` |
| `bool` | attempt parse of `original` | `NullValue{"true"}` → `true` | `NullValue{"x"}` → `NullValue{"x"}` |

**Design principle:** converting to `string` never fails (everything has a string representation). Converting from `string` to a narrower type may produce `NullValue`. Converting between `int64` and `uint64` may fail on overflow/sign. Converting to/from `bool` never fails (well-defined numeric mapping).

### 2.4 Hybrid Conversion Strategy

When a key's declared type changes (or a new type is declared for a previously untyped key), existing stored values for **that key only** must be converted. This is a two-layer strategy: **lazy reads for immediate correctness**, **automatic background batches for full convergence**.

Every type declaration triggers conversion automatically for the affected key. Multiple conversions for different keys can run in parallel.

#### 2.4.1 Layer 1: Lazy Read-Time Conversion (Always Active)

Any read that encounters a value whose stored type doesn't match the declared schema type applies the conversion matrix on-the-fly before returning. This check is cheap (one type comparison per read, branch-predicted hot path when types match).

Additionally, during **generation rotation** (every ~`GenerationThreshold` Raft entries, ~1000 by default), metadata entries being compacted are converted opportunistically in the same Pebble batch. This piggybacks on `cache.go:rotateLocked()` — zero new concurrency primitives.

This layer guarantees that **reads always return the declared type** (or `NullValue`) regardless of what's in the store.

#### 2.4.2 Layer 2: Automatic Batched Conversion (Triggered on Schema Change)

When the FSM applies a `SetMetadataFieldTypeOrder`, the type declaration is updated instantly and a background conversion is **automatically started** for that key on the leader (following the existing `Sealer`/`Archiver` pattern):

1. Type declaration Raft entry applied → `LedgerInfo.metadata_schema` updated for this key, conversion status set to `CONVERTING`
2. Background goroutine on the leader receives the conversion task via channel (one task per key)
3. Goroutine scans Pebble for metadata entries matching (ledger, key) that don't match the declared type
4. Groups entries into batches (e.g., 10K entries per batch)
5. Proposes each batch as a Raft entry: `ConvertMetadataBatchOrder { entries: [...] }`
6. FSM applies each batch deterministically: read current value → convert → write back
7. After all batches, leader proposes `MetadataConversionCompleteOrder { key: "..." }`
8. FSM marks that key's conversion as `COMPLETE`

Multiple keys can be converted in parallel — each key has its own independent conversion lifecycle. The number of concurrent background conversions is capped **globally** (across all ledgers) and configurable via the `--max-concurrent-metadata-conversions` flag (default: **2**). This prevents Raft log flooding when many ledgers trigger schema changes simultaneously. Excess conversions are queued and start as slots become available.

This layer guarantees that **all entries for a key are eventually converted**, including cold entries that would never be reached by lazy reads or rotation alone.

#### 2.4.3 How the Two Layers Interact

```
Schema change applied
  ├── Immediately: all new writes use the declared type
  ├── Layer 1 (lazy): reads convert on-the-fly, rotations convert incrementally
  └── Layer 2 (batch): background goroutine scans and proposes conversion batches
                        ↓
              After all batches complete → status = COMPLETE
              Layer 1 type checks become no-ops (types already match)
```

Layer 1 ensures correctness from the first read after a schema change. Layer 2 ensures the store fully converges, eliminating the lazy conversion overhead on reads.

#### 2.4.4 Writes During Conversion

Writes during an ongoing conversion are safe and consistent:

1. The schema is already declared (applied by the first Raft entry)
2. A new write arrives for a key with a declared type
3. The write applies the conversion matrix to the input value → stores the result in the correct type
4. If the background batch later encounters this key, it reads the current value, sees it's already the correct type, and skips it (no-op)

No race condition is possible because:
- Lazy conversion runs inside `ApplyEntries()` (holds `fsm.mu`), same goroutine as writes
- Batch conversion entries are Raft entries, serialized with other entries by the FSM

#### 2.4.5 Concurrent Schema Changes

If a new schema change arrives while a conversion is in progress, the current conversion is **abandoned** and a new one starts for the updated schema. The old batches that haven't been applied yet are simply dropped (the leader stops proposing them). Already-applied batches may have converted values to the old type — the new conversion will re-convert them to the new type.

One active conversion **per key** at a time. If a new type change arrives for a key that is already converting, the current conversion for that key is abandoned and restarted with the new type. Conversions for different keys proceed independently.

#### 2.4.6 Leader Changes

If the leader changes during a batch conversion, the new leader detects that the conversion status is `CONVERTING` and restarts the scan from scratch. This is idempotent — already-converted keys are skipped.

#### 2.4.7 Conversion Status

A dedicated RPC allows clients to track conversion progress:

```protobuf
message GetMetadataSchemaStatusRequest {
  string ledger = 1;
}

message MetadataFieldStatus {
  MetadataType declared_type = 1;
  MetadataConversionStatus status = 2;
  uint64 converted_keys = 3;  // meaningful when status = CONVERTING
  uint64 total_keys = 4;      // meaningful when status = CONVERTING
}

enum MetadataConversionStatus {
  METADATA_CONVERSION_CONVERTING = 0; // batch conversion in progress
  METADATA_CONVERSION_COMPLETE = 1;   // all values match declared type (or NullValue)
}

message GetMetadataSchemaStatusResponse {
  map<string, MetadataFieldStatus> account_fields = 1;
  map<string, MetadataFieldStatus> transaction_fields = 2;
}
```

#### 2.4.8 Cost Estimation

| Scenario | Metadata keys | Batched conversion time |
|----------|--------------|------------------------|
| Small | 100 | ~130ms |
| Medium | 100K | ~15-30s |
| Large | 1M | ~3-5 min |

The lazy read-time conversion per-call cost is negligible (~one type comparison). The rotation piggyback cost is absorbed into the existing compaction cycle.

### 2.5 Numscript Integration

In the first phase, Numscript remains **string-only**. The typed metadata system bridges via conversion at the boundary:

#### Write path (Numscript → Store)

1. Numscript calls `set_tx_meta("threshold", "1000")` (always a string)
2. The processing layer receives the string value
3. If a schema declares `threshold` as `int64`, the conversion matrix is applied: `"1000"` → `int_value: 1000`
4. The converted value is stored
5. If the string cannot be converted to the declared type, `NullValue` is stored. This **can** be surfaced as a processing error since the Numscript is under the client's control.

#### Read path (Store → Numscript)

1. Numscript requests `meta(@account, "threshold")`
2. The store returns `int_value: 1000`
3. The Numscript bridge converts to string: `1000` → `"1000"`
4. Numscript receives `"1000"` as a string variable

In a later phase, Numscript can be extended to support typed literals natively (coordinated with the "typed variables" TODO item). At that point, the string conversion bridge becomes unnecessary for typed keys.

### 2.6 HTTP API

The HTTP compatibility layer infers types from JSON and serializes `NullValue` as `null`. See section 5.2 for the JSON type inference rules.

---

## 3. gRPC API & Examples

### 3.1 Proto Changes in `service.proto`

**Extended `CreateLedgerRequest`** (optional initial schema at creation):

```protobuf
message CreateLedgerRequest {
  string name = 1;
  common.MetadataSet metadata = 2;
  repeated common.SetMetadataFieldTypeCommand initial_schema = 3;  // NEW
}
```

**New `Request` variants** (added to the `Request.type` oneof):

```protobuf
message Request {
  string idempotency_key = 1;
  oneof type {
    // ... existing variants ...
    SetMetadataFieldTypeRequest set_metadata_field_type = 18;
    RemoveMetadataFieldTypeRequest remove_metadata_field_type = 19;
  }
  signature.RequestSignature signature = 5;
}

// Sets or changes the declared type of a single metadata key on a ledger.
// Automatically triggers background conversion for that key (section 2.4).
message SetMetadataFieldTypeRequest {
  string ledger = 1;
  common.Target.TargetType target_type = 2;  // ACCOUNT or TRANSACTION
  string key = 3;
  common.MetadataType type = 4;
}

// Removes the type declaration for a single metadata key.
// Values are converted back to string (always succeeds).
message RemoveMetadataFieldTypeRequest {
  string ledger = 1;
  common.Target.TargetType target_type = 2;
  string key = 3;
}
```

**New RPCs on `BucketService`:**

```protobuf
service BucketService {
  // ... existing RPCs ...
  rpc GetMetadataSchemaStatus(GetMetadataSchemaStatusRequest) returns (GetMetadataSchemaStatusResponse);
}
```

The aggregate schema is already available on `LedgerInfo.metadata_schema` (returned by `GetLedger`) — no separate `GetMetadataSchema` RPC needed.

### 3.2 Raft Commands in `raftcmd.proto`

Per-key orders (one key per Raft entry):

```protobuf
// Applied when a SetMetadataFieldTypeRequest is received.
// Updates the schema in LedgerInfo and starts background conversion.
message SetMetadataFieldTypeOrder {
  common.Target.TargetType target_type = 1;
  string key = 2;
  common.MetadataType type = 3;
}

// Applied when a RemoveMetadataFieldTypeRequest is received.
// Removes the key from schema and converts existing values back to string.
message RemoveMetadataFieldTypeOrder {
  common.Target.TargetType target_type = 1;
  string key = 2;
}

// Applied by the background conversion goroutine (section 2.4.2).
message ConvertMetadataBatchOrder {
  string key = 1;
  repeated ConvertMetadataEntry entries = 2;
}

message ConvertMetadataEntry {
  bytes canonical_key = 1;
  common.MetadataValue converted_value = 2;
}

// Applied after the last batch for a key is done.
message MetadataConversionCompleteOrder {
  string key = 1;
  common.Target.TargetType target_type = 2;
}
```

Added to the `LedgerApplyOrder` oneof:

```protobuf
message LedgerApplyOrder {
  string ledger = 1;
  oneof data {
    CreateTransactionOrder create_transaction = 2;
    SaveMetadataOrder add_metadata = 3;
    RevertTransactionOrder revert_transaction = 4;
    DeleteMetadataOrder delete_metadata = 5;
    SetMetadataFieldTypeOrder set_metadata_field_type = 6;
    RemoveMetadataFieldTypeOrder remove_metadata_field_type = 7;
    ConvertMetadataBatchOrder convert_metadata_batch = 8;
    MetadataConversionCompleteOrder conversion_complete = 9;
  }
}
```

### 3.3 Usage Examples (Go gRPC Client)

#### Create a ledger with initial metadata schema

```go
_, err := client.Apply(ctx, &servicepb.ApplyRequest{
    Requests: []*servicepb.Request{{
        Type: &servicepb.Request_CreateLedger{
            CreateLedger: &servicepb.CreateLedgerRequest{
                Name: "payments",
                InitialSchema: []*commonpb.SetMetadataFieldTypeCommand{
                    {TargetType: commonpb.TARGET_ACCOUNT, Key: "kyc_verified", Type: commonpb.METADATA_TYPE_BOOL},
                    {TargetType: commonpb.TARGET_ACCOUNT, Key: "balance_threshold", Type: commonpb.METADATA_TYPE_INT64},
                    {TargetType: commonpb.TARGET_ACCOUNT, Key: "external_id", Type: commonpb.METADATA_TYPE_UINT64},
                    {TargetType: commonpb.TARGET_TRANSACTION, Key: "priority", Type: commonpb.METADATA_TYPE_UINT64},
                },
            },
        },
    }},
})
```

#### Save typed metadata on an account

```go
_, err := client.Apply(ctx, &servicepb.ApplyRequest{
    Requests: []*servicepb.Request{{
        Type: &servicepb.Request_Apply{
            Apply: &servicepb.LedgerApplyRequest{
                Ledger: "payments",
                Data: &servicepb.LedgerApplyRequest_AddMetadata{
                    AddMetadata: &commonpb.SaveMetadataCommand{
                        Target: &commonpb.Target{
                            Target: &commonpb.Target_Account{
                                Account: &commonpb.TargetAccount{Addr: "users:alice"},
                            },
                        },
                        Metadata: &commonpb.MetadataSet{
                            Metadata: []*commonpb.Metadata{
                                {
                                    Key:   "kyc_verified",
                                    Value: &commonpb.MetadataValue{
                                        Type: &commonpb.MetadataValue_BoolValue{BoolValue: true},
                                    },
                                },
                                {
                                    Key:   "balance_threshold",
                                    Value: &commonpb.MetadataValue{
                                        Type: &commonpb.MetadataValue_IntValue{IntValue: 100000},
                                    },
                                },
                                {
                                    Key:   "external_id",
                                    Value: &commonpb.MetadataValue{
                                        Type: &commonpb.MetadataValue_UintValue{UintValue: 98765},
                                    },
                                },
                                {
                                    Key:   "label",
                                    Value: &commonpb.MetadataValue{
                                        Type: &commonpb.MetadataValue_StringValue{StringValue: "Alice's Account"},
                                    },
                                },
                            },
                        },
                    },
                },
            },
        },
    }},
})
```

#### Create a transaction with typed metadata

```go
_, err := client.Apply(ctx, &servicepb.ApplyRequest{
    Requests: []*servicepb.Request{{
        Type: &servicepb.Request_Apply{
            Apply: &servicepb.LedgerApplyRequest{
                Ledger: "payments",
                Data: &servicepb.LedgerApplyRequest_CreateTransaction{
                    CreateTransaction: &servicepb.CreateTransactionPayload{
                        Postings: []*commonpb.Posting{{
                            Source:      "users:alice",
                            Destination: "merchants:bob",
                            Amount:      commonpb.NewUint256FromUint64(5000),
                            Asset:       "USD/2",
                        }},
                        Metadata: &commonpb.MetadataSet{
                            Metadata: []*commonpb.Metadata{
                                {
                                    Key:   "priority",
                                    Value: &commonpb.MetadataValue{
                                        Type: &commonpb.MetadataValue_UintValue{UintValue: 1},
                                    },
                                },
                                {
                                    Key:   "category",
                                    Value: &commonpb.MetadataValue{
                                        Type: &commonpb.MetadataValue_StringValue{StringValue: "purchase"},
                                    },
                                },
                            },
                        },
                    },
                },
            },
        },
    }},
})
```

#### Read account and inspect typed metadata

```go
account, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
    Ledger:  "payments",
    Address: "users:alice",
})

for _, md := range account.Metadata.Metadata {
    switch v := md.Value.Type.(type) {
    case *commonpb.MetadataValue_StringValue:
        fmt.Printf("%s = %q (string)\n", md.Key, v.StringValue)
    case *commonpb.MetadataValue_IntValue:
        fmt.Printf("%s = %d (int64)\n", md.Key, v.IntValue)
    case *commonpb.MetadataValue_UintValue:
        fmt.Printf("%s = %d (uint64)\n", md.Key, v.UintValue)
    case *commonpb.MetadataValue_BoolValue:
        fmt.Printf("%s = %t (bool)\n", md.Key, v.BoolValue)
    case *commonpb.MetadataValue_NullValue:
        fmt.Printf("%s = NULL (original: %q)\n", md.Key, v.NullValue.Original)
    }
}
// Output:
//   balance_threshold = 100000 (int64)
//   external_id = 98765 (uint64)
//   kyc_verified = true (bool)
//   label = "Alice's Account" (string)
```

#### Set a metadata field type (add or change a single key)

```go
// Add a new typed field: "risk_score" as int64 on accounts.
// Only this key is affected — no impact on other keys or ongoing conversions.
_, err := client.Apply(ctx, &servicepb.ApplyRequest{
    Requests: []*servicepb.Request{{
        Type: &servicepb.Request_SetMetadataFieldType{
            SetMetadataFieldType: &servicepb.SetMetadataFieldTypeRequest{
                Ledger:     "payments",
                TargetType: commonpb.TARGET_ACCOUNT,
                Key:        "risk_score",
                Type:       commonpb.METADATA_TYPE_INT64,
            },
        },
    }},
})
```

#### Poll conversion status after type change

```go
// SetMetadataFieldType automatically triggers background conversion for that key.
// Poll conversion status to track progress per key:
status, err := client.GetMetadataSchemaStatus(ctx, &servicepb.GetMetadataSchemaStatusRequest{
    Ledger: "payments",
})

for key, field := range status.AccountFields {
    fmt.Printf("account:%s type=%s status=%s",
        key, field.DeclaredType, field.Status)
    if field.Status == servicepb.METADATA_CONVERSION_CONVERTING {
        fmt.Printf(" (%d/%d)", field.ConvertedKeys, field.TotalKeys)
    }
    fmt.Println()
}
// Output:
//   account:kyc_verified type=BOOL status=COMPLETE
//   account:balance_threshold type=INT64 status=COMPLETE
//   account:external_id type=UINT64 status=COMPLETE
//   account:risk_score type=INT64 status=CONVERTING (45000/100000)
```

#### Type mismatch → automatic conversion

```go
// Schema declares "kyc_verified" as BOOL.
// Client sends an int64 → converted automatically via the matrix (non-zero → true).
_, err := client.Apply(ctx, &servicepb.ApplyRequest{
    Requests: []*servicepb.Request{{
        Type: &servicepb.Request_Apply{
            Apply: &servicepb.LedgerApplyRequest{
                Ledger: "payments",
                Data: &servicepb.LedgerApplyRequest_AddMetadata{
                    AddMetadata: &commonpb.SaveMetadataCommand{
                        Target: &commonpb.Target{
                            Target: &commonpb.Target_Account{
                                Account: &commonpb.TargetAccount{Addr: "users:bob"},
                            },
                        },
                        Metadata: &commonpb.MetadataSet{
                            Metadata: []*commonpb.Metadata{{
                                Key: "kyc_verified",
                                Value: &commonpb.MetadataValue{
                                    Type: &commonpb.MetadataValue_IntValue{IntValue: 1},
                                },
                            }},
                        },
                    },
                },
            },
        },
    }},
})

// Reading back: value is stored as bool (converted from int64)
account, _ := client.GetAccount(ctx, &servicepb.GetAccountRequest{
    Ledger: "payments", Address: "users:bob",
})
// account.Metadata.Metadata[0].Value.Type == &MetadataValue_BoolValue{BoolValue: true}
```

---

## 4. Implementation Plan

### Phase 1: Proto + Conversion Engine

1. Add `NullValue` message, extend `MetadataValue` with `oneof` (including `uint_value`) in `common.proto`
2. Add `MetadataSchema`, `MetadataFieldSchema`, `MetadataType` to `common.proto`
3. Add `metadata_schema` field to `LedgerInfo`
4. Run `just generate-proto`
5. Implement conversion matrix in a new `internal/proto/commonpb/metadata_convert.go`
6. Update `MetadataFromMap` / `MetadataToMap` conversion functions for the new oneof
7. Verify all tests pass (existing string data still works)

### Phase 2: Per-Key Schema Declaration + Lazy Conversion

1. Add `SetMetadataFieldTypeOrder` / `RemoveMetadataFieldTypeOrder` to `raftcmd.proto`
2. Implement per-key schema storage in `LedgerInfo.metadata_schema`
3. Apply type enforcement on metadata write path in `processAddMetadata()`
4. Implement lazy read-time conversion (type check + convert in `GetAccountMetadata()` path)
5. Extend `rotateLocked()` to convert metadata entries during generation rotation
6. Add gRPC per-key commands: `SetMetadataFieldTypeRequest` / `RemoveMetadataFieldTypeRequest` in `Request` oneof
7. Add `initial_schema` to `CreateLedgerRequest`

### Phase 3: Automatic Batched Conversion + Status

1. Add `ConvertMetadataBatchOrder` and `MetadataConversionCompleteOrder` to `raftcmd.proto`
2. Implement background conversion goroutine (follow Sealer/Archiver pattern), triggered automatically on schema change
3. Implement conversion status tracking
4. Add `GetMetadataSchemaStatus` RPC

### Phase 4: HTTP Compatibility Layer

1. Change HTTP handlers to accept/return typed JSON (numbers, booleans)
2. Implement JSON → `MetadataValue` type inference (for unschema'd keys)
3. Implement `NullValue` → JSON `null` serialization
4. Update ClickHouse conversion (keep `Map(String, String)` with string serialization)
5. Update `openapi.yml`

### Phase 5: Numscript Bridge

1. Implement typed-value-to-string conversion for Numscript read path
2. Implement string-to-typed-value conversion for Numscript write path
3. (Later) Extend Numscript parser for typed literals — coordinate with "typed variables" TODO

### Phase 6: Go Value Type (optimization)

1. Introduce stack-allocated `metadata.Value` union type
2. Replace `map[string]string` with `map[string]Value` in hot paths
3. Benchmark GC impact

---

## 5. Appendix

### 5.1 ClickHouse Events

Currently `Map(String, String)`. Keep as-is: serialize typed values back to strings for ClickHouse. The events layer is a projection for analytics, not a source of truth. Can evolve to `Map(String, Variant(String, Int64, UInt64, Bool))` when advanced read queries land.

### 5.2 JSON Type Inference (HTTP API)

When no schema is declared for a key, the HTTP layer infers the type from JSON:
- JSON `true` / `false` → `bool_value`
- JSON number (positive integer, fits `uint64`) → `uint_value`
- JSON number (negative integer, fits `int64`) → `int_value`
- JSON string → `string_value`
- JSON number with decimal point → **rejected** (error: floats not supported)
- JSON `null` → **deletes** the key
- JSON object / array → **rejected** (error)

`NullValue` is serialized as JSON `null`. The `original` field is not exposed in the HTTP API.

### 5.3 OpenAPI Schema Change

```yaml
# Before
additionalProperties:
  type: string

# After
additionalProperties:
  oneOf:
    - type: string
    - type: integer
      format: int64
    - type: boolean
  nullable: true
```

---

## 6. Impact Assessment

### Files Requiring Changes

| File | Change |
|------|--------|
| `misc/proto/common.proto` | `MetadataValue` oneof, `NullValue`, `uint_value`, `MetadataSchema`, `MetadataType` |
| `misc/proto/raftcmd.proto` | Per-key type orders + conversion batch orders, extend `LedgerApplyOrder` |
| `misc/proto/service.proto` | `CreateLedgerRequest` initial schema, per-key `Request` variants, status RPC |
| `internal/proto/commonpb/metadata.go` | Conversion functions for typed values |
| `internal/proto/commonpb/metadata_convert.go` | New: conversion matrix implementation |
| `internal/proto/commonpb/transaction.go` | JSON marshaling for typed metadata |
| `internal/service/processing/processor.go` | Schema-aware conversion on write, handle schema/conversion orders |
| `internal/service/state/machine.go` | Extend `rotateLocked()` for lazy conversion (approach A) |
| `internal/service/state/metadata_converter.go` | New: background conversion goroutine (approach B) |
| `internal/service/events/clickhouse_data.go` | Serialize typed values to strings for ClickHouse |
| `internal/service/ctrl/store.go` | Lazy read-time conversion in `GetAccountMetadata()` |
| `internal/compat/http/handlers_*.go` | JSON type inference, typed responses, schema endpoints |
| `openapi.yml` | Schema endpoints, typed metadata |
| `docs/dev/architecture/attributes.md` | Document typed values and schema |

### Risk Assessment

| Risk | Severity | Mitigation |
|------|----------|------------|
| FSM rotation cost with conversion | Low | Conversion is per-evicted-generation only, O(active keys) |
| Raft log bloat (batch conversion) | Medium | Configurable batch size, per-key granularity limits entry count |
| Leader change during batch conversion | Low | Restart scan on new leader (idempotent) |
| ClickHouse schema mismatch | Low | Keep `Map(String, String)`, serialize |
| Numscript bridge complexity | Low | Simple string⇔typed conversion at boundary |
| Concurrent type changes on same key | Medium | Abandon current conversion for that key, restart with new type |

---

## 7. Alternatives Considered

### 7.1 Keep `map[string]string`, Parse on Read

Store everything as strings, parse to typed values at query time.

**Rejected because:** defeats the performance goal (parsing cost on every read), no type safety, cannot build typed indexes.

### 7.2 Use `google.protobuf.Value` (struct.proto)

The well-known `Value` type supports null, number (double), string, bool, struct, list.

**Rejected because:**
- Uses `double` for numbers (lossy for int64)
- Includes struct/list (complexity we don't want)
- Larger wire size than a simple `oneof`
- Not vtprotobuf-optimized in the same way

### 7.3 Encode Type in Key

Store type information in the metadata key: `"balance_threshold:int" → "1000"`.

**Rejected because:** pollutes the key namespace, requires parsing keys everywhere, breaks existing key-based lookups.

### 7.4 Store as JSON bytes

Store `MetadataValue.value` as a JSON-encoded string: `"42"`, `"true"`, `"\"hello\""`.

**Rejected because:** still allocates strings, requires JSON parsing on every read, ambiguous (is `"42"` a number or a string?).

### 7.5 Error on Inconvertible Values

Reject metadata writes where the value doesn't match the declared schema type.

**Rejected because:** metadata is user-controlled free-form data. The system should be permissive on write and clear on read. `NullValue` signals "this value doesn't match the schema" without blocking writes. The client is responsible for ensuring correct types.

### 7.6 Synchronous Conversion in FSM

Convert all existing values in a single FSM apply step.

**Rejected because:** blocks the FSM hot path for potentially minutes on large key sets. Violates the "FSMs must be fast" design principle.

---

## 8. Open Questions

1. **ClickHouse evolution:** when advanced read queries land, should we evolve ClickHouse to `Map(String, Variant(String, Int64, UInt64, Bool))`? Defer to that phase.

2. **Numscript typed literals timeline:** depends on the "typed variables" RFC. Can be decoupled — the string bridge works in the interim.
