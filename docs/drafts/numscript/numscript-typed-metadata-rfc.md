# RFC: Numscript Typed Metadata Integration

- **Status:** Draft
- **Target:** Ledger v3
- **Audience:** Numscript / Ledger implementers
- **Scope:** Extending Numscript to produce and consume **typed metadata values** natively, eliminating the string-only bottleneck at the Numscript boundary. Scoped to the current architecture (Go library import, no WASM).

---

## 0. Context and Motivation

### 0.1 Current State

The ledger has a full typed metadata system (`MetadataValue` oneof: `string`, `int64`, `uint64`, `bool`, `NullValue`) with schema enforcement, conversion matrix, and background conversion. However, **Numscript remains string-only**:

- `set_tx_meta("key", value)` — the `Value` interface always serializes to string at the boundary
- `set_account_meta(account, "key", value)` — values are converted to string via `.String()`
- `meta(@account, "key")` — typed storage values are converted back to string for Numscript consumption

This creates a lossy round-trip:

```
Numscript set_account_meta($user, "age", "25")  →  string "25"
  → Schema enforcement converts to int64(25) at storage time
  → Later: meta($user, "age") reads int64(25) → converts back to string "25"
  → Numscript sees "25" (string), not 25 (int)
```

The system works correctly thanks to the schema enforcement layer, but the string intermediary introduces:

1. **Unnecessary conversion overhead** — string→typed→string on every read cycle
2. **Semantic ambiguity** — Numscript cannot distinguish `"true"` (string) from `true` (bool)
3. **Loss of expressiveness** — scripts cannot set metadata with explicit type intent
4. **Schema dependency** — correctness depends entirely on the schema layer; without it, metadata degrades to strings

### 0.2 Goal

Extend Numscript so that:

1. Metadata values can carry explicit type information through the Numscript boundary
2. `set_tx_meta` and `set_account_meta` can produce typed values directly
3. `meta()` returns preserve type information from storage
4. The existing schema enforcement layer remains as a safety net, not the sole correctness mechanism

### 0.3 Relation to Other Documents

- **[Static Inputs RFC](./numscript-static-inputs-rfc.md)** — `meta()` reads are declared in `Requirements.accountMetadataReads`. This RFC does not change that contract; it changes the **value type** flowing through the declared reads.
- **[Typed Metadata Architecture](../../technical/architecture/subsystems/read-path/typed-metadata.md)** — This RFC builds on the existing typed metadata system and reuses its `MetadataValue` oneof, `MetadataType` enum, and conversion matrix.

### 0.4 Scope

This RFC targets the **current architecture**: the Numscript library is imported as a Go module (`github.com/formancehq/numscript`), executed in-process on the leader and replicas. Future evolutions (WASM compilation, serialized IR, etc.) are out of scope — see the [WASM Compilation RFC](./numscript-wasm-compilation-rfc.md) for that direction.

### 0.5 Existing Numscript Types Audit

Numscript already supports these variable types in `vars { }`:

| Type | Value type | Parsing | Use in metadata |
|------|-----------|---------|-----------------|
| `account` | `AccountAddress` (string) | raw string | `.String()` → string |
| `monetary` | `Monetary` (Asset + MonetaryInt) | `"USD/2 1000"` | `.String()` → string |
| `string` | `String` | raw string | identity → string |
| `number` | `MonetaryInt` (big.Int) | `big.Int.SetString(s, 10)` | `.String()` → string |
| `portion` | `Portion` (big.Rat) | fraction/percent | `.String()` → string |
| `asset` | `Asset` (string) | validated string | `.String()` → string |

**Key observation:** `number` already parses integers from string input, but produces `MonetaryInt` (a `big.Int` alias) — designed for monetary amounts, not metadata. All types go through `.String()` at the metadata boundary, losing any type information.

The only missing type is **`bool`**. There is no existing Numscript type that can carry a boolean from input to metadata. `number` already covers integers.

---

## 1. Design Principles

### 1.1 Type at the Source

Metadata types should be expressible where the value originates — in the Numscript script — not only at the storage layer. The script author states intent; the schema layer validates it.

### 1.2 Backward Compatibility

Existing scripts that use string literals (`set_tx_meta("key", "value")`) must continue to work unchanged. Typed metadata is opt-in via new syntax.

### 1.3 Schema as Safety Net

When a schema is declared, it validates and converts values. When a schema is not declared, the value's type is preserved as-is from the script. The two mechanisms are complementary, not competing.

### 1.4 Reuse Before Invention

`number` already covers integers — no need to add a redundant `int` type. Only add `bool`, which has no existing equivalent.

### 1.5 Schema-Driven Numeric Type Resolution

Numscript emits a raw numeric value (`MonetaryInt`). The boundary maps it to a wide proto type (`int_value` or `uint_value`). The **metadata schema** is authoritative for the final storage type — `enforceSchema()` applies range checks and narrows to the declared type (`INT8`, `UINT32`, `INT64`, etc.). This keeps Numscript unaware of the ledger's integer subtypes.

---

## 2. Proposed Numscript Library Changes

### 2.1 New `bool` Variable Type

Add `bool` to the `vars { }` block:

```numscript
vars {
    account  $source
    monetary $amount
    bool     $is_recurring
}

set_tx_meta("is_recurring", $is_recurring)

send $amount (
    source = $source
    destination = @merchant
)
```

| New Type | Go wire type | Parsing | Description |
|----------|-------------|---------|-------------|
| `bool` | `Bool` (new Value variant) | `"true"`/`"1"` → `true`, `"false"`/`"0"` → `false` | Boolean metadata value |

> **Why only `bool`?** `number` already parses integers from string input. Adding a redundant `int` type would create confusion about when to use `number` vs `int`.

#### 2.1.1 Variable Parsing from Input Strings

When `bool` variables are provided as strings (e.g., CLI `--var "verified=true"` or JSON `{"verified": "true"}`):

| Input string | Parsed value | Error |
|-------------|-------------|-------|
| `"true"`, `"1"` | `Bool(true)` | — |
| `"false"`, `"0"` | `Bool(false)` | — |
| anything else | — | `INVALID_VAR_TYPE` |

### 2.2 Typed Literals in Metadata Operations

Allow typed literals directly in `set_tx_meta` and `set_account_meta`:

```numscript
// String (existing behavior, unchanged)
set_tx_meta("type", "payment")

// Boolean literals (new)
set_tx_meta("is_recurring", true)
set_account_meta($user, "kyc_verified", true)

// Integer literals (existing bare numbers, now typed at boundary)
set_tx_meta("priority", 5)
set_account_meta($user, "age", 25)
```

#### 2.2.1 Literal Syntax

| Literal | Type | Examples |
|---------|------|---------|
| `"..."` | String | `"hello"`, `"USD"` |
| `true` / `false` | Bool | `true`, `false` |
| `[0-9]+` | MonetaryInt | `42`, `0`, `100` |

> **Note on integers:** Bare numbers already exist in Numscript as `MonetaryInt`. We do **not** change their parser type. The ledger boundary recognizes `MonetaryInt` and maps it to the appropriate proto numeric type. The metadata schema then narrows it to the declared type (see §1.5).

### 2.3 New `Bool` Value Type

```go
type Bool bool

func (b Bool) value()        {}
func (b Bool) String() string {
    if bool(b) {
        return "true"
    }
    return "false"
}
```

This is the only new `Value` variant needed. `MonetaryInt` already exists and covers integers.

### 2.4 AccountMetadata Type Change

`AccountMetadata` must carry typed values instead of strings:

```go
// Before
type AccountMetadata  map[string]string

// After
type AccountMetadata  map[string]Value
```

This is a **breaking change** in the Numscript library API. `AccountsMetadata` (`map[string]AccountMetadata`) inherits the change.

The `set_account_meta` implementation changes from `accountMeta[key] = (*meta).String()` to `accountMeta[key] = *meta`, preserving the `Value` type.

### 2.5 Store Interface Adaptation

The `Store.GetAccountsMetadata` return type changes accordingly:

```go
type Store interface {
    GetBalances(context.Context, BalanceQuery) (Balances, error)
    GetAccountsMetadata(context.Context, MetadataQuery) (AccountsMetadata, error)
    // AccountsMetadata is now map[string]map[string]Value instead of map[string]map[string]string
}
```

The ledger's `numscriptStoreAdapter` returns typed `Value` instances instead of strings.

### 2.6 Typed `meta()` Returns

When `meta()` reads a value from storage, the type is preserved:

```numscript
vars {
    account $user
    // meta() returns a Value — the type is preserved from storage.
    // If stored as bool_value(true), Numscript sees Bool(true).
    // If stored as int_value(3), Numscript sees MonetaryInt(3).
    // A string variable coerces any type to string (backward-compatible).
    string $tier_label = meta($user, "tier")
}
```

With a `string` target variable, any typed value converts to string via `.String()` (never fails). With a `bool` target, the store adapter returns `Bool(true/false)`. With a `number` target, the adapter returns `MonetaryInt`.

The coercion from storage type to Numscript target type reuses the conversion matrix semantics:

| Target variable type | Stored `string_value("42")` | Stored `int_value(42)` | Stored `bool_value(true)` |
|---------------------|----------------------------|----------------------|-------------------------|
| `string` | `String("42")` | `String("42")` | `String("true")` |
| `number` | `MonetaryInt(42)` | `MonetaryInt(42)` | `MonetaryInt(1)` |
| `bool` | `Bool(true)` if `"true"`/`"1"` | `Bool(true)` if non-zero | `Bool(true)` |

---

## 3. Ledger Integration Changes

### 3.1 Numeric Type Resolution Strategy

Numscript emits `MonetaryInt` (a `big.Int`). The boundary converts it to the widest matching proto type:

```go
func numericToMetadataValue(bi *big.Int) *commonpb.MetadataValue {
    switch {
    case bi.Sign() >= 0 && bi.IsUint64():
        return commonpb.NewUintValue(bi.Uint64())
    case bi.IsInt64():
        return commonpb.NewIntValue(bi.Int64())
    default:
        // Exceeds 64 bits: fall back to string
        return commonpb.NewStringValue(bi.String())
    }
}
```

This produces a **wide** proto value (`uint_value` or `int_value`). The metadata schema then narrows it:

```
MonetaryInt(25) → boundary → uint_value(25)
  → enforceSchema():
    - Schema declares "age" as INT8 → range check [-128, 127] → passes → stored as int_value(25)
    - Schema declares "age" as UINT64 → type matches → stored as uint_value(25)
    - No schema → stored as uint_value(25) as-is
```

**Numscript does not need to know about `INT8`, `UINT16`, `INT32`, etc.** The schema handles narrowing. This keeps the Numscript→ledger contract simple: Numscript emits numbers, the schema types them.

### 3.2 numscriptStoreAdapter.GetAccountsMetadata

Stop discarding type information when returning metadata to Numscript:

```go
// Before: convert typed value to string
str := commonpb.MetadataValueToString(value)
if str != "" {
    accountMeta[key] = str
}

// After: preserve type as Numscript Value
accountMeta[key] = metadataValueToNumscriptValue(value)
```

Mapping function:

```go
func metadataValueToNumscriptValue(v *commonpb.MetadataValue) interpreter.Value {
    switch t := v.Type.(type) {
    case *commonpb.MetadataValue_StringValue:
        return interpreter.String(t.StringValue)
    case *commonpb.MetadataValue_IntValue:
        return interpreter.MonetaryInt(*big.NewInt(t.IntValue))
    case *commonpb.MetadataValue_UintValue:
        var bi big.Int
        bi.SetUint64(t.UintValue)
        return interpreter.MonetaryInt(bi)
    case *commonpb.MetadataValue_BoolValue:
        return interpreter.Bool(t.BoolValue)
    case *commonpb.MetadataValue_NullValue:
        return interpreter.String(t.NullValue.Original)
    default:
        return interpreter.String("")
    }
}
```

### 3.3 numscriptPostingProducer.produce — Account Metadata

Stop forcing `NewStringValue`; map from Numscript `Value` to `MetadataValue`:

```go
// Before: always NewStringValue
mv := commonpb.NewStringValue(value)

// After: preserve type from Numscript
mv := numscriptValueToMetadataValue(value)
```

Mapping function:

```go
func numscriptValueToMetadataValue(v interpreter.Value) *commonpb.MetadataValue {
    switch t := v.(type) {
    case interpreter.String:
        return commonpb.NewStringValue(string(t))
    case interpreter.MonetaryInt:
        bi := big.Int(t)
        return numericToMetadataValue(&bi)
    case interpreter.Bool:
        return commonpb.NewBoolValue(bool(t))
    default:
        // Asset, Portion, AccountAddress, Monetary → fall back to string
        return commonpb.NewStringValue(v.String())
    }
}
```

### 3.4 numscriptPostingProducer.produce — Transaction Metadata

Transaction metadata already uses `map[string]Value`, but the current code discards the type:

```go
// Before
Value: commonpb.NewStringValue(value.String())

// After
Value: numscriptValueToMetadataValue(value)
```

### 3.5 Schema Enforcement Interaction

The existing `enforceSchema()` runs **after** the boundary mapping. With typed values:

1. **Value matches schema type exactly** — no conversion needed (fast path)
2. **Value is a wider type than schema** — conversion matrix narrows it (e.g., `uint_value(25)` → `INT8` with range check)
3. **Value doesn't match schema type** — conversion matrix applied (e.g., `bool_value(true)` → `INT64` → `int_value(1)`)
4. **No schema declared** — value stored as-is with the wide type from boundary

---

## 4. Numscript Grammar Changes

### 4.1 New Type Keyword

```ebnf
type = "account" | "monetary" | "string" | "number" | "portion" | "asset"
     | "bool"
```

### 4.2 New Literal Tokens

```ebnf
bool_literal = "true" | "false"
```

> Bare integer literals already exist as `MonetaryInt`. No grammar change needed for integers.

### 4.3 Metadata Value Expression

The value argument of `set_tx_meta` and `set_account_meta` already accepts `TypeAny`. Adding `bool_literal` as a new expression form is the only parser change:

```ebnf
meta_value = string_literal | bool_literal | number_literal | variable
```

---

## 5. Volume Discovery Impact

### 5.1 No Impact on Volume Discovery

Typed literals in metadata operations do not affect balance queries or volume preloading. Dependency resolution (`discover.go`, over a `ValueSource` reading real state via the upstream `ResolveDependencies` API — which replaced the old emulation store) is unaffected.

### 5.2 No Impact on Static Analysis

The `Requirements` object from the [Static Inputs RFC](./numscript-static-inputs-rfc.md) declares which `(account, key)` pairs are read. The **type** of the value is not part of `Requirements` — it is a runtime concern.

---

## 6. Migration Strategy

### Phase 1: Ledger-Side Type Preservation (No Numscript Library Changes)

Improve the boundary without touching the Numscript library:

1. **Transaction metadata write path**: Type-switch on existing `Value` variants in `produce()` — `MonetaryInt` → `numericToMetadataValue()`, etc.
2. **Account metadata read path**: Cannot return typed values yet (`AccountMetadata` is `map[string]string`), but can improve opportunistic schema conversion.

This phase requires **zero changes to the Numscript library** and already eliminates string→typed→string round-trips for transaction metadata.

> **Why this works:** `ExecutionResult.Metadata` is already `map[string]Value`. The current code calls `.String()` on every value and wraps with `NewStringValue`. A type-switch captures the existing type information without parser changes.

### Phase 2: Numscript Library — Bool Value Type + AccountMetadata Change

1. Add `Bool` to the `Value` union
2. Add `bool` to the `vars { }` type system with string parsing (`"true"/"false"/"1"/"0"`)
3. Change `AccountMetadata` from `map[string]string` to `map[string]Value`
4. Update `set_account_meta` to preserve `Value` type instead of calling `.String()`

**Breaking change** in the library API. The ledger adapter is the only consumer, so the blast radius is contained.

### Phase 3: Numscript Library — Boolean Literals in Parser

1. Add `true` / `false` as boolean literal tokens
2. These produce `Bool` values when used in `set_tx_meta` / `set_account_meta`

---

## 7. Examples

### 7.1 Bool Variable + Number Variable in a Payment Script

```numscript
vars {
    account  $source
    account  $destination
    monetary $amount
    bool     $is_recurring
    number   $priority
}

set_tx_meta("type", "payment")
set_tx_meta("is_recurring", $is_recurring)
set_tx_meta("priority", $priority)

send $amount (
    source = $source
    destination = $destination
)
```

CLI:
```bash
ledgerctl transactions create --ledger demo \
    --script payment.num \
    --var "source=users:alice" \
    --var "destination=merchants:shop" \
    --var "amount=USD/2 1000" \
    --var "is_recurring=true" \
    --var "priority=5"
```

Result (stored metadata, no schema):
```json
{
    "type": "payment",
    "is_recurring": true,
    "priority": 5
}
```

Result (with schema `priority: INT8`):
```json
{
    "type": "payment",
    "is_recurring": true,
    "priority": 5
}
```

Same JSON output, but `priority` is stored as `int_value(5)` with INT8 range validation instead of `uint_value(5)`.

### 7.2 Schema-Driven Narrowing

```numscript
set_account_meta(@users:alice, "age", 25)
```

| Schema declaration | Boundary output | After `enforceSchema()` | Stored |
|-------------------|-----------------|------------------------|--------|
| None | `uint_value(25)` | no-op | `uint_value(25)` |
| `age: UINT64` | `uint_value(25)` | type matches → no-op | `uint_value(25)` |
| `age: INT64` | `uint_value(25)` | convert → `int_value(25)` | `int_value(25)` |
| `age: INT8` | `uint_value(25)` | range check `[-128,127]` → passes | `int_value(25)` |
| `age: UINT8` | `uint_value(25)` | range check `[0,255]` → passes | `uint_value(25)` |
| `age: BOOL` | `uint_value(25)` | non-zero → `bool_value(true)` | `bool_value(true)` |
| `age: STRING` | `uint_value(25)` | → `string_value("25")` | `string_value("25")` |

### 7.3 Typed Literals

```numscript
set_account_meta(@users:alice, "kyc_verified", true)
set_account_meta(@users:alice, "login_count", 0)
set_tx_meta("automated", true)
set_tx_meta("retry_count", 3)
```

### 7.4 Backward-Compatible Script (No Changes Needed)

```numscript
// Unchanged. String values. Schema enforcement converts if declared.
set_tx_meta("type", "payment")
set_tx_meta("category", "purchase")

send [USD/2 1000] (
    source = @users:alice
    destination = @merchant
)
```

### 7.5 Reading Typed Metadata via `meta()`

```numscript
vars {
    account $user
    bool    $is_vip = meta($user, "is_vip")
    number  $tier   = meta($user, "tier")
    string  $label  = meta($user, "label")
}

// $is_vip: Bool(true) from storage bool_value(true)
// $tier:   MonetaryInt(3) from storage int_value(3) or uint_value(3)
// $label:  String("gold") from storage string_value("gold")
```

---

## 8. Schema Conflict Behavior

If a script sets `set_account_meta($user, "age", true)` (bool literal) but the schema declares `age` as `INT64`:
- The boundary produces `NewBoolValue(true)`
- `enforceSchema` applies the conversion matrix: `bool(true) → int64(1)` (succeeds)
- Stored value: `int_value(1)`

The schema is authoritative. This is consistent with existing behavior.

---

## 9. Open Questions

| # | Question | Notes |
|---|----------|-------|
| 1 | Should `meta()` support a default value? | `bool $vip = meta(@user, "is_vip") or false` — useful for optional metadata. Orthogonal to this RFC. |
| 2 | Should `set_tx_meta` / `set_account_meta` accept expressions? | `set_tx_meta("total", get_amount($payment))` — computed metadata. Increases complexity. |
| 3 | Should `NullValue` be exposed in Numscript? | Probably not — `NullValue` is an internal detail of the conversion matrix. Scripts should see the original string or a type error. |
| 4 | Should `portion` be a typed metadata value? | Numscript has a `portion` type (`big.Rat`) used for splits (e.g., `portion $commission = meta($seller, "commission_rate")`). Adding it as a `MetadataValue` variant would require a structured proto field (`RationalValue { int64 numerator, denominator }`), complex conversion rules (normalization: `15/100` vs `3/20`), and unclear JSON serialization (floats are banned, `"15/100"` is a string, `{"num":15,"den":100}` breaks flat key→scalar format). The string round-trip already works: store `"15/100"`, `meta()` returns it, Numscript parses it into `Portion`. Defer unless real-world usage shows the string round-trip is insufficient. |

---

## 10. Summary

| Aspect | Current | Proposed |
|--------|---------|----------|
| Numscript metadata values | String-only | String + Bool + numeric (via boundary) |
| `set_tx_meta` / `set_account_meta` | String literals only | + `true`/`false` literals, bare integers preserved |
| `meta()` return | String (type discarded) | Typed `Value` (preserved from storage) |
| Variable types in `vars { }` | account, monetary, string, number, portion, asset | + `bool` |
| Numeric type resolution | `.String()` → string | Boundary maps `MonetaryInt` → wide proto type; schema narrows to declared type |
| Schema role for numerics | Sole correctness mechanism | Boundary provides wide type, schema narrows (`INT8`, `UINT32`, etc.) |
| Numscript `Value` union | String, Asset, Portion, AccountAddress, MonetaryInt, Monetary | + Bool |
| `AccountMetadata` type | `map[string]string` | `map[string]Value` |
