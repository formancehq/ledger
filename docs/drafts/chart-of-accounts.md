# RFC: Chart of Accounts

- **Status:** Draft
- **Target:** Ledger v3
- **Audience:** Ledger implementers, API consumers
- **Scope:** Account address validation via a declarative chart of accounts. Covers the data model, validation algorithm, enforcement modes, API surface, and protobuf changes. Transaction templates, query templates, and default metadata are **out of scope** (see §4 for the rationale on default metadata).

---

## 0. Context and Motivation

### 0.1 Current State

The v3 ledger imposes no restrictions on account addresses. Any string matching `[a-zA-Z0-9_-]+(:[a-zA-Z0-9_-]+)*` is valid, and accounts are created implicitly on first use in a posting. The only check is in `Postings.Validate()` (`internal/proto/commonpb/posting.go`), which calls `accounts.ValidateAddress()` from the reference implementation:

```go
func (p Postings) Validate() (int, error) {
    for i, posting := range p {
        // ...
        if !accounts.ValidateAddress(posting.Source) {
            return i, errors.New("invalid source address")
        }
        if !accounts.ValidateAddress(posting.Destination) {
            return i, errors.New("invalid destination address")
        }
        // ...
    }
    return 0, nil
}
```

This is a pure format check. There is no way to restrict which addresses are semantically valid for a given ledger.

### 0.2 v2 Recap

Ledger v2 introduced a "Schema" system that bundled three concepts: a chart of accounts, transaction templates, and query templates. The chart of accounts used a JSON format that relies on **special character prefixes** to encode semantics into object keys:

| Prefix | Meaning | Example |
|--------|---------|---------|
| `$` | Variable segment | `"$iban": { ... }` |
| `.self` | "This segment is a valid account" | `".self": {}` |
| `.pattern` | Regex constraint on a variable | `".pattern": "^[0-9]{10}$"` |
| `.metadata` | Default metadata for the account | `".metadata": { "type": "bank" }` |

```json
{
  "banks": {
    "$iban": {
      ".pattern": "^[0-9]{10}$",
      ".self": {},
      "main": { ".self": {} },
      "out": { ".self": {}, ".metadata": { "type": "outgoing" } }
    }
  }
}
```

**Problems with this format:**

1. **Not self-describing** — `$` and `.` prefixes are ad-hoc conventions, not schema-enforced. Any tool consuming this format must hardcode the prefix logic to distinguish directives from real segment names. There is no way for a reader to understand the structure without prior knowledge of the conventions.
2. **Cannot be typed in OpenAPI/protobuf** — Because fixed children, variables, and directives all live as dynamic keys in the same object, the format can only be described as `additionalProperties: true` in OpenAPI, and as `map<string, ...>` in protobuf. There is no way to declare the structure with known, typed fields. This is not only a server-side problem: every client SDK inherits the same limitation. Generated clients receive an untyped `map[string]any` (or equivalent) instead of a proper struct, forcing client code to manually parse prefixes, cast values, and handle edge cases that a typed schema would eliminate at compile time. The burden of understanding the convention shifts to every consumer of the API.
3. **Namespace collision** — Fixed children, variable declarations, directives, and metadata all share the same flat namespace (the JSON object keys). A segment named `.pattern` or `$data` would collide with the directives. The format implicitly reserves entire character prefixes from the segment namespace.
4. **Poor discoverability** — Without dedicated fields, users must read documentation to learn that `.self` marks an account endpoint, that `$` declares a variable, and that `.metadata` sets defaults. An explicit `"account": true` or `"variable": { "name": "iban" }` is immediately understandable.

### 0.3 Goals

1. **Validate account addresses** against a per-ledger declarative structure
2. **Support enforcement modes** — strict (reject) and audit (log + allow)
3. **Per-ledger configuration** — each ledger defines its own chart (or none, for backward compatibility)
4. **Idiomatic format** — fully typeable in protobuf and OpenAPI, with explicit fields instead of prefix conventions

### 0.4 Scope

This RFC covers **account definitions only**:

| v2 Schema Concept | v3 Status |
|-------------------|-----------|
| Chart of accounts (validation) | **This RFC** |
| Default metadata | Excluded — client-side via Numscript / `accountMetadata` (see §4) |
| Transaction templates | Superseded by Numscript |
| Query templates | Superseded by native gRPC/HTTP queries |

---

## 1. Data Model

### 1.1 JSON Format

The chart of accounts is a tree of segments. The root object maps fixed segment names to `ChartSegment` objects:

```json
{
  "banks": {
    "variable": {
      "name": "iban",
      "pattern": "^[0-9]{10}$",
      "children": {
        "main": { "account": true },
        "out": { "account": true },
        "pendingOut": { "account": true }
      }
    }
  },
  "users": {
    "variable": {
      "name": "userId",
      "account": true,
      "children": {
        "main": { "account": true },
        "wallets": {
          "variable": {
            "name": "walletId",
            "children": {
              "main": { "account": true }
            }
          }
        }
      }
    }
  },
  "platform": {
    "children": {
      "fees": { "account": true },
      "revenue": { "account": true }
    }
  }
}
```

Key properties:

- **`account: true`** marks a segment as a valid account endpoint (an address that terminates here is valid)
- **`children`** maps fixed segment names to child `ChartSegment` objects
- **`variable`** declares a single variable child (at most one per segment)
- **Root level** is a `map<string, ChartSegment>` — the top-level keys are fixed segments

### 1.2 Type Definitions

#### ChartSegment

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `account` | `bool` | No | Is this segment a valid account endpoint? Default `false`. |
| `children` | `map<string, ChartSegment>` | No | Fixed sub-segments |
| `variable` | `ChartVariable` | No | Variable child segment (at most one) |

#### ChartVariable

`ChartVariable` extends `ChartSegment` with two additional fields:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | `string` | Yes | Label for the variable (documentation/debugging) |
| `pattern` | `string` | No | Regex constraint the variable value must match |
| `account` | `bool` | No | Inherited from ChartSegment |
| `children` | `map<string, ChartSegment>` | No | Inherited from ChartSegment |
| `variable` | `ChartVariable` | No | Inherited from ChartSegment |

A variable segment can itself be an account endpoint and can have further children, enabling deep nesting like `users:<userId>:wallets:<walletId>:main`.

### 1.3 Protobuf Messages

Added to `misc/proto/common.proto`:

```protobuf
// ChartOfAccounts defines the account address structure for a ledger.
message ChartOfAccounts {
  map<string, ChartSegment> roots = 1;
}

// ChartSegment represents a node in the chart of accounts tree.
message ChartSegment {
  bool account = 1;
  map<string, ChartSegment> children = 2;
  ChartVariable variable = 3;
}

// ChartVariable represents a variable child segment with an optional regex constraint.
message ChartVariable {
  string name = 1;
  string pattern = 2;
  bool account = 3;
  map<string, ChartSegment> children = 4;
  ChartVariable variable = 5;
}

// ChartEnforcementMode controls how chart violations are handled.
enum ChartEnforcementMode {
  CHART_ENFORCEMENT_STRICT = 0;  // Reject transactions with invalid addresses (default)
  CHART_ENFORCEMENT_AUDIT = 1;   // Log warning but allow the transaction
}
```

### 1.4 Storage on LedgerInfo

The chart of accounts and enforcement mode are stored on `LedgerInfo`, following the same pattern as `metadata_schema` (field 5):

```protobuf
message LedgerInfo {
  string name = 1;
  Timestamp created_at = 2;
  uint32 id = 3;
  Timestamp deleted_at = 4;
  MetadataSchema metadata_schema = 5;
  ChartOfAccounts chart_of_accounts = 6;       // NEW (field 6)
  ChartEnforcementMode enforcement_mode = 7;   // NEW (field 7)
}
```

This ensures the chart is:
- Replicated via Raft (part of `LedgerState`)
- Available in `InMemoryStore` for FSM validation
- Included in snapshots (via `LedgerAttributeEntry`)
- Returned by `GetLedger` / `ListLedgers` RPCs

### 1.5 ChartEnforcementMode

| Mode | Value | Behavior |
|------|-------|----------|
| `CHART_ENFORCEMENT_STRICT` | `0` | **Default.** Reject transactions that use addresses not in the chart. Returns `ErrAccountNotInChart`. |
| `CHART_ENFORCEMENT_AUDIT` | `1` | Log a warning with the violating address(es) but allow the transaction to proceed. |

The default is strict (`0`) because proto3 defaults zero-value enums, and new ledgers should be strict by default when they declare a chart.

---

## 2. Validation Algorithm

### 2.1 Address Matching

Given an address like `banks:1234567890:out`, validation proceeds as follows:

1. **Split** the address on `:` into segments: `["banks", "1234567890", "out"]`
2. **Walk the tree** starting from the chart roots:
   - For each segment, look up the current node:
     a. **Fixed children first:** check `children[segment]`. If found, descend.
     b. **Variable child:** if no fixed child matches, check `variable`. If present, validate the segment against `variable.pattern` (if declared). If the pattern matches (or no pattern is declared), descend into the variable node.
     c. **No match:** the address is invalid.
3. **Terminal check:** after consuming all segments, verify that the current node has `account: true`. If not, the address is structurally valid but not a valid account endpoint.

```
Address: banks:1234567890:out

Step 1: roots["banks"] → found (ChartSegment with variable)
Step 2: variable.pattern = "^[0-9]{10}$", "1234567890" matches → descend
Step 3: children["out"] → found, account=true → VALID
```

```
Address: banks:ABC:out

Step 1: roots["banks"] → found
Step 2: variable.pattern = "^[0-9]{10}$", "ABC" does NOT match → INVALID
```

```
Address: banks:1234567890

Step 1: roots["banks"] → found
Step 2: variable matches → descend, but account=false at this node → INVALID
  (the variable node has account=false by default in this example;
   address must continue to a child like "main" or "out")
```

### 2.2 Special Accounts

The `world` account is **always allowed** regardless of the chart. It is the universal source in Numscript and must never be rejected.

### 2.3 No Chart = No Validation

When a ledger has no chart of accounts (`chart_of_accounts` is nil/unset), no validation is performed. This preserves full backward compatibility — existing ledgers and scripts continue to work unchanged.

### 2.4 Chart Self-Validation

The chart itself is validated at parse time (when set via API). Validation rules:

| Rule | Error |
|------|-------|
| Segment names must match `[a-zA-Z0-9_-]+` | `INVALID_CHART_SEGMENT_NAME` |
| Variable `name` must be non-empty | `INVALID_CHART_VARIABLE_NAME` |
| Variable `pattern` must be a valid Go `regexp.Compile`-compatible regex | `INVALID_CHART_PATTERN` |
| At most one `variable` per segment | Enforced by the data model (single field, not a list) |
| At least one node must have `account: true` | `INVALID_CHART_NO_ACCOUNTS` |
| No duplicate fixed children (enforced by `map` semantics) | N/A |

---

## 3. Enforcement Modes

### 3.1 Strict Mode

When `enforcement_mode` is `CHART_ENFORCEMENT_STRICT` (default):

- Every account address in every posting (source and destination) is validated against the chart
- If any address fails validation, the entire transaction is rejected
- Error: `ErrAccountNotInChart` with reason code `ACCOUNT_NOT_IN_CHART`
- The error includes the first invalid address for debugging

```go
var ErrAccountNotInChart = errors.New("account not in chart")
```

### 3.2 Audit Mode

When `enforcement_mode` is `CHART_ENFORCEMENT_AUDIT`:

- Every account address is validated the same way
- If any address fails validation, a **warning is logged** with the address and ledger name
- The transaction proceeds normally
- Useful for gradual adoption: enable the chart in audit mode, monitor logs for violations, then switch to strict

### 3.3 Validation Point

Validation occurs in the **processor** (FSM), at two points:

1. **`processCreateTransaction`** — after `producer.produce()` returns the postings and before `applyPosting()`. This is the same stage where `enforceSchema()` runs. All posting source/destination addresses are checked.

2. **`processRevertTransaction`** — after constructing the reversed postings. The revert creates new postings with swapped source/destination. These must also be valid in the chart.

```
processCreateTransaction:
  1. producer.produce() → postings
  2. validateAccountsInChart(postings, chart)   ← NEW
  3. enforceSchema(metadata, schema)
  4. applyPosting() for each posting
  5. return LedgerLogPayload_CreatedTransaction

processRevertTransaction:
  1. construct reversed postings
  2. validateAccountsInChart(postings, chart)   ← NEW
  3. applyPosting() for each posting
  4. return LedgerLogPayload_RevertedTransaction
```

> **Why in the processor (FSM)?** The processor has access to the `LedgerInfo` (via `InMemoryStore`) which contains the chart. Validation must run deterministically on all replicas, so it belongs in the FSM — the same layer that enforces balance checks and schema.

---

## 4. Why Default Metadata Is Out of Scope

The v2 chart of accounts included a `.metadata` directive to apply default metadata on first account use. This RFC deliberately excludes default metadata from the chart. The chart focuses on **address validation** — it answers "is this address structurally valid?", not "what data should this account carry?".

### 4.1 No "First Use" Detection in the Current Architecture

Applying defaults requires knowing whether an account is **new** (never appeared in any posting). The current architecture has no mechanism for this:

- **Volumes are keyed by `(account, asset)`, not by `account`.** An account `users:alice` may have volumes in `USD` but not in `EUR`. Detecting "the account has never been seen in any posting for any asset" would require a **prefix scan** across all volume keys for that account — not a single `O(1)` lookup.

- **The preload system loads only what is needed.** The admission phase (`admission.go`) fetches specific `(account, asset)` volume pairs referenced by the postings in the current transaction. Adding a "does this account exist globally?" check means an additional Pebble read per distinct account in every transaction, in the admission hot path.

- **The FSM does not distinguish "volume at zero" from "never seen".** When `GetVolume()` returns `ErrNotFound`, the processor creates an empty `VolumePair{}` and continues. There is no flag, counter, or index that tracks whether an account has ever been used. Adding this semantic would contaminate the volume model, which is intentionally append-only and stateless per account.

### 4.2 Cost in the Critical Path

For a transaction touching N distinct accounts, implementing default metadata would add:

| Concern | Without defaults | With defaults |
|---------|-----------------|---------------|
| Preload (admission) | Volume `(account, asset)` lookups | + 1 "account exists?" read per account |
| FSM processing | `applyPosting()` per posting | + chart tree walk per account, metadata merge, `enforceSchema()` on defaults |
| Raft proposal size | Postings + metadata | + resolved defaults to replicate |

This overhead is paid on **every transaction**, whether or not any account in it is new.

### 4.3 The Client Already Controls Metadata

Default metadata is business logic — the client decides what metadata an account should carry. The existing mechanisms already cover this use case without any server-side cost:

**Via Numscript** (in the same transaction that creates the account):
```numscript
set_account_meta(@banks:FR123:out, "type", "outgoing")
set_account_meta(@banks:FR123:out, "status", "active")

send [EUR/2 1000] (
    source = @world
    destination = @banks:FR123:out
)
```

**Via `accountMetadata` in the `CreateTransaction` request:**
```json
{
  "postings": [{ "source": "world", "destination": "banks:FR123:out", "amount": 1000, "asset": "EUR/2" }],
  "accountMetadata": {
    "banks:FR123:out": {
      "type": "outgoing",
      "status": "active"
    }
  }
}
```

Both approaches give the client full control over metadata values, support typed metadata (via the metadata schema), and add zero overhead to the server when no metadata is needed.

### 4.4 Separation of Concerns

The chart of accounts answers a structural question: **"is this address valid for this ledger?"** Default metadata answers a business question: **"what data should this account carry?"** Mixing both in the same data structure creates coupling between the account topology and the business rules. Changing a default value would require updating and replicating the entire chart, even though the account structure itself has not changed.

---

## 5. Why Chart Versioning Is Not Needed

The v2 design included chart versioning so that clients could specify a chart version in API requests, allowing a progressive rollout across multiple applications: service A continues to validate against chart v2 while service B already uses chart v3.

This RFC does not include chart versioning. The rationale follows.

### 5.1 Charts Are Naturally Additive

In practice, charts evolve by **adding** account types as the business grows (new payment rails, new wallet structures), not by removing or restricting existing ones. Existing accounts already use the current addresses — removing a segment would invalidate live data.

When a new segment is added (e.g., `users:$userId:wallets:$walletId:main`), existing services are unaffected. They continue using the addresses they already know. Only the services that need the new account type need to be updated. The chart update does not break anything.

### 5.2 Audit Mode Already Covers Gradual Rollout

The `CHART_ENFORCEMENT_AUDIT` mode (§3.2) provides exactly the gradual rollout mechanism that versioning aims to solve:

1. Deploy the new chart in **audit mode** — all transactions proceed, violations are logged
2. Monitor logs to identify which services still use addresses that are no longer in the chart
3. Update the offending services
4. Switch to **strict mode** when all services comply

This is simpler than per-request version negotiation and does not require any client-side changes.

### 5.3 Per-Request Chart Version Breaks Ledger Integrity

The chart is a property of the **ledger**, not of the request. If transaction A is validated against chart v2 and transaction B against chart v3 in the same ledger, the structural integrity of the ledger depends on which client sent which request. The guarantee that "all addresses in this ledger conform to the same rules" is lost.

This contradicts the purpose of a chart of accounts: centralized, authoritative validation of account structure.

### 5.4 The Log Chain Is the Version History

Every chart change produces a `SetChartOfAccountsLog` entry in the immutable, hash-linked log chain. Each entry contains the **complete chart** and a timestamp. Combined with the timestamps on `CreatedTransaction` entries, one can always reconstruct which chart was in effect for any transaction by replaying the log.

This is the same pattern used by the metadata schema — there is no `schema_version` field on transactions. The log is the authoritative history.

Explicit version numbers would add a field to every `CreatedTransaction` and `RevertedTransaction` (and therefore to every Raft proposal), for information that is already derivable from the log.

---

## 6. API Design

### 6.1 Ledger Creation with Chart

Extend `CreateLedgerRequest` and `CreateLedgerOrder` to accept an optional chart at creation time:

```protobuf
// bucket.proto
message CreateLedgerRequest {
  string name = 1;
  repeated common.SetMetadataFieldTypeCommand initial_schema = 3;
  common.ChartOfAccounts chart_of_accounts = 4;         // NEW (field 4)
  common.ChartEnforcementMode enforcement_mode = 5;     // NEW (field 5)
}

// raft_cmd.proto
message CreateLedgerOrder {
  string name = 1;
  repeated common.SetMetadataFieldTypeCommand initial_schema = 3;
  common.ChartOfAccounts chart_of_accounts = 4;         // NEW (field 4)
  common.ChartEnforcementMode enforcement_mode = 5;     // NEW (field 5)
}
```

### 6.2 Update Chart After Creation

Add a `SetChartOfAccounts` variant to `LedgerApplyOrder`:

```protobuf
// raft_cmd.proto
message LedgerApplyOrder {
  string ledger = 1;
  oneof data {
    CreateTransactionOrder create_transaction = 2;
    SaveMetadataOrder add_metadata = 3;
    RevertTransactionOrder revert_transaction = 4;
    DeleteMetadataOrder delete_metadata = 5;
    SetMetadataFieldTypeOrder set_metadata_field_type = 6;
    RemoveMetadataFieldTypeOrder remove_metadata_field_type = 7;
    // NOTE: ConvertMetadataBatchOrder and MetadataConversionCompleteOrder
    // have been moved to direct Proposal fields (metadata_conversion_batches,
    // metadata_conversions_complete) -- they are no longer LedgerApplyOrder variants.
    SetChartOfAccountsOrder set_chart_of_accounts = 8;             // NEW (field 8)
    SetChartEnforcementModeOrder set_chart_enforcement_mode = 9;   // NEW (field 9)
  }
}

message SetChartOfAccountsOrder {
  common.ChartOfAccounts chart_of_accounts = 1;
}

message SetChartEnforcementModeOrder {
  common.ChartEnforcementMode enforcement_mode = 1;
}
```

Corresponding `LedgerApplyRequest` in `bucket.proto`:

```protobuf
message LedgerApplyRequest {
  string ledger = 1;
  oneof data {
    CreateTransactionPayload create_transaction = 2;
    common.SaveMetadataCommand add_metadata = 3;
    RevertTransactionPayload revert_transaction = 4;
    common.DeleteMetadataCommand delete_metadata = 5;
    SetChartOfAccountsRequest set_chart_of_accounts = 6;          // NEW (field 6)
    SetChartEnforcementModeRequest set_chart_enforcement_mode = 7; // NEW (field 7)
  }
}

message SetChartOfAccountsRequest {
  common.ChartOfAccounts chart_of_accounts = 1;
}

message SetChartEnforcementModeRequest {
  common.ChartEnforcementMode enforcement_mode = 1;
}
```

And corresponding system-level requests in `Request`:

```protobuf
message Request {
  string idempotency_key = 1;
  oneof type {
    // ... existing fields 2-20 ...
    SetChartOfAccountsLedgerRequest set_chart_of_accounts = 21;          // NEW (field 21)
    SetChartEnforcementModeLedgerRequest set_chart_enforcement_mode = 22; // NEW (field 22)
  }
  signature.RequestSignature signature = 5;
}

message SetChartOfAccountsLedgerRequest {
  string ledger = 1;
  common.ChartOfAccounts chart_of_accounts = 2;
}

message SetChartEnforcementModeLedgerRequest {
  string ledger = 1;
  common.ChartEnforcementMode enforcement_mode = 2;
}
```

### 6.3 Update Enforcement Mode

`SetChartEnforcementModeOrder` (shown above in §6.2) allows changing the enforcement mode independently of the chart itself. This supports the gradual adoption workflow:

1. Create ledger with chart in audit mode
2. Monitor logs for violations
3. Switch to strict mode when confident

### 6.4 Read Chart

A dedicated read endpoint returns the chart and enforcement mode for a ledger. No new proto messages needed — `GetLedger` already returns `LedgerInfo`, which now includes `chart_of_accounts` and `enforcement_mode`.

For HTTP compatibility, a dedicated endpoint is cleaner:

```
GET /{ledgerName}/chart-of-accounts
```

Returns:
```json
{
  "chartOfAccounts": { ... },
  "enforcementMode": "STRICT"
}
```

This is a convenience — the same data is available via `GetLedger`.

### 6.5 Whole-Chart Replacement

Charts are replaced as a whole — there is no incremental patching API. Reasons:

- Charts are small (tens to hundreds of nodes, not thousands)
- Charts change rarely (initial setup, occasional restructuring)
- Whole replacement is simpler to reason about, validate, and replicate
- Partial updates would require complex merge semantics (what happens when you remove a segment that existing accounts depend on?)

### 6.6 Ledger Log Payloads

New log payload variants for auditability:

```protobuf
message LedgerLogPayload {
  oneof payload {
    // ... existing fields 1-8 ...
    SetChartOfAccountsLog set_chart_of_accounts = 9;         // NEW (field 9)
    SetChartEnforcementModeLog set_chart_enforcement_mode = 10; // NEW (field 10)
  }
}

message SetChartOfAccountsLog {
  ChartOfAccounts chart_of_accounts = 1;
}

message SetChartEnforcementModeLog {
  ChartEnforcementMode enforcement_mode = 1;
}
```

---

## 7. Proto Changes Summary

### 7.1 `common.proto`

| Change | Details |
|--------|---------|
| New message `ChartOfAccounts` | `map<string, ChartSegment> roots = 1` |
| New message `ChartSegment` | Fields: `account` (1), `children` (2), `variable` (3) |
| New message `ChartVariable` | Fields: `name` (1), `pattern` (2), `account` (3), `children` (4), `variable` (5) |
| New enum `ChartEnforcementMode` | `CHART_ENFORCEMENT_STRICT` (0), `CHART_ENFORCEMENT_AUDIT` (1) |
| Extended `LedgerInfo` | New field 6: `chart_of_accounts`, new field 7: `enforcement_mode` |
| Extended `LedgerLogPayload` | New field 9: `set_chart_of_accounts`, new field 10: `set_chart_enforcement_mode` |
| New message `SetChartOfAccountsLog` | Field: `chart_of_accounts` (1) |
| New message `SetChartEnforcementModeLog` | Field: `enforcement_mode` (1) |

### 7.2 `raft_cmd.proto`

| Change | Details |
|--------|---------|
| Extended `CreateLedgerOrder` | New field 4: `chart_of_accounts`, new field 5: `enforcement_mode` |
| Extended `LedgerApplyOrder` | New field 10: `set_chart_of_accounts`, new field 11: `set_chart_enforcement_mode` |
| New message `SetChartOfAccountsOrder` | Field: `chart_of_accounts` (1) |
| New message `SetChartEnforcementModeOrder` | Field: `enforcement_mode` (1) |

### 7.3 `bucket.proto`

| Change | Details |
|--------|---------|
| Extended `CreateLedgerRequest` | New field 4: `chart_of_accounts`, new field 5: `enforcement_mode` |
| Extended `LedgerApplyRequest` | New field 6: `set_chart_of_accounts`, new field 7: `set_chart_enforcement_mode` |
| Extended `Request` | New field 21: `set_chart_of_accounts`, new field 22: `set_chart_enforcement_mode` |
| New message `SetChartOfAccountsRequest` | Field: `chart_of_accounts` (1) |
| New message `SetChartEnforcementModeRequest` | Field: `enforcement_mode` (1) |
| New message `SetChartOfAccountsLedgerRequest` | Fields: `ledger` (1), `chart_of_accounts` (2) |
| New message `SetChartEnforcementModeLedgerRequest` | Fields: `ledger` (1), `enforcement_mode` (2) |
| New RPC (optional) | `GetChartOfAccounts` if a dedicated RPC is preferred over `GetLedger` |

---

## 8. Implementation Files

### New Files

| File | Description |
|------|-------------|
| `internal/service/processing/processor_chart.go` | Chart validation logic: `validateAccountsInChart()`, chart self-validation, tree walker |
| `internal/service/processing/processor_chart_test.go` | Unit tests for chart validation |
| `internal/compat/http/handler_get_chart_of_accounts.go` | HTTP handler for `GET /{ledgerName}/chart-of-accounts` |
| `internal/compat/http/handler_set_chart_of_accounts.go` | HTTP handler for `PUT /{ledgerName}/chart-of-accounts` |
| `internal/compat/http/handler_set_chart_enforcement_mode.go` | HTTP handler for `PUT /{ledgerName}/chart-of-accounts/enforcement-mode` |

### Modified Files

| File | Changes |
|------|---------|
| `misc/proto/common.proto` | New messages, enum, extended `LedgerInfo` and `LedgerLogPayload` |
| `misc/proto/raft_cmd.proto` | Extended `CreateLedgerOrder`, `LedgerApplyOrder`; new order messages |
| `misc/proto/bucket.proto` | Extended `CreateLedgerRequest`, `LedgerApplyRequest`, `Request`; new request messages |
| `internal/service/processing/processor_transaction.go` | Add `validateAccountsInChart()` call after `produce()` |
| `internal/service/processing/processor_revert_transaction.go` | Add `validateAccountsInChart()` call after reversed postings construction |
| `internal/service/processing/processor.go` | New `processSetChartOfAccounts()` and `processSetChartEnforcementMode()` handlers |
| `internal/ctrl/controller.go` | New controller methods for chart operations |
| `internal/ctrl/controller_default.go` | Engine interface extension |
| `internal/compat/http/router.go` | Register new chart routes |
| `openapi.yml` | New endpoints and schemas |
| `docs/technical/contributing/api-comparison.md` | Document new endpoints |

---

## 9. v2 Comparison Table

| Aspect | v2 (prefix-based) | v3 (this RFC) |
|--------|-------------------|---------------|
| Variable segment | `"$iban": { ... }` | `"variable": { "name": "iban", ... }` |
| Account endpoint marker | `".self": {}` | `"account": true` |
| Regex constraint | `".pattern": "^...$"` | `"pattern": "^...$"` |
| Default metadata | `".metadata": { "key": "val" }` | Out of scope — client-side via Numscript / `accountMetadata` (see §4) |
| Fixed children | Keys without prefix | Inside `"children": { ... }` |
| Typeable in OpenAPI | No (`additionalProperties`) | Yes (strict typed schema) |
| Typeable in protobuf | No | Yes (dedicated messages) |
| Enforcement mode | Always strict (implicit) | Configurable: `STRICT` / `AUDIT` |
| Storage | Part of broader "Schema" object | Dedicated fields on `LedgerInfo` |
| Scope | Chart + default metadata + tx templates + query templates | Validation only (see §4) |
| Nesting depth | Flat (one variable level) | Recursive (unlimited nesting) |
| Update API | Part of schema update | Dedicated whole-chart replacement |

---

## 10. Examples

### 10.1 Full Chart Example

A fintech platform with banks, users, and platform accounts:

```json
{
  "banks": {
    "variable": {
      "name": "iban",
      "pattern": "^[A-Z]{2}[0-9]{2}[A-Z0-9]{4}[0-9]{7}([A-Z0-9]?){0,16}$",
      "children": {
        "main": { "account": true },
        "out": { "account": true },
        "pendingOut": { "account": true }
      }
    }
  },
  "users": {
    "variable": {
      "name": "userId",
      "pattern": "^[a-f0-9-]{36}$",
      "account": true,
      "children": {
        "main": { "account": true },
        "wallets": {
          "variable": {
            "name": "walletId",
            "pattern": "^[a-f0-9-]{36}$",
            "children": {
              "main": { "account": true },
              "held": { "account": true }
            }
          }
        }
      }
    }
  },
  "platform": {
    "children": {
      "fees": { "account": true },
      "revenue": { "account": true },
      "suspense": { "account": true }
    }
  }
}
```

### 10.2 Address Validation Examples

Using the chart from §10.1:

| Address | Valid? | Reason |
|---------|--------|--------|
| `banks:FR7630006000011234567890189:main` | Yes | IBAN matches pattern, `main` has `account: true` |
| `banks:FR7630006000011234567890189:out` | Yes | IBAN matches, `out` has `account: true` |
| `banks:FR7630006000011234567890189` | No | Variable node has `account: false` (no `account: true`) |
| `banks:INVALID:main` | No | `"INVALID"` does not match IBAN pattern |
| `users:550e8400-e29b-41d4-a716-446655440000` | Yes | UUID matches pattern, variable has `account: true` |
| `users:550e8400-e29b-41d4-a716-446655440000:main` | Yes | UUID matches, `main` has `account: true` |
| `users:550e8400-e29b-41d4-a716-446655440000:wallets:a1b2c3d4-e5f6-7890-abcd-ef1234567890:main` | Yes | Both UUIDs match, terminal `main` has `account: true` |
| `users:550e8400-e29b-41d4-a716-446655440000:wallets` | No | `wallets` node has no `account: true` |
| `platform:fees` | Yes | Fixed child with `account: true` |
| `platform` | No | No `account: true` at this level |
| `unknown:something` | No | `unknown` is not a root segment |
| `world` | Yes | Always allowed (special account) |

### 10.3 Numscript Interaction

```numscript
vars {
    account  $source
    monetary $amount
}

set_account_meta($source, "lastUsed", "2024-01-15")

send $amount (
    source = $source
    destination = @platform:fees
)
```

With the chart from §10.1:
- `$source` is validated at execution time — the resolved address must be in the chart
- `@platform:fees` is a literal — validated against the chart (and it passes)
- `set_account_meta` sets metadata as usual — the chart is not involved in metadata logic

### 10.4 Gradual Adoption Workflow

```bash
# 1. Create ledger with chart in AUDIT mode
ledgerctl ledgers create --name payments \
    --chart-of-accounts chart.json \
    --enforcement-mode audit

# 2. Run production traffic, monitor logs for violations
# Look for: "chart validation warning: account not in chart: ..."

# 3. Fix any scripts using invalid addresses

# 4. Switch to STRICT mode
ledgerctl ledgers set-chart-enforcement-mode --name payments --mode strict
```

---

## 11. Open Questions

| # | Question | Notes |
|---|----------|-------|
| 1 | Should removing the chart (setting to nil) be allowed after creation? | This would effectively disable validation. Could be useful for migration, but dangerous in production. Consider requiring explicit `--force` flag. |
| 2 | Should chart updates validate existing accounts? | When updating a chart, should we check that all existing accounts still conform? This could be expensive for large ledgers. Consider an async validation mode similar to metadata conversion. |
| 3 | Should variable segments support enumerated values? | Instead of (or in addition to) regex patterns, allow `"enum": ["checking", "savings"]` for a closed set of values. Simpler validation, but less flexible than regex. |
