# RFC: Numscript Static Input Declaration Contract

- **Status:** Draft
- **Target:** Ledger v3 (strict enforcement)
- **Audience:** Ledger / Numscript implementers
- **Scope:** Numscript → Ledger contract for declaring **all external ledger inputs** required by a Numscript script at **static analysis time**.

> **Note on the Go API:** the Go interfaces/types in this RFC are **illustrative** and **MAY be refined** as the Ledger v3 architecture evolves. They exist to make the contract concrete.

---

## 0. Current Interim Solution

> **Note:** Until this RFC is implemented, the ledger uses a **Numscript emulation** approach for volume discovery. The admission layer runs the script once with a "discovery store" that returns infinite balances (`2^256`) to discover which accounts/assets are queried. See `internal/domain/processing/numscript/emulate.go` and the [Numscript documentation](../../technical/contributing/numscript.md#volume-preloading-numscript-emulation) for details. This approach has known limitations (e.g., `oneof` may not discover all sources) and will be replaced by the static analysis described in this RFC.

## 1. Motivation

Numscript scripts may read ledger state during execution (e.g., balances, account metadata).  
Without knowing those reads ahead of time, the ledger cannot reliably:
- isolate execution over the correct subset of state,
- enforce read policies (authZ / tenancy),
- guarantee determinism and reproducibility,
- provide strong auditability (“what state did this execution depend on?”).

This RFC introduces a contract where **all reads are declared prior to execution**.

---

## 2. Invariant

### 2.1 Invariant #1 — Static Input Declaration

Numscript **MUST** be able to declare the **complete set** of external inputs required for execution at **static analysis time**.

Formally, for a script `S` evaluated under an analysis context `C` (including provided vars), Numscript produces:

```
Requirements = Analyze(S, C)
```

such that:
- `Requirements` is **finite**,
- `Requirements` is **complete** for all ledger reads that may occur during execution under `C`,
- runtime execution is constrained to the declared reads (see §7).

---

## 3. Environment Policy

### 3.1 Configurability

Enforcement of this contract **MAY** be configurable by the environment (the ledger):
- **Legacy/Off mode**: static input declaration not required.
- **Strict mode**: static input declaration required; non-static selectors rejected.

### 3.2 Ledger v3 Requirement

For **Ledger v3**, **Strict mode MUST be enforced systematically**, because it is necessary for the global correctness of the system (execution isolation and overall runtime model).

> In Ledger v3, any script that uses read primitives (`balance`, `account_metadata`) **MUST** be statically resolvable (script + vars supplied at analysis). Otherwise it is rejected.

---

## 4. Confirmed Numscript Primitives (In-Scope)

This RFC only references primitives that are **confirmed** to exist.

### 4.1 Write primitives (no external reads)
- `send`
- `set_tx_meta`

### 4.2 Read primitives (source of external inputs)
- `balance(account, asset)`
- `account_metadata(account, key)`

### 4.3 Declarative
- `vars { … }`

No other primitives are specified here.

---

## 5. Requirements Object

### 5.1 Definition

Numscript **MUST** expose a `Requirements` object containing:
- required **balance reads**
- required **account metadata reads**

The ledger is responsible for:
- normalization,
- deduplication,
- ordering,
- policy enforcement.

Numscript is only responsible for **declaring** the necessary reads.

### 5.2 JSON shape (canonical examples)

```json
{
  "balanceReads": [
    { "account": "users:alice", "asset": "USD/2" }
  ],
  "accountMetadataReads": [
    { "account": "users:alice", "key": "kyc_level" }
  ]
}
```

---

## 6. Static Selector Constraints (Vars-aware)

### 6.1 Analysis context includes Vars

Static analysis is performed over:
- the Numscript source,
- a set of **vars supplied at analysis time** by the environment (ledger).

Vars supplied at analysis time are part of the static context.  
If a selector references a var and that var is supplied, the selector can be resolved statically.

### 6.2 Rule: selectors MUST be statically determinable (strict mode)

In **strict mode**, all selectors of read primitives **MUST** be statically determinable under (script + analysis vars):

- `balance(account, asset)`:
  - `account` MUST be statically determinable
  - `asset` MUST be statically determinable
- `account_metadata(account, key)`:
  - `account` MUST be statically determinable
  - `key` MUST be statically determinable

If any selector cannot be resolved statically, analysis **MUST fail**.

### 6.3 Vars substitution examples

#### Valid (variable provided)

```numscript
vars {
  asset string
}

bal = balance(@users:alice, $asset)
```

If analysis vars contain:

```json
{ "asset": "USD/2" }
```

Then `Requirements` includes:

```json
{
  "balanceReads": [
    { "account": "users:alice", "asset": "USD/2" }
  ]
}
```

#### Invalid (missing variable)

Same script, but `$asset` not provided at analysis time:

- analysis MUST fail in strict mode (selector not statically determinable).

### 6.4 Computed selectors MUST be rejected (strict mode)

Selectors built via computation/concatenation/interpolation **MUST be rejected** in strict mode, as they are not statically bounded:

```numscript
bal = balance(@("users:" + $id), "USD/2")
```

---

## 7. Requirements Extraction Rules

### 7.1 Completeness (MUST)

In strict mode, analysis **MUST** return a `Requirements` that includes all external reads that may occur during execution (under the provided analysis vars).

Given the current “linear” model (no confirmed conditionals/loops), this means:
- every occurrence of `balance()` contributes a `BalanceRead`,
- every occurrence of `account_metadata()` contributes an `AccountMetadataRead`,
after resolving selectors using analysis vars.

### 7.2 Finitude (MUST)

`Requirements` **MUST** be finite.

---

## 8. Rejection Rules (Strict mode)

A script **MUST** be rejected in strict mode if any read primitive cannot be resolved statically.

### 8.1 Unresolved account selector

```numscript
bal = balance(@$account, "USD/2")
```

This is valid only if `$account` is supplied at analysis time with a concrete account identifier.  
Otherwise it MUST be rejected.

### 8.2 Unresolved asset selector

```numscript
bal = balance(@users:alice, $asset)
```

This is valid only if `$asset` is supplied at analysis time with a concrete asset.  
Otherwise it MUST be rejected.

### 8.3 Unresolved metadata key selector

```numscript
lvl = account_metadata(@users:alice, $key)
```

This is valid only if `$key` is supplied at analysis time with a concrete key.  
Otherwise it MUST be rejected.

### 8.4 Computed selectors

Any computed selector MUST be rejected (see §6.4).

---

## 9. Runtime Safety Rule

At runtime, any read primitive **MUST** match an entry declared in `Requirements`.  
Undeclared reads **MUST** fail execution.

This rule protects the ledger even if a bug causes divergence between analysis and runtime.

---

## 10. End-to-end Examples

### 10.1 Balance + metadata + tx meta + send (vars provided)

```numscript
vars {
  amount int
  asset  string
}

bal = balance(@users:alice, $asset)
lvl = account_metadata(@users:alice, "kyc_level")

set_tx_meta("category", "payout")

send [$asset $amount] (
  source = @users:alice
  destination = @treasury
)
```

Analysis vars:

```json
{ "amount": 100, "asset": "USD/2" }
```

Requirements:

```json
{
  "balanceReads": [
    { "account": "users:alice", "asset": "USD/2" }
  ],
  "accountMetadataReads": [
    { "account": "users:alice", "key": "kyc_level" }
  ]
}
```

### 10.2 Rejection (vars missing)

Same script, but analysis vars do not contain `asset`:

- strict analysis MUST fail (cannot resolve `balance(@users:alice, $asset)` statically).

---

## 11. Go Interfaces (Illustrative, MAY be refined)

### 11.1 Requirements types

```go
package numscriptreq

type Requirements struct {
    BalanceReads         []BalanceRead         `json:"balanceReads,omitempty"`
    AccountMetadataReads []AccountMetadataRead `json:"accountMetadataReads,omitempty"`
}

type BalanceRead struct {
    Account string `json:"account"` // e.g. "users:alice"
    Asset   string `json:"asset"`   // e.g. "USD/2"
}

type AccountMetadataRead struct {
    Account string `json:"account"` // e.g. "users:alice"
    Key     string `json:"key"`     // e.g. "kyc_level"
}
```

### 11.2 Policy (environment-driven)

```go
package numscriptreq

type StaticMode string

const (
    StaticModeOff    StaticMode = "off"    // legacy/compat
    StaticModeStrict StaticMode = "strict" // required in Ledger v3
)
```

### 11.3 Vars at analysis time

This is an example encoding. The concrete representation MAY change.

```go
package numscriptreq

type Vars map[string]Value

type Value struct {
    String *string `json:"string,omitempty"`
    Int    *int64  `json:"int,omitempty"`
    Bool   *bool   `json:"bool,omitempty"`
}
```

### 11.4 Analyzer API

```go
package numscriptreq

import "context"

type AnalyzeRequest struct {
    Script string     `json:"script"`
    Vars   Vars       `json:"vars,omitempty"`
    Mode   StaticMode `json:"mode"`
}

type Analyzer interface {
    Analyze(ctx context.Context, req AnalyzeRequest) (Requirements, error)
}
```

### 11.5 Typed errors (optional)

The error taxonomy is illustrative and MAY be refined.

```go
package numscriptreq

import "fmt"

type ErrorCode string

const (
    ErrNonStaticRequirement ErrorCode = "NON_STATIC_REQUIREMENT"
    ErrMissingVar           ErrorCode = "MISSING_VAR"
    ErrInvalidVarType       ErrorCode = "INVALID_VAR_TYPE"
)

type AnalyzeError struct {
    Code    ErrorCode
    Message string
}

func (e *AnalyzeError) Error() string {
    return fmt.Sprintf("%s: %s", e.Code, e.Message)
}
```

---

## 12. Extension Rule

Any future Numscript primitive that reads ledger state **MUST**:
1. define its selectors,
2. require static determinability in strict mode,
3. be reflected in `Requirements`.

---

## 13. Summary

- Numscript declares all required reads at static analysis time.
- Vars supplied at analysis time are part of the static context and may resolve selectors.
- Strict enforcement is optional by environment, but **mandatory in Ledger v3**.
- Runtime must reject any undeclared read.
