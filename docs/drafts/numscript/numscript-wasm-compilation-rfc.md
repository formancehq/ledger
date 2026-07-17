# RFC: Numscript WASM Compilation Pipeline

- **Status:** Draft
- **Audience:** Numscript implementers, host environment integrators (Ledger v3, SDKs, tooling)
- **Scope:** Compiling Numscript to a serializable, executable representation via WebAssembly, with upfront variable validation, zero-allocation numeric types, and a host-agnostic execution model.

> **Note:** Numscript is a general-purpose financial scripting language. While the Ledger v3 is the primary consumer today, Numscript may be embedded in other environments (other ledger implementations, simulation tools, browser playgrounds, SDK runtimes). Part I of this RFC defines the **Numscript contract** independent of any specific host. Part II describes **Ledger v3 integration** as one concrete consumer.

---

## 1. Summary of Changes Required in Numscript

| # | Change | Why |
|---|--------|-----|
| 1 | **WASM distribution** — parser + executor available as a WASM module | Single source of truth, reusable across runtimes (Go server, browser, CLI, SDKs) |
| 2 | **Serializable compiled output** — parser produces a compact IR (bytecode), not just an in-memory AST | The compiled form can be stored and transmitted without re-parsing |
| 3 | **Variable validation as a separate pass** — validate and bind variables before execution | Allows the host to reject bad inputs early, before committing to execution |
| 4 | **Uint256 native type** — replace `*big.Int` with a 256-bit unsigned integer | Zero-allocation numeric boundary between Numscript and the host |
| 5 | **Volume-based balance interface** — receive `(input, output)` instead of a signed balance | Eliminates signed arithmetic; flexible for hosts with different storage models |
| 6 | **Data-in-memory execution** — all state (balances, metadata) is provided upfront, no host callbacks during execution | Zero host↔WASM round-trips; the module reads from its own linear memory |

---

# Part I — Numscript Changes

## 2. WASM Distribution

### 2.1 Requirement

The Numscript parser and executor **MUST** be compiled to a WASM module so that they can be embedded in:

- **Go server** — via a WASM runtime (wazero, wasmtime-go, etc.)
- **Browser** — native WASM support (playground, documentation)
- **CLI tooling** — linting, formatting, static analysis
- **Other languages** — SDKs, tests — WASM is language-agnostic

Today the parser is a Go library. This couples it to Go and prevents reuse.

### 2.2 Implementation language

The parser can be written in any language that compiles to WASM:

| Language | Pros | Cons |
|----------|------|------|
| Rust | Excellent WASM support, zero-cost abstractions, mature tooling | Steeper learning curve |
| Go | Team familiarity, existing parser code | Larger WASM binary, GC overhead |
| TypeScript (AssemblyScript) | Easy for browser scenarios | Less suitable for compiler internals |

The choice is out of scope for this RFC. What matters is that the parser exposes a WASM-compatible API.

### 2.3 WASM exports

The WASM module **MUST** export at minimum:

```
parse(source: bytes) → ParseResult

ParseResult:
  ok:    CompiledProgram (serializable bytes)
  error: ParseError[]
```

```
analyze(compiled: bytes, vars: bytes) → AnalyzeResult

AnalyzeResult:
  ok:    Requirements
  error: AnalyzeError[]
```

See [Static Inputs RFC](../../technical/contributing/numscript.md#volume-preloading-dependency-resolution) for the `Requirements` contract.

---

## 3. Serializable Compiled Output (IR)

### 3.1 Requirement

The parser **MUST** produce a serializable compiled representation — not just an in-memory AST. This representation is called the IR (Intermediate Representation).

### 3.2 Properties

The IR **MUST**:

1. **Be serializable** — flat bytes, no pointers, no host references.
2. **Be deterministic** — the same source always produces the same bytes (bitwise).
3. **Be versionable** — include a version tag so consumers can reject incompatible artifacts.
4. **Be compact** — smaller than the source text for typical scripts.
5. **Be self-contained** — no external references needed to execute (except runtime state).

### 3.3 Version tag

Every compiled artifact **MUST** start with a version header:

```
[version: uint32] [payload: bytes]
```

### 3.4 Two-tier approach

#### Tier 1: Serialized IR (minimum viable)

The parser produces a serialized bytecode (protobuf-encoded AST, stack machine, or register machine). An interpreter decodes and executes it.

```
Source → [WASM parser] → IR (bytes) → [interpreter] → postings
```

#### Tier 2: WASM executable (ideal target)

The compiler produces a **WASM module** as output. The consumer loads the WASM module and calls its `execute` export directly.

```
Source → [WASM compiler] → WASM module (bytes) → [WASM runtime] → postings
```

**Recommendation:** Start with Tier 1. Design the version tag so Tier 2 artifacts can coexist.

---

## 4. Variable Validation as a Separate Pass

### 4.1 Requirement

Numscript **MUST** expose variable validation as a **distinct step**, separate from execution:

```
validate_vars(compiled: bytes, vars: map[string, string]) → ValidateResult

ValidateResult:
  ok:    BoundVars (serializable bytes, typed values)
  error: ValidationError[]
```

This allows the caller to:
1. **Reject bad inputs early** — before committing to execution.
2. **Transmit pre-validated values** — the executor receives `BoundVars` and skips all type-checking.

### 4.2 BoundVars wire types

`BoundVars` is a serializable representation of the variables, already converted to their typed form:

| Numscript type | Wire type (BoundVars) |
|----------------|----------------------|
| `account`      | `string` (validated account address) |
| `monetary`     | `asset: string` + `amount: Uint256` |
| `string`       | `string` |
| `number`       | `int64` or `Uint256` |
| `portion`      | `numerator: int64` + `denominator: int64` |

### 4.3 Executor contract

The executor **MUST** accept `BoundVars` directly and **MUST NOT** re-validate types, check for missing variables, or perform string→typed conversion. The caller guarantees validity.

---

## 5. Uint256 Native Type

### 5.1 Problem

Numscript currently uses Go's `*big.Int` for all monetary amounts. This is a heap-allocated, pointer-heavy, GC-visible type. Every amount crossing the Numscript boundary requires allocation.

### 5.2 Requirement

Numscript **MUST** use a 256-bit unsigned integer as its native numeric type for monetary amounts. The concrete type can be `holiman/uint256.Int` (Go), a 4×`u64` struct (Rust/WASM), or any ABI-compatible equivalent.

The key property: **zero allocation** when passing amounts between the host and Numscript.

### 5.3 What changes

| Interface | Today (`*big.Int`) | Proposed (`uint256`) |
|-----------|-------------------|----------------------|
| `Posting.Amount` | `*big.Int` | `uint256` |
| Balance query result | `map[string]map[string]*big.Int` | Replaced by volume interface (see §6) |
| Variable type `number` | `*big.Int` | `uint256` (or `int64` for small values) |
| Overdraft limits | `*big.Int` | `uint256` |

### 5.4 WASM compatibility

In WASM, a uint256 is 4 × `i64` — same as the Go `[4]uint64` layout. No GC, no heap. Host functions pass 4 × `i64` values directly.

This makes the BigInt→Uint256 migration a **prerequisite** for the WASM compilation pipeline.

---

## 6. Volume-based Balance Interface

### 6.1 Problem

`uint256` is unsigned, but balances can be negative (output > input, i.e. overdraft). Passing a signed balance to Numscript would require either:
- A signed wrapper type — adds complexity
- A heap-allocated arbitrary-precision integer — defeats the zero-allocation goal

### 6.2 Requirement

Numscript **MUST** accept account state as a pair `(input, output)` — two unsigned uint256 values — instead of a single signed balance.

```
Today:    signed balance  →  Numscript
Proposed: (input, output) →  2 × uint256  →  Numscript
```

### 6.3 Host flexibility

Different host environments store account state differently. The `(input, output)` interface accommodates all of them:

| Host storage model | How to provide `(input, output)` |
|--------------------|----------------------------------|
| **Separate input/output volumes** (e.g., Ledger v3) | Pass volumes directly — zero conversion |
| **Single signed balance** (e.g., simple ledger, simulation) | Pass `(balance, 0)` if balance ≥ 0, or `(0, abs(balance))` if balance < 0 |
| **Single unsigned balance** (no overdraft) | Pass `(balance, 0)` |

The conversion for hosts with a single balance is trivial and happens once at the injection boundary.

### 6.4 How Numscript uses it

Numscript only needs to answer:

1. **Does the account have enough funds?** → `input >= output + amount` (unsigned comparison, no subtraction)
2. **What is the available balance?** → `input - output` (safe when `input >= output`, or overdraft is explicit)
3. **What is the overdraft amount?** → `output - input` (always positive when output > input)

No signed arithmetic is ever needed. Both values are unsigned.

### 6.5 Benefits

- Eliminates signed integers from the Numscript numeric model entirely.
- Gives Numscript full information about the account state (input and output separately).
- Host-agnostic: any environment can map its storage model to `(input, output)`.
- Zero-allocation: two uint256 value copies.

---

## 7. Data-in-Memory Execution (No Host Callbacks)

### 7.1 Problem

Today Numscript uses a callback interface — the host provides a `Store` that the interpreter calls during execution to fetch balances and metadata. Each call is a host↔WASM boundary crossing with register save/restore, memory copies, and context switching overhead.

For a script reading 10 accounts × 2 assets, that's 20+ host round-trips during a single execution.

### 7.2 Requirement

The WASM module **MUST** start with all required data already in its linear memory. There must be **zero host function calls** during script execution.

The execution model becomes:

```
1. Host writes all state (volumes, metadata) into WASM linear memory
2. Host calls execute(state_ptr, state_len, vars_ptr, vars_len)
3. WASM module reads state from its own linear memory — ZERO host calls
4. Host reads results (postings, metadata) from WASM linear memory
```

Only **2 boundary crossings**: enter (`execute`) and exit (read results).

This model is host-agnostic: the host is responsible for collecting the required state (however it stores it) and injecting it into WASM memory before execution.

### 7.3 Memory layout (illustrative)

```
WASM linear memory:
┌─────────────────────────────────────────────────┐
│ Header: num_volumes, num_metadata_entries        │
├─────────────────────────────────────────────────┤
│ Volume table (sorted by account+asset):         │
│   [account_hash, asset_hash, input_v0..v3,      │
│    output_v0..v3]                               │
│   ...                                           │
├─────────────────────────────────────────────────┤
│ Metadata table (sorted by account+key):         │
│   [account_hash, key_hash, value_offset,        │
│    value_len]                                   │
│   ...                                           │
├─────────────────────────────────────────────────┤
│ String pool (account names, keys, values)       │
├─────────────────────────────────────────────────┤
│ Bound vars (typed, pre-validated)               │
├─────────────────────────────────────────────────┤
│ Result area (postings, metadata output)         │
└─────────────────────────────────────────────────┘
```

### 7.4 Prerequisite

This model requires the [Static Inputs RFC](../../technical/contributing/numscript.md#volume-preloading-dependency-resolution): the `Requirements` object declares **all** state that will be read, so the host knows what to collect and inject before execution. The host is free to source this data from any backing store — volumes, a database, an in-memory map, or even hardcoded test fixtures.

---

# Part II — Ledger Integration Details

## 8. Motivation (Current State)

Today the Numscript lifecycle in the ledger is:

1. **Parse** the script source (Go, on the leader).
2. **Emulate** to discover volume reads (Go, on the leader).
3. **Encode the source text** into the Raft command.
4. On every replica, **re-parse + re-execute** the same source text.

| Problem | Impact |
|---------|--------|
| Parsing happens on every replica | Wasted CPU on the FSM hot path |
| Variable validation happens at execution | Errors surface too late, wasted Raft round-trip |
| The interpreter is coupled to Go | No reuse in browser, CLI tooling, other languages |
| Source text is stored in the Raft log | Larger log entries than necessary |
| `*big.Int` at the Numscript boundary | Heap allocations on every balance read and posting conversion |

---

## 9. Architecture Overview

With the Numscript changes from Part I, the ledger pipeline becomes:

```
                        ┌──────────────────────────────────┐
                        │         Admission Layer           │
                        │         (leader node)             │
                        │                                   │
  Numscript source ───► │  1. Parse (WASM parser)           │
  + vars               │  2. Static analysis → Requirements│
                        │  3. Validate variables → BoundVars│
                        │  4. Compile → executable artifact │
                        │  5. Encode artifact + bound vars  │
                        │     into Raft command              │
                        └───────────────┬──────────────────┘
                                        │
                                        ▼
                        ┌──────────────────────────────────┐
                        │         Raft Consensus            │
                        │   (replicated to all nodes)       │
                        │                                   │
                        │   Command = artifact + bound vars │
                        │   (opaque bytes, no source text)  │
                        └───────────────┬──────────────────┘
                                        │
                                        ▼
                        ┌──────────────────────────────────┐
                        │         FSM Apply (hot path)      │
                        │         (every replica)           │
                        │                                   │
                        │  1. Decode artifact + bound vars  │
                        │  2. Write preloaded state into    │
                        │     WASM linear memory            │
                        │  3. Execute (no parse, no         │
                        │     validation, no host calls)    │
                        │  4. Read results, apply postings  │
                        └──────────────────────────────────┘
```

### What changes compared to today

| Step | Today | Proposed |
|------|-------|----------|
| Parse | Every replica (from source text) | Once at admission (WASM parser) |
| Variable validation | At execution time | At admission; FSM trusts pre-validated values |
| Raft log payload | Source text + vars (string) | Compiled artifact + bound vars (bytes) |
| Execution | Go interpreter on every replica | WASM executor on every replica (decode + run) |
| Balance reads | Host callbacks during execution | Pre-injected into WASM memory, zero host calls |
| Numeric type | `*big.Int` (heap alloc per value) | `uint256` (zero alloc, 4 × `i64` copy) |

---

## 10. Raft Command Changes

The Raft command for a Numscript transaction becomes:

```protobuf
message CreateLogCommand {
  // ... existing fields ...

  // Replaces the script source text:
  bytes   compiled_program = N;  // opaque compiled artifact
  bytes   bound_vars       = N;  // pre-validated, typed variables
  uint32  compiler_version = N;  // artifact version tag
}
```

The source text is **not** stored in the Raft log. If auditability of the original source is needed, it can be stored separately (e.g., in a content-addressed blob store keyed by the script hash).

---

## 11. FSM Execution Contract

On the hot path, the FSM:

1. Decodes `compiled_program` and `bound_vars`.
2. **Does NOT** validate variables — the admission layer already did.
3. Writes preloaded state (balances as `(input, output)` pairs, metadata) into WASM linear memory.
4. Calls `execute` — single boundary crossing, zero host callbacks.
5. Reads results (postings, metadata) from WASM linear memory.
6. Applies postings to ledger state.

```go
// Illustrative — the concrete API will depend on the WASM runtime choice.
type Executor interface {
    Execute(ctx context.Context, req ExecuteRequest) (ExecuteResult, error)
}

type ExecuteRequest struct {
    Program   []byte            // compiled artifact
    BoundVars []byte            // pre-validated variables
    State     ExecutionState    // preloaded volumes (input, output) + metadata
}

type ExecuteResult struct {
    Postings    []Posting
    TxMetadata  map[string]string
    AccMetadata map[string]map[string]string
}
```

---

## 12. Determinism Guarantee

### 12.1 Why it matters

Every replica applies the same Raft entry and **MUST** produce the same result.

### 12.2 WASM determinism

WASM is almost deterministic by specification. Known sources of non-determinism and mitigations:

| Source | Mitigation |
|--------|------------|
| Floating-point NaN bit patterns | Numscript does not use floats — all amounts are uint256 |
| `memory.grow` failure | Bound memory at module instantiation |
| Host function behavior | No host calls during execution — all state pre-injected |
| Thread scheduling | Single-threaded; no threads in Numscript |

---

## 13. Code-level Impact (Before / After)

### 13.1 Balance reads (`numscriptStoreAdapter`)

```go
// Before — allocates 3 *big.Int per balance read:
balance := new(big.Int).Sub(inputVal.ToBig(), outputVal.ToBig())
accountBalance[asset] = balance

// After — zero-alloc, volumes passed directly:
// (not applicable — balances are pre-injected into WASM memory as (input, output) pairs)
```

### 13.2 Posting conversion

```go
// Before — allocates + overflow check per posting:
if overflow := u256Amount.SetFromBig(posting.Amount); overflow { ... }

// After — direct copy, no allocation:
u256Amount = posting.Amount  // already uint256
```

---

## 14. Script Caching

### 14.1 Compilation cache

The leader caches compiled artifacts by script content hash (blake3):

```
cache: map[blake3(source)] → CompiledProgram
```

Local to the leader node, not replicated.

### 14.2 WASM module instance pooling (Tier 2)

For the WASM executable approach, the runtime can pool pre-compiled modules:

```
pool: map[blake3(compiled_program)] → pre-compiled WASM module
```

wazero and wasmtime both support module caching natively.

---

## 15. End-to-end Flow

### Step 1: Client submits a transaction

```json
{
  "script": {
    "plain": "vars { account $src monetary $amt } send $amt ( source = $src destination = @treasury )",
    "vars": { "src": "users:alice", "amt": "USD/2 1000" }
  }
}
```

### Step 2: Admission layer (leader)

```
1. Parse source           → CompiledProgram (via WASM parser)
2. Analyze(program, vars) → Requirements { balanceReads: [{account: "users:alice", asset: "USD/2"}] }
3. Validate vars          → BoundVars { src: "users:alice", amt: {asset: "USD/2", amount: 1000} }
4. Preload state          → { "users:alice/USD/2": (input, output) }
5. Encode into Raft cmd:
     compiled_program = <bytes>
     bound_vars       = <bytes>
```

### Step 3: Raft consensus

Command replicated. All replicas receive `(compiled_program, bound_vars)`.

### Step 4: FSM apply (every replica)

```
1. Decode compiled_program, bound_vars
2. Write preloaded state into WASM linear memory
3. Call execute → postings
4. Apply postings to ledger state
```

No parsing, no variable validation, no host callbacks.

---

## 16. Migration Path

### Phase 1: Serialized IR + variable validation + uint256

1. Numscript exposes `parse → IR`, `validate_vars → BoundVars`, and uses uint256.
2. Ledger stores `(IR, bound_vars)` in the Raft command instead of source text.
3. FSM decodes IR and executes via an interpreter (Go).
4. Balance interface changed to `(input, output)`.

**Benefit:** No re-parsing on replicas, no re-validation, zero-alloc numeric boundary.

### Phase 2: WASM parser

1. Compile the Numscript parser/compiler to WASM.
2. Replace the Go parser call in the admission layer with a WASM parser call.
3. Same IR format, same interpreter — but the parser is now reusable across runtimes.

**Benefit:** Single source of truth; enables browser playground, SDK tooling.

### Phase 3: WASM executable + data-in-memory

1. The compiler produces a WASM module as output.
2. The FSM executes the WASM module directly via a WASM runtime.
3. All state pre-injected into WASM linear memory; zero host calls.

**Benefit:** Maximum hot-path performance — WASM JIT/AOT, no interpreter, no host round-trips.

---

## 17. Open Questions

| # | Question | Notes |
|---|----------|-------|
| 1 | Should the Raft log store the original source text for auditability? | Could use a content-addressed store (keyed by blake3 hash) outside the Raft log |
| 2 | What WASM runtime for Go? | wazero (pure Go, no CGo) vs wasmtime-go (faster, requires CGo) |
| 3 | How to handle compiler version upgrades across a rolling cluster? | Version tag in artifact + minimum cluster version negotiation |
| 4 | Should the preloaded state be part of the Raft command or loaded independently on each replica? | Trade-off: larger Raft entries vs requiring each replica to load state |
| 5 | What language for the Numscript compiler? | Rust (best WASM target), Go (team familiarity), or other |

---

## 18. Relation to Other RFCs

- **[Static Inputs RFC](../../technical/contributing/numscript.md#volume-preloading-dependency-resolution)** — Defines the `Requirements` contract. This RFC extends it: Requirements are computed at admission and used to preload state that is injected into WASM memory.
- **Request Signing** — The `signed_payload` envelope pattern is orthogonal. The compiled artifact replaces the script source in the Raft command, but signing still covers the original request.

---

## 19. Summary

| Principle | Rule |
|-----------|------|
| Parse once | Parsing happens at admission, never on the FSM hot path |
| Validate once | Variables are type-checked at admission; the FSM trusts pre-validated values |
| Compile once | The compiled artifact is stored in the Raft log; replicas decode and execute |
| Zero-alloc numerics | All amounts are uint256 — no `*big.Int` at any boundary |
| Volume interface | Balances passed as `(input, output)` — no signed arithmetic |
| Zero host calls | All state pre-injected into WASM memory; execution has no callbacks |
| WASM parser | The parser is a WASM module, reusable across runtimes |
| WASM executor (ideal) | The compiled Numscript program is itself a WASM module |
| Deterministic | WASM execution + preloaded state = fully deterministic on all replicas |
