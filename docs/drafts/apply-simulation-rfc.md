# RFC: Unified Hot-Path Apply Simulation

- **Status:** Draft (investigation spike — EN-1545)
- **Target:** Ledger v3
- **Audience:** Ledger FSM / admission implementers
- **Scope:** Collapse admission's hand-derived shadow of the FSM apply path into a single order interpreter, `simulate(order, StateReader, WriteSink)`, run in two modes (admission dry-run, FSM apply) over an abstract state interface.
- **Depends on:** **EN-1406 / #1560 must land first** (`feat/en-1406-numscript-resolve-deps`). This RFC targets the post-EN-1406 shape: the `numscript.ValueSource` abstraction, the `SafeResolveDependencies` / `SafeRun` panic-recovery wrappers, the apply-time triage block in `processor_transaction_numscript.go`, and the `batchEffects` intra-batch overlay in admission. None of those exist on `release/v3.0` yet.

> **Note on the Go API:** the interfaces and types below are **illustrative** and **MAY be refined** during implementation. They exist to make the contract concrete and to pin the correctness argument.

---

## 0. Recommendation (read this first)

**Go — but phased and narrowed.** The unified model is viable and worth doing, with two important qualifications that the investigation surfaced:

1. **The abstraction the ticket calls `StateReader` already exists** as `numscript.ValueSource` (`internal/domain/processing/numscript/store.go`), with exactly the two backends the RFC posits: `admissionValueSource` (Pebble snapshot + `batchEffects` overlay) and `scopeValueSource` (coverage-gated cache). What does **not** exist is (a) a *single pass* that produces both the preload set/InputsHash **and** the intra-batch effects — today that is two evaluations, `SafeResolveDependencies` + `SafeRun` — and (b) a generalization of the read/write abstraction to the **non-numscript** order types, whose effects are hand-folded in `admission.go`.

2. **The biggest win is P1 (collapse the double numscript evaluation), and it is nearly free of correctness risk.** The biggest *risk* is P3 (a single interpreter body shared verbatim between admission and FSM for **all** order types). P3 is where an invariant-#3/#6/#7/#9 mistake would be catastrophic (node desync), and it is the phase most tempting to over-engineer. The recommendation is to **commit to P1 and P2, and gate P3 behind an explicit design review** once P1/P2 have proven the `StateReader`/`WriteSink` seam holds.

The three top risks and the phased plan are in §8 and §7.

---

## 1. Motivation

Admission re-derives, by hand, a shadow copy of what the FSM apply path does. The shadow is split per order type and spread across files. Two concrete problems follow.

### 1.1 Drift risk — every hand-fold shadows an FSM `process*`

Each admission hand-derivation mirrors an FSM `process*` function. When apply semantics change, the shadow silently diverges, and the failure modes are severe:

- **wrong preload** → a cache miss on the FSM hot path. Per invariant #6 a cache miss turns an apply read into a silent no-op that desyncs nodes (invariant #7).
- **wrong intra-batch effects** → a later same-batch order resolves against the wrong predecessor state.
- **poisoned `InputsHash`** → a spurious `ErrStaleInputsResolution`, which spins the client in a re-admit loop against state that never diverged.

The current shadows (all on `feat/en-1406-numscript-resolve-deps`):

| Order type | Admission hand-fold | FSM function it shadows |
|---|---|---|
| CreateTransaction + script | `numscript.DiscoverNumscriptDependencies` (`SafeRun` effects pass) + `effects.mergeDiscovery` (`admission.go`) | `processCreateTransaction` → `processPosting` |
| CreateTransaction, postings-only | inline balance fold in `resolveScriptsAndEnrichNeeds` (`admission.go:~1493`) | `processCreateTransaction` → `processPosting` |
| RevertTransaction | inline reversed-delta fold (`admission.go:~1421`) | `processRevertTransaction` |
| AddMetadata (account target) | `effects.setMetadata` (`admission.go:~1452`) | `processAddMetadata` |
| DeleteMetadata (account target) | `effects.deleteMetadata` tombstone (`admission.go:~1465`) | `processDeleteMetadata` |
| Caller account metadata precedence | `foldCallerAccountMetadata`, folded **after** script writes (`numscript_source.go:18`) | `processCreateTransaction` metadata merge (order > script) |

### 1.2 Redundant work — a script is evaluated twice

`DiscoverNumscriptDependencies` runs **two** full evaluations of the same script against the same source:

- `SafeResolveDependencies` → dependency set (accounts/assets/metadata read+written) **and** `InputsHash` (via `RecordingStore`), for the stale check.
- `SafeRun` → intra-batch effects (`NetBalanceDeltas`, `MetadataWrites`), consumed only by a later same-batch order.

For a single-order batch, the effects pass produces something nobody reads: the batch has no successor order to resolve against. So every single-order script pays 2× for nothing.

---

## 2. The Proposed Model

A single interpreter per order type:

```
simulate(order, StateReader, WriteSink) -> outcome
```

called in two modes over an abstract state interface. Business logic lives once; the two modes differ **only** in the state backend and whether writes commit.

| | Admission (dry-run) | FSM (apply) |
|---|---|---|
| `StateReader` | Pebble snapshot + `batchEffects` overlay (`admissionValueSource`) | coverage-gated cache `Scope` (`scopeValueSource`) |
| `WriteSink` | `batchEffects` overlay (intra-batch, discarded after) | `WriteSet` (committed via Merge) |
| errors | best-effort (no reject on balance failure — see §4.2) | authoritative |
| commit | never | yes |
| by-products | reads → preload set + `InputsHash`; writes → intra-batch effects | none (writes are the real state change) |

The single pass makes the effects a **by-product of the read/resolve pass**, so `SafeRun` (§1.2) disappears: whatever the interpreter writes to the `WriteSink` *is* the effect set, at admission and at apply alike.

---

## 3. The `StateReader` and `WriteSink` interfaces

### 3.1 `StateReader` — the sole read horizon

The read half already exists as `numscript.ValueSource`:

```go
// internal/domain/processing/numscript/store.go (today, EN-1406)
type ValueSource interface {
    Balance(account, asset string) (*big.Int, error)
    Metadata(account, key string) (value string, present bool, err error)
}
```

To serve **all** order types (not just numscript), it must also expose the reads the non-numscript `process*` paths do — transaction state and reference existence, reverted-bitset membership:

```go
// Illustrative — the generalized read horizon for the unified interpreter.
type StateReader interface {
    // Numscript-shaped reads (already ValueSource today).
    Balance(account, asset string) (*big.Int, error)
    Metadata(account, key string) (value string, present bool, err error)

    // Reads the non-numscript process* paths need.
    TransactionState(txID uint64) (*TransactionState, bool, error) // revert source, metadata target
    ReferenceExists(ref string) (bool, error)                       // create dedup
    IsReverted(txID uint64) (bool, error)                           // revert guard
}
```

**Invariant #3 holds iff the shared interpreter reads only through this interface.** The FSM backend implements every method against the coverage-gated `Scope`; it never touches Pebble. This is the load-bearing property: because the *only* way the shared code can read is `StateReader`, and the FSM's `StateReader` is cache-only, no shared-code change can introduce a Pebble read on the hot path. The compiler enforces it (the FSM backend has no `*dal.Store`).

**Invariant #9 holds because the FSM backend's reads are the coverage-gated `Scope`.** Each `StateReader` method on the FSM side goes through `Scope.GetX(...)`, so the per-order `coverage_bits` admit it. A read the interpreter makes that admission never declared hits `*state.ErrCoverageMiss` — see §4.

### 3.2 `WriteSink` — the write half

Today the two write halves are `batchEffects` (admission) and `WriteSet` (FSM). Generalize:

The write half's **balance primitive is the posting**, because that is what the FSM apply path needs. `applyPosting` requires the full posting — source, destination, asset, amount, and side — to increment the source's `Output` and the destination's `Input` **separately** (materialized volumes) and to run the source balance check. A net-per-volume-key delta cannot carry that: it loses which side moved and the source/destination pairing, so it cannot reconstruct the materialized-volume semantics §3.2 and §4 require. The sink's balance operation is therefore the posting; a net delta is a *projection* admission derives from it, not a sink primitive.

```go
// Illustrative — the write half of the interpreter.
type WriteSink interface {
    // Balance mutation is posting-shaped: the FSM needs source/destination,
    // asset, amount and side to update materialized volumes (source Output++,
    // destination Input++) and run the source balance check. Admission's
    // implementation projects this to a net (input−output) delta per volume key
    // for its intra-batch overlay; the FSM's implementation materializes the two
    // volume sides. force is threaded so each backend applies the balance-check
    // policy identically (§8, force question).
    ApplyPosting(p Posting, force bool) error

    SetMetadata(key domain.MetadataKey, value string)
    DeleteMetadata(key domain.MetadataKey)
    PutTransactionState(txID uint64, state *TransactionState)
    MarkReverted(txID uint64)
    PutReference(ref string, txID uint64)
}

// Posting is the balance-mutation unit the interpreter emits (already the shape
// of a numscript-produced posting and of RevertTransaction's reversed postings).
type Posting struct {
    Source, Destination, Asset string
    Amount                     *big.Int
}
```

- Admission's `WriteSink` = the `batchEffects` overlay. `ApplyPosting` **projects** the posting to a net (input−output) delta per volume key (source `−amount`, destination `+amount`) so a later same-batch `balance()` sees the right number — exactly what `batchEffects.addBalanceDelta` does today, but now driven by the posting rather than being the interpreter's primitive. `SetMetadata` / `DeleteMetadata` accumulate as today. All writes are discarded after the batch. Admission's `ApplyPosting` swallows the balance-check failure (best-effort, §4.2); it still records the delta so downstream orders resolve correctly.
- FSM's `WriteSink` = the `WriteSet`. `ApplyPosting` materializes both volume sides via the existing `applyPosting` path (source `Output += amount`, destination `Input += amount`) and runs the authoritative source balance check unless `force`. Other writes commit via Merge.

**The asymmetry the posting-shaped sink preserves:** admission needs only *net balance deltas* (input−output) for a later `balance()`; the FSM needs *materialized volumes* (Input and Output separately). Because the interpreter emits **postings** and each backend's `ApplyPosting` decides how to record them, the interpreter stays representation-agnostic, admission keeps its cheap net-delta overlay, and the FSM keeps its exact volume math and balance check — with no information lost at the seam.

---

## 4. The coverage gate as the divergence detector

This is the heart of the correctness argument and the delicate part of the recommendation.

### 4.1 Preload = the simulated path, not all branches

`ResolveDependencies` today explores **all** oneof branches of a script to build the dependency set. The unified model narrows this: it preloads only the branch the single simulation pass actually takes.

This is **narrower**, and it is correct **iff the `InputsHash` pins branch selection**. The argument, confirmed by the EN-1406 independent review:

1. `Run`'s reads are a **subset** of `ResolveDependencies`' reported reads — a single execution touches one branch; branch selection is a pure function of the resolved inputs.
2. Admission binds those inputs into `InputsHash` via the `RecordingStore` (including metadata *absence*, via the NUL-prefixed absent sentinel — a key that gains a value between admit and apply hashes differently).
3. At apply, the FSM re-resolves against the coverage-gated `Scope` and compares hashes **before** taking the divergent path (§4.3). If any input shifted, the branch could differ, and the hash mismatch rejects as stale *before* the interpreter can read a key the narrower preload never loaded.

So single-path preload is safe **because** the InputsHash makes any branch-changing input shift a stale rejection, not a silent divergent read.

### 4.2 A divergent apply-time read surfaces LOUD, never silent

The just-merged coverage-miss fix (EN-1406, `processor_transaction_numscript.go`) is what makes single-path preload *safe to get wrong*: if the narrowing is ever incorrect — the apply path reaches a key admission didn't declare — the read does **not** silently no-op (which would desync nodes, invariant #6/#7). It hits the coverage gate and surfaces as `*state.ErrCoverageMiss`, triaged loud:

```go
// processor_transaction_numscript.go — apply-time triage (EN-1406)
if _, resolveErr := numscript.SafeResolveDependencies(parsed, ctx, vars, recording); resolveErr != nil {
    if numscript.IsPanic(resolveErr) {
        return nil, resolveErr                 // (1) recovered panic — loud (#7)
    }
    if isCoverageContractViolation(resolveErr) {
        return nil, resolveErr                 // (2) ErrCoverageMiss / ErrInvalidExecutionPlan — loud (#7)
    }
    return nil, domain.ErrStaleInputsResolution // (3) genuine input-shift — retryable stale
}
if !bytes.Equal(expected, recording.Hash()) {
    return nil, domain.ErrStaleInputsResolution // hash mismatch — retryable stale
}
```

**The unified model must preserve this exact four-way triage** — panic (loud) / coverage-contract violation (loud) / resolve-error input-shift (stale) / hash-mismatch (stale). In the unified model the coverage gate becomes the *divergence detector*: an admission-shadow bug (wrong preload) can no longer cause a silent wrong result — it can only cause a loud `ErrCoverageMiss` reject. That is precisely the property that makes the whole refactor safe: **the FSM's legitimate read horizon is admission's declared key set, and any interpreter change that widens it fails loudly rather than desyncing.**

`isCoverageContractViolation` matches on `domain.Reason` (`ErrReasonCoverageMiss`, `ErrReasonInvalidExecutionPlan`) rather than importing `internal/infra/state` (import cycle). The unified interpreter, living in `internal/domain/processing`, keeps that constraint.

### 4.3 Hash-check MUST short-circuit before any divergent read

The apply path checks `expected == recording.Hash()` **before** committing to the resolved branch's writes. In the unified model, the ordering contract is: **re-resolve (recording reads) → compare hash → only then execute writes / take branch-specific reads.** If the hash mismatches, reject stale *before* the interpreter reaches a key the narrower preload might not cover. The RFC's implementation must not let any write or branch-divergent read happen ahead of the hash gate.

### 4.4 Determinism (#2)

`simulate` must be pure/deterministic at fixed inputs: same order + same `StateReader`-visible state → identical outcome and identical write set on every node. The numscript library is already deterministic under fixed inputs; the non-numscript folds (balance deltas, metadata merges, revert reversal) are pure arithmetic and map operations. The one hazard is map-iteration order leaking into the write set — this must be avoided (cf. EN-1521, FSM map-determinism). No `time.Now()`, no node-local state inside `simulate` (revert's `at_effective_date` fallback to the FSM date is passed **in** as a command/context value, not read from the clock inside the interpreter).

---

## 5. Mapping every current hand-fold to the unified model

| Current construct | File (EN-1406) | Where it lives in the unified model | Fate |
|---|---|---|---|
| `DiscoverNumscriptDependencies` `SafeResolveDependencies` pass | `numscript/discover.go` | the single `simulate` pass (numscript order), reads recorded → preload + InputsHash | **kept** (becomes the one pass) |
| `DiscoverNumscriptDependencies` `SafeRun` effects pass | `numscript/discover.go` | **removed** — effects are the `WriteSink` writes of the single pass | **DELETED** |
| `NetBalanceDeltas` / `MetadataWrites` on `DiscoveryResult` | `numscript/discover.go` | become `WriteSink.ApplyPosting` (net delta is a projection) / `SetMetadata` calls | **DELETED** (as separate result fields) |
| postings-only inline balance fold | `admission.go:~1493` | `simulate` for CreateTransaction (postings branch) → `WriteSink.ApplyPosting` | **DELETED** (moves into interpreter) |
| reversed-delta fold | `admission.go:~1421` | `simulate` for RevertTransaction → `WriteSink` (reversed postings) | **DELETED** (moves into interpreter) |
| `effects.setMetadata` (AddMetadata) | `admission.go:~1452` | `simulate` for AddMetadata → `WriteSink.SetMetadata` | **DELETED** (moves into interpreter) |
| `effects.deleteMetadata` tombstone (DeleteMetadata) | `admission.go:~1465` | `simulate` for DeleteMetadata → `WriteSink.DeleteMetadata` | **DELETED** (moves into interpreter) |
| `foldCallerAccountMetadata` (caller > script precedence) | `numscript_source.go:18` | `simulate` for CreateTransaction, caller metadata merged after script writes into `WriteSink` | **DELETED** (moves into interpreter, precedence encoded once) |
| `batchEffects` overlay | `numscript_source.go` | admission's `WriteSink` **and** the overlay half of admission's `StateReader` | **kept** (renamed to the `WriteSink` impl) |
| `admissionValueSource` | `numscript_source.go` | admission's `StateReader` | **kept** |
| `scopeValueSource` | `processor_transaction_numscript.go` | FSM's `StateReader` | **kept** |
| `WriteSet` (FSM) | `internal/infra/state` | FSM's `WriteSink` | **kept** |
| apply-time four-way triage | `processor_transaction_numscript.go` | wraps the FSM-mode `simulate` call | **kept** (generalized over order types) |

**Net deletions:** the second numscript evaluation (`SafeRun` in discovery), the `NetBalanceDeltas`/`MetadataWrites` computation in `discover.go`, and all five per-order-type effect folds in `admission.go` + `foldCallerAccountMetadata`. The per-order **preload declaration** (`extractLedgerScopedNeeds`) stays as-is — it is admission's contract (invariant #6) and is *derived from* the simulate reads, not hand-maintained separately once P1/P3 land.

---

## 6. Repatriating the precedence / reversal / tombstone semantics

These three semantics are hand-folded in `admission.go` today and must move **into** `simulate` so there is exactly one definition:

- **Caller-metadata precedence over `set_account_meta`.** FSM `processCreateTransaction` merges order metadata with precedence over script metadata (order keys win on collision). Admission mirrors this by folding caller metadata *after* script writes. In `simulate`, the CreateTransaction interpreter writes script metadata to the `WriteSink`, then overwrites with caller metadata — one code path, both modes.
- **Revert reversed-posting deltas.** FSM `processRevertTransaction` applies each original posting reversed (source↔dest swap). Admission mirrors the net delta (source +amount, dest −amount). In `simulate`, the RevertTransaction interpreter reads the original `TransactionState` via `StateReader.TransactionState`, emits reversed postings to the `WriteSink`; the FSM backend materializes volumes, the admission backend folds net deltas.
- **Delete tombstones.** FSM `processDeleteMetadata` deletes the key; admission records a tombstone so a later same-batch `meta()` resolves absent. In `simulate`, one `WriteSink.DeleteMetadata` call; the admission backend records the tombstone, the FSM backend deletes from the `WriteSet`.

---

## 7. Phased migration plan

The phases are ordered by value-to-risk ratio. **Each phase is independently shippable and independently revertible.**

### Phase 0 — prerequisite

EN-1406 / #1560 lands on `release/v3.0`. Nothing in this RFC is actionable before that.

### Phase 1 — unify numscript discovery into one pass (high value, low risk)

Collapse `DiscoverNumscriptDependencies`' two evaluations into one. Introduce a numscript-scoped `WriteSink` and have the *single* `SafeResolveDependencies`/execution pass emit effects to it as a by-product. Remove `SafeRun` from `discover.go`. `admissionValueSource` and `scopeValueSource` become the two `StateReader` backends explicitly (they already are, structurally).

- **What's deleted:** the second numscript evaluation; `NetBalanceDeltas`/`MetadataWrites` as separately-computed fields.
- **Risk:** low. The read/hash pass is unchanged; only the effects derivation moves from a second pass to the first. Guarded by the existing exhaustive numscript e2e coverage (task #50) and the apply-time triage.
- **Correctness check:** `Run`'s reads ⊆ `ResolveDependencies`' reads (already established), so folding effects into the resolve pass cannot *lose* a preload key.

### Phase 2 — absorb revert + metadata folds into `simulate` (medium value, medium risk)

Move the RevertTransaction, AddMetadata, DeleteMetadata, and caller-metadata-precedence folds out of `admission.go` into per-order `simulate` bodies that both admission and FSM call. These are pure arithmetic/map folds — no numscript, no branching over resolved inputs, so no InputsHash subtlety.

- **What's deleted:** the four inline folds in `resolveScriptsAndEnrichNeeds` + `foldCallerAccountMetadata`.
- **Risk:** medium. The delicate part is ensuring the FSM `process*` and the admission fold, which are currently *separately* correct, are byte-for-byte reproduced by one shared body — especially revert's volume materialization vs net-delta representation (§3.2). Requires a differential test: for a corpus of orders, assert admission's derived effects equal the FSM's committed writes.

### Phase 3 — single interpreter body for all order types, remove hand-derivations from `admission.go` (high value, highest risk)

Fully unify: `admission.go` no longer contains any per-order effect logic; it constructs the admission-mode `StateReader`/`WriteSink`, calls `simulate`, and reads back the preload set + InputsHash + effects. The FSM apply path constructs the FSM-mode backends and calls the same `simulate`.

- **What's deleted:** `resolveScriptsAndEnrichNeeds`' per-order dispatch (reduced to a loop calling `simulate`); the shadow disappears entirely.
- **Risk:** highest — this is where an invariant-#3/#9 mistake in the shared body would desync nodes. **Gate behind explicit design review.** Do not start P3 until P1+P2 have proven the seam in production.

---

## 8. Risks and open questions

### Top 3 risks

1. **A coverage-gated miss must be a loud reject on the simulated path without weakening #6/#7 elsewhere.** The whole safety argument rests on the FSM `StateReader` routing every read through `Scope.GetX` so a divergent read hits `ErrCoverageMiss`. If any `StateReader` method is implemented against a raw `Registry.X.KeyStore()` iterator (invariant #9 violation) — e.g. to make a "convenient" existence check — the gate is bypassed and a wrong preload silently no-ops again. **Mitigation:** the FSM `StateReader` must hold only a `Scope`, never a parent cache handle; enforce by construction (no `*dal.Store`, no `Registry` on the struct) and by review.

2. **Apply must reject on hash mismatch before any divergent read (§4.3).** If the unified interpreter is restructured such that a branch-divergent read or a write happens before the hash gate, a stale order could touch an un-preloaded key (loud `ErrCoverageMiss` in the best case, but the *intent* is a clean stale reject). **Mitigation:** the FSM-mode `simulate` must run in the fixed order re-resolve → hash-compare → execute; encode this as a single wrapper the interpreter cannot reorder.

3. **P2/P3 differential correctness — the shared body must reproduce both the FSM `process*` and the admission fold byte-for-byte.** The two are *separately* correct today; unifying them risks a representation mismatch (net delta vs materialized volume; metadata merge order; revert reversal). **Mitigation:** a differential/property test asserting admission-derived effects equal FSM-committed writes over a broad order corpus, run before P2 and P3 land; treat any divergence as a release blocker.

### Open questions

- **Does every order type genuinely fit one interpreter?** CreateTransaction (postings + script), RevertTransaction, Add/DeleteMetadata all fit the `StateReader`/`WriteSink` shape cleanly (verified against the EN-1406 `process*` functions). System-scoped orders (create/delete ledger, indexes, account types, cluster config) do **not** read/write the balance-metadata-txstate horizon and largely have empty or nil preload needs — they should be **out of scope** for `simulate`; forcing them through it would add a fake `StateReader` for no benefit. The RFC recommends `simulate` cover **ledger-scoped mutating orders only**; system-scoped orders keep their direct `process*` (they have no admission shadow to unify).
- **What about the `force` flag?** `force` makes numscript see `MaxForceBalance` and skips FSM balance enforcement. It is already threaded through `numscript.NewStore(source, force)` and the FSM apply. In the unified model `force` is a `simulate` parameter, not a `StateReader` concern — the `StateReader` still returns real balances; the interpreter decides whether to enforce. This must be explicit so admission (best-effort, §4.2) and FSM (authoritative) apply `force` identically.
- **Best-effort vs authoritative error handling (§4.2).** Admission must *not* reject an order because it can't execute against admission-time state (a later concurrent order may move balances); the FSM is authoritative. In the unified model this is a mode flag on `simulate` (`commit bool` / `authoritative bool`): admission swallows balance/runtime failures and contributes no effects for that order; FSM returns the definitive error. The interpreter body is shared; only the error disposition differs. This must be carefully specified so a *panic* or *coverage violation* is **never** swallowed in either mode (those stay loud, §4.2).

---

## 9. Summary

The unified `simulate(order, StateReader, WriteSink)` model is viable for ledger-scoped mutating orders. The read abstraction already exists (`ValueSource`); the work is (1) collapsing the numscript double-evaluation, (2) moving the revert/metadata/precedence folds into a shared interpreter, and (3) removing admission's per-order shadow. The coverage gate (invariant #9) plus the just-merged loud-triage of a coverage miss turns any narrowing mistake into a loud reject rather than a silent node desync, which is what makes single-path preload safe. **Recommendation: proceed with P1 immediately (after EN-1406 lands), P2 next, and gate P3 behind a design review.** System-scoped orders stay out of scope.
