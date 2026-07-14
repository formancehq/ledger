# RFC: Safe reuse between admission planning and FSM apply

- **Status:** Draft — **BLOCKED** investigation spike (EN-1545). This is a v3.1 architecture RFC and a **proof/investigation ticket only**: it must produce evidence and a go/no-go recommendation, and must not change production behavior.
- **Target:** Ledger v3.1 (explicitly **not** v3.0; not on the v3.0 critical path).
- **Audience:** Ledger FSM / admission implementers and reviewers.
- **Question under investigation:** Can admission planning and FSM apply safely reuse more typed logic without weakening the hot-path invariants (#2 determinism, #3 no-Pebble-in-FSM, #6 preload/coverage, #7 loud-on-impossible, #9 coverage gate)? If so, which bounded shape — and at what cost?

> **This RFC does not recommend the universal simulator.** An earlier draft proposed a single universal `simulate(order, StateReader, WriteSink)` interpreter shared verbatim between admission and FSM apply, and recommended "Go". The deep EN-1545 review (Jira, 2026-07-13) disproved the assumptions that recommendation rested on. This revision demotes the universal simulator to **one option among four (Option D), presented as the control / no-go alternative**, and frames the document as a *neutral comparison* rather than a decision. The authoritative decision on record is: **do not implement the universal model; investigate bounded alternatives for v3.1.**

---

## 0. Blocked-on dependencies (read this first)

This RFC **cannot be finalized or executed** until the following v3.0 boundaries settle, because each one changes a code path the comparison depends on. The RFC must be re-based and every code claim re-validated against the resulting baseline before a recommendation is made — existing line numbers are **not** authoritative.

| Dependency | Why it blocks this RFC |
|---|---|
| **EN-1406 / PR #1560** (`ResolveDependencies` API + stale-inputs detection) | Introduces the `numscript.ValueSource` abstraction, `SafeResolveDependencies`/`SafeRun` panic wrappers, the apply-time triage block, the `InputsResolutionHash`, and the intra-batch effects overlay. **None of these exist on `release/v3.0` today** (see §1). The entire "digest / stale-input / preflight" analysis is meaningless until this lands, and must be re-validated against what actually merges — not against the PR-branch shape. |
| **EN-1532** (preserve CreateTransaction order metadata when merging Numscript output; accepted-order immutability) | Establishes the accepted-order-immutability invariant that any shared-planning or transcript design must preserve, and fixes the Numscript transaction-metadata aliasing bug. The RFC must treat accepted-order immutability as an **input**, not redefine it. |
| **EN-1288** (Numscript library: immutable semver history, selector resolution incl. `latest`) | Owns preserving the submitted Numscript selector and moving resolution out of the audited business payload. The transcript/sidecar option (C) must preserve EN-1288's audited-selector invariant. |

Until these merge (or their final boundaries are otherwise settled), EN-1545 remains **Blocked** and no option below should be selected.

> **Note on the current baseline.** As of this writing, `release/v3.0` uses `numscript.DiscoverNumscriptDependencies` (`internal/domain/processing/numscript/emulate.go`) — a single `SafeRun` with infinite balances used **only to discover the preload set** (source/destination volumes, written metadata) — and re-runs `SafeRun` against the coverage-gated `Scope` at apply (`processor_transaction_numscript.go`). There is **no** `ResolveDependencies`, **no** `InputsResolutionHash`, **no** stale-input triage, and **no** balance/metadata effects overlay on the baseline; the `bulkOverlay` (`admission/overlay.go`) covers only numscript-library saves and sinks. The post-EN-1406 machinery this RFC reasons about is therefore *prospective*. Every "today" statement below is conditional on EN-1406 landing as designed and must be re-checked at that point.

---

## 1. Motivation

Admission re-derives, by hand, a shadow of what the FSM apply path does. Each hand-derivation mirrors an FSM `process*` function, and when apply semantics change the shadow can silently diverge. The failure modes are severe:

- **wrong preload** → a cache miss on the FSM hot path. Per invariant #6 a cache miss turns an apply read into a silent no-op that desyncs nodes (invariant #7).
- **wrong intra-batch effects** (post-EN-1406) → a later same-batch order resolves against the wrong predecessor state.
- **poisoned stale check** (post-EN-1406) → a spurious `ErrStaleInputsResolution`, spinning the client in a re-admit loop against state that never diverged.

The intent of this RFC is to evaluate whether DRY reuse can *reduce* this drift surface without *introducing* a worse failure mode (node desync). It is not assumed that any reuse is worth it: **"no-go, keep the current model plus targeted optimizations" is a valid and possibly preferred outcome.**

### 1.1 The shadows (prospective, post-EN-1406)

Once EN-1406 lands, admission is expected to carry per-order hand-folds that mirror the FSM. This table is a **hypothesis to validate against the merged baseline**, not a description of current `release/v3.0` code:

| Order type | Admission hand-fold (expected post-EN-1406) | FSM function it shadows |
|---|---|---|
| CreateTransaction + script | discovery pass (effects) + intra-batch merge | `processCreateTransaction` → `processPosting` |
| CreateTransaction, postings-only | inline balance fold | `processCreateTransaction` → `processPosting` |
| RevertTransaction | inline reversed-delta fold | `processRevertTransaction` |
| AddMetadata (account target) | metadata overlay put | `processAddMetadata` |
| DeleteMetadata (account target) | metadata overlay tombstone | `processDeleteMetadata` |
| Caller account metadata precedence | caller metadata folded after script writes | `processCreateTransaction` metadata merge (order > script) |

### 1.2 Redundant work — a script may be evaluated twice (prospective)

Under EN-1406, discovery is expected to run two evaluations of the same script: `SafeResolveDependencies` (dependency set + `InputsResolutionHash`) and `SafeRun` (intra-batch effects). For a single-order batch the effects pass produces something nobody reads. This is a real, bounded optimization opportunity (Option A) independent of any universal-simulator design.

---

## 2. Non-negotiable safety properties

Before comparing designs, translate the repository invariants into falsifiable gates every option must satisfy. An option that cannot demonstrate all of these is a **no-go**.

1. **Deterministic FSM at fixed input (#2).** Same order + same visible state → identical outcome and identical write set on every node. No `time.Now()`, no node-local state, no map-iteration order leaking into the write set (cf. EN-1521).
2. **No Pebble capability in the FSM / hot path (#3).** No FSM-facing interface may expose `dal.Store`, `PebbleGetter`, parent `KeyStore` iteration, or a generic escape hatch. Enforced by construction and by a compile-time/wiring proof.
3. **Every FSM cache read is coverage-gated (#9).** All reads go through `Scope.GetX(...)`; a read admission never declared must hit the gate.
4. **Coverage misses remain loud (#6/#7).** A read the preload did not cover surfaces as a loud invariant failure (`ErrCoverageMiss` / equivalent), never a silent no-op, and is never reclassified as retryable stale.
5. **Admission planning is advisory, not authoritative.** Admission cannot become the business authority; the FSM remains responsible for balance/overflow checks, account types, references, metadata behavior, atomic failure and rollback.
6. **Atomic per-order and per-batch semantics.** A failed order contributes **no** effects (§4). A first-order failure stops and rolls back the atomic proposal exactly as today.
7. **Accepted business-order bytes are unchanged** through admission and apply (EN-1532 invariant; EN-1288 audited selector).
8. **The audit chain remains the source of truth** (invariant #8). Any *new* persisted projection requires checker derivation and comparison — so no option here may add a persisted projection without that pass.

---

## 3. The abstraction that may or may not be shared

The ticket's original framing named a `StateReader` (read horizon) and a `WriteSink` (write half). Post-EN-1406, the read half is expected to exist as `numscript.ValueSource`:

```go
// internal/domain/processing/numscript/store.go (EXPECTED post-EN-1406, does not exist today)
type ValueSource interface {
    Balance(account, asset string) (*big.Int, error)
    Metadata(account, key string) (value string, present bool, err error)
}
```

To serve non-numscript order types, a generalized read horizon would additionally expose transaction-state, reference existence and reverted-bitset reads. **Whether this generalization is worth building is exactly what Options B/C/D test.** Two properties are load-bearing for *any* shared read interface:

- **#3 holds iff the shared code reads only through the interface and the FSM backend is cache-only** (no `*dal.Store`, compiler-enforced).
- **#9 holds iff the FSM backend routes every method through `Scope.GetX(...)`** so `coverage_bits` admit it. A method implemented against a raw `Registry.X.KeyStore()` iterator silently bypasses the gate — this is the single most dangerous mistake any shared-interface option can make (Option D's chief risk).

### 3.1 The write / sink boundary — atomic per order, typed metadata

Any shared write half (Options B, C, D) must satisfy two boundary constraints the original draft got wrong.

**(a) Atomic per-order boundary — a failed order contributes NO effects.** With a *streaming* sink, a CreateTransaction can emit several postings and metadata writes before a later posting fails a balance/overflow check, an account-type validation, or a metadata-key validation (`processCreateTransaction` validates Numscript-produced metadata keys *after* postings are applied — see `processor_transaction_numscript.go`). If those partial writes have already landed in the admission overlay, a later same-batch order resolves against a state the FSM will never produce, and admission's advisory plan diverges from the authoritative outcome. This contradicts safety property #6.

The sink boundary must therefore be **atomic per order**, not streaming-with-partial-retention. Two acceptable shapes:

- **Buffer-and-commit-per-order:** the order's effects accumulate in a per-order staging buffer; they are merged into the batch overlay (admission) / write set (FSM) **only after** the whole order succeeds. On any failure the staging buffer is discarded.
- **Rollback-on-failure:** the sink supports a savepoint taken before the order and rolled back on failure.

The buffer-and-commit shape is preferred: it matches the FSM's existing `overlay_scope.go` per-order overlay (`orderOverlayScope`), which already stages a single order's writes and rolls back on failure without touching the parent scope. The admission side must adopt the same discipline — a per-order buffer flushed only on success — so the two modes are **structurally identical at the failure boundary**, not merely "best-effort" on one side. Note this is a *stricter* contract than "admission swallows the failure and keeps its delta": admission may still *decline to reject* an order it cannot execute at admission time (a concurrent order may move balances — property #5), but declining to reject is not the same as retaining partial effects. The correct behavior is **decline to reject, and contribute nothing**.

**(b) Typed metadata at the boundary.** A `string` metadata value cannot reproduce current FSM writes. `processAddMetadata` and caller account metadata store `*commonpb.MetadataValue` **verbatim** (`s.AccountMetadata().Put(metaKey, value)` where `value` is a `*commonpb.MetadataValue` from `order.GetMetadata() map[string]*commonpb.MetadataValue`). `MetadataValue` is a oneof with six variants — string, int, uint, datetime, bool, null (`internal/proto/commonpb/common.pb.go`). The `AccountMetadata()` accessor is typed `Accessor[domain.MetadataKey, *commonpb.MetadataValue, commonpb.MetadataValueReader]`. Flattening to `string` at the sink would drop the variant, change the stored bytes, and (via the read-side index hint) change how the value is indexed. The sink metadata operations must therefore carry the typed value:

```go
// Illustrative — a shared write sink, IF one is built (Options B/C/D).
type WriteSink interface {
    // Balance mutation is posting-shaped: the FSM needs source/destination,
    // asset, amount and side to materialize volumes (source Output++,
    // destination Input++) and run the source balance check. Admission
    // projects this to a net (input−output) delta per volume key.
    ApplyPosting(p Posting, force bool) error

    // Metadata carries the typed *commonpb.MetadataValue verbatim — NOT a
    // flattened string — so the sink reproduces processAddMetadata exactly.
    SetMetadata(key domain.MetadataKey, value *commonpb.MetadataValue)
    DeleteMetadata(key domain.MetadataKey)

    PutTransactionState(txID uint64, state *TransactionState)
    MarkReverted(txID uint64)
    PutReference(ref string, txID uint64)
}

type Posting struct {
    Source, Destination, Asset string
    Amount                     *big.Int
}
```

The posting-shaped balance primitive is deliberate: `applyPosting` needs source, destination, asset, amount and side to increment materialized volumes separately and run the source balance check. A net-per-volume-key delta loses which side moved; admission derives that net delta as a *projection*, it is not the sink primitive.

---

## 4. The coverage gate as the (only) divergence detector

For any option that lets the FSM apply path re-run resolution, the coverage gate is what converts a wrong preload into a **loud reject** instead of a silent desync. Post-EN-1406, apply-time resolution is expected to triage four ways:

```go
// EXPECTED shape post-EN-1406 — validate against the merged code.
if _, resolveErr := numscript.SafeResolveDependencies(parsed, ctx, vars, recording); resolveErr != nil {
    if numscript.IsPanic(resolveErr) {
        return nil, resolveErr                 // (1) recovered panic — LOUD (#7)
    }
    if isCoverageContractViolation(resolveErr) {
        return nil, resolveErr                 // (2) coverage miss / invalid plan — LOUD (#7)
    }
    return nil, domain.ErrStaleInputsResolution // (3) genuine input-shift — retryable stale
}
if !bytes.Equal(expected, recording.Hash()) {
    return nil, domain.ErrStaleInputsResolution // (4) hash mismatch — retryable stale
}
```

The property every option must preserve: **a coverage miss (2) is never reclassified as stale (3)/(4).** Conflating them hides violations of #6/#7/#9. One of the RFC's required experiments is to *remove a declared key and prove the result is a loud coverage/internal error, never stale.*

### 4.1 The digest is NOT a preflight manifest

The original draft claimed the FSM can check the `InputsResolutionHash` **before** any divergent read, so a stale order is rejected before it can touch an un-preloaded key. **This claim is unsound and must not be relied on.** The reasons, from the EN-1406 review:

1. **The digest is a hash, not a key manifest.** `InputsResolutionHash` carries the resolution inputs only through a **digest** — it does *not* carry the identities/presence of the keys that produced it. You cannot enumerate "the keys I must preflight" from the digest alone.
2. **Rebuilding the digest requires re-running resolution.** To compare hashes, the FSM must re-run `ResolveDependencies` against live state to recompute the digest. That rerun is what issues the reads — including, potentially, a *newly-derived, undeclared* key **before** the hash comparison can happen. So "compare hash before any read" is not achievable with a digest: the reads are how the hash is rebuilt.
3. **The dependency set and the hash-input set are intentionally different.** Upstream `ResolveDependencies` returns a *conservative* read/write set, while a `RecordingStore` hashes only the values actually obtained through store calls. For example, a bounded source's volume may be *declared* a read without its balance being *queried* during resolution. So the digest does not even bind the full declared dependency horizon.
4. **`Run` is not a conservative-analysis API.** A single successful `Run` exposes the actual effects of one branch, but not a proven conservative read/write horizon; a failed `Run` exposes no usable partial plan. Replacing conservative resolution with actual-path execution would *under-specify* preload coverage.

**Correct framing:** the digest is a *tamper/shift detector after re-resolution*, not a *before-read gate*. The only mechanism that catches an undeclared read is the **coverage gate**, and it does so *at the moment of the read* (loud), not preflight. Preflight-before-any-read is only achievable with an explicit **typed key manifest** — canonical identities + presence/absence + value encoding of every resolution input — which is precisely what **Option C (transcript/sidecar)** proposes to build and prove. Absent such a manifest, the ordering guarantee the original draft asserted does not hold, and the safety argument must rest on the coverage gate catching the undeclared read loudly, not on a preflight rejection.

### 4.2 Determinism (#2)

Any shared body must be pure/deterministic at fixed inputs. The Numscript library is deterministic under fixed inputs; non-numscript folds (balance deltas, metadata merges, revert reversal) are pure arithmetic/map operations. The one hazard is map-iteration order leaking into the write set — must be avoided (cf. EN-1521). Revert's `at_effective_date` fallback must be passed **in** as a command/context value, never read from a clock inside any interpreter.

---

## 5. Option comparison (the core of this RFC)

Four bounded alternatives, evaluated against the §2 safety properties and cost. The universal simulator is included only as a control.

### Option A — current conservative model + targeted optimizations

Keep conservative dependency resolution, the explicit stale hash, and the hand-maintained per-order effects. Apply only narrow, measured optimizations:

- **Skip the effects-only pass when no later Numscript in the atomic batch can observe predecessor effects.** For a single-order batch, or a batch whose only state-dependent consumer is absent, the effects pass produces nothing anyone reads.
- **Share a panic boundary** (`SafeRun`-equivalent) between admission and FSM so a user-controlled script or store-triggered upstream panic becomes a deterministic typed error, never an FSM unwind.

**Safety:** trivially preserves every §2 property — nothing is shared into the FSM read horizon that is not already there. **Cost:** best-case win is the skipped second evaluation on batches that cannot observe it; measurable and bounded. **Drift surface:** unchanged (the hand-folds remain). **Verdict:** the safe floor. If no other option clears the safety gates with an acceptable cost, **this is the recommendation.**

### Option B — shared typed planning / effect helpers, authoritative `process*` retained

Extract the *typed effect calculation* (posting → materialized-volume delta; caller-metadata precedence; revert reversal; tombstone) into shared helpers, but keep the authoritative `process*` handlers and keep **separate admission and FSM adapters** around them. Admission and FSM both call the shared helper for the arithmetic, but the FSM's own `process*` remains the single point that reads through `Scope` and writes through `WriteSet`.

**Safety:** #3/#9 are preserved because the shared helpers do *not* read state — they operate on values the caller already fetched through its own (coverage-gated for FSM) reader. The helper cannot widen the FSM read horizon because it does no reading. **Cost:** modest; removes the arithmetic-duplication drift without touching the read path. **Risk:** the shared helper must reproduce both the FSM `process*` and the admission fold *byte-for-byte* — requires a differential test (§7). Metadata helpers must carry typed `*commonpb.MetadataValue` (§3.1b). **Verdict:** the most likely "go" if any reuse is justified — it captures most of the DRY benefit while keeping the read/write authority typed and unshared.

### Option C — immutable typed execution transcript / sidecar with preflight

Admission emits an **immutable, versioned transcript** alongside the accepted order (without rewriting the order — EN-1532/EN-1288 invariants): canonical identities of every resolution input, presence/absence + deterministic value encoding, expected digest and declared writes, and selected-script/reference evidence. The FSM **preflights** the transcript through the coverage-gated typed `Scope` after preceding same-batch effects and before re-execution: a value/presence mismatch is explicit **stale**; any later undeclared read remains a loud **coverage miss**.

**Safety:** this is the *only* option that could deliver the "preflight before any divergent read" property §4.1 shows the digest alone cannot — because the transcript carries the **key identities**, not just a digest. **Cost/risk (high):** requires proving (a) an upstream Numscript contract that returns conservative requirements + actual effects + a canonical read transcript without losing partial info on failure (this contract **does not exist today** and may be a hard blocker — do not emulate it in Ledger); (b) a decision on whether the sidecar is ephemeral Raft data, auditable evidence, or a rebuildable projection — each with checker/replay consequences (invariant #8); (c) no proto/persistence change in *this* ticket. **Verdict:** the most powerful and the most expensive; go/no-go hinges on the upstream contract and the persistence classification. Prototype outside production code first.

### Option D — universal `simulate(order, StateReader, WriteSink)` (control / no-go)

One untyped interpreter body shared verbatim between admission and FSM for all order types, differing only in the state backend and whether writes commit.

**Why it is the control, not the recommendation:**

- It tends to **recreate `Scope` behind a broader interface**, and the broader the interface the easier it is to smuggle in a raw-cache/Pebble read that bypasses #9 — the single catastrophic mistake (silent node desync).
- The blast radius covers admission, every mutating order, the FSM hot path, error precedence, atomic-batch rollback, audit identity and cache-coverage invariants simultaneously.
- Its safety argument leaned on the digest-as-preflight claim that §4.1 disproves, and on admission errors being equivalent to FSM errors, which property #5 rejects.

**Verdict: no-go** as a v3.0 refactor (risk: critical) and not the preferred v3.1 direction. Retained here so the comparison is explicit about *why* the broad interface loses to B/C.

### Summary matrix

| | A: current + opts | B: shared typed helpers | C: transcript/sidecar | D: universal simulator |
|---|---|---|---|---|
| Preserves #3 (no Pebble) | trivially | yes (helpers don't read) | yes (cache-only preflight) | fragile (broad iface) |
| Preserves #9 (coverage gate) | trivially | yes | yes, + preflight | **at risk** |
| Coverage-miss stays loud | yes | yes | yes | yes if disciplined |
| Preflight-before-read possible | n/a | n/a | **yes** (key manifest) | no (digest only, §4.1) |
| Admission stays advisory (#5) | yes | yes | yes | conflated |
| Atomic per-order (§3.1a) | yes | yes (per-order buffer) | yes | must be re-proven |
| Typed metadata (§3.1b) | yes | yes | yes | dropped in original draft |
| Drift reduction | none | high | high | high |
| Cost / risk | low / low | modest / medium | high / high | high / **critical** |
| Upstream Numscript contract needed | no | no | **yes (blocker)** | yes |

---

## 6. Upstream Numscript contract audit (required before any go)

Options C and D depend on upstream guarantees that must be established, not assumed:

- What does `ResolveDependencies` guarantee for conservative reads/writes, actual store calls, branch traversal, failed execution, panic handling and partial results?
- What does `Run` expose for actual reads, complete writes and effects on success **and** failure?
- Can a single upstream call safely return conservative requirements, canonical read evidence **and** actual effects without losing partial information on failure?

If the upstream API cannot provide a required guarantee, that is a **documented blocker** and the recommendation is to *propose an upstream contract*, not to emulate the missing guarantee in Ledger. The current baseline offers only `SafeRun` (actual-path execution with infinite balances for discovery); it is not a conservative-analysis API.

---

## 7. Required experiments (evidence, not assertion)

No option may be selected without reproducible evidence:

- **Differential/property tests** across the pinned Numscript language comparing admission planning vs FSM apply for read sets, effects, error precedence and **unchanged order bytes**.
- **Mutation tests:** mutate every recorded value and presence bit; prove detection occurs **before** business execution (Option C) or, for A/B, that the stale/coverage classification is correct.
- **Coverage-miss proof:** remove one declared/manifest key; prove the result is coverage/internal, **never** stale.
- **Atomic sequences:** postings, scripts, revert, add/delete metadata, caller-metadata precedence, tombstones, insufficient funds, overflow, predecessor failure and rollback — proving a failed order contributes no effects (§3.1a).
- **Audit/idempotency:** prove submitted order bytes are unchanged through admission and apply (EN-1532/EN-1288).
- **Cache rotation & pipelining:** exercise generation rotations and concurrent proposal pipelining.
- **Capability proof:** a compile-time/interface or minimal-wiring test showing a Pebble-backed admission adapter **cannot** be injected into FSM composition.
- **Benchmarks:** single script, terminal script, multiple independent scripts, state-dependent multi-order batch — admission cost separated from FSM cost; any FSM-latency or proposal-payload growth quantified and justified; compared against Option A's narrow skip-the-effects-pass optimization.

---

## 8. Go / no-go gates

**Go** for a given option only if it proves: dependency completeness or safe preflight before any undeclared read; an exact stale-vs-invariant taxonomy; atomic failure equivalence; accepted-order immutability; deterministic replay; and acceptable, measured cost.

**No-go** if any guarantee relies on a hash without key evidence (§4.1), on actual-path observation as a substitute for conservative coverage, on admission errors becoming authoritative (#5), or on a broad capability interface (#3/#9). **A no-go is a valid outcome** — in that case recommend only the bounded DRY/performance changes justified by measurements (Option A, and possibly B's non-reading helpers).

On current evidence, the ordering of likelihood is roughly **A ≥ B > C ≫ D**, but this must be re-derived against the merged EN-1406/EN-1532/EN-1288 baseline with the experiments above before any option is chosen.

---

## 9. Follow-up (prepare, do not create)

Provide a phased ticket outline with ownership boundaries, dependency ordering, migration/rolling-upgrade questions and test requirements. **Stop for owner review before creating implementation tickets or selecting a final sidecar/wire model.** No production behavior, wire format, persisted state or business semantics change in this ticket.

---

## 10. Summary

The question is whether admission and FSM apply can safely share more typed logic. This RFC compares four bounded options against the repository's non-negotiable invariants. The universal simulator (D) is a **no-go**: its safety argument rested on treating the `InputsResolutionHash` as a preflight manifest (it is only a digest — §4.1), on admission errors being authoritative (they are advisory — #5), and on a flattened-string metadata sink (metadata is typed `*commonpb.MetadataValue` — §3.1b), and its broad interface most easily bypasses the coverage gate. The viable directions are the low-risk floor (A) and non-reading shared helpers (B); the transcript/sidecar (C) is the only design that could deliver true preflight, but hinges on an upstream Numscript contract that does not yet exist. The document remains **Blocked** on EN-1406 / EN-1532 / EN-1288 and must be re-based and re-validated against the merged baseline before any option is selected.
