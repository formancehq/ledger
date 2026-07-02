# Validation — Admission vs FSM

## Principle

Every write request is validated **twice**: once at admission (before consensus), once at the FSM (after consensus). The two layers don't duplicate each other — they catch different things, on purpose.

| Layer | Validates | Why here |
|-------|-----------|----------|
| **Admission** | Structural correctness of the *request* — shapes, sizes, allowed character sets, well-formed identifiers, parsing of inline scripts. | UX — reject malformed input fast, give the client a clear error before paying for consensus. |
| **FSM** | Behavioural invariants of the *execution* — balance sufficiency, account-type rules, overflow, already-reverted transactions, script runtime errors. | Audit trail — once the proposal is in the chain, any rejection is itself hash-bound and replayable. |

The rule is in [`feedback_validation_defense_in_depth`](../../../../../AGENTS.md): **do not move validators out of the FSM "for replay determinism" — FSM rejects are a feature**. They are how the audit chain records the fact that some specific batch was attempted and refused, and how the checker can later re-derive the same conclusion from the audit alone.

The complementary rule is in [`feedback_admission_structural_fsm_invariant`](../../../../../AGENTS.md): **admission validates that the order is well-constructed (structural); FSM validates that execution respects invariants (behavioural, post-producer).** Where both layers could detect the same end-state, they share a single sentinel rather than minting two parallel error types.

## What each layer catches

| Rule | Admission | FSM | Notes |
|------|-----------|-----|-------|
| Account address shape (length, allowed chars) | ✅ | — | `ValidateAccountAddress` (`internal/domain/validation.go`). |
| Asset code shape and precision range | ✅ | — | `ValidateAsset`. Precision encoded as `uint8` in Pebble keys; admission ensures it fits. |
| Metadata key shape (no null byte, ≤ size limit) | ✅ | — | `ValidateMetadataKey`. Null bytes would break Pebble key boundaries. |
| Metadata value shape | ✅ | — | `ValidateMetadataValue`. |
| Ledger name shape (no null, printable ASCII, ≤ `dal.LedgerNameFixedSize`) | ✅ | — | `ValidateLedgerName`. The fixed size matters because ledger names pad into Pebble keys. |
| Idempotency key (UTF-8, ≤ 256 bytes) | ✅ | — | The *uniqueness* of the key is checked at FSM time against `SubIdempKeys`; the *shape* is checked at admission. |
| Numscript parses + dependency analysis | ✅ | — | `DiscoverNumscriptDependencies` in admission. Syntax errors caught here. |
| Numscript runtime execution | — | ✅ | `processCreateTransaction` runs the program with preloaded state. Runtime errors → `AuditFailure`. |
| Account balance sufficient for posting | — | ✅ | Requires committed state — admission can't know. `processPosting`. |
| Account-type constraints (SOURCE_ONLY, DEST_ONLY) | — | ✅ | Per-ledger account-type schema. |
| Uint256 overflow on posting deltas | — | ✅ | `calculateAmountDelta`. |
| Already-reverted transaction | — | ✅ | Requires the reversion bitset. `processRevertTransaction`. |
| Existing-ledger / unknown-ledger checks | — | ✅ | The set of ledgers is committed state. |

## Shared sentinels

Where admission and the FSM can both reach the same end-state, they raise the **same** `domain.ErrXxx`, wrapped in a `domain.BusinessError`. The gRPC layer maps the business error to a stable gRPC status code, so the client sees the same code regardless of which layer produced the error.

Example: an empty account address can be caught by `ValidateAccountAddress` at admission (the request had `""`) **or** by the FSM if a numscript program generates an empty source/destination at runtime. Both paths return `domain.ErrAccountAddressEmpty`. The client sees a uniform error contract; the auditor sees the FSM-side rejection in the audit chain when it happens that way.

## Why FSM rejections matter

A rejection at admission is invisible to the audit chain — the proposal never reached Raft, there is nothing to record. That is the right answer for malformed requests (clients shouldn't be able to fill the audit log with garbage), but it is the wrong answer for behavioural failures.

When a Raft proposal is committed and the FSM then rejects it, the rejection produces an `AuditEntry` with `outcome = Failure` carrying the order index, the reason, and the same signature the request was submitted with. That `AuditEntry` is hash-bound exactly like a successful one (see [audit-chain.md](../checker/audit-chain.md)). The checker re-derives expected outcomes from the audit chain and verifies the frozen idempotency outcomes match — meaning a behavioural failure is, in the long run, just as auditable as a success.

Moving a behavioural check out of the FSM into admission would silently delete that record. That's the trade-off the rule guards against.

## Mid-batch failure handling

The FSM applies orders in batch order. If order N fails behaviourally, the whole proposal returns `Failure` for that batch — orders N+1…M are not applied. The audit entry captures the failure at order N; the client sees per-request error reporting in the response.

(There is no partial-success mode: the cluster either applies the full batch or none of it. This is required for the idempotency replay cache to behave correctly — the cache stores one outcome per proposal, not one per order.)

## The domain package

`internal/domain/validation.go` is where the structural validators live and is the natural home for new ones today. The longer-term direction (see [`feedback_formance_domain_repo`](../../../../../AGENTS.md)) is to migrate cross-SDK rules — anything that needs to behave identically in Go, in client SDKs, and in offline tooling — into a shared `formancehq/invariants` repository, leaving Go-only rules under `internal/domain/validation.go`. That repository is **not currently consumed** by this codebase; new shared rules should still land in `internal/domain/validation.go` for now, with the understanding that they may move once the shared package exists.

## Reading the error in tests

The standard pattern in tests is to assert on the domain sentinel, not the gRPC status code:

```go
err := admission.Admit(ctx, batch)
require.ErrorIs(t, err, domain.ErrAccountAddressEmpty)
```

`require.ErrorIs` matches both admission-side wraps and FSM-side wraps because the sentinel is shared. If a future refactor moves a check between layers, the test does not have to change.
