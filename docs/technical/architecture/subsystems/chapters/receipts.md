# Receipts

## Overview

A **receipt** is a short JWT, issued when a transaction is created, that carries a signed copy of the transaction's postings. A client can present it when reverting so that admission does not have to read the transaction's postings itself.

The reversal is always applied by the FSM from the transaction's own state (`TransactionState`), which lives in the coverage-gated cache and **survives chapter archival** — archival purges only the cold `Log` / `Audit` / `AppliedProposal` ranges, never `TransactionState` (see `WriteSet.executePurge`). No revert path — receipt or not — reads cold storage. The receipt's role is purely admission-side: it is one source (versus admission's own store read) for the postings admission uses to declare the volume-preload coverage the reversed postings touch (invariant #9).

> **Open question.** Because `TransactionState` survives archival and the FSM reverts from it, admission can normally read the postings directly, so the receipt's distinct value is now thin. Whether the receipt feature is still needed is under review (tracked in the Ledger v3.0 backlog).

Source: `internal/infra/receipt/receipt.go`.

## Claim shape

`receipt.go` — the JWT carries the signed postings admission uses to declare the revert's coverage:

```go
type Claims struct {
    jwt.RegisteredClaims            // iss, iat
    Ledger    string                // ledger name
    TxID      uint64                // original transaction ID
    Postings  []PostingClaim        // the original transaction's postings
    ChapterID uint64                // chapter that held the original
}
```

The `Postings` field is a signed, frozen copy of the original transaction's postings. **Admission** consumes it at propose time — to declare the volume-preload coverage the reversed postings will touch — as an alternative to reading the postings from the transaction's own state. It is not consumed by the FSM: the FSM reverts from the coverage-gated `TransactionState` (see [The revert path](#the-revert-path)).

## Signing

`receipt.go:19, 70` — HMAC-SHA256 (`jwt.SigningMethodHS256`) with a cluster-wide HMAC key configured at boot.

This is a **symmetric** scheme — the same key signs and verifies. The trade-off, vs. Ed25519:

| Concern | HS256 receipt | Ed25519 signing (admission/signing.md) |
|---------|---------------|----------------------------------------|
| Verification | Server-only — clients can't independently verify a receipt is valid. | Anyone with the public key can verify. |
| Key surface | One HMAC secret on the cluster. | Per-key public/private pairs, managed via Raft orders. |
| Revocation | Rotate the HMAC secret (invalidates every outstanding receipt). | Cascade-revoke through the signing-key hierarchy. |

Receipts are deliberately the simpler primitive: the only consumer is the cluster itself, and the only operation they unlock is the revert path. No external auditor needs to verify them.

## Verification

`receipt.go:84-102` — `Verify(tokenString)`:

1. `jwt.WithValidMethods([]string{"HS256"})` — algorithm pinned, no algorithm-confusion attacks.
2. `jwt.WithIssuer("ledger-v3")` — issuer pinned, no cross-product confusion if other systems use the same library.
3. HMAC check.
4. Standard `exp` / `nbf` handling (claims expose `IssuedAt`; expiration is a deployment-policy choice).

Verification runs in **admission** (`convertApplyRequest`), which also checks that the receipt's `Ledger` and `TxID` match the resolved request. If verification passes, admission has a trusted `Claims` struct and uses its `Postings` to declare the revert's volume coverage.

## Lifecycle

```mermaid
sequenceDiagram
    actor C as Client
    participant S as Server
    participant Cold as Cold storage

    C->>S: CreateTransaction(...)
    S->>S: apply, sign receipt
    S-->>C: response + receipt (JWT)
    Note over C: client stores receipt
    ...
    Note over S,Cold: Chapter sealed → archived
    ...
    C->>S: RevertTransaction(receipt)
    S->>S: admission: receipt.Verify(token) → declare volume coverage from receipt.Postings
    S->>S: FSM: revert from coverage-gated TransactionState
    S-->>C: revert log
```

The receipt is issued **once**, when the transaction is created; the client is responsible for storing it and the server keeps no copy. It is only ever an admission-side convenience: the postings are also recoverable from the transaction's own state (which the FSM reads on revert regardless), so the receipt just saves admission a store read.

## The revert path

`RevertTransactionOrder` carries only caller intent — `transaction_id`, `force`, `at_effective_date`, `metadata`. It does **not** carry the original postings: that would be state-derived execution data on an audit-bound order (see [audit-vs-technical-state.md](../../audit-vs-technical-state.md#accepted-order-enrichment)).

At apply time the FSM sources the original postings from the target's `TransactionState`, read through the coverage-gated scope — admission preloads that entry (`addTransactionTargetNeeds`), so the read never touches Pebble (invariant #3). It builds the reversed postings from `origState.GetPostings()`; a missing state for an allocated id is a loud inconsistency, and a non-existent id is caught earlier by the `txID >= NextTransactionId` boundary check.

The receipt (when present) never reaches the FSM. Admission verifies it and uses its `Postings` only to declare the volume-preload coverage the reversed postings touch — the same coverage it would otherwise derive from its own read of `TransactionState`.

The resulting `RevertedTransaction` log is hash-bound by the audit chain exactly like any other revert — the fact that the original chapter was archived has no effect on chain integrity.

## What a receipt does not authorise

- **It does not authenticate the caller.** Receipt verification proves "this content was signed by the cluster" — it says nothing about who is allowed to invoke it. The standard [auth](../api/auth.md) flow (JWT bearer + scopes) still runs in front. A valid receipt without a valid `ledger:TransactionWrite` scope is rejected at admission.
- **It is not idempotency.** Re-submitting the same receipt twice produces two revert attempts; the second one is rejected by the FSM because the original is already reverted (`checkReversionInvariants` enforces no-double-revert). If the client needs idempotent retries, it uses the standard idempotency key on the request — receipts and idempotency keys are orthogonal.
- **It is not arbitrary forging power.** The receipt's `Postings` are fixed at issuance time and can't be edited without the HMAC key. And even a validly-signed receipt can't determine the reversal: the FSM builds the revert from the target's `TransactionState`, not from the receipt. A receipt whose postings disagreed with the stored state would only mis-declare coverage — at worst a coverage-miss error, never a wrong revert.

## Security envelope

Three things keep the scheme honest:

1. **One receipt per transaction.** The cluster only ever issues a receipt for a transaction that exists, with its actual postings. There is no path by which a forged-but-correctly-signed receipt could exist without the HMAC key.
2. **The HMAC key is cluster-local.** If it leaks, the operator rotates it (every outstanding receipt becomes invalid; clients need fresh ones). Receipts are intended to be relatively short-lived in operator policy, exactly so the blast radius of a rotation is small.
3. **Double-revert detection.** The FSM enforces "no transaction is reverted twice" via the reversion bitset and the checker's `checkReversionInvariants` pass (see [checker.md](../checker/checker.md)). So even a valid replay of a receipt cannot double-spend.

## Audit binding

Receipts themselves are **not** stored in the audit chain — they are external artefacts. The audit-bound order (`AuditItem.SerializedOrder`) carries only the caller's revert intent (`transaction_id`, `force`, `at_effective_date`, `metadata`), not the postings. The reversed postings appear in the produced `RevertedTransaction` log — a checker-verified projection, re-derivable by replaying the target's `TransactionState` — so an independent auditor can still confirm "the cluster reverted txID X with these postings" from the chain-bound log.

A receipt that is signed but never used leaves no trace anywhere — there is no audit row for "receipt issued but never redeemed".

## Where to look in the code

| Concern | File |
|---------|------|
| Signer / Verifier | `internal/infra/receipt/receipt.go` |
| Claims shape | `internal/infra/receipt/receipt.go` |
| Receipt verification + coverage declaration (admission) | `internal/application/admission/admission.go` (`convertApplyRequest`) |
| `RevertTransactionOrder` proto | `misc/proto/raft_cmd.proto` |
| Revert FSM processor (postings from `TransactionState`) | `internal/domain/processing/processor_revert_transaction.go` |
| Double-revert invariant | `internal/application/check/checker.go` |
