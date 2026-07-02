# Receipts

## Overview

A **receipt** is a short JWT that lets a client revert an **archived** transaction without anyone reading cold storage. The client gets the receipt at the time the transaction is archived (or already has it from when the transaction was created); on revert, it presents the receipt to the server; the server verifies the receipt and applies the reversal directly, with no round-trip to S3.

Receipts are how the cold-storage layer stays write-only: the system never needs to thaw an archived chapter just to revert a single transaction inside it.

Source: `internal/infra/receipt/receipt.go`.

## Claim shape

`receipt.go:32-40` — the JWT carries everything the FSM needs to construct the reversal:

```go
type Claims struct {
    Ledger    string             // ledger name
    TxID      uint64             // archived transaction ID
    Postings  []*commonpb.Posting// the original transaction's postings
    Asset     string
    ChapterID uint64             // which chapter holds the original
    jwt.RegisteredClaims         // iss, iat
}
```

The `Postings` field is the load-bearing one: the receipt is essentially **a frozen copy of the postings to invert**. The FSM consumes them at revert time without ever consulting the chapter's archived log.

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

If verification passes, the FSM has a trusted `Claims` struct and can apply the revert deterministically.

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
    S->>S: receipt.Verify(token)
    S->>S: apply reversal from receipt.Postings
    S-->>C: revert log
```

The receipt is issued **once**, at the time of the transaction's archival (or directly when the transaction is created, for forward-compatibility — the client gets the receipt regardless of whether the chapter has been archived yet). The client is responsible for storing it; the server does not keep a copy. From the cluster's point of view, the receipt's content is recoverable from the chapter even after archival — the receipt just saves the round-trip.

## The revert path

`RevertTransactionOrder` (`raft_cmd.proto:337-344`) carries an optional `original_postings` field. Under normal conditions (transaction still in a hot chapter), the field is left empty and the FSM reads the postings from Pebble. For an archived transaction, the field is populated **from the verified receipt's `Postings`**, and the FSM applies the inverse without touching cold storage.

The resulting `RevertedTransaction` log is hash-bound by the audit chain exactly like any other revert — the fact that the original was archived has no effect on the chain integrity.

## What a receipt does not authorise

- **It does not authenticate the caller.** Receipt verification proves "this content was signed by the cluster" — it says nothing about who is allowed to invoke it. The standard [auth](../api/auth.md) flow (JWT bearer + scopes) still runs in front. A valid receipt without a valid `transactions:write` scope is rejected at admission.
- **It is not idempotency.** Re-submitting the same receipt twice produces two revert attempts; the second one is rejected by the FSM because the original is already reverted (`checkReversionInvariants` enforces no-double-revert). If the client needs idempotent retries, it uses the standard idempotency key on the request — receipts and idempotency keys are orthogonal.
- **It is not arbitrary forging power.** The receipt's `Postings` are fixed at issuance time; they cannot be edited and re-signed. A receipt revert produces a revert log that is, byte-for-byte, the revert the cluster would have produced from cold-storage replay.

## Security envelope

Three things keep the scheme honest:

1. **One receipt per transaction.** The cluster only ever issues a receipt for a transaction that exists, with its actual postings. There is no path by which a forged-but-correctly-signed receipt could exist without the HMAC key.
2. **The HMAC key is cluster-local.** If it leaks, the operator rotates it (every outstanding receipt becomes invalid; clients need fresh ones). Receipts are intended to be relatively short-lived in operator policy, exactly so the blast radius of a rotation is small.
3. **Double-revert detection.** The FSM enforces "no transaction is reverted twice" via the reversion bitset and the checker's `checkReversionInvariants` pass (see [checker.md](../checker/checker.md)). So even a valid replay of a receipt cannot double-spend.

## Audit binding

Receipts themselves are **not** stored in the audit chain — they are external artefacts. The **revert log** that uses a receipt is bound by the chain like every other log, carrying the original-postings field in the order's `SerializedOrder`. The audit chain therefore records "the cluster reverted txID X with these postings", which is exactly what an independent auditor needs to verify.

A receipt that is signed but never used leaves no trace anywhere — there is no audit row for "receipt issued but never redeemed".

## Where to look in the code

| Concern | File |
|---------|------|
| Signer / Verifier | `internal/infra/receipt/receipt.go` |
| Claims shape | `internal/infra/receipt/receipt.go:32-40` |
| `RevertTransactionOrder` proto | `misc/proto/raft_cmd.proto:337-344` |
| Revert FSM processor | `internal/domain/processing/processor_revert_transaction.go` |
| Double-revert invariant | `internal/application/check/checker.go:1959-2006` |
