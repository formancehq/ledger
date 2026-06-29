# Request and Response Signing

## Overview

Request signing is what makes a Ledger v3 batch **tamper-evident end-to-end**: the client signs the exact bytes it submits, the server verifies them without re-serialising, and the resulting log entries carry the original signature into the audit trail. Response signing does the symmetric thing on the way back: the server signs the committed logs so clients can audit them after the fact.

Signing is **optional by default** and can be made mandatory cluster-wide via `signing require`. When mandatory, unsigned batches are rejected at admission. Operations documentation lives in [`docs/ops/signing.md`](../../../../ops/signing.md) and the maintenance-mode interaction is covered in [`docs/ops/maintenance-mode.md`](../../../../ops/maintenance-mode.md); this page covers the architecture.

## Algorithm

**Ed25519** throughout. Public keys are 32 B, private seeds are 32 B, signatures are 64 B. The `internal/domain/crypto/signing` package wraps Go's stdlib `crypto/ed25519` — no custom curve work.

## What is signed

### Request signature: the whole batch, opaque

The client builds an `ApplyBatch` (the list of requests + an idempotency key) and serialises it with `vtprotobuf` (deterministic encoding — no map iteration randomness, default-valued fields elided consistently). It then signs the resulting bytes once and wraps them in a `SignedApplyBatch{key_id, signature, payload}`. The payload is **opaque to the wire path**: the server doesn't decode it before verification.

Two consequences:

1. The signature covers the **composition and ordering** of the batch, not each request individually. Reordering, splitting, or removing requests breaks the signature.
2. **Cross-language clients work** without coordination on canonical-form rules — vtprotobuf's determinism gives every SDK (Go, Java, Python, …) the exact same bytes for the exact same `ApplyBatch`, so all of them produce a signature the server will accept. See [`project_cross_language_clients`](../../../../../AGENTS.md) for the constraint and why it matters.

### Response signature: per log

For every log that ends up in the response, the server signs an `ApplyLog` derived from it (with the `receipt` and `response_signature` fields cleared so the signed bytes are deterministic). The signature is then attached back on `log.response_signature` as a `SignedLog`. Clients verifying receipts on disk re-clear the same fields, recompute the bytes, and `ed25519.Verify` against the server's known public key.

## Verification

### Server side

```go
func Verify(pubkey []byte, payload []byte, sig []byte) error
```

Source: `internal/domain/crypto/signing/signing.go`. The function checks `len(payload) > 0`, `len(sig) == 64`, then calls `ed25519.Verify(pubkey, payload, sig)`. No re-serialisation, no field-by-field validation — only the raw payload signed by the client.

Verification happens in the admission stage, **before** the proposal reaches Raft (`resolveBatch()` in `internal/application/admission/admission.go`). Doing it here is a deliberate DoS-rejection point: invalid signatures cost the cluster nothing more than a public-key lookup + an Ed25519 verify.

### Audit-time

The signature is propagated all the way into the audit chain:

- `Proposal.Signature` carries the client's signature.
- The header payload that gets hashed by `BuildHashedHeaderPayload` includes the signature (see [audit-chain.md](../checker/audit-chain.md)).
- The signature ends up on each `Log` produced by the proposal as well, so an auditor reading a single log row can re-verify it independently against the client's public key.

This means tampering with a stored `Log.Signature` is detectable by the checker — it would break the audit-chain hash, not just the signature.

## Key management

### Storage

Public keys and signing config live under Pebble zone `Global`:

| Sub | Content |
|-----|---------|
| `SubSigningKey` | Public key entries (per key ID, parent, status). |
| `SubSigningConfig` | Cluster-wide signing config (`require_signatures`, etc.). |

An in-memory `KeyStore` mirrors the on-disk rows for hot lookups during admission. The mirror is rebuilt on boot by replaying the relevant log range.

### Lifecycle

Keys are managed by Raft-replicated orders (`processor_signing.go`):

| Order | Effect |
|-------|--------|
| `RegisterSigningKey` | Add a new public key. Its parent is the **signer of the registration request** — the hierarchy is automatic and immutable. |
| `RevokeSigningKey` | Cascade-revoke a key and every key descended from it (BFS over the parent relation). |
| `SetSigningConfig` | Cluster-wide flags such as `require_signatures`. |

The **first** `RegisterSigningKey` is the bootstrap: it has no parent because no signing key exists yet to authorise it. `authorizeUnsignedBatch()` in admission allows that single unsigned bootstrap to land. Every subsequent registration must be signed by an existing key.

Because registrations and revocations go through Raft, key changes are subject to consensus latency: between submission and FSM apply, a soon-to-be-revoked key remains valid. Operationally this is the same trade-off as any Raft-mediated control plane change.

### Maintenance mode interaction

When signing is required and the operator needs to register a fresh key without an existing signer (e.g. all parents revoked), the cluster can be put into maintenance mode. In maintenance, admission rejects every request type *except* `SetMaintenanceMode` — and `authorizeUnsignedBatch()` is allowed to admit an unsigned registration on the way out. See `docs/ops/maintenance-mode.md`.

## Signing is not replay protection

Signatures prove **non-repudiation**: the client cannot deny submitting a request whose signature verifies under their public key. They do not, by themselves, prevent a man-in-the-middle from re-submitting the same signed batch later.

Replay protection is the job of the **idempotency key** + the FSM-side replay cache (see [pipeline.md § Idempotency](pipeline.md#idempotency)). A re-submitted signed batch with the same idempotency key returns the original outcome reference rather than executing again. A re-submitted batch with a fresh idempotency key is, by design, treated as a new request — the client is responsible for not reusing or rotating idempotency keys.

## Cross-language constraint

The signed payload format is fixed by vtprotobuf encoding rules — `internal/pkg/vtmarshal`. Any change to those rules (custom marshal options, new "compact" encoding variants, field-tag renumbering, etc.) would break every client SDK at once. This is captured as a project-wide invariant: **preserve the envelope on the wire, never assume the Go SDK is the only consumer**.

## Failure modes

| Symptom | Cause |
|---------|-------|
| `ErrInvalidSignature` returned at admission | Wrong key ID, tampered payload, or wrong private key. |
| Bootstrap registration rejected | `RequireSignatures()` is true and there is no parent key — solution is the maintenance-mode workaround above. |
| Client cannot verify a stored log | Server's response key has rotated; client must refresh via the Discovery RPC or the `--response-verify-key` flag. |
| Audit chain hash mismatch on entries with valid `Log.Signature` | Log entry tampered after the fact — caught by the checker, not by signature verification (the signature still verifies; the hash chain doesn't). |
