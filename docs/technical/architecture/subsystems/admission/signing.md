# Request and Response Signing

## Overview

Request signing is what makes a Ledger v3 batch **tamper-evident end-to-end**: the client signs the exact bytes it submits, the server verifies them without re-serialising, and the resulting log entries carry the original signature into the audit trail. Response signing does the symmetric thing on the way back: the server signs the committed logs so clients can audit them after the fact.

Signing is **optional by default** and can be made mandatory cluster-wide via `signing require`. When mandatory, unsigned batches are rejected at admission, except for the narrow cases below (bootstrap registration, maintenance mode, and leader-internal proposals). Operations documentation lives in [`docs/ops/signing.md`](../../../../ops/signing.md) and the maintenance-mode interaction is covered in [`docs/ops/maintenance-mode.md`](../../../../ops/maintenance-mode.md); this page covers the architecture.

## Algorithm

**Ed25519** throughout. Public keys are 32 B, private seeds are 32 B, signatures are 64 B. The `internal/domain/crypto/signing` package wraps Go's stdlib `crypto/ed25519` — no custom curve work.

## What is signed

### Request signature: the whole batch, opaque

The client builds an `ApplyBatch` (the list of requests + an idempotency key) and serialises it with `vtprotobuf` (deterministic encoding — no map iteration randomness, default-valued fields elided consistently). It then signs the resulting bytes once and wraps them in a `SignedApplyBatch{key_id, signature, payload}`. The payload is **opaque to the wire path**: the server doesn't decode it before verification.

Two consequences:

1. The signature covers the **composition and ordering** of the batch, not each request individually. Reordering, splitting, or removing requests breaks the signature.
2. **Cross-language clients work** without coordination on canonical-form rules — vtprotobuf's determinism gives every SDK (Go, Java, Python, …) the exact same bytes for the exact same `ApplyBatch`, so all of them produce a signature the server will accept. See [`project_cross_language_clients`](../../../../../AGENTS.md) for the constraint and why it matters.

### Response signature: per log

For every log that ends up in the response, the server signs an `ApplyLog` derived from it (with the `response_signature` field cleared so the signed bytes are deterministic). The signature is then attached back on `log.response_signature` as a `SignedLog`. Clients verifying signatures on disk re-clear the same field, recompute the bytes, and `ed25519.Verify` against the server's known public key.

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
| `RegisterSigningKey` | **Upsert** a public key. Its parent is the **signer of the registration request** — the hierarchy is automatic, and re-registering an existing key ID replaces both its public key and its parent link. |
| `RevokeSigningKey` | Revoke a key, and with `cascade` every key descended from it (BFS over the parent relation). |
| `SetSigningConfig` | Cluster-wide flags such as `require_signatures`. |

The **first** `RegisterSigningKey` is the bootstrap: it has no parent because no signing key exists yet to authorise it. `authorizeUnsignedBatch()` in admission allows that single unsigned bootstrap to land. Every subsequent registration must be signed by an existing key.

Registration is an upsert rather than an insert: nothing rejects a duplicate key ID. Re-registering moves the key under whoever signed that batch, and re-registering with no parent makes it a root. The hierarchy is therefore mutable — a key's parent is whatever its **latest** registration assigned. See [Cascade and the effective parent](#cascade-and-the-effective-parent).

### Cascade and the effective parent

`RevokeSigningKey` with `cascade` removes the revoked key and its entire descendant subtree, and records the descendants in `RevokedSigningKeyLog.cascaded_key_ids`.

Which keys are descendants is decided by the **effective** parent relation: the committed key store with the current batch's staged signing updates folded over it **in order**, last write per key ID winning (`state.WriteSet.GetSigningKeyChildren`). That is the same sequence `WriteSet.Merge` replays into Pebble and the in-memory `KeyStore`, and the same one `backup.RebuildDelta` replays on restore, so all three agree on which keys a cascade reached.

Two consequences inside a single signed batch, both following from "the last registration decides":

- A key revoked earlier in the batch and then **re-registered** under the revoke target **is** cascaded. The registration supersedes the removal, so the key is back in the subtree when the cascade runs.
- A key **reassigned** earlier in the batch is cascaded or not according to where its new parent sits. Reassignment removes the old edge, so the key is no longer reachable through it — but the walk is transitive, so a new parent that is *itself* inside the revoked subtree still carries the cascade to it. Only a new parent the cascade never reaches takes the key out of range.

Since the parent is the batch signer, reassignment means submitting the registration in a batch signed by the intended new parent, and an empty parent is reachable only for the unsigned bootstrap registration — once any key exists, `authorizeUnsignedBatch` rejects unsigned registration, so every registration is signed and therefore parented. The FSM still handles an empty `ParentKeyId` deterministically: recovery and `RebuildDelta` both rebuild roots from parent-less persisted rows.

A batch boundary does not change either answer: the same ordered operations produce the same surviving key set whether they are submitted as one signed batch or several. Deriving the cascade from an unordered view of the staged updates is what made those two disagree (EN-2011).

Revocation's only persistent representation is **row absence** — the stored row carries no revoked flag — so a cascade that misses a key leaves a fully working credential behind. Signature verification looks the requested key ID up in the `KeyStore` and verifies against that key's own public key; it never walks up to the parent, so revoking a parent does not by itself disable a surviving child.

The parent graph is not validated: registration shape-checks the two key IDs and never confirms the parent exists, so `register a under b` followed by `register b under a` is accepted end to end and makes the graph cyclic. The cascade walk carries a visited set for that reason — it runs inside Raft apply, where a non-terminating walk would wedge every replica at once and replay on every restart. A cycle is an operator mistake, not a supported topology.

The audit-side re-derivation in `check.signingVerifier` folds the same orders in the same sequence and walks the same relation, so it reaches the same descendant set without reproducing any batch-local bookkeeping. It never reads `cascaded_key_ids` off the log — re-deriving the cascade is the point of the pass, and trusting the recorded list would let a tampered projection justify itself.

Because registrations and revocations go through Raft, key changes are subject to consensus latency: between submission and FSM apply, a soon-to-be-revoked key remains valid. Operationally this is the same trade-off as any Raft-mediated control plane change.

### Maintenance mode interaction

When signing is required and the operator needs to register a fresh key without an existing signer (e.g. all parents revoked), the cluster can be put into maintenance mode. In maintenance, admission rejects every request type *except* `SetMaintenanceMode` — and `authorizeUnsignedBatch()` is allowed to admit an unsigned registration on the way out. See `docs/ops/maintenance-mode.md`.

### Leader-internal proposals

Background work the leader schedules — query-checkpoint creation and other scheduled maintenance — travels through admission as an unsigned batch under a **system actor** (`auth.WithSystemActor`, attributed to a `commands.Component*`). `authorizeUnsignedBatch()` admits these even when signing is mandatory. The exception is safe because the system actor is set only by server-internal code and is never derived from client input, so it cannot be forged over the wire; and the resulting order is audited under a named `system` principal, so it is still attributable in the audit chain. Mandatory signing authenticates *client* writes; the leader's own audited proposals are already inside the trust boundary. Without this exception a signed cluster could never create query checkpoints.

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

## Credential-bearing read projections

Public audit/log reads apply the [credential projection contract](../api/secret-redaction.md).
The original stored bytes remain verifiable. A redacted read payload is a
display projection: it cannot reproduce the original signature or audit hash,
and its cryptographic signature bytes are omitted when the payload changes.
Unchanged read payloads retain their exact bytes and original signatures.
Request admission and signed write responses keep the contract described above.
