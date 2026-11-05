# Request and Response Signing

## Overview

Request signing is what makes a Ledger v3 batch **tamper-evident end-to-end**: the client signs the exact bytes it submits, the server verifies them without re-serialising, and the batch audit entry retains the original proof. Response signing does the symmetric thing on the way back: the server signs the committed logs so clients can audit them after the fact.

Signing is **optional by default** and can be made mandatory cluster-wide via `signing require`. When mandatory, unsigned batches are rejected at admission, except for the narrow cases below (bootstrap registration, maintenance mode, and leader-internal proposals). Operations documentation lives in [`docs/ops/signing.md`](../../../../ops/signing.md) and the maintenance-mode interaction is covered in [`docs/ops/maintenance-mode.md`](../../../../ops/maintenance-mode.md); this page covers the architecture.

## Algorithm

**Ed25519** throughout. Public keys are 32 B, private seeds are 32 B, signatures are 64 B. The `internal/domain/crypto/signing` package wraps Go's stdlib `crypto/ed25519` — no custom curve work.

## What is signed

### gRPC request signature: the whole batch, opaque

The client builds an `ApplyBatch` (the ordered requests, their skippable reasons, and the batch idempotency key), serializes it once, and signs those exact bytes. Go clients can use `actions.SignBatch`. The resulting `SignedApplyBatch{key_id, signature, payload}` retains the original serialized payload; admission verifies those bytes before trusting their decoded contents. This existing format remains unchanged when HTTP signatures are introduced.

Two consequences:

1. The signature covers the **composition and ordering** of the batch, not each request individually. Reordering, splitting, or removing requests breaks the signature.
2. **Cross-language clients work** without a shared canonical byte encoding. Protobuf implementations may serialize equivalent batches differently. Verification accepts a valid signature over the exact transmitted bytes; the server never reserializes the batch to reconstruct the signed bytes.

### HTTP transaction signing boundary

HTTP transaction mutations carry ordinary `application/json` and RFC 9421
`Signature-Input` / `Signature` headers with an RFC 9530 SHA-256
`Content-Digest`. Clients sign their original body bytes and HTTP components;
they do not construct protobuf batches. The supported routes and standalone
standard-library client recipe are documented in
[HTTP request signing](../../../../ops/signing.md#http-request-signing).

The restricted `sig1` profile requires exactly one occurrence of each covered
component: `@method`, `@authority`, `@path`, `@query`, `content-type`,
`content-digest`, and `idempotency-key`. Component order may vary. Parameters
are exactly one `keyid` and `alg="ed25519"`, in either order. No additional
signature, digest algorithm, component or parameter is accepted; in particular
there is no `created` / `expires` / `nonce` policy. Each actual signing header,
including an explicitly empty `Idempotency-Key`, is present once; content type
is exactly `application/json`. Authority is lowercased and default HTTP/HTTPS ports are removed using the
retained scheme; path retains its escaped form and query always begins with `?`.
The adapter uses the URL scheme or local TLS state, never forwarded-host/proto
headers, so proxies must preserve compatible signed component derivation.

The HTTP adapter captures a `SignedHTTPRequest` containing original method,
origin-form target, scheme, authority, content type, digest, idempotency key,
`Signature-Input`, `Signature`, and body. The adapter compares the complete
`ApplyBatch` derived by `servicepb.DecodeHTTPRequest` with the ordinary handler's
batch using `proto.Equal`. Count/order, nested oneofs, typed metadata,
skippable reasons, targets and batch identity must all agree before dispatch.
That comparison is untrusted parsing, not authentication.

The proof crosses `ApplyRequest.http_signed` and peer forwarding unchanged.
Admission selects the registered key from untrusted `keyid`, calls `VerifyHTTP`
to authenticate the RFC signature base and body digest, and only then calls the
same pure decoder to derive the authoritative batch. No externally supplied
batch is accepted alongside the proof. gRPC scope routing may peek through the
decoder, but only admission's verified result determines executed commands.
JWT/scopes/caller attribution remain separate requirements and no HTTP key cache
is introduced.

Signed bulk is nonempty and `atomic=true`, with the header key and ignored
per-element `ik` matching ordinary atomic bulk. Every element keeps its scope
and skip policy. `continueOnFailure` never suppresses 401/403, and processing
responses retain all element outcomes. Signed JSON is a complete single value;
revert accepts zero bytes or null for defaults, while metadata DELETE requires
zero bytes.

Capture, hashing, decoding and comparison cost O(request bytes), bounded by the
4 MiB body budget and a 4 MiB + 16 KiB retained-proof limit, repeated at admission
for authenticated peer forwarding. Public gRPC cannot submit HTTP proofs: the
RPC interceptor requires the matching configured cluster secret even when JWT
authentication is disabled, without altering scopes or caller attribution.
The unchanged unsigned and protobuf-signed paths do not perform
HTTP proof verification. The original HTTP bytes add bounded proposal, WAL and
audit storage overhead alongside the derived orders; clients no longer transmit
a duplicate base64 protobuf body.

`Proposal.http_signature` propagates to `AuditEntry.http_signature`; the
canonical header hash conditionally binds every field of the new proof while
preserving historical preimages for entries without it. This changes committed
proof encoding, requiring the approved mandatory stopped upgrade to
`v3.0.0-rc.4`. Existing keys, committed data and historical audit rows remain
valid without a rewrite. Do not admit new proofs across mixed old/new voters or
downgrade after one commits. Full checkpoints and incremental exports preserve
the original proof. Protocol revision remains 40; old gRPC signing remains
supported. See [upgrade policy](../../../../ops/upgrades.md).

### Response signature: per log

For every log that ends up in the response, the server signs a `Log` derived from it (with the `response_signature` field cleared). The signature is then attached back on `log.response_signature` as a `SignedLog`. A client first verifies the signature over the envelope's exact payload bytes, decodes those authenticated bytes as a `Log`, and compares that value with the visible log after clearing its `response_signature`. A valid envelope alone does not authenticate the separately supplied visible fields. `ledgerctl` rejects the response before consuming a mismatched log. Comparing decoded values also permits equivalent protobuf wire encodings without requiring client and server serialization to be byte-identical.

## Verification

### Server side

`signing.Verify(*signaturepb.SignedApplyBatch, ed25519.PublicKey)` verifies
the exact existing protobuf payload. `signing.VerifyHTTP` reconstructs the
restricted RFC 9421 signature base from the retained HTTP evidence, checks the
body's SHA-256 digest, and verifies Ed25519. Neither verifier reconstructs JSON
bytes from decoded values.

Verification occurs in `Admission.resolveBatch` before proposing to Raft.
Invalid proofs fail before consensus. Decoding authenticated HTTP evidence into
commands is a separate required step: a valid signature alone does not establish
the meaning of an independently supplied command batch.

### Audit-time

The two request-proof formats have separate authoritative homes:

- `Proposal.signature` becomes `AuditEntry.signature` for protobuf signing.
- `Proposal.http_signature` becomes `AuditEntry.http_signature` for HTTP signing.
- `BuildHashedHeaderPayload` binds the retained evidence into the audit chain.
  Tampering with a proof field changes that chain's hash.

Client request proofs are batch-level audit evidence, not per-`Log` fields.
An external verifier needs the original audit proof and the relevant client's
public key. HTTP verification authenticates the original request bytes and the
shared decoder establishes its batch intent; audit items bind the committed
orders. The checker verifies the hash binding and signing-key/config projections;
it does not independently rerun admission cryptography for every stored proof.
See [audit-chain.md](../checker/audit-chain.md).

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

HTTP signatures retain the same replay contract and introduce no freshness window. Replay protection is the job of the **idempotency key** + the FSM-side replay cache (see [pipeline.md § Idempotency](pipeline.md#idempotency)). A re-submitted signed batch with the same idempotency key returns the original outcome reference rather than executing again. A newly signed batch with a fresh idempotency key is, by design, treated as a new request; changing that key under an existing signature fails verification. An empty key permits repeated execution, and an expired key follows the existing eviction/replay contract.

## Cross-language constraint

For gRPC signing, the signed payload is the published protobuf `ApplyBatch` format, not a Go-specific canonical encoding. Preserve its wire contract and the original envelope bytes through forwarding and audit capture. A different valid protobuf encoding may decode to the same value and still authenticate, provided the client signs the bytes it actually sends. Never assume the Go SDK is the only consumer.

## Failure modes

| Symptom | Cause |
|---------|-------|
| `ErrInvalidSignature` returned at admission | Wrong key ID, tampered payload, or wrong private key. |
| Bootstrap registration rejected | `RequireSignatures()` is true and there is no parent key — solution is the maintenance-mode workaround above. |
| Client cannot verify a stored log | Server's response key has rotated; client must refresh via the Discovery RPC or the `--response-verify-key` flag. |
| Audit chain hash mismatch despite a valid request proof | Another chain-bound field changed after the fact; a valid request signature alone does not authenticate that field. |
