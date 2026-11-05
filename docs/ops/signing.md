# Signing (Ed25519)

The ledger supports Ed25519 signing in two directions:

- **Request signing** (client → server): gRPC clients sign the complete protobuf batch; HTTP clients sign their ordinary request with HTTP Message Signatures. Both authenticate the complete atomic operation, including ordering and idempotency identity. The original proof is retained on the batch audit entry for independent verification.
- **Response signing** (server → client): guarantees that response logs truly come from the server and haven't been tampered with.

## Request Signing

## Overview

Signing is **optional by default**. It can be made mandatory via the `signing require` command. Keys are managed dynamically through the gRPC API — there are no server-side configuration files.

```
                    ┌──────────────────────────────────────────────┐
                    │               Client (ledgerctl)             │
                    │                                              │
                    │  1. Build ApplyBatch{requests, idem key}     │
                    │  2. Serialize → payload (bytes)              │
                    │  3. Ed25519.Sign(privkey, payload)           │
                    │  4. Build SignedApplyBatch{key_id, sig, b}   │
                    │  5. Send as ApplyRequest.signed              │
                    └──────────────────┬───────────────────────────┘
                                       │ gRPC Apply()
                                       ▼
                    ┌──────────────────────────────────────────────┐
                    │            Admission Layer (leader)          │
                    │                                              │
                    │  1. Lookup public key by key_id              │
                    │  2. Ed25519.Verify(pubkey, payload)          │
                    │  3. Unmarshal(payload) → trusted ApplyBatch  │
                    │  4. Convert each request → Order (1 Proposal)│
                    │  5. Put SignedApplyBatch on Proposal.Signature│
                    └──────────────────┬───────────────────────────┘
                                       │ Raft Proposal
                                       ▼
                    ┌──────────────────────────────────────────────┐
                    │              FSM (all replicas)              │
                    │                                              │
                    │  Proposal.Signature → AuditEntry.Signature    │
                    │  HTTP proof → AuditEntry.HttpSignature │
                    └──────────────────────────────────────────────┘
```

## Envelope Pattern

Protobuf serialization is **not deterministic** across implementations (Go, Java, Python, etc.): map field iteration order, default value encoding, and unknown field handling all vary. If the server tried to re-serialize a client's `ApplyBatch` to verify a signature, two implementations would diverge.

The solution is the **opaque envelope**: a signed batch does not travel as a structured `ApplyBatch` on the wire. The client serializes its `ApplyBatch` once, signs those exact bytes, and ships the result as `SignedApplyBatch{key_id, signature, payload}`. The server verifies the Ed25519 signature against `payload` and then unmarshals `payload` to obtain the trusted `ApplyBatch` — **the server never re-serializes anything**, so cross-language clients are safe regardless of their protobuf implementation's quirks.

```protobuf
// bucket.proto — the Apply RPC input: one atomic batch, signed or not.
message ApplyRequest {
  oneof variant {
    ApplyBatch unsigned = 1;          // raw batch (used when signing is not required)
    signature.SignedApplyBatch signed = 2;  // existing opaque protobuf envelope
    signature.SignedHTTPRequest http_signed = 5; // original HTTP proof
  }
  // ... forwarded_caller_snapshot, skip_response (outside the signed payload)
}

// signature.proto
message SignedApplyBatch {
  string key_id = 1;    // ID of the public key used to sign
  bytes signature = 2;  // Ed25519 signature (64 bytes)
  bytes payload = 3;    // Exact serialized ApplyBatch bytes signed by the client
}
```

For gRPC, the discriminated `oneof` carries the raw batch, the existing signed protobuf envelope, or the additive `http_signed` HTTP proof variant. Ordinary HTTP clients send JSON with signature headers; the adapter captures that proof for admission, as described below.

## HTTP request signing

Send the ordinary JSON body with `Content-Type: application/json` and
[RFC 9421 HTTP Message Signatures](https://www.rfc-editor.org/rfc/rfc9421.html)
headers. The client hashes the exact transmitted body bytes using SHA-256 and
sends [RFC 9530 Content-Digest](https://www.rfc-editor.org/rfc/rfc9530.html).
No protobuf construction, duplicate request body, or JSON canonicalization is
required. The server verifies the digest and signature before admission derives
the executable batch from the authenticated HTTP request.

Supported paths are relative to `/v3/{ledgerName}`:

| Method and path | Body |
| --- | --- |
| POST `/transactions` | Ordinary create-transaction JSON |
| POST `/transactions/{transactionId}/revert` | Revert options JSON, `null`, or zero bytes for defaults |
| POST `/transactions/{transactionId}/metadata` | Ordinary metadata JSON |
| DELETE `/transactions/{transactionId}/metadata/{key}` | Zero bytes |
| POST `/bulk?atomic=true` | Nonempty ordinary bulk JSON array |

### Signature profile

The request must contain exactly one of each header: `Content-Type`,
`Content-Digest`, `Idempotency-Key`, `Signature-Input`, and `Signature`.
`Idempotency-Key` must be explicitly present even when its value is empty.
`Content-Type` must be exactly `application/json`, without parameters.
The digest dictionary contains only `sha-256`; the signature dictionaries
contain only `sig1`.

```http
POST /v3/main/transactions/12/metadata HTTP/1.1
Host: ledger.example.com
Content-Type: application/json
Content-Digest: sha-256=:<base64 SHA-256 of exact body bytes>:
Idempotency-Key: metadata-12-reviewed
Signature-Input: sig1=("@method" "@authority" "@path" "@query" "content-type" "content-digest" "idempotency-key");keyid="ops";alg="ed25519"
Signature: sig1=:<base64 Ed25519 signature>:

{"reviewed":"yes"}
```

Every component above must be covered exactly once; component order can vary.
The only signature parameters are `keyid` and `alg="ed25519"`, each exactly
once, in either order. This restricted profile rejects additional components,
parameters, signatures, and digest algorithms. In particular, `created`,
`expires`, and `nonce` are not accepted and no freshness window is enforced.

The signature base follows RFC 9421. `@authority` uses the lowercased HTTP
Host/authority, removing port 80 for HTTP and 443 for HTTPS; `@path` retains
its escaped spelling and `@query` includes its leading `?` even when absent.
Scheme is captured from the request URL or inferred from TLS; forwarded-host
and forwarded-proto headers are not trusted. The final
`"@signature-params"` line includes the canonical Structured Fields serialization
of the covered-component list and parameters, retaining their order. Sign that
base with Ed25519; send the exact JSON bytes whose digest it covers. A proxy must
preserve the signed authority, path, query and headers. Configure clients for the
authority observed by Ledger rather than relying on forwarded-host headers.

The signed request includes ignored JSON fields and response-affecting query
options as bytes. JWT claims and responses are outside this proof. Existing JWT
authentication, route scopes, bulk element scopes and caller attribution still
apply independently of the signing key. Register the public key through the
existing signing-management API and keep the private seed local.

### Go client recipe

This standalone standard-library example builds a metadata-save request; it
requires no Ledger protobuf bindings. Supply the full supported endpoint URL,
an already registered key, and a private key of the correct size. Send the
returned request with an ordinary `http.Client`.

```go
package example

import (
    "bytes"
    "context"
    "crypto/ed25519"
    "crypto/sha256"
    "encoding/base64"
    "errors"
    "net/http"
    "strings"
)

func SignedMetadataRequest(ctx context.Context, endpoint, token, keyID, idempotencyKey string,
    privateKey ed25519.PrivateKey) (*http.Request, error) {
    if len(privateKey) != ed25519.PrivateKeySize || keyID == "" {
        return nil, errors.New("a registered key ID and Ed25519 private key are required")
    }
    for i := range len(keyID) {
        if keyID[i] < 0x20 || keyID[i] > 0x7e {
            return nil, errors.New("key ID must be a Structured Fields string")
        }
    }
    body := []byte(`{"reviewed":"yes"}`)
    req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
    if err != nil {
        return nil, err
    }
    digest := sha256.Sum256(body)
    contentDigest := "sha-256=:" + base64.StdEncoding.EncodeToString(digest[:]) + ":"
    quotedKey := `"` + strings.NewReplacer(`\`, `\\`, `"`, `\"`).Replace(keyID) + `"`
    params := `("@method" "@authority" "@path" "@query" "content-type" "content-digest" "idempotency-key");keyid=` + quotedKey + `;alg="ed25519"`
    authority := strings.ToLower(req.URL.Host)
    if (req.URL.Scheme == "http" && req.URL.Port() == "80") ||
        (req.URL.Scheme == "https" && req.URL.Port() == "443") {
        authority = strings.ToLower(req.URL.Hostname())
        if strings.Contains(authority, ":") {
            authority = "[" + authority + "]"
        }
    }
    base := strings.Join([]string{
        `"@method": POST`,
        `"@authority": ` + authority,
        `"@path": ` + req.URL.EscapedPath(),
        `"@query": ?` + req.URL.RawQuery,
        `"content-type": application/json`,
        `"content-digest": ` + contentDigest,
        `"idempotency-key": ` + idempotencyKey,
        `"@signature-params": ` + params,
    }, "\n")
    req.Header.Set("Content-Type", "application/json")
    req.Header.Set("Content-Digest", contentDigest)
    req.Header.Set("Idempotency-Key", idempotencyKey)
    req.Header.Set("Signature-Input", "sig1=" + params)
    req.Header.Set("Signature", "sig1=:" + base64.StdEncoding.EncodeToString(ed25519.Sign(privateKey, []byte(base))) + ":")
    if token != "" {
        req.Header.Set("Authorization", "Bearer " + token)
    }
    return req, nil
}
```

For another supported operation, change the method, body and endpoint together
and rebuild the digest and signature base. Encode a metadata key as one URL
segment. For DELETE, hash the empty body and still send all five headers. Atomic
bulk signs the entire ordered array; `skippableReasons` remain per element,
`Idempotency-Key` identifies the whole batch, and per-element `ik` is ignored.
Unitary create does not acquire bulk-only skip semantics.

### Failures and rollout

| Condition | HTTP result |
| --- | --- |
| Missing or duplicate required signing header, unsupported content type, malformed body, empty or non-atomic signed bulk | 400 |
| HTTP-derived batch mismatch, invalid signature/profile/digest, unknown or revoked key | 403 `PERMISSION_DENIED` |
| Signature absent while signing is mandatory | 401 `UNAUTHENTICATED` |
| Signature headers on an unsupported route | 415 |
| Signed body exceeds 4 MiB, including trailing whitespace and atomic bulk | 413 `BODY_TOO_LARGE` |

Ordinary JSON requests without signature headers retain optional unsigned
behavior; mandatory signing rejects them without fallback. `continueOnFailure`
never suppresses bulk 401/403, and processing responses retain every element
outcome. The body limit remains 4 MiB, now entirely available for the original
body rather than a base64 wrapper. The retained proof has a separate maximum of
4 MiB plus 16 KiB, enforced again at admission for gRPC forwarding. Signed bulk
is read at signature capture, so exceeding its byte budget returns
`413 BODY_TOO_LARGE`; the ordinary unsigned bulk decoder retains its existing
`400 VALIDATION` oversized-body mapping.

Send header-signed requests through HTTP. The gRPC `http_signed` variant is
reserved for peer forwarding and requires the configured matching cluster
secret, even when public JWT authentication is disabled. This ensures an HTTP
node attested the observed authority and request target; public gRPC callers
cannot submit self-declared HTTP evidence. Configure the existing cluster
secret and its required TLS transport on every node before sending signed
requests through followers. A standalone leader can admit HTTP requests without
that forwarding setup. Existing protobuf-signed gRPC calls remain supported.
Peer-proof authentication preserves the original auth-disabled attribution and
does not grant public scopes or general forwarded-caller trust.

This feature requires the mandatory stopped upgrade to `v3.0.0-rc.4`; see
[server upgrades](upgrades.md#operator-required-stopped-restart). New HTTP proofs
change committed proposal/audit encoding, so old and new voters must not apply
them together. The upgrade preserves existing keys, business data and audit
history without rewriting historical hashes. Protocol revision stays 40 and
existing gRPC signing remains compatible. After the first HTTP proof commits,
downgrading to a version without HTTP proof support is unsupported. Full and
incremental backups retain the original proof bytes for independent verification.

Signing adds no replay protection: a retained idempotency key returns its frozen
outcome, an empty key permits repeated execution, and an expired key follows the
ordinary eviction contract. Preserve the key and signed request bytes for
retries. A transport failure does not establish whether an operation committed.
HTTP request signing adds no HTTP response authenticity.

## Key Management

Keys are managed via gRPC API calls, replicated through Raft consensus:

| Operation | Command | Description |
|-----------|---------|-------------|
| List keys | `signing list-keys` | List all registered signing keys |
| Register key | `signing register-key` | Register an Ed25519 public key |
| Revoke key | `signing revoke-key` | Revoke a registered key |
| Require signatures | `signing require` | Enable/disable mandatory signatures |

### Hierarchical Keys

Keys form a **parent-child hierarchy**. When a new key is registered by signing the request with an existing key, the signing key becomes the **parent** of the new key. The first key (bootstrap) has no parent.

```
          root-key (bootstrap, no parent)
           ├── ops-key (parent: root-key)
           │    └── ci-key (parent: ops-key)
           └── admin-key (parent: root-key)
```

The parent relationship is:
- **Automatic**: deduced from the signature used to register the key (no explicit parameter needed)
- **Replaced on re-registration**: registering an existing key ID again is an upsert — it replaces both the public key and the parent link. Re-registering with no parent makes the key a root
- **Persisted**: stored in Pebble alongside the public key and restored on startup

### Cascade Revocation

By default, revoking a key only removes that specific key. Use `--cascade` to also revoke all its descendants (children, grandchildren, etc.):

```bash
# Given: root-key → ops-key → ci-key

# Revoke only ops-key (ci-key remains valid)
ledgerctl signing revoke-key --key-id ops-key --signing-key ./root-keys/seed.hex

# Revoke ops-key AND ci-key (cascade)
ledgerctl signing revoke-key --key-id ops-key --cascade --signing-key ./root-keys/seed.hex
```

When `--cascade` is used, the revocation log includes a `cascaded_key_ids` field listing all descendant keys that were also revoked, providing a complete audit trail. Each descendant is listed once, and the target key itself is not repeated there.

With `--cascade`, revoking a root key revokes the entire subtree under it. Other subtrees are unaffected.

#### Which keys a cascade reaches

A cascade removes every key that is a descendant of the target **at the moment the revoke is applied**, using each key's current parent — the one its most recent registration assigned.

Because registration is an upsert, a single signed batch can move keys around before the revoke in that same batch runs. The rule is that the last registration wins:

| Earlier in the same batch | `revoke --cascade parent` |
|---|---|
| `child` revoked, then re-registered under `parent` | `child` **is** revoked — the re-registration put it back in the subtree |
| `child` re-registered under a parent that is **outside** `parent`'s subtree | `child` **survives** — it left the subtree before the revoke ran |
| `child` re-registered under a parent that is **inside** `parent`'s subtree | `child` **is** revoked — the cascade reaches it through its new parent |
| `child` untouched | `child` **is** revoked |

Submitting those operations as one batch or as several gives the same result. The batch boundary does not change who keeps a key.

Rows two and three are the pair to be careful with: reassigning a key only saves it if the new parent is outside the subtree being revoked. With `root → parent`, `parent → sibling` and `parent → child`, re-registering `child` under `sibling` does **not** save it — `sibling` is itself revoked as a descendant of `parent`, and the cascade continues through it. Only a parent that the cascade never reaches takes the key out of range.

The first row is the one worth remembering operationally: revoking a key and immediately issuing a replacement for it in the same batch does **not** protect that replacement from a cascade of its parent later in that batch.

Note that the new parent is never chosen explicitly — it is always the key that signed the batch (see [Hierarchical Keys](#hierarchical-keys)). So "re-register `child` under a different parent" means submitting that registration in a batch signed by the intended new parent, and that signer must be outside the subtree about to be revoked. Registering a key with *no* parent is only possible for the unsigned bootstrap registration on a cluster that has no keys yet; once any key exists, every registration is signed and therefore parented.

`cascaded_key_ids` on the revocation log lists exactly the keys the cascade removed, so the audit trail always shows which of these applied.

#### Cycles

Nothing prevents a key from being re-registered under one of its own descendants, which makes the parent graph cyclic. A cascade over such a graph still terminates and revokes each key in the cycle exactly once — but a cycle is an operator mistake, not a supported topology: `signing keys list` shows the parent of every key, and the graph should be a forest.

### Listing Keys

List all registered signing keys and their parent relationships:

```bash
ledgerctl signing list-keys
```

Output includes key ID, public key (hex), and parent key ID. Root keys (bootstrap) show `(root)` as parent.

### Bootstrap

The first `RegisterSigningKey` can be **unsigned** — this is the bootstrap path. Once at least one key is registered, all key management operations must be signed by an existing key.

The bootstrap path opens only on a cluster that has genuinely never registered a key. It stays **closed** when the store holds signing-key rows that failed to decode, even though skipping them can leave the in-memory key store empty. Boot deliberately survives an undecodable row — logging it, and letting `CheckStore` report it as `SIGNING_KEY_MISMATCH` — rather than crash-looping the replica, so "no usable keys" no longer implies "fresh cluster". Reopening the bootstrap path there would let whoever corrupted the rows register their own key unsigned, and because that registration takes the normal path it would be **audited**: the checker would see a legitimately chain-bound key and flag only the corrupt row it replaced. A cluster in that state therefore refuses all signing management until an operator restores the rows (from a backup, or by rebuilding the projection from the audit log).

```bash
# Step 1: Bootstrap — register the first key (no --signing-key needed)
ledgerctl signing register-key --key-id admin --public-key-file /path/to/pubkey.hex

# Step 2: Register additional keys (must be signed)
ledgerctl signing register-key --key-id ops --public-key <hex> --signing-key /path/to/admin-seed

# Step 3: Optionally enforce mandatory signatures
ledgerctl signing require true --signing-key /path/to/admin-seed
```

### Key Format

- **Public key**: 32 bytes, provided as hex-encoded string or raw binary file
- **Private key (seed)**: 32 bytes, provided as raw binary or hex-encoded file. The Ed25519 keypair is derived via `ed25519.NewKeyFromSeed(seed)`

## Persistence

Signing keys and configuration are persisted in **Pebble** under the Global zone (`0x06`), using compound keys `{0x06, 0x04}` for keys and `{0x06, 0x05}` for config. They are applied atomically in the same batch as other state changes via `WriteSet.Merge()`.

- **On startup**: keys are loaded from Pebble into the in-memory KeyStore
- **On follower snapshot restore**: keys are reloaded from the restored Pebble checkpoint (`SynchronizeWithLeader`)
- **On apply**: signing key changes flow through the `processing.Store` interface → `WriteSet` → `Merge()`, consistent with how all other state (volumes, metadata, ledgers) is managed

## Signature Propagation

The original proof is carried to the batch audit entry:

```text
ApplyRequest.signed → Proposal.signature → AuditEntry.signature
ApplyRequest.http_signed → Proposal.http_signature → AuditEntry.http_signature
```

`BuildHashedHeaderPayload` binds the proof into the audit hash chain. Request
proofs are not stored on each committed `Log`; response signatures have their
separate `Log.response_signature` field. Non-repudiation is per atomic batch.
For protobuf signing, verify the retained payload bytes against the client's
public key, then decode the authenticated `ApplyBatch`. For HTTP, verify the
retained RFC signature base and body digest, then derive batch intent from the
authenticated method/target/headers/body. Audit items bind the executed orders.
The checker verifies audit hash binding and key/config projections; it does not
rerun request cryptography for every entry.

## Design Decisions

### Verification in Admission, not FSM

Signature verification is performed in the **Admission layer** (before Raft consensus), not in the FSM. This is deliberate:

- **Valid signatures**: the original proof is propagated from Proposal to AuditEntry. Auditors can verify the retained protobuf or HTTP proof against the relevant client public key.
- **Invalid/missing signatures**: rejected immediately in Admission. These failures do **not** appear in the replicated audit log because no Proposal is created.

Moving verification into the FSM was considered but rejected for two reasons:

1. **DoS risk**: invalid signatures would traverse Raft consensus (network round-trips + log persistence on all replicas) before being rejected, allowing an attacker to waste cluster resources with bad signatures.
2. **Architectural complexity**: `SignedApplyBatch.payload` contains a serialized `ApplyBatch`, but the FSM operates on `Order` objects. Verifying in the FSM would require re-converting the batch → orders to confirm the Order matches the signed content, duplicating the Admission conversion logic.

If traceability of rejected signatures is needed, application-level logging on the leader node (non-replicated) can be added at the Admission layer.

### Revocation Latency

Signing key changes follow the Raft consensus path: they take effect only when the FSM applies the corresponding log entry. This means:

- When a `RevokeSigningKey` order is submitted, the key **remains valid** in the in-memory KeyStore until the FSM applies the revocation.
- Concurrent requests arriving at the Admission layer between submission and FSM apply will still accept signatures made with the revoked key.
- The same applies to `SetSigningConfig` (enabling/disabling mandatory signatures).

This is inherent to the Raft consensus model where all state changes are eventually consistent through the log. In practice, this window is very short (milliseconds under normal conditions) and is acceptable for a financial ledger where the audit trail records all operations with their signatures.

## Code Structure

| Package | Description |
|---------|-------------|
| `internal/domain/crypto/signing/` | Core Ed25519 sign/verify logic (transport-agnostic), including `ResponseSigner` |
| `internal/domain/crypto/keystore/` | Thread-safe in-memory key cache (`sync.RWMutex`) |
| `internal/application/admission/` | Signature verification, bootstrap logic, Request → Order conversion |
| `internal/infra/state/write_set.go` | Signing key changes accumulated during processing, applied in `Merge()` |
| `internal/storage/dal/` | Pebble persistence for signing keys (compound keys `{0x06, 0x04}`/`{0x06, 0x05}` within the Global zone) |
| `misc/proto/signature.proto` | `SignedApplyBatch`, `SignedHTTPRequest` and `SignedLog` protobuf messages |

## CLI Reference

### Global Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--signing-key` | | Path to Ed25519 seed file (32 bytes raw or hex) |
| `--signing-key-id` | `default` | Key ID for request signatures |
| `--response-verify-key` | | Path to Ed25519 public key file for response signature verification |

### Commands

#### Generate a keypair

```bash
ledgerctl signing generate-key ./my-keys
```

Creates `seed.hex` (mode 0600) and `pubkey.hex` in the specified directory.

#### Full workflow (request signing)

```bash
# 1. Generate a keypair
ledgerctl signing generate-key ./root-keys

# 2. Bootstrap: register the root key (unsigned, no parent)
ledgerctl signing register-key --key-id root --public-key-file ./root-keys/pubkey.hex

# 3. Sign write commands with the key
ledgerctl --signing-key ./root-keys/seed.hex ledgers create my-ledger
ledgerctl --signing-key ./root-keys/seed.hex transactions create --ledger my-ledger --posting "world,bank,1000,USD"

# 4. Register child keys (signed by root → parent is root)
ledgerctl signing generate-key ./ops-keys
ledgerctl signing register-key --key-id ops --public-key-file ./ops-keys/pubkey.hex --signing-key ./root-keys/seed.hex

# 5. Register grandchild keys (signed by ops → parent is ops)
ledgerctl signing generate-key ./ci-keys
ledgerctl signing register-key --key-id ci --public-key-file ./ci-keys/pubkey.hex --signing-key ./ops-keys/seed.hex

# 6. List all registered keys and their hierarchy
ledgerctl signing list-keys

# 7. Revoke a key only (ops is revoked, ci and root remain)
ledgerctl signing revoke-key --key-id ops --signing-key ./root-keys/seed.hex

# 7b. Or revoke a key and its descendants (ops + ci are revoked, root remains)
ledgerctl signing revoke-key --key-id ops --cascade --signing-key ./root-keys/seed.hex

# 8. Enable/disable mandatory signatures (must be signed)
ledgerctl signing require true --signing-key ./root-keys/seed.hex
ledgerctl signing require false --signing-key ./root-keys/seed.hex
```

---

## Response Signing

Response signing allows clients to verify that response logs returned by the server are authentic and haven't been tampered with. The server signs every `Log` in an `ApplyResponse` with an Ed25519 key.

### Overview

```
                    ┌──────────────────────────────────────────────┐
                    │                Server                         │
                    │                                              │
                    │  1. Process request through Raft consensus   │
                    │  2. Get Log result                           │
                    │  3. Clone Log, clear signature fields        │
                    │  4. MarshalVT() → payload                    │
                    │  5. Ed25519.Sign(privkey, payload)           │
                    │  6. Attach SignedLog to Log.response_signature│
                    └──────────────────┬───────────────────────────┘
                                       │ gRPC ApplyResponse
                                       ▼
                    ┌──────────────────────────────────────────────┐
                    │               Client (ledgerctl)             │
                    │                                              │
                    │  1. Get public key via Discovery RPC         │
                    │     (or --response-verify-key flag)          │
                    │  2. Ed25519.Verify(pubkey, payload)          │
                    │  3. Decode signed payload and compare it      │
                    │     with the visible Log (without envelope)  │
                    │  4. Consume the Log only if both checks pass │
                    └──────────────────────────────────────────────┘
```

### Server Setup

1. Generate an Ed25519 keypair:

```bash
ledgerctl signing generate-key ./response-keys
```

2. Start the server with the response signing key:

```bash
ledger run --response-signing-key ./response-keys/seed.hex [other flags...]
```

3. The server will sign every `Log` in `ApplyResponse` messages. The key ID (SHA256 fingerprint of the public key) is included in each signature.

### Client-Side Verification

Clients can verify response signatures in two ways:

#### Using the Discovery RPC

The `Discovery` RPC returns the server's public key:

```bash
# Programmatic: call Discovery RPC to get the public key
grpcurl -plaintext localhost:8888 ledger.BucketService/Discovery
```

#### Using --response-verify-key

Pass the server's public key file to `ledgerctl`:

```bash
ledgerctl --response-verify-key ./response-keys/pubkey.hex \
  transactions create --ledger my-ledger --posting "world,bank,1000,USD"
```

If the server's response signature is missing or invalid, or if the visible log differs from the authenticated payload, the command fails before consuming the log. This includes `transactions set-metadata`, `transactions delete-metadata`, and `numscripts save`: they verify returned logs before reporting success or displaying returned script details. Verification remains optional when the flag is unset.

The RPC has already completed when the response is verified. A verification failure does not roll back a mutation and leaves its outcome unauthenticated.

### Kubernetes Deployment

In Helm, configure response signing via a Kubernetes secret:

```yaml
config:
  responseSigning:
    enabled: true
    secretName: "ledger-response-signing-key"
    secretKey: "seed"  # Key within the secret
```

Create the secret:

```bash
kubectl create secret generic ledger-response-signing-key \
  --from-file=seed=./response-keys/seed.hex
```

### Protobuf Messages

```protobuf
message SignedLog {
  string key_id = 1;    // SHA256 fingerprint of server public key
  bytes signature = 2;  // Ed25519 signature (64 bytes)
  bytes payload = 3;    // Exact serialized Log bytes that were signed
}
```

The `payload` contains the serialized `Log` message with the `response_signature` field cleared before signing (it is node-local and non-deterministic).

### Design Decisions

- **Signing happens in the gRPC handler, not the FSM**: response signing is a presentation-layer concern. The FSM produces deterministic state; signing adds a transport-level proof on top.
- **Single server keypair**: no per-client keys needed. All clients verify using the same public key, obtained via `Discovery` or out-of-band.
- **No changes to Raft/FSM/Pebble**: the response signature is computed after Raft consensus and is not persisted. It only appears in the gRPC response.
