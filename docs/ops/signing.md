# Signing (Ed25519)

The ledger supports Ed25519 signing in two directions:

- **Request signing** (client → server): the client signs the whole **batch** (an `ApplyBatch` — its ordered requests plus idempotency key), guaranteeing **authenticity** (who issued the batch), **integrity** (detect any modification in transit, including reordering, regrouping, or dropping requests), and **non-repudiation** (the batch signature is stored with every log the batch commits, for audit). Signing the batch — rather than each request — is what makes the *composition and ordering* of the atomic operation provable.
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
                    │  Proposal.Signature → every Log.Signature    │
                    │  Log → AuditEntry (contains signature proof) │
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
    signature.SignedApplyBatch signed = 2;  // opaque envelope for a signed batch
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

Because the wire form for a batch is a discriminated `oneof`, the batch bytes appear only once. There is no wrapper-vs-payload divergence class to guard against — the proto schema itself enforces "either you ship the raw `ApplyBatch`, or you ship the opaque signed envelope, not both".

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
- **Immutable**: once set, the parent cannot be changed
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

When `--cascade` is used, the revocation log includes a `cascaded_key_ids` field listing all descendant keys that were also revoked, providing a complete audit trail.

With `--cascade`, revoking a root key revokes the entire subtree under it. Other subtrees are unaffected.

### Listing Keys

List all registered signing keys and their parent relationships:

```bash
ledgerctl signing list-keys
```

Output includes key ID, public key (hex), and parent key ID. Root keys (bootstrap) show `(root)` as parent.

### Bootstrap

The first `RegisterSigningKey` can be **unsigned** — this is the bootstrap path. Once at least one key is registered, all key management operations must be signed by an existing key.

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

The batch signature is carried through the entire pipeline for audit purposes:

```
ApplyRequest.signed (SignedApplyBatch) → Proposal.Signature → every Log.Signature → AuditEntry
```

The same signature is shared by every log the batch commits. An auditor verifies a log entry by calling `Ed25519.Verify(pubkey, log.Signature.Payload, log.Signature.Signature)` — `payload` is the serialized `ApplyBatch`, so the proof covers the whole atomic operation (its requests and their order), and verifying one log pulls in its batch-mates. Non-repudiation is therefore **per batch**, not per individual request.

## Design Decisions

### Verification in Admission, not FSM

Signature verification is performed in the **Admission layer** (before Raft consensus), not in the FSM. This is deliberate:

- **Valid signatures**: the cryptographic proof is propagated through Proposal → Log → AuditEntry. Auditors can verify signatures from the stored `SignedApplyBatch.payload` at any time.
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
| `misc/proto/signature.proto` | `SignedApplyBatch` and `SignedLog` protobuf messages |

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
ledgerctl --signing-key ./root-keys/seed.hex ledgers create --name my-ledger
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
                    │  3. Clone Log, clear receipt + sigs          │
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
                    │  3. Trust the Log if verification passes     │
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

If the server's response signature is missing or invalid, the command fails with an error.

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

The `payload` contains the serialized `Log` message with `response_signature` and `receipt` fields cleared before signing (both are node-local and non-deterministic).

### Design Decisions

- **Signing happens in the gRPC handler, not the FSM**: response signing is a presentation-layer concern. The FSM produces deterministic state; signing adds a transport-level proof on top.
- **Single server keypair**: no per-client keys needed. All clients verify using the same public key, obtained via `Discovery` or out-of-band.
- **No changes to Raft/FSM/Pebble**: the response signature is computed after Raft consensus and is not persisted. It only appears in the gRPC response.
- **Receipt is cleared before signing**: the `receipt` field (JWT) is node-local and not part of the signed content. This ensures signature verification is independent of receipt computation.
