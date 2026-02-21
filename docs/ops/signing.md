# Signing (Ed25519)

The ledger supports Ed25519 signing in two directions:

- **Request signing** (client → server): guarantees **authenticity** (who issued a request), **integrity** (detect any modification in transit), and **non-repudiation** (cryptographic proof stored with each log entry for audit).
- **Response signing** (server → client): guarantees that response logs truly come from the server and haven't been tampered with.

## Request Signing

## Overview

Signing is **optional by default**. It can be made mandatory via the `signing require` command. Keys are managed dynamically through the gRPC API — there are no server-side configuration files.

```
                    ┌──────────────────────────────────────────────┐
                    │               Client (ledgerctl)             │
                    │                                              │
                    │  1. Build Request (without signature)        │
                    │  2. Serialize → signed_payload (bytes)       │
                    │  3. Ed25519.Sign(privkey, signed_payload)    │
                    │  4. Attach RequestSignature to Request       │
                    └──────────────────┬───────────────────────────┘
                                       │ gRPC Apply()
                                       ▼
                    ┌──────────────────────────────────────────────┐
                    │            Admission Layer (leader)          │
                    │                                              │
                    │  1. Lookup public key by key_id              │
                    │  2. Ed25519.Verify(pubkey, signed_payload)   │
                    │  3. Unmarshal(signed_payload) → trusted Req  │
                    │  4. Convert trusted Request → Order          │
                    │  5. Propagate signature to Order             │
                    └──────────────────┬───────────────────────────┘
                                       │ Raft Proposal
                                       ▼
                    ┌──────────────────────────────────────────────┐
                    │              FSM (all replicas)              │
                    │                                              │
                    │  Order.Signature → Log.Signature             │
                    │  Log → AuditEntry (contains signature proof) │
                    └──────────────────────────────────────────────┘
```

## Envelope Pattern

Protobuf serialization is **not deterministic** across implementations (Go, Java, Python, etc.): map field iteration order, default value encoding, and unknown field handling all vary. This means two implementations serializing the same logical message can produce different bytes.

The solution is the **envelope pattern**: the client sends the exact bytes it serialized and signed in a `signed_payload` field. The server verifies the signature on those bytes, then deserializes them to get the authoritative content.

```protobuf
message RequestSignature {
  string key_id = 1;         // ID of the public key used
  bytes signature = 2;       // Ed25519 signature (64 bytes)
  bytes signed_payload = 3;  // Exact bytes serialized and signed by the client
}
```

This means the request payload appears twice on the wire: once in the Request fields, and once in `signed_payload`. The server **ignores** the outer Request fields for signed requests and uses the deserialized `signed_payload` as the authoritative content.

## Key Management

Keys are managed via gRPC API calls, replicated through Raft consensus:

| Operation | Command | Description |
|-----------|---------|-------------|
| Register key | `signing register-key` | Register an Ed25519 public key |
| Revoke key | `signing revoke-key` | Revoke a registered key |
| Require signatures | `signing require` | Enable/disable mandatory signatures |

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

Signing keys and configuration are persisted in **Pebble** (key prefixes `0x0B` for keys, `0x0C` for config) and applied atomically in the same batch as other state changes via `Buffered.Merge()`.

- **On startup**: keys are loaded from Pebble into the in-memory KeyStore
- **On follower snapshot restore**: keys are reloaded from the restored Pebble checkpoint (`SynchronizeWithLeader`)
- **On apply**: signing key changes flow through the `processing.Store` interface → `Buffered` → `Merge()`, consistent with how all other state (volumes, metadata, ledgers) is managed

## Signature Propagation

The signature is carried through the entire pipeline for audit purposes:

```
Request.Signature → Order.Signature → Log.Signature → AuditEntry (via Order)
```

Any auditor can verify a log entry's signature after the fact by calling `Ed25519.Verify(pubkey, log.Signature.SignedPayload, log.Signature.Signature)`.

## Design Decisions

### Verification in Admission, not FSM

Signature verification is performed in the **Admission layer** (before Raft consensus), not in the FSM. This is deliberate:

- **Valid signatures**: the cryptographic proof is propagated through Order → Log → AuditEntry. Auditors can verify signatures from the stored `signed_payload` at any time.
- **Invalid/missing signatures**: rejected immediately in Admission. These failures do **not** appear in the replicated audit log because no Proposal is created.

Moving verification into the FSM was considered but rejected for two reasons:

1. **DoS risk**: invalid signatures would traverse Raft consensus (network round-trips + log persistence on all replicas) before being rejected, allowing an attacker to waste cluster resources with bad signatures.
2. **Architectural complexity**: `signed_payload` contains a serialized `Request`, but the FSM operates on `Order` objects. Verifying in the FSM would require re-converting Request → Order to confirm the Order matches the signed content, duplicating the Admission conversion logic.

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
| `internal/crypto/signing/` | Core Ed25519 sign/verify logic (transport-agnostic), including `ResponseSigner` |
| `internal/crypto/keystore/` | Thread-safe in-memory key cache (`sync.RWMutex`) |
| `internal/service/admission/` | Signature verification, bootstrap logic, Request → Order conversion |
| `internal/service/state/buffer.go` | Signing key changes accumulated during processing, applied in `Merge()` |
| `internal/storage/data/` | Pebble persistence for signing keys (prefixes `0x0B`/`0x0C`) |
| `misc/proto/signature.proto` | `RequestSignature` and `ResponseSignature` protobuf messages |

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
ledgerctl signing generate-key ./my-keys

# 2. Bootstrap: register the first key (unsigned)
ledgerctl signing register-key --key-id admin --public-key-file ./my-keys/pubkey.hex

# 3. Sign write commands with the key
ledgerctl --signing-key ./my-keys/seed.hex ledgers create --name my-ledger
ledgerctl --signing-key ./my-keys/seed.hex transactions create --ledger my-ledger --posting "world,bank,1000,USD"

# 4. Register additional keys (must be signed)
ledgerctl signing generate-key ./ops-keys
ledgerctl signing register-key --key-id ops --public-key-file ./ops-keys/pubkey.hex --signing-key ./my-keys/seed.hex

# 5. Revoke a key (must be signed)
ledgerctl signing revoke-key --key-id ops --signing-key ./my-keys/seed.hex

# 6. Enable/disable mandatory signatures (must be signed)
ledgerctl signing require true --signing-key ./my-keys/seed.hex
ledgerctl signing require false --signing-key ./my-keys/seed.hex
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
                    │  3. Clone Log, clear receipt + response_sig  │
                    │  4. MarshalVT() → signed_payload             │
                    │  5. Ed25519.Sign(privkey, signed_payload)    │
                    │  6. Attach ResponseSignature to Log          │
                    └──────────────────┬───────────────────────────┘
                                       │ gRPC ApplyResponse
                                       ▼
                    ┌──────────────────────────────────────────────┐
                    │               Client (ledgerctl)             │
                    │                                              │
                    │  1. Get public key via Discovery RPC         │
                    │     (or --response-verify-key flag)          │
                    │  2. Ed25519.Verify(pubkey, signed_payload)   │
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
ledger-v3-poc run --response-signing-key ./response-keys/seed.hex [other flags...]
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
message ResponseSignature {
  string key_id = 1;         // SHA256 fingerprint of server public key
  bytes signature = 2;       // Ed25519 signature (64 bytes)
  bytes signed_payload = 3;  // Exact serialized bytes that were signed
}
```

The `signed_payload` contains the serialized `Log` message with `response_signature` and `receipt` fields cleared (both are node-local and non-deterministic).

### Design Decisions

- **Signing happens in the gRPC handler, not the FSM**: response signing is a presentation-layer concern. The FSM produces deterministic state; signing adds a transport-level proof on top.
- **Single server keypair**: no per-client keys needed. All clients verify using the same public key, obtained via `Discovery` or out-of-band.
- **No changes to Raft/FSM/Pebble**: the response signature is computed after Raft consensus and is not persisted. It only appears in the gRPC response.
- **Receipt is cleared before signing**: the `receipt` field (JWT) is node-local and not part of the signed content. This ensures signature verification is independent of receipt computation.
