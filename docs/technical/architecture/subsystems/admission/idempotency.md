# Idempotency Keys

## Overview

Idempotency keys provide a mechanism to safely retry requests without risking duplicate operations. When a client includes an idempotency key with a request, the system guarantees that the operation will only be executed once, even if the request is sent multiple times.

Idempotency keys are stored under the dedicated `ZoneIdempotency` zone (`0x05`) with an in-memory bridge map for inter-proposal visibility. They are not part of the shared attribute/cache system.

## Key Characteristics

| Characteristic | Description |
|----------------|-------------|
| **Scope** | System-level (not per-ledger) |
| **Uniqueness** | Keys must be globally unique across all ledgers |
| **Hash verification** | Content is hashed (BLAKE3) to detect conflicts |
| **Persistence** | Stored under `{0x05, 0x01}` (`ZoneIdempotency` + `SubIdempKeys`) with a time index at `{0x05, 0x02}` (`ZoneIdempotency` + `SubIdempTimeIdx`) |
| **TTL** | Set by the Raft-replicated cluster policy (default: 24h, 0 = no expiration); frozen per outcome as an absolute `expires_at` at write time |
| **Eviction** | Deterministic cleanup via Raft `IdempotencyEviction` commands |

## How It Works

### Request Flow

```mermaid
flowchart TD
    REQ[Request with Idempotency Key] --> CHECK{Key exists?}
    CHECK -->|No| PROCESS[Process request normally]
    PROCESS --> STORE[Store key + hash + sequence + created_at + expires_at]
    STORE --> RETURN[Return response]
    
    CHECK -->|Yes| EXPIRED{TTL expired?}
    EXPIRED -->|Yes| PROCESS
    EXPIRED -->|No| HASH{Hash matches?}
    HASH -->|Yes| CACHED[Return cached response<br/>Reference to original log]
    HASH -->|No| CONFLICT[Return conflict error]
```

### Hash Computation

When processing a request with an idempotency key:

1. **Hash computation**: The request content (excluding the idempotency key itself) is hashed using BLAKE3
2. **Storage**: The idempotency key maps to:
   - `LogSequence`: The global log sequence number of the original response
   - `Hash`: BLAKE3 hash of the request content
   - `CreatedAt`: HLC microsecond timestamp from the Raft entry
   - `ExpiresAt`: absolute HLC-microsecond expiry, frozen from the committed policy TTL (`created_at + ttl`; 0 = never)

```go
type IdempotencyKeyValue struct {
    LogSequence uint64  // Global sequence number of the original log
    Hash        []byte  // BLAKE3 hash of the request content
    CreatedAt   uint64  // HLC microseconds (from Raft entry timestamp)
    ExpiresAt   uint64  // absolute HLC-microsecond expiry (0 = never)
}
```

### Behavior Matrix

| Scenario | Result |
|----------|--------|
| New idempotency key | Process normally, store key |
| Same key + same content (within TTL) | Return reference to original log |
| Same key + different content (within TTL) | Return `idempotency key conflict` error |
| Same key (after TTL expiration) | Process normally (key treated as new) |
| No idempotency key | Process normally, no idempotency tracking |

## Numscript Dependency-Resolution Failures

Idempotency keys also govern a narrower, forward-vs-terminate decision admission has to make while *preparing* a `CreateTransaction` order that references a Numscript script. Before a script's order can be proposed, admission statically discovers the accounts, assets, and metadata it depends on (`DiscoverNumscriptDependencies`, `internal/domain/processing/numscript/discover.go` — see [Numscript Library](../scripting/numscript-library.md)) so the FSM never has to touch Pebble to resolve them. When that discovery fails, admission must decide whether the failure is **deterministic** — the script could never have succeeded, no matter how many times it is retried — or **state-dependent** — current state caused it, and a different (or later) state might not. `Admission.classifyResolutionFailure` (`internal/application/admission/admission.go`) makes this call from two signals Ledger already owns; it never inspects error strings or Numscript internals. A dedicated public Numscript resolver-error taxonomy (EN-1563) was evaluated for this purpose and cancelled as unnecessary.

### The two signals

1. **Selector mutability.** Under the numscript-library versioning model (see [Version Resolution](../scripting/numscript-library.md#version-resolution)), a script reference is either an exact immutable semver, the literal `latest`, or an inline script body. A `latest` reference can resolve to a *different, previously-saved* version on a later attempt, so a `latest` failure stays forwardable under an idempotency key even when the currently-selected version failed before reading any state. An inline script or an exact pinned version is deterministic: the same input always produces the same failure.
2. **Read-attempt provenance.** `RecordingStore.MutableReadAttempted()` (`internal/domain/processing/numscript/store.go`) reports whether resolution delegated any balance/metadata lookup to the inner store before failing — **including a lookup that itself returned an error**. This is carried out of `DiscoverNumscriptDependencies` through the typed `DependencyResolutionError` (`internal/domain/processing/numscript/resolution_error.go`). The pre-existing `RecordingStore.ReadNothing()` is insufficient for this purpose: it reflects only *successfully recorded* values, so it cannot distinguish "no read was attempted" from "a read was attempted and failed" — exactly the case that matters here.

### Decision flow

```mermaid
flowchart TD
    FAIL[Dependency discovery fails] --> PANIC{Recovered<br/>Numscript panic?}
    PANIC -->|Yes| LOUD1[Surface loudly<br/>never forwarded]
    PANIC -->|No| CANCEL{Context canceled /<br/>deadline exceeded?}
    CANCEL -->|Yes| LOUD2[Surface loudly<br/>never forwarded]
    CANCEL -->|No| FREEZE{Freezable deterministic<br/>rejection?<br/>parse / validation / not-found / …}
    FREEZE -->|Yes| FRZLATEST{Selector is 'latest'<br/>AND idempotency key present?}
    FRZLATEST -->|No| TERM1[Terminate:<br/>surface the real cause]
    FRZLATEST -->|Yes| FWD3["Forward: PRELOAD_UNAVAILABLE<br/>(latest may replay a frozen outcome)"]
    FREEZE -->|No| READ{Mutable read<br/>attempted?}
    READ -->|Yes| KEY1{Idempotency<br/>key present?}
    KEY1 -->|Yes| FWD1["Forward: PRELOAD_UNAVAILABLE<br/>(FSM may replay a frozen outcome)"]
    KEY1 -->|No| TERM2[Terminate: fail fast]
    READ -->|No| LATEST{Selector is<br/>'latest'?}
    LATEST -->|Yes| KEY2{Idempotency<br/>key present?}
    KEY2 -->|Yes| FWD2[Forward: PRELOAD_UNAVAILABLE]
    KEY2 -->|No| TERM3[Terminate: fail fast]
    LATEST -->|No: inline or exact version| TERM4["Terminate: surface the real cause<br/>(EN-1557 fix)"]
```

A recovered Numscript panic and context cancellation/deadline are surfaced loudly and never softened to `PRELOAD_UNAVAILABLE` — they are "should not happen" or host-level conditions (invariant #7), not business outcomes with a frozen replay to preserve. A freezable deterministic rejection (parse error, validation failure, not-found, already-exists, …) is likewise terminal: there is nothing a retry could change — *except* for a `latest` reference under an idempotency key, where the resolved content can differ from the version that froze an outcome on the original attempt (the idempotency hash binds the reference `name`+`latest`, not the resolved content), so that one case falls through to the forward path for the FSM to replay.

Asset scaling (`… with scaling through …`) is a special member of that freezable class worth calling out: resolution rejects it unconditionally, independent of any balance/metadata, so it is deterministic — yet a `balance()`/`meta()` var origin bound *before* the scaling statement is walked will already have set the read-attempt flag. `convertNumscriptError` therefore maps the one publicly-exposed deterministic sentinel (`numscriptlib.ErrScalingNotSupported`) to the freezable `ErrNumscriptScalingUnsupported` (`KindValidation`), so the freezable guard above terminates it *before* the provenance signals below are consulted (for an inline or exact reference; a `latest` reference under a key forwards instead, per the freezable carve-out above). Without this, a read-then-scaling inline/exact script under an idempotency key would be forwarded as `PRELOAD_UNAVAILABLE` and retried forever (EN-1557). The remaining deterministic post-read failures the library exposes only as internal types stay conservatively forwarded — splitting them would need the upstream taxonomy (EN-1563).

Past those guards, the provenance signals decide. A mutable read attempted before failing — even one that itself errored — means the failure is state-dependent. A *forced* balance query is not such a read: under force the resolver's store short-circuits to a synthetic max without consulting the value source, so it never sets the read-attempt flag (metadata reads consult real state even under force and still do); this keeps a forced `balance()` origin followed by a deterministic error from being misclassified as state-dependent and forwarded into an indefinite retry loop. When the failure is genuinely state-dependent: with an idempotency key, the order is stamped `PreloadUnavailable` on its `OrderTechnical` (the one field invariant #10 permits mutating post-acceptance) and forwarded, so the FSM can replay a frozen outcome if the batch is a retry, or reject with the retryable, non-frozen `ERROR_REASON_PRELOAD_UNAVAILABLE` if it isn't. Without a key there is no frozen outcome to preserve, so admission fails fast instead.

When no mutable read was attempted, only a `latest` selector keeps the same forwarding behavior, because a later attempt could still resolve to a different saved version. An inline script or an exact immutable version with no attempted read is fully deterministic — this is the case the EN-1557 fix changed: it previously forwarded as a retryable `PRELOAD_UNAVAILABLE` with no frozen outcome to ever converge on, producing an unbounded retry loop. It now terminates immediately, surfacing the real Numscript cause.

## TTL and Eviction

### Configuration

| Flag | Default | Description |
|------|---------|-------------|
| `--idempotency-ttl` | `24h` | Seeds the *desired* idempotency TTL in the cluster policy (0 = no expiration) |
| `--idempotency-eviction-interval` | `60s` | How often the leader proposes eviction |

`--idempotency-ttl` is node-local admission-side configuration: it names the desired TTL that the reconciler proposes into the Raft-replicated [cluster policy](../fsm/deterministic-fsm.md#35-raft-replicated-cluster-policy). The FSM never reads the flag during apply. Instead, each apply freezes the outcome's absolute `expires_at` once, from the TTL in the committed policy (`FSMState.ClusterPolicy.IdempotencyTtlMicros`), as `created_at + ttl` (0 = never; overflow saturates). Because the expiry is stamped from committed state and stored on the outcome, every node computes the same value and the expiry decision (`IsExpired`) reads no node-local configuration — the FSM-determinism requirement. Changing the TTL is a cluster-policy revision bump, not a boot-config change, so it needs no `--unsafe-skip-config-validation`.

### Eviction Mechanism

Expired idempotency keys are cleaned up via a dedicated Raft command (`IdempotencyEviction`):

1. The leader periodically takes `cutoff = now` (wall-clock) and scans the time index — keyed by `expires_at` — for entries whose `expires_at <= cutoff`, embedding their key hashes in the proposal
2. All nodes apply the eviction deterministically: delete exactly the pre-scanned hashes from the in-memory map + Pebble, guarding against evicting a still-live or never-expiring entry named by a stale scan
3. The cutoff and hashes are embedded in the Raft proposal, so all nodes agree on exactly what to evict and the apply path performs no Pebble reads
4. No race conditions: eviction is serialized with business proposals in the FSM

### Memory Bounds

The in-memory map grows between eviction commands and shrinks on each eviction:
- With interval=60s and 1000 IK/s: ~60K entries x ~80B = ~5MB
- With interval=60s and 10K IK/s: ~600K entries x ~80B = ~48MB

## Storage Architecture

### Pebble Layout

```
[0x05][0x01][key_hash 16 bytes]                -> IdempotencyKeyValue protobuf
[0x05][0x02][expires_at BE 8 bytes][key_hash 16 bytes]  -> empty (time index for eviction scan; never-expiring outcomes are omitted)
```

The key hash is a 16-byte BLAKE3 truncation of the idempotency key string.

### In-Memory Bridge

An in-memory map (`IdempotencyStore`) bridges state between consecutive proposals. A key written by proposal N must be visible to proposal N+1 even if N+1's preload ran before N was applied to Pebble.

```
Admission (preload) ─── direct Pebble Get ──> ExecutionPlan
                                                  │
FSM (apply) ─── in-memory map (bridge) ──────────>│
                │                                  │
                └── DerivedIdempotencyStore ───> Merge ──> Pebble [0x05][0x01]
```

### Preloading

During admission, idempotency keys are loaded directly from Pebble (no bloom filter, no dual-generation cache). The loading happens in `Builder.Build` (`internal/infra/plan/builder.go`) as a dedicated slot running in parallel with the per-`attrCode` resolver slots, opening its own read handle and emitting a `ReloadIdempotencyKey` on a dedicated channel of `ExecutionPlan` (separate from the `AttributeCoverage` channel that drives the coverage gate):

```go
value, err := state.LoadIdempotencyKey(reader, ik.Key)
if err != nil {
    results[i].err = err
    return
}

if value != nil {
    keys = append(keys, &raftcmdpb.ReloadIdempotencyKey{
        Key:   ik.Key,
        Value: value,
    })
}
// ...
// The slot stores `keys` on its resolveResult; Build then folds every
// slot's result into the plan:
//     executionPlan.IdempotencyKeys = append(
//         executionPlan.IdempotencyKeys, results[i].resolve.idempotencyKeys...)
```

## API Usage

### HTTP API

Include the idempotency key in the `Idempotency-Key` HTTP header:

```bash
curl -X POST http://localhost:9000/my-ledger/transactions \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: unique-request-id-123" \
  -d '{
    "postings": [
      {"source": "world", "destination": "bank", "amount": 100, "asset": "USD"}
    ]
  }'
```

### gRPC API

Include the idempotency key in the `idempotency_key` field of the request.

## Supported Operations

All write operations support idempotency keys:

| Operation | Endpoint | Idempotency Support |
|-----------|----------|---------------------|
| Create transaction | `POST /{ledger}/transactions` | Yes |
| Revert transaction | `POST /{ledger}/transactions/{id}/revert` | Yes |
| Save account metadata | `POST /{ledger}/accounts/{addr}/metadata` | Yes |
| Delete account metadata | `DELETE /{ledger}/accounts/{addr}/metadata/{key}` | Yes |
| Save transaction metadata | `POST /{ledger}/transactions/{id}/metadata` | Yes |
| Delete transaction metadata | `DELETE /{ledger}/transactions/{id}/metadata/{key}` | Yes |
| Create ledger | `POST /{ledger}` | Yes |
| Delete ledger | `DELETE /{ledger}` | Yes |
| Bulk operations | `POST /{ledger}/bulk` | Yes (per action) |

## Key Validation

| Rule | Limit | Error |
|------|-------|-------|
| Maximum length | 256 characters | `VALIDATION` (HTTP 400 / gRPC `INVALID_ARGUMENT`) |

## Error Handling

### Conflict Error

When a conflict is detected (same key, different content, within TTL):

**HTTP Response:**
```json
{
  "errorCode": "CONFLICT",
  "errorMessage": "idempotency key conflict: same key used with different request content"
}
```

**Status Code:** `409 Conflict`

### Best Practices

1. **Use unique keys**: UUIDs or composite keys (e.g., `{client-id}-{request-id}`)
2. **Be aware of TTL**: Keys expire after the configured TTL (default 24h)
3. **Don't reuse keys**: Even for "similar" operations
4. **Handle conflicts**: Implement retry logic with new keys on conflict

## Related Documentation

- [Numscript Library](../scripting/numscript-library.md) — the versioning model behind selector mutability, and dependency discovery / resolution.
- [Admission Pipeline](pipeline.md) — where numscript resolution and coverage enrichment sit in the overall admission flow.
