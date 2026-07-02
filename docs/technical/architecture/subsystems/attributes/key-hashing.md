# In-Memory Attribute Storage via 128-bit Identifiers

## Context

In the ledger, accounts are identified by **user-defined addresses**.

Example:

```
users:<uuid>:products:<uuid>:sells:<id>
```

The ledger is generic:

- addresses may take any format
- accounts can have associated assets and metadata
- business-relevant elements are called **attributes**

The ledger runs on a custom **Raft** cluster implementation (based on the etcd library).

The goal is to store a large volume of attributes in RAM with minimal memory footprint.

## Attribute Concept

**Attributes** are a newly introduced concept in the ledger to support generic business logic.

An attribute represents any information associated with an account or address, such as:

- business metadata (category, status, labels...)
- dynamic properties used by rules
- association with assets or external identifiers

Attributes are intentionally **free-form**: their structure and semantics depend on application needs.

They provide a flexible layer to:

- enrich accounts without changing the core model
- support a wide range of use cases
- offer primitives for filtering, rules, and indexing

Given their potential volume, attributes must be stored in RAM in a compact way.

## Problem

Storing raw addresses in memory (`string` / `[]byte`) as map keys is expensive:

- many allocations
- string overhead
- higher GC pressure
- slower comparisons

A more efficient approach is to replace long keys with compact identifiers.

## Solution: 128-bit Identifier

Each address (and its context) is transformed into a compact **128-bit identifier**.

### Go Representation

Go does not provide a native `uint128`, so we use:

```go
type U128 [16]byte
```

This type can be used directly as a map key:

```go
map[U128]...
```

## Hash Function: XXH3

A deterministic 128-bit hash is computed from the canonical representation of the address.

We use **XXH3** (seeded) from the `github.com/zeebo/xxh3` library:

- `xxh3.Hash128Seed(data, seed)` -> 128-bit ID
- `xxh3.HashSeed(data, seed)` -> 64-bit collision tag

The pipeline:

```
address -> canonical bytes -> XXH3-128 -> U128
```

Two independent seeds are derived at startup from a 32-byte master key using domain-separated BLAKE3:

```
IDSeed  = uint64(BLAKE3("attrid:v1:id128:" || masterKey)[:8])
TagSeed = uint64(BLAKE3("attrid:v1:tag64:" || masterKey)[:8])
```

This is the **hot path** — every `Get`, `Put`, and `Delete` in the attribute cache calls `MakeKey()`.

### Why XXH3 over BLAKE3

| Property | XXH3-128 Seeded | BLAKE3 Keyed |
|---|---|---|
| Speed (43-byte input, M1 Pro) | ~13 ns/op | ~205 ns/op |
| Speedup | **16x** | 1x |
| State | Stateless (lock-free) | Stateful (requires mutex) |
| Output | 128-bit native | 256-bit, truncated |

BLAKE3's cryptographic properties are unnecessary for attribute key hashing because:

1. **Inputs are not adversarial** — keys come from Numscript programs and validated API requests
2. **Injection is expensive** — each input must pass through Raft consensus before reaching the hash
3. **128-bit collision space** — infeasible to find collisions even with a non-cryptographic hash
4. **Seed is in-process** — unpredictable to external attackers, preventing hash-flooding

### Hash Flooding Analysis

Hash flooding requires an attacker to craft many inputs that collide under the hash function. This attack is infeasible here because:

- The attacker cannot observe hash outputs (they are in-process memory only)
- The XXH3 seed is derived from a 32-byte master key and never exposed
- Each attempt to inject a key requires a Raft proposal (network round-trip + disk sync + consensus)
- Even without seeding, XXH3-128 has 2^128 output space — birthday bound is ~2^64 attempts

## Collision Detection

Even though collisions on 128 bits are extremely unlikely, we add a secondary 64-bit fingerprint.

### Principle

- `key128 = XXH3-128(address, IDSeed)`
- `tag64  = XXH3-64(address, TagSeed)`

We store:

```go
type Entry[T any] struct {
    Tag  uint64
    Data T
}

var attrs map[U128]Entry[T]
```

### Lookup

1. Recompute `key128` and `tag64`
2. Lookup `key128` in the map
3. Verify `tag64`

- absent -> attribute not present
- present + tag matches -> valid attribute
- present + tag mismatch -> collision detected locally

### Benefits

- no DB read required
- no need to store original keys
- minimal overhead (+8 bytes per entry)

## Implementation

The `KeyHasher` struct is lock-free:

```go
type KeyHasher struct {
    seeds Seeds
}

func (kh *KeyHasher) MakeKey(canonical []byte) (U128, uint64) {
    u := xxh3.Hash128Seed(canonical, kh.seeds.IDSeed)
    tag := xxh3.HashSeed(canonical, kh.seeds.TagSeed)
    return NewU128(u.Hi, u.Lo), tag
}
```

No mutex, no Reset/Write/Sum cycle, no pre-allocated buffers — just two stateless function calls.

## Where BLAKE3 Is Still Used

BLAKE3 remains the correct choice for:

- **Log hash chaining** (`internal/domain/processing/log_hasher.go`) — cryptographic integrity of the audit trail
- **Numscript cache** (`internal/domain/processing/numscript_cache.go`) — content-addressed deduplication
- **Idempotency content hash** (`internal/domain/processing/processor.go`) — conflict detection for same-key-different-content
- **Hash chain verification** (`internal/application/check/checker.go`) — integrity checking

## Benchmark Results

Run benchmarks with:

```bash
go test -bench=BenchmarkHashComparison ./internal/infra/attributes/
```
