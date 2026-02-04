# Draft — In-Memory Attribute Storage via 128-bit Identifiers

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

---

## Attribute Concept

**Attributes** are a newly introduced concept in the ledger to support generic business logic.

An attribute represents any information associated with an account or address, such as:

- business metadata (category, status, labels…)  
- dynamic properties used by rules  
- association with assets or external identifiers  

Attributes are intentionally **free-form**: their structure and semantics depend on application needs.

They provide a flexible layer to:

- enrich accounts without changing the core model  
- support a wide range of use cases  
- offer primitives for filtering, rules, and indexing  

Given their potential volume, attributes must be stored in RAM in a compact way.

---

## Problem

Storing raw addresses in memory (`string` / `[]byte`) as map keys is expensive:

- many allocations  
- string overhead  
- higher GC pressure  
- slower comparisons  

A more efficient approach is to replace long keys with compact identifiers.

---

## Solution: 128-bit Identifier

Each address (and its context) is transformed into a compact **128-bit identifier**.

### Go Representation

Go does not provide a native `uint128`, so we use:

```go
type U128 struct {
    Hi uint64
    Lo uint64
}
```

This type can be used directly as a map key:

```go
map[U128]...
```

---

## ID Generation

A deterministic 128-bit hash is computed from the canonical representation of the address.

Recommended hashes:

- **BLAKE3**
- **xxHash3-128**

The first 16 bytes of the digest become the identifier:

```
address -> canonical bytes -> hash128 -> U128
```

---

## Collision Detection (Option 2)

Even though collisions on 128 bits are extremely unlikely, we add a secondary fingerprint.

### Principle

- `key128 = hash128(address)`
- `tag64  = hash64(address)`

We store:

```go
type AttributeEntry struct {
    Tag  uint64
    Data AttributePayload
}

var attrs map[U128]AttributeEntry
```

### Lookup

1. Recompute `key128` and `tag64`  
2. Lookup `key128` in the map  
3. Verify `tag64`  

- absent → attribute not present  
- present + tag matches → valid attribute  
- present + tag mismatch → collision detected locally  

### Benefits

- no DB read required  
- no need to store original keys  
- minimal overhead (+8 bytes per entry)  

---

## Usage: Attribute Storage

This format will be used to store ledger attributes in RAM:

- key: 128-bit identifier  
- value: business payload + fingerprint  

This provides:

- minimal memory usage  
- fast maps  
- reduced GC pressure  
- local collision detection  

---

## Conclusion

The ledger will use compact 128-bit identifiers to index attributes in memory.

The chosen approach (Option 2) offers an excellent compromise:

- performance  
- simplicity  
- protection against accidental collisions  

More complex collision bucket handling can be introduced later if needed.
