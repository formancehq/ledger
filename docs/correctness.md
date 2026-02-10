# Log Integrity and Correctness

This document describes the mechanisms ensuring the integrity and immutability of the ledger's log chain.

## Log Hash Chaining

Every log produced by the ledger is cryptographically chained to its predecessor using BLAKE3 hashing. This creates a verifiable chain where any tampering with a historical log invalidates all subsequent hashes.

### Hash Formula

```
hash = blake3(lastLogHash || deterministicSerialize(logWithoutHash))
```

Where:
- `lastLogHash` is the hash of the immediately preceding log (empty for the genesis log)
- `deterministicSerialize(logWithoutHash)` is the Protocol Buffers deterministic serialization of the log **before** the hash field is set
- `||` denotes concatenation

### Genesis Case

The first log in the chain has no predecessor. In this case, `lastLogHash` is empty (zero-length), so the hash is simply:

```
hash = blake3(deterministicSerialize(log))
```

### Implementation Details

- **Hash algorithm**: BLAKE3 via `github.com/zeebo/blake3`
- **Serialization**: `proto.MarshalOptions{Deterministic: true}` ensures stable byte output across runs
- **Hasher reuse**: A shared `blake3.Hasher` is reused (with `Reset()`) across log hash computations to avoid allocation overhead
- **Hash field**: Stored as `bytes hash` (field 4) in the `Log` protobuf message

### State Persistence

The `lastLogHash` is maintained as volatile state in the `Machine` (FSM). It is:
- **Persisted** in Raft snapshots via the `last_log_hash` field in `MemorySnapshot`
- **Restored** when a node installs a snapshot from the leader
- **Propagated** through the `Buffered` layer during proposal processing

The chain is **not broken** by Raft snapshots: the snapshot captures the latest hash, and new logs computed after snapshot restoration continue the chain correctly.

### Failure Isolation

If a proposal fails (e.g., insufficient funds, ledger not found), the `Buffered` state is discarded without being merged into the `Machine`. This means `lastLogHash` remains unchanged, preserving the chain's integrity.

### Idempotent Responses

When an idempotent request matches a previously processed order, the processor returns a **reference** to the existing log rather than creating a new one. References do not advance the hash chain.

## Integrity Guarantees

1. **Tamper detection**: Modifying any historical log invalidates all subsequent hashes in the chain
2. **Ordering proof**: The hash chain proves the exact ordering of all logs
3. **Determinism**: Given the same initial state and the same sequence of operations, the exact same hash chain is produced (verified by tests)
4. **Crash recovery**: The hash chain survives node restarts via Raft snapshot persistence
