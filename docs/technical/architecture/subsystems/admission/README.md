# Admission

The admission pipeline (`internal/application/admission`) is the gateway every write request goes through before reaching the FSM. It authenticates the request, validates signatures, converts external requests into internal orders, preloads dependent state, and proposes the resulting command into Raft.

## Documents

| Document | Description |
|----------|-------------|
| [idempotency.md](idempotency.md) | Idempotency key mechanism, hash-based conflict detection, and TTL eviction. |
| [admission-cache-horizon.md](admission-cache-horizon.md) | Rejecting proposals when the predicted apply-time generation is ≥ 2 ahead of the FSM's current generation. |

## Related

- [Read path](../read-path/) — the read counterpart that bypasses Raft via ReadIndex.
- [FSM](../fsm/) — the apply-side that admission proposes into.
- [Checker & audit](../checker/) — the audit chain admission's commands are bound by.
