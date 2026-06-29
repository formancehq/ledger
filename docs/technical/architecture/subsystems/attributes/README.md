# Attributes

System attributes (`internal/infra/attributes`, `internal/infra/cache`, `internal/infra/bloom`) cover the in-memory caches the FSM apply path reads from — volumes, metadata, transactions, references, boundary state, etc. — plus the bloom filters in front of Pebble and the U128 key-hashing scheme used to address them.

## Documents

| Document | Description |
|----------|-------------|
| [attributes.md](attributes.md) | System attributes (volumes, metadata, reversions, idempotency), storage format, and the generation-based attribute cache. |
| [key-hashing.md](key-hashing.md) | U128 hashing scheme for attribute keys and collision detection. |
| [bloom.md](bloom.md) | Per-attribute bloom filters — preload-time cache-prediction optimisation. Lifecycle, persistence, rotation interaction, metrics. |

## Related

- [FSM](../fsm/) — apply-path consumer of the attribute cache.
- [Storage](../storage/) — Pebble layout that holds attribute persistence.
- [Indexer](../indexer/) — its own derived projection over the same source attributes.
