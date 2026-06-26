# FSM

The deterministic state machine (`internal/infra/state`, `internal/infra/plan`, `internal/infra/preload`) that every node runs in lock-step with Raft commit. The apply path is CPU-bound and read-only against Pebble — its only inputs are the in-memory attribute cache, the preloaded `Needs`, and the command itself.

## Documents

| Document | Description |
|----------|-------------|
| [deterministic-fsm.md](deterministic-fsm.md) | Deterministic FSM with generation-based caching and preloading. |
| [cache-layers.md](cache-layers.md) | FSM-side read/write layering: WriteSet → DerivedKeyStore → Plan → KeyStore → AttributeCache. |

## Related

- [Consensus](../consensus/) — Raft commit pipeline that feeds the FSM.
- [Attributes](../attributes/) — the cache the FSM reads through.
- [Admission](../admission/) — declares the `preload.Needs` the FSM consumes.
