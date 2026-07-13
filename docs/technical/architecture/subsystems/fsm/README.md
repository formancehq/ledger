# FSM

The deterministic state machine (`internal/infra/state`, `internal/infra/plan`, `internal/infra/preload`) that every node runs in lock-step with Raft commit. The apply path is CPU-bound and read-only against Pebble — its only inputs are the in-memory attribute cache, the preloaded `Needs`, and the command itself.

## Documents

| Document | Description |
|----------|-------------|
| [deterministic-fsm.md](deterministic-fsm.md) | Deterministic FSM with generation-based caching and preloading. |
| [cache-layers.md](cache-layers.md) | FSM-side read/write layering: WriteSet → DerivedKeyStore → Plan → KeyStore → AttributeCache. |
| [preload.md](preload.md) | Preload contract: `Needs` declaration, `MirrorPreload`, `PredictedIndex` stale-detection, and the component-owns-its-needs rule. |
| [coverage-gate.md](coverage-gate.md) | The per-order coverage bits the FSM uses to gate every cache read against admission's declared `Needs`. |
| [skippable-orders.md](skippable-orders.md) | Continue-on-failure batches: the commit-or-discard `orderOverlayScope` / `skipSafeScope` rollback mechanism, the per-action skippable whitelist, and checker verification. |

## Related

- [Consensus](../consensus/) — Raft commit pipeline that feeds the FSM.
- [Attributes](../attributes/) — the cache the FSM reads through.
- [Admission](../admission/) — declares the `preload.Needs` the FSM consumes.
