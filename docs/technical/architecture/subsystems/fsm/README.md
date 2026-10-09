# FSM

The deterministic state machine (`internal/infra/state`, `internal/infra/plan`, `internal/infra/preload`) that every node runs in lock-step with Raft commit. The apply path is CPU-bound and read-only against Pebble — its only inputs are the in-memory attribute cache, the preloaded `plan.Coverage` state, and the command itself.

The audit hash key is generated outside apply and committed once as a
technical update before any audited order. Apply installs those committed
bytes and never reads node-local cluster identity to derive the hash. Recovery
loads the key from the primary store and checks it against the genesis hash
before swapping FSM state; an audit history without a matching key is an
invariant failure.

Numscript-generated transaction metadata is validated in ascending key order at
apply. If several entries are invalid, the first selected error becomes the
authoritative audit failure (including its reason, code, and facts), so selection
must not depend on Go map iteration. A failed order discards its posting and
metadata writes while preserving the failure audit entry.

## Documents

| Document | Description |
|----------|-------------|
| [deterministic-fsm.md](deterministic-fsm.md) | Deterministic FSM with generation-based caching, preloading, authoritative sequence-exhaustion rules, and deletion-aware post-commit accounting sentinels. |
| [cache-layers.md](cache-layers.md) | FSM-side read/write layering and ledger configuration reader/clone ownership: gatedScope → WriteSet → DerivedKeyStore → KeyStore → AttributeCache. |
| [preload.md](preload.md) | Preload contract: `plan.Coverage` declaration, `MirrorPreload`, `PredictedIndex` stale-detection, and the producer-owns-its-declaration rule. |
| [coverage-gate.md](coverage-gate.md) | The per-order coverage bits the FSM uses to gate every cache read against admission's declared `plan.Coverage`. |
| [skippable-orders.md](skippable-orders.md) | Continue-on-failure batches: the commit-or-discard `orderOverlayScope` / `skipSafeScope` rollback mechanism, the per-action skippable whitelist, and checker verification. |

## Related

- [Internal Antithesis assertions](../../../contributing/antithesis-assertions.md) — contract reports, commit milestones, local verification and campaign applicability.
- [Consensus](../consensus/) — Raft commit pipeline that feeds the FSM.
- [Attributes](../attributes/) — the cache the FSM reads through.
- [Admission](../admission/) — declares the `plan.Coverage` the FSM consumes.
