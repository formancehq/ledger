# FSM

The deterministic state machine (`internal/infra/state`, `internal/infra/plan`, `internal/infra/preload`) that every node runs in lock-step with Raft commit. The apply path is CPU-bound and read-only against Pebble — its only inputs are the in-memory attribute cache, the preloaded `plan.Coverage` state, and the command itself.

## Documents

| Document | Description |
|----------|-------------|
| [deterministic-fsm.md](deterministic-fsm.md) | Deterministic FSM with generation-based caching, preloading, authoritative sequence-exhaustion rules, and accounting sentinels. |
| [cache-layers.md](cache-layers.md) | FSM-side read/write layering and ledger configuration reader/clone ownership: gatedScope → WriteSet → DerivedKeyStore → KeyStore → AttributeCache. |
| [preload.md](preload.md) | Preload contract: `plan.Coverage` declaration, `MirrorPreload`, `PredictedIndex` stale-detection, and the producer-owns-its-declaration rule. |
| [coverage-gate.md](coverage-gate.md) | The per-order coverage bits the FSM uses to gate every cache read against admission's declared `plan.Coverage`. |
| [skippable-orders.md](skippable-orders.md) | Continue-on-failure batches: the commit-or-discard `orderOverlayScope` / `skipSafeScope` rollback mechanism, the per-action skippable whitelist, and checker verification. |

## Related

- [Internal Antithesis assertions](../../../contributing/antithesis-assertions.md) — contract reports, commit milestones, local verification and campaign applicability.
- [Consensus](../consensus/) — Raft commit pipeline that feeds the FSM.
- [Attributes](../attributes/) — the cache the FSM reads through.
- [Admission](../admission/) — declares the `plan.Coverage` the FSM consumes.

## Ephemeral account lifecycle

An `EPHEMERAL` account is current state only while at least one of its
asset/color volumes is non-zero. Admission expands every touched account into a
closed set of its persisted volume and metadata keys. At the proposal boundary
the FSM evaluates that set through the proposal-wide coverage gate. When every
volume is zero, the same primary-store batch deletes all account volumes and
metadata and persists `LedgerLog.purged_accounts`.

The rule does not apply to `NORMAL` or `TRANSIENT` accounts. Historical
transactions and their address/source/destination mappings are not account-owned
current state and survive the purge. Re-funding the address therefore creates a
fresh current-state incarnation without restoring old metadata.
