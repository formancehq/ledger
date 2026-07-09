# Checker

The checker (`internal/application/check`) is the integrity verification subsystem. It does **not** sit on the request path — it is invoked on demand via gRPC (`BucketService.CheckStore`) and produces a stream of `CheckStoreEvent`s describing any divergence between the persisted projections and what the audit chain says they should hold.

Two pages cover what the checker depends on and what it does.

## Documents

| Document | Description |
|----------|-------------|
| [audit-chain.md](audit-chain.md) | The BLAKE3-keyed audit hash chain — the only cryptographically-bound dataset in the system. Structure, lifecycle, tampering model. |
| [checker.md](checker.md) | The verification passes: how each persisted projection is re-derived from the audit chain and compared against what's stored. Error taxonomy. |

## Why a dedicated subsystem

The audit chain and the checker are tightly coupled by an explicit invariant: **every persisted dataset is either hash-bound (the audit log itself) or derivable from the hash-bound data, in which case the checker must verify it on every `Check()` run** ([CLAUDE.md invariant #8](../../../../../AGENTS.md)). This rule shapes design decisions across every other subsystem — what to refactor versus what to bind — so the checker's coverage is its own first-class architectural concern.

For new persisted state, first classify whether it is business truth, governance
truth, operational consensus state, or a rebuildable projection using
[Audit-Bound vs Technical State](../../audit-vs-technical-state.md). Only
business truth and business-visible projections belong in the checker; purely
operational state must remain unable to silently change ledger business results.

## Related

- [Consensus → global-log.md](../consensus/global-log.md) — what produces the audit entries the chain links.
- [Indexer → indexes.md](../indexer/indexes.md) — the index registry is a projection the checker verifies via `compareIndexes`.
- [Attributes](../attributes/) — volumes / metadata / reversion / idempotency projections the checker also verifies.
- [Chapters → lifecycle.md](../chapters/lifecycle.md) — sealing hash verification is one of the checker's passes.
