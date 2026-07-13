# Counters and Storage Schema

This page documents the projections the usagebuilder materialises: what each counter counts, how the FSM plumbs the source data through the log payload, how the counters are keyed in the usagestore, and how the checker verifies them.

For the pipeline that populates these keys, see [usagebuilder.md](usagebuilder.md).

## Counter Catalogue

Every counter is keyed by `(ledger, counter_id)` in the usagestore (`internal/storage/usagestore/keys.go`) — one byte per counter, stable on-disk identifiers, never renumbered.

| ID | Name | Source | Delta per event |
|----|------|--------|-----------------|
| `0x01` | `CounterPosting` | `Transaction.Postings` (`CreatedTransaction`) + `RevertTransaction.Postings` (`RevertedTransaction`) | `+len(Postings)` per applicable log |
| `0x02` | `CounterRevert` | `RevertTransactionOrder` unmarshalled from `AuditItem.SerializedOrder` (covers both direct and mirror-ingested reverts) | `+1` per order |
| `0x03` | `CounterNumscriptExecution` | `CreateTransactionOrder` with a non-empty `Script.Plain` or a non-nil `NumscriptReference` | `+1` per order |
| `0x04` | `CounterReference` | `CreateTransactionOrder.Reference != ""` | `+1` per order |
| `0x05` | `CounterEphemeralEvicted` | `len(LedgerLog.EphemeralVolumes)` per log — the pure-ephemeral tuples (new + purged same log) | `+len(EphemeralVolumes)` |
| `0x06` | `CounterTransientUsed` | `len(AppliedProposal.TransientVolumes[ledger].Volumes)` — batch-level, keyed by audit sequence | `+len(TransientVolumes[ledger])` per proposal |
| `0x07` | `CounterVolume` | `len(LedgerLog.NewKeptVolumes) − len(LedgerLog.PurgedVolumes)` per log | net change in live volume-key cardinality |

Missing keys read as `0`. Every counter clamps at zero on underflow via `applyDelta` — a subsystem bug that emits a spurious `−1` cannot drive the counter into an out-of-band representation.

## Template Usage

Per-Numscript-template records live under a separate key shape:

```
[usagestore prefix][ledger 64B][template_name]  → TemplateUsage { fixed64 count; Timestamp last_used }
```

Populated when a `CreateTransactionOrder` carries a non-nil `NumscriptReference`. `last_used` is the order's `Timestamp` (deterministic — same on every replica). Multiple invocations in the same batch fold via max on `last_used` and sum on `count`.

Read path: `GET /v3/{ledger}/numscripts/{name}/usage` (`internal/adapter/http/handlers_get_numscript_usage.go`) — returns the persisted `TemplateUsage` proto or a zero-valued one when the template has never been invoked (not a 404).

## The Log-Payload Contract

Six of the seven counters plus template usage need FSM-side annotations on the log or the audit chain. Two categories:

- **Straight from the raw order** (posting, revert, numscript-exec, reference, template usage): no FSM enrichment needed — the fields are already on `raftcmdpb.CreateTransactionOrder` / `RevertTransactionOrder`, which the usagebuilder unmarshalls from `AuditItem.SerializedOrder`.
- **Volume annotations on the log** (ephemeral evicted, volume count): the FSM computes three DISJOINT per-log lists during `WriteSet.Merge` and injects them into the `LedgerLog` message.

### `LedgerLog` volume annotations

`misc/proto/common.proto` — three disjoint fields on `LedgerLog`:

```protobuf
message LedgerLog {
  LedgerLogPayload data       = 1;
  Timestamp        date       = 2;
  fixed64          id         = 3;
  repeated TouchedVolume purged_volumes    = 4;  // DRAINING only
  repeated TouchedVolume new_kept_volumes  = 5;  // new + survives
  repeated TouchedVolume ephemeral_volumes = 6;  // new + purged same log
}
```

The three sets partition every volume update the log touched into DISJOINT categories:

| Category | Prior value | Post-commit state | Field |
|----------|-------------|-------------------|-------|
| Draining | non-zero in Pebble | evicted (zero balance) | `purged_volumes` |
| New + kept | undefined or zero placeholder | persisted | `new_kept_volumes` |
| Pure ephemeral | undefined or zero placeholder | evicted (zero balance) | `ephemeral_volumes` |
| Normal update | defined + non-zero | still persisted (updated value) | (none — no annotation needed) |
| Transient | any | never persisted | (none — carried on `AppliedProposal.TransientVolumes` at batch level) |

### Why disjoint (and not overlapping)

An earlier encoding included ephemeral tuples in BOTH `purged_volumes` (as "evicted") and `new_volumes` (as "newly created"). Every ephemeral (account, asset) tuple paid its wire cost twice. On workloads with high ephemeral throughput (payout fan-out, escrow pass-throughs — 100+ ephemeral accounts per transaction is normal), the doubling adds a few MB/s of WAL / audit-chain growth for no functional benefit — the ephemeral net delta on `VolumeCount` is +0.

The disjoint encoding pays exactly `len(ephemeral)` per log instead of `2 × len(ephemeral)`. The three lists are still complete: consumers that need "everything evicted from Pebble" take the union (see [indexer consumer](#indexer-consumer)); the volume-count formula becomes clean subtraction (`new_kept − purged`) with ephemeral contributing zero.

### FSM emission

`internal/infra/state/write_set.go` computes the three sets during `Merge`:

1. `partitionVolumes(volumeUpdates)` (existing) yields `partResult.{kept, purged, transient}`.
2. `splitPurged(partResult.purged)` (new, in `write_set_new_volumes.go`) partitions `purged` further into:
   - **ephemeral**: `!Old.IsDefined() || isVolumePreloadZero(Old.Value())` — the key had no prior state, was touched, immediately purged.
   - **draining**: `Old.IsDefined() && !isVolumePreloadZero(Old.Value())` — had a prior non-zero balance, drained to zero, evicted.
3. `makeNewKeptKeySet(partResult.kept)` (new) yields the subset of `kept` where `Old` was undefined / zero-placeholder — i.e. new persistent volumes.
4. `buildTouchedByLog(volumes.Slots(), setX)` (new, generalised from the previous `buildPurgedByLog`) intersects the per-order touched-volume tracking with each of the three sets to produce the per-log annotation lists. Deduplication + deterministic sort by (account, asset) keeps the log payload byte-identical across nodes.

The three lists are injected into each `LedgerLog` inside the same `createdLogs` build loop that also injects `purged_volumes`. Note the audit hash chain does **not** cover `LedgerLog` content — it binds the audit header plus each item's order index, log sequence, and serialized order (see [Checker consumer](#checker-consumer) for what this means for tamper detection of the derived counters).

### The preload contract this depends on

The classification "new vs existing" is decided by the preloaded prior value at merge time — specifically `Update.Old.IsDefined()` combined with the zero-placeholder check. This is safe **because volume preload is structurally required by the FSM**: balance checks, Uint256 arithmetic and numscript resolution all read the current volume value, so admission has to preload every touched key. That contract is documented as invariant #6 in AGENTS.md.

The comparable metadata preload was removed opportunistically once the indexer no longer needed it — the corresponding `MetadataCount` counter had to be dropped (see the EN-1420 commit) because `Old.IsDefined()` no longer distinguished "new key" from "overwrite" on the metadata merge. The volume analog holds because the FSM cannot function without those old values.

## Usagestore Layout

```
[template_prefix][ledger 64B][template_name]   → TemplateUsage proto
[counter_prefix][ledger 64B][counter_id 1B]    → uint64 BE
[0xFE][0x01]                                    → progress cursor (uint64 BE, last consumed audit sequence)
```

Full keyspace conventions in `internal/storage/usagestore/keys.go`. The `[ledger 64B]` block is zero-padded fixed-width (`dal.LedgerNameFixedSize`) so the comparer can extract a per-ledger prefix for bloom-filter scoping — same technique as the readstore.

## Consumers

### API — `GetLedgerStats` reader

`internal/application/ctrl/controller_default.go` — opens a single `usagestore.Snapshot` and routes all seven counter Gets through it. See [usagebuilder.md § Snapshot on the reader side](usagebuilder.md#snapshot-on-the-reader-side).

### API — template usage endpoint

`internal/adapter/http/handlers_get_numscript_usage.go` → `ctrl.Controller.GetTemplateUsage` → `usagestore.GetTemplateUsage`. Single point-read against the live store (no snapshot needed for a single-value query).

### Indexer consumer

`internal/application/indexbuilder/applied_proposal_sync.go`'s `extractPurgedVolumes` returns the UNION of `LedgerLog.PurgedVolumes ∪ LedgerLog.EphemeralVolumes` because both categories share the same downstream treatment (Pebble entry gone → skip acct→tx mapping). The protowire fast path (`protowire_postings.go`) parses field 6 alongside field 4 and exposes `GetEphemeralVolumes` on `parsedLog`.

### Checker consumer

`compareExclusionProjections` (`internal/application/check/checker.go`) accumulates `PurgedVolumes` and `EphemeralVolumes` from every log into the stored projection set, then compares against the exclusion set derived by replaying the audit chain (`AppliedProposal.TransientVolumes` union). Both eviction lists have identical semantics for the checker — the split is a pure log-payload compaction, not an invariant change. This pass verifies the exclusion projection *in the primary store* — the set the indexbuilder consumes.

### Why the usagestore counters are not a checker target (design decision, not a gap)

The checker (invariant #8) verifies projections persisted **in the primary Pebble store** — the store it operates on: `Volume`, `Metadata`, `Transaction`, `Reference`, `Boundary`, idempotency outcomes, the index registry, and the exclusion projection above. The usagestore is a **distinct, peer secondary Pebble instance** (`<data-dir>/usage/`) holding a *derived cache* of counters, rebuildable from the audit chain via the automatic boot/tick fold (and `usagestore.Reset()` on rollback detection). Its authoritativeness is explicitly bounded by "eventually consistent with the FSM".

Consequently, the three volume-annotation categories are deliberately **not** given a dedicated usagestore checker pass:

- `NewKeptVolumes` has **no** primary-store consumer — it feeds only `CounterVolume` in the usagestore.
- The primary-store-relevant signal, the exclusion set `PurgedVolumes ∪ EphemeralVolumes` consumed by the indexbuilder, **is** already verified by `compareExclusionProjections` above. The indexbuilder consumes the union and is indifferent to the Purged-vs-Ephemeral split, so the union is the correct verification granularity for the primary store.

Corruption of a usagestore counter is therefore a *rebuild*, not an integrity incident: the recovery contract is "drop and replay from the audit chain", and the audit chain itself **is** cryptographically verified (`verifyAuditHashChain`). Serving a derived, rebuildable value through `GetLedgerStats` / `GetTemplateUsage` does not make it authoritative primary-store state. Extending audit-derived tamper coverage to the usagestore counters (which would require threading a new-volume collector through the shared `internal/domain/replay` package, also used by backup restore) is a separately-scoped effort tracked under EN-1422 — not a prerequisite for this subsystem.

## Metrics

Registered in `misc/devenv/monitoring-dashboards/jsonnet/lib/metrics.libsonnet`:

| Metric | Description |
|--------|-------------|
| `usage.builder.last_indexed_sequence` | Highest audit sequence the builder has committed for this replica. |
| `usage.builder.audit_last_sequence` | Highest audit sequence present in Pebble on this replica. |
| `usage.builder.lag` | Difference between the two (indicator of the eventual-consistency window). |
| `usagestore.level.bytes` / `memtable.bytes` / `cache.hits` / `cache.misses` | Pebble-internal metrics for the usagestore instance (parallel to the readindex namespace). |

The three progress gauges are registered through `tailworker.RegisterTailGauges` — the same helper the audit indexer uses — so the naming pattern (`{ns}.last_indexed_sequence`, `{ns}.{source}_last_sequence`, `{ns}.lag`) stays consistent across every tail-worker subsystem.
