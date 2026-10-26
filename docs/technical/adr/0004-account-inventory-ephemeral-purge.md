# 0004 — Do not adopt the tested account inventory for EPHEMERAL purge

**Status:** Rejected (2026-10-23). The measured implementation is preserved in the closed [code PR #2366](https://github.com/formancehq/ledger/pull/2366). This decision covers that implementation and workload, not every possible account inventory design.

## Context

When an EPHEMERAL account's final volume reaches zero, Ledger must remove its current volumes and metadata. Raft apply cannot read Pebble to discover attributes that admission missed. The experiment tested whether a persisted per-account inventory could make preload completeness checkable while preserving deterministic apply.

The proposed inventory also made account materialization explicit. It recorded a creation date that survived cache rotation and ordinary updates, and changed after the account was fully removed and created again.

## Decision

Do not merge the tested per-account inventory implementation. On the corrected ACME configuration it reduced successful throughput by 6.9% for `world_to_bank` and 31.8% for `any_unbounded_to_any`, and it introduced client-visible failures under load. The available data does not identify which part of the implementation caused most of the loss. No replacement purge design is selected by this ADR.

## Design evaluated

The experimental branch `codex/en-2036-incarnation` added `SubAttrAccountInventory` (`0x0D`) under the primary attribute key space. An entry existed for every account with at least one persisted volume or metadata key, regardless of account type. Its `AccountInventoryState` held `birth_index`, `inventory_revision`, `volume_count`, `metadata_count`, and `created_at`.

`WriteSet.writeAccountInventories` computed deltas from final changes to the persistent key set. It sorted accounts before applying those deltas and wrote one inventory update per changed account. Value-only changes did not advance the inventory revision. When both counts reached zero, the FSM removed the persisted inventory and tombstoned its cache entry. A later materialization received a new `birth_index` and `created_at`; ordinary updates preserved both. Cache snapshots and restore/rebuild handled the inventory.

Admission added inventory coverage for every touched account. For accounts that could be EPHEMERAL, it enumerated persisted volume and metadata rows in one read snapshot, added them to the preload, and compared the enumerated counts with the inventory. An account-type mutation could reclassify existing accounts, so admission also scanned ledger-scoped volume and metadata rows for that case. The execution plan carried the cache generation, the observed inventory for purge candidates, and an account-type digest for every touched ledger.

The builder checked that the cache generation had not changed during build and submit. Before apply, the FSM compared the generation and the observed account types and inventories with its state. A mismatch returned `STALE_PROPOSAL`. Once a proposal passed those checks, the FSM could purge the account's current primary rows without reading Pebble. The existing purge signal removed current secondary-index entries while leaving audit and historical transaction mappings available.

This made account existence explicit, but all account types paid for inventory maintenance. All proposals touching an account also carried a ledger-type observation, even when no EPHEMERAL account was involved.

## Benchmark method

The runtime comparison used base commit `d1ea475c4` and experimental commit `e1a0952f8`. Each ACME run used a fresh ledger and volumes. k6 sent bulk requests of 50 transactions with a one-minute ramp to 100 VUs, five minutes at 100 VUs, and a one-minute ramp down. The reported throughput divides successful transactions by the complete seven-minute run, including ramps. Base and experiment used the same configuration within each pair.

The initial and repeated pairs used 6 CPU and 12 GiB. After those runs, we found that the benchmark file had placed `auth`, `pebble`, `raft`, and `cache` under `Cluster.spec.config`. The ACME CRD expects these fields directly under `Cluster.spec`, so those settings were likely ignored. The final pair corrected the manifest and used Maxence's node and Pebble settings: one replica on a dedicated `ledger-m8a` node, 8 CPU, 16 GiB, io2 volumes, a 512 MiB Pebble memtable, a 4 GiB Pebble cache, L0 stop threshold 24, four concurrent compactions, and a cache rotation threshold of 1,000 Raft entries. The deployed Cluster objects were checked for these values. The corrected benchmark file was committed as `e25740577` on the experimental branch.

| Configuration and scenario | Base tx/s | Experiment tx/s | Change | Experiment HTTP errors |
| --- | ---: | ---: | ---: | ---: |
| Initial, `world_to_bank` | 61,152 | 59,647 | -2.5% | 1,325 / 502,369 (0.26%) |
| Initial, `any_unbounded_to_any` | 28,673 | 18,828 | -34.3% | 2,525 / 160,684 (1.57%) |
| Repeat, `world_to_bank` | 52,321 | 48,400 | -7.5% | 1,054 / 407,617 (0.26%) |
| Repeat, `any_unbounded_to_any` | 24,627 | 17,972 | -27.0% | 2,790 / 153,760 (1.81%) |
| Corrected, `world_to_bank` | 119,830 | 111,539 | **-6.9%** | 1,366 / 938,304 (0.14%) |
| Corrected, `any_unbounded_to_any` | 55,029 | 37,527 | **-31.8%** | 4,826 / 320,053 (1.50%) |

With the corrected configuration, HTTP p95 rose from **54 to 59 ms** for `world_to_bank` and from **120 to 176 ms** for `any_unbounded_to_any`. The base had 3 and 0 HTTP errors in those runs, respectively. The higher absolute throughput under the corrected configuration does not remove the relative regression.

## Interpretation and limits

The goal was complete, safe purge without a material cost to ordinary writes. The corrected pair lost about **7%** throughput on `world_to_bank` and **32%** on `any_unbounded_to_any`, and exposed failures to clients under load. This implementation did not meet that goal.

The corrected `world_to_bank` errors were HTTP 503 `STALE_PROPOSAL`. A short diagnostic also found `STALE_PROPOSAL` in `any_unbounded_to_any`, but the regular script did not log response bodies. We therefore did not classify every error in its final run. The same error can result from cache-generation drift, changed account types or inventories, predicted Raft index mismatch, or cache-epoch mismatch. The first runs did not measure which check produced most rejections; the follow-up below adds stage counters.

The benchmark compares the **whole feature** with the base. It does not isolate the physical inventory write from admission scans, preload construction, validation, and work lost on rejected proposals. The observed error percentages alone are too small to explain the full throughput loss. The inventory mechanism and its guards remain the leading suspects, but attributing the 32% loss to Pebble catalog writes alone would exceed the evidence.

## Costs identified before the follow-up

Code inspection found costs that can be separated in a follow-up experiment. `world_to_bank` repeatedly updates the same `world` and `bank` volume keys. `writeAccountInventories` changes an inventory only when the persistent key set changes, so the steady-state transactions in this scenario do not imply an inventory write. Their regression also includes the admission read snapshot, account extraction, inventory coverage and preload, ledger-type observation, and FSM checks. A local single-order microbenchmark of `expandAccountLifecycleCoverage` measured about 2.4 µs and 62 allocations for a normal account, and 6.7 µs and 139 allocations for an ephemeral account with one asset. These are local admission costs, not ACME throughput estimates.

`any_unbounded_to_any` creates a new source and destination for every transaction. It therefore requires two new inventories per successful transaction in this design, plus their cache entries and preloads. In a 50-order bulk, admission also rebuilds account sets from coverage in several passes. For ephemeral candidates, its inventory-count validation walks the aggregate coverage separately for each candidate, making that part quadratic in the number of distinct accounts in the bulk. The benchmark does not establish how much each cost contributed.

Candidates for controlled tests were: (1) reuse account ownership information collected during coverage construction and count persisted keys in one pass; (2) avoid full ledger serialization and hashing per proposal when only account-type rules matter, while still detecting concurrent type changes; and (3) measure the inventory write path separately from admission and stale-proposal guards. Any guard change must preserve deterministic apply, complete preload coverage, and rejection of a normal-to-ephemeral reclassification. Count `STALE_PROPOSAL` by its precise rejection point before tuning the cache-generation rule.

Each table row contains one run per version. Variation between the first two pairs prevents a precise percentage or confidence interval. The direction of the regression repeats across all three pairs. The benchmark operator reported failure after k6 completed because its Grafana snapshot URL was unset; k6 summaries supplied the figures in this report. Temporary ACME benchmark resources, private GHCR images, and local benchmark files were removed after the report was recorded.

## Follow-up: reduce admission passes

A follow-up branch based on `e25740577` tested the first candidate. Commit `731de8dc9` reused the account map collected during coverage construction, added accounts found by the account-type mutation scan, and counted covered attributes in one pass. It also recorded admission lifecycle and FSM inventory durations. The optimization retained the inventory key, the preload rules, and the stale-proposal guards.

Local admission benchmarks showed that the change reduced allocations for 50 normal accounts from 1,387 to 887 with a shared asset and from 1,451 to 951 with distinct assets. For 50 EPHEMERAL accounts with four assets each, the median time fell from 874 to 317 µs and allocations from 36,832 to 6,034. These results measure admission in isolation. They do not predict throughput under Raft and Pebble load.

We then repeated ACME runs with the corrected 8 CPU configuration. Each row used a fresh cluster and ledger, the same `bulk50` k6 schedule described above, and the same dedicated node. `main` was `d1ea475c4`; the original experiment was `e25740577`; the optimized image contained `731de8dc9`. Only the optimized image contained the added timing instruments, so the original-to-optimized comparison also includes their cost.

| Scenario and run | `main` tx/s | Original tx/s | Optimized tx/s | Optimized vs `main` | Optimized HTTP errors |
| --- | ---: | ---: | ---: | ---: | ---: |
| `world_to_bank`, A1 | 115,999 | 110,404 | 112,376 | -3.1% | 1,241 / 945,218 (0.13%) |
| `world_to_bank`, A2 | — | 110,760 | 112,988 | — | 1,178 / 950,293 (0.12%) |
| `any_unbounded_to_any`, A1 | 55,025 | 37,462 | 37,068 | -32.6% | 4,474 / 315,848 (1.42%) |

The optimization improved `world_to_bank` by 1.8% and 2.0% across the two pairs, but it remained below `main` and still returned errors. It did not improve `any_unbounded_to_any` in the single follow-up pair: the optimized run was 1.0% below the original experiment. Its HTTP p95 was 177 ms, compared with 177 ms for the original and 119 ms for `main`. The `main` runs returned no HTTP errors.

In the optimized `any_unbounded_to_any` run, the FSM inventory timing histogram averaged about 0.11 ms per proposal over a three-minute window. This measures direct inventory processing, not secondary effects on Pebble. A CPU profile of the original run placed substantial time in `plan.resolveCoverage` and Pebble `DB.Get`; those are cumulative profile samples, so they cannot be added as disjoint percentages. The new inventory key has no Bloom filter in the tested code, making Pebble absence checks for new accounts a plausible additional cost. The profile and the single A1 pair do not isolate that cost. The tested admission optimization therefore does not change the decision.

Commit `3df037cd0` added counters for `STALE_PROPOSAL` returned by admission's lifecycle expansion, preload build, and proposal stages, plus a counter for the FSM guard's rejection reason. A separate three-minute `any_unbounded_to_any` diagnostic used that image with admission metrics enabled. Its 1,346 HTTP errors matched exactly 9 lifecycle expansion, 32 preload build, and 1,305 proposal-stage stale rejections. The FSM guard counter had no samples. The dominant errors therefore arose before FSM apply. In the proposal stage, `Builder.AcquireProposalGuard` checks whether the cache generation changed after the lifecycle snapshot; that is the likely source of the 1,305 rejections, although this counter does not split guard acquisition from synchronous proposal submission. This diagnostic run used a shorter schedule and extra metrics, so its throughput is not part of the comparison table.

The corresponding three-minute `world_to_bank` diagnostic had 398 HTTP errors. Admission counted 43 lifecycle expansion, 1 preload build, and 354 proposal-stage stale rejections, again totaling 398. Its FSM guard counter also had no samples. These runs show that the client-visible failures in the diagnostic images occurred before FSM apply. They do not separate a generation change at guard acquisition from any other synchronous proposal-stage rejection. The stage counters were added after the seven-minute performance images were built, so they do not classify each error in the performance table directly.

## Consequences and revisit conditions

The experiment demonstrated an explicit account inventory and creation date, along with a way to reject a stale or incomplete purge proposal. Neither the inventory key nor its `created_at` field becomes part of the accepted Ledger architecture through this ADR. The existing code path remains the baseline until a separate design is selected.

Revisit this choice if a candidate can keep purge completeness without the measured client errors and throughput cost. Split the proposal-stage `STALE_PROPOSAL` counter by exact return path, then test whether cache-generation validation can be narrowed while preserving safe purge. Measure inventory preload reads and writes separately, including the effect of a Bloom filter for inventory keys. Repeat paired benchmarks with enough runs to measure variability before accepting a new design.

`bash scripts/agent-check` passed on the experimental branch. A broader `agent-check-pr` run passed normalization, lint, compilation, and affected product race tests before an unrelated timeout in `scripts/internal/testenv`. That test passed five focused race reruns; the aggregate command did not pass.
