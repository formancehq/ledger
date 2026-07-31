# resources-logical-run-debt-reconverges — Logical run debt drains after maintenance recovers

| | |
|---|---|
| **Priority** | P1 |
| **Type** | Liveness |
| **Confidence** | High |
| **Property** | After demonstrated logical run debt, source publication quiescence, and fault recovery, ordinary maintenance leaves fewer than the configured threshold runs at every logical level within the recovery checkpoint's derived completed-pass budget. |
| **Invariant** | At the recovery checkpoint let `R0` be total manifest runs, `N` the threshold, `M` the compactions-per-pass limit, `Kmax=floor((R0-1)/(N-1))`, and `Pmax=ceil(Kmax/M)`. Use `Sometimes(recoveryPhase && everyLevelRunCount < N, "pit: logical run debt reconverges after maintenance recovery")`, plus `AlwaysOrUnreachable(!recoveryPhase || completedFaultFreePasses < Pmax || everyLevelRunCount < N, "pit: logical run debt respects derived recovery pass budget")`. The phase begins only after debt was observed and the builder reached the stopped workload's source head. Add `Reachable("pit: compaction resumed after injected maintenance stall")` as the replay/search anchor. |
| **Antithesis Angle** | Block or repeatedly fail compaction/tiering S3 work while the ticker publishes level-zero runs, then heal MinIO/disk faults, stop writes, wait for the builder's final publication, and snapshot the recovery budget. Kill/restart the replica at prepared-run and manifest-publication points; a restarted process begins a new checkpoint after readiness because its maintenance worker performs an immediate bounded pass. |
| **Why It Matters** | Persistent run debt increases per-query cursors, memory, snapshots, local bytes, and file descriptors. The static capacity inequality covers an uninterrupted schedule, not coalesced ticks, process crashes, or a shared maintenance goroutine stalled in remote I/O. |

## Resolved observability

`balancehistory.store.runs{level}` is an exact bounded-cardinality observation of
the current manifest when its OpenTelemetry callback is collected. It is not,
however, available to the current Antithesis workload:

- `tests/antithesis/k8s/cluster.yaml` has no `spec.monitoring.metrics`
  configuration or balance-history enablement;
- the Antithesis manifests contain no collector or queryable metrics backend;
- the workload receives Ledger gRPC addresses and Kubernetes control access,
  but no metrics endpoint; and
- no gRPC, HTTP, CLI, or debug API exposes the balance-history manifest or its
  per-level run counts.

The gauge remains useful for operational corroboration after the PIT campaign
enables metric export, but it cannot be the current workload oracle. Export
cadence and the callback's omission of empty levels also make backend samples a
weaker pass-bound oracle than one atomic store snapshot.

The property therefore requires a small test/debug diagnostic returning one
coherent snapshot with:

- process identity and manifest version;
- builder processed/source-head watermarks, so the checkpoint starts after the
  final level-zero publication;
- configured `N`, `M`, and maintenance interval;
- current total runs and `runsByLevel` (absent levels mean zero); and
- completed fault-free compaction-pass count.

The workload owns the recovery phase and recomputes its checkpoint after a
process restart. A SUT-side `Sometimes` alone is insufficient as the
authoritative oracle because process-local phase state would be lost across the
restart schedules this property targets. The proposed SUT-side `Reachable` is
still valuable search guidance and remains missing.

## Derived recovery budget

For a quiescent checkpoint with `R0 > 0` total manifest runs:

```text
Kmax = floor((R0 - 1) / (N - 1))
Pmax = ceil(Kmax / M)
```

Use zero for both values when `R0 == 0`. The phase guard normally makes
`R0 >= N` because at least one level must first have demonstrated debt.

One compaction consumes exactly `N` runs at one level and publishes one run at
the next, reducing total logical runs by `N-1`. At least one run remains when
`R0 > 0`, so no execution can require more than `Kmax` successful merges. In a
fault-free, publication-free phase, `runCompactions` performs up to `M` such
merges and exits early only when `CompactContext` reports that no level is
eligible. Consequently `Pmax` completed successful passes are a conservative
deterministic bound independent of the run distribution across levels.

`MaintenanceInterval` bounds when scheduled passes start: the worker runs one
bounded pass immediately on process start and then uses the configured ticker.
It does not bound cold hydration, checksum verification, Pebble streaming, or
manifest publication duration. The property must therefore count completed
fault-free passes, not fail at the synthetic wall-clock value
`Pmax * MaintenanceInterval`. The ordinary driver context remains the hang
guard if a pass never completes.

## Code evidence

- `internal/bootstrap/balance_history_config.go:197-225` rejects configurations whose nominal run-retirement capacity is below the ticker publication capacity.
- `internal/bootstrap/balance_history_config.go:26-32,81-104` defines the one-second, threshold-four, two-compactions defaults; the property reads the configured values rather than hard-coding them.
- `internal/bootstrap/balance_history_maintenance.go:114-178` runs an immediate bounded startup pass, then serially invokes at most `maxCompactionsPerPass` compactions on each maintenance tick and retries on later ticks after errors.
- `internal/storage/balancehistorystore/compact.go:31-82` serializes compaction, streams outside the mutation lock, publishes by CAS-like validation, and performs lease-aware GC afterward.
- `internal/storage/balancehistorystore/compact.go:123-143` defines debt as at least `N` runs at a logical level and selects exactly `N` inputs for one merge.
- `internal/bootstrap/balance_history_maintenance_test.go:135-173` proves later-tick retry after a compaction error; `balance_history_maintenance_test.go:232-297` deterministically proves the quiet-period per-level convergence predicate after ordinary bounded passes, but does not establish a wall-clock SLO or inject real process/S3/disk faults.
- `internal/storage/balancehistorystore/metrics.go:28-35,92-106` already exports run counts by level.
- `tests/antithesis/k8s/cluster.yaml` and `tests/antithesis/k8s/workload.yaml` contain neither metrics export/query wiring nor a diagnostic route for run state; the current Cluster also leaves opt-in PIT disabled.

## Instrumentation status

Existing status: **partial**. The per-level gauge and internal manifest provide
the monetary-store side of the oracle, but the current workload cannot consume
either and there is no completed-maintenance-pass counter. Add the coherent
test/debug snapshot described above for the authoritative workload assertion.
No existing SDK assertion marks a resumed compaction; the proposed `Reachable`
message is missing per `existing-assertions.md`.

## Open questions

None.

### Investigation Log

#### Can the workload read the OpenTelemetry run gauges, or is diagnostic/SUT-side instrumentation required?

- **Examined:** store metric registration and callback contents; bootstrap metric
  registration; server and operator monitoring configuration; the Antithesis
  Cluster, workload environment, service topology, and RBAC; workload clients;
  and repository searches for a manifest/run-count API or metrics query path.
- **Found:** `balancehistory.store.runs{level}` reports current non-empty manifest
  levels when collected. The current Antithesis deployment configures neither
  monitoring export nor a collector/query backend, does not enable PIT, and
  gives the workload no metrics address. Run state is internal-only.
- **Not found:** a workload-readable metrics endpoint, balance-history debug
  RPC, ledgerctl inspection command, completed maintenance-pass counter, or
  existing PIT SDK assertion.
- **Conclusion:** resolved: the current workload cannot read the gauge. Use a
  coherent test/debug snapshot for the authoritative cross-restart oracle and
  retain a SUT-side resumed-compaction `Reachable` only as search guidance.

#### How long may the fault-free, write-free recovery phase last?

- **Examined:** configuration defaults and capacity validation; maintenance
  startup/ticker/error behavior; exact compaction selection and publication;
  deterministic error-retry and convergence tests; CLI documentation; and the
  repository's rule against treating Antithesis as a latency benchmark.
- **Found:** every successful threshold-`N` merge removes `N-1` logical runs,
  every successful pass attempts at most `M` merges, and a no-op result proves
  no eligible level remains. This yields `Kmax=floor((R0-1)/(N-1))` and
  `Pmax=ceil(Kmax/M)` after publication quiescence. Restart schedules an
  immediate bounded pass before the ticker loop.
- **Not found:** any upper bound for the execution time of one real compaction;
  the interval controls scheduling cadence only. The deterministic test's ten
  quiet passes demonstrate convergence for its fixture but are not a product
  latency or universal pass-count contract.
- **Conclusion:** resolved with a completed-pass budget, not wall time. At the
  `Pmax`-th completed fault-free pass the level counts must already be below
  `N`; the standard driver deadline separately detects a pass that never
  completes.
