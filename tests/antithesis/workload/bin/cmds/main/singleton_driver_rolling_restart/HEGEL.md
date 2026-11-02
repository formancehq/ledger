# Operational driver generation boundaries

Each generation callback owns its Source and produces values only. RPCs run
after the callback returns, and every RunOnce error aborts that witness or round.
`internal.RunOperationalGeneration` emits a failing Antithesis assertion before
returning an engine error, including for the shared sentinel. A log-only caller
cannot silently skip the affected coverage. No Source crosses a goroutine boundary. Rolling-restart burst writes use a fresh
episode per tick, never one episode around the restart or the unbounded loop.

| Driver / helper | Classification |
| --- | --- |
| `internal.PreCommitSentinel` | Generate the witness amount with Hegel. Keep world-to-isolated-sentinel shape, COIN and Force. Retain Antithesis entropy for unique reference and destination identities on the shared cluster. |
| `singleton_driver_rolling_restart` | Generate each burst amount with Hegel; the shared sentinel also generates its amount. Keep pod order, leader transfer, timings and voter checks. |
| `singleton_driver_scaling_structured` | Generate fresh-commit and sentinel amounts. Keep the fixed 3 -> 5 -> 3 -> 7 -> 3 topology schedule and volume checks. |
| `singleton_driver_config_change` | Hegel selects a change from the existing configuration menus, plus the shared sentinel amount. Keep convergence predicates, rollout and follower checks. |
| `singleton_driver_quorum_recovery` | Hegel enters through the shared sentinel. No driver source change: victim selection, force-removal, cleanup and quorum checks remain operational. |
| `singleton_driver_signing_keys` | Draw all 32 Ed25519 seed bytes with Hegel and derive the coherent key pair. Keep Antithesis entropy for unique key IDs; retain register/list/signed-revoke/list assertions. |
| `eventually_quiesce_read` | Generate the users-pool destination and a positive uint64 witness amount before the bounded write retries. Keep ledger discovery, retry sequence and Antithesis idle menu {2, 5, 8}. No generation or write occurs during quiescence or the read probe. |
| `singleton_driver_backup` | Operational, unchanged. The fixed S3 target and backup result checks contain no business input generator. |
| `singleton_driver_incremental_backup` | Operational, unchanged. Preserve full-checkpoint precondition, two incrementals and sequence assertions. Do not add generated deltas without an independent restore oracle. |
| `singleton_driver_scaling` | Operational, unchanged. Antithesis retains topology targets, stepped moves and convergence checks. |
| `singleton_driver_scaling_chaos` | Operational, unchanged. Antithesis retains rapid topology changes, step count, pauses and recovery. |
| `parallel_driver_compaction` | Operational, unchanged. Antithesis chooses primary/secondary compaction; existing stats and world-account read checks remain. |
| `parallel_driver_barrier_monotonic` | Check-only, unchanged. Antithesis retains session count {1, 2, 8, 32} and leader-observation frequency; Barrier has no business parameters. Preserve cross-invocation high-water-mark ordering. |
| `parallel_driver_transfer_leadership` | Operational, unchanged. Choose an existing non-leader voter; retain transfer and recovery checks. |

These changes generate workload values only. They do not alter persisted formats,
backup semantics, service protocol revision, assertion identities or production
behavior. Local generation tests do not establish Kubernetes or cloud execution.
