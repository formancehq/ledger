# integrity-atomic-publication-restart — Restart exposes either the old or the complete new history publication

## Catalog entry

| | |
|---|---|
| **Priority** | P0 |
| **Type** | Safety |
| **Property** | After a crash at any publication point, the latest visible manifest either remains at the previous complete prefix or references the complete newly published run; it never exposes a torn run/manifest pair. |
| **Invariant** | `Always(complete_manifest, "pit: restart exposes no torn history publication")`, where every manifest-referenced run has its metadata and complete checksummed records and the latest pointer names that exact manifest. `Always` matches the atomicity guarantee at every observation. |
| **Antithesis Angle** | Terminate the process around NoSync batch commit and WAL barriers, including after the API has observed a new local view but before the periodic durability sync. Repeated restart explores both allowed suffix-loss outcomes. |
| **Why It Matters** | A torn publication could pair an advanced source cursor with missing monetary records, causing permanent silent data loss because the builder would resume after the omitted prefix. |
| **Confidence** | High |
| **Focus** | Data Integrity |

**Open Questions:**

- None for atomic visibility: repeated hard process kills may recover either
  complete old or complete new publication and must reject torn state. Proven
  NoSync byte loss is owned by `recovery-unsynced-suffix-replays` and remains
  conditional on the environment's storage fault semantics.

## Evidence trail

- `internal/storage/balancehistorystore/publish.go:281-285` explicitly states the atomic visibility and asynchronous durability contract.
- `internal/storage/balancehistorystore/publish.go:369-417` stages run records, optional run metadata, immutable manifest, and latest-manifest pointer in one Pebble batch, then commits with `pebble.NoSync`.
- `internal/storage/balancehistorystore/store.go:176-188` serializes `SyncWAL` with `mutationMu` and uses Pebble's synchronous WAL barrier.
- `internal/application/balancehistory/builder.go:437-462` advances `lastDurableAuditSequence` only after the barrier succeeds; failures stay unhealthy and are retried.
- `internal/application/balancehistory/builder.go:510-540` forces a durability barrier at the caught-up boot head before readiness.
- `internal/storage/balancehistorystore/hardening_test.go:287-347` proves uncommitted publication bytes and durable orphan runs do not advance the manifest.
- `internal/storage/balancehistorystore/hardening_test.go:349-387` restores an explicitly synced prefix, replays the lost NoSync suffix, and obtains the same logical digest.

## Failure scenario to explore

1. Drive distinct transactions whose expected aggregate changes at every log.
2. Repeatedly kill the target replica immediately after local publication, around periodic WAL sync, and during shutdown.
3. Restart the same persistent volume.
4. Inspect the first readable manifest indirectly through the PIT trailer and query results.
5. Accept loss only as a suffix that the builder replays from the primary audit. Reject advanced watermarks with missing effects, missing manifests/runs, or a post-replay result different from the oracle.

## Instrumentation status

- **Existing SDK instrumentation:** missing for PIT. Existing Raft/WAL assertions cover primary durability but do not describe the peer history batch.
- **Missing SUT-side guidance:** `Always` after `readManifest` during startup that a successful verification found every referenced run; `Reachable` markers for publication committed, durability barrier completed, and startup replayed an unsynced suffix would materially guide scheduling.
- **Workload-side check:** missing. The workload needs returned watermark/result snapshots before and after each hard restart.

## Boundary clarification

The property does not require every NoSync publication to survive. Losing a suffix is allowed because the audit remains authoritative. The violation is a non-prefix or internally torn recovery state, or failure to replay the lost suffix once the source is available.
