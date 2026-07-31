---
sut_path: /Users/flemzord/Developer/Formance/ledger
commit: fb3f9f8334c7fbbcc27c2c211c77da36762e6aaf
updated: 2026-07-31
external_references: []
---

# Ledger v3 PIT system-under-test analysis

## Summary

The point-in-time balance feature is a replica-local, asynchronous monetary
projection over the hash-chained audit and resolved ledger logs. It adds no
read or write to the deterministic FSM path. Each replica independently builds
immutable temporal runs, serves pinned views, compacts them, optionally tiers
them to a node-owned S3 namespace, and reclaims remote orphans.

The existing Antithesis topology is a good physical fit: three Ledger replicas,
MinIO, an operator and a stateful workload already exist. The feature is not
currently tested there because it is opt-in and no Antithesis manifest sets
`BALANCE_HISTORY_ENABLED`.

Antithesis should focus on cross-process and cross-service transitions that the
substantial deterministic suite cannot cover: kills between NoSync publication
and WAL sync, follower-local projection lag, rollback/restore, archive/purge
races, asymmetric MinIO failures, pinned reads during compaction/tiering, and
remote delete-before-ack.

## Product contract

The public feature answers aggregate balance and volume queries at arbitrary
effective or insertion timestamps. It intentionally historizes only monetary
effects:

- resolved postings and their input/output volumes;
- normal and `at_effective_date` reversals;
- effective-time and insertion-time axes.

Metadata, schemas, account types, Numscript definitions and other non-monetary
attributes are current-state-only. Metadata filters select accounts using the
current read store and then aggregate historical monetary effects. This is an
accepted product semantic, not a missing implementation.

From a user's perspective, the critical failures are:

- a successful response with an omitted or duplicated monetary effect;
- a six-month-old answer that changes on the wrong time axis;
- a query silently falling back to current balances;
- an old ledger incarnation leaking into a recreated ledger name;
- permanent PIT unavailability after a recoverable fault;
- PIT maintenance delaying acknowledged Ledger writes or normal live reads.

## Architecture and data flow

### Synchronous write path

```text
client Apply
  -> admission and preload
  -> Raft proposal
  -> deterministic FSM
  -> authoritative cache + primary Pebble
  -> acknowledged write response
```

The delivered PIT runtime does not add a fifth synchronous notification target.
It receives no FSM wake channel and polls every 200 ms. This structural
separation is central to the claim that PIT has no direct synchronous write-path
cost.

### Asynchronous projection path

```text
per-replica 200 ms ticker
  -> BalanceHistory Builder
  -> consistent hot or hot+cold source snapshot
  -> consecutive AuditEntry
  -> matching AuditItem rows
  -> referenced resolved LedgerLog rows
  -> monetary reducer
  -> effective/insertion effects
  -> immutable local run
  -> atomic local manifest publication
```

The reducer consumes postings after Numscript, mirror and reversal resolution.
Each posting produces an output effect for the source and an input effect for
the destination, preserving ledger incarnation, account, asset base, precision
and color.

The source is proposal-aware. A missing audit, audit item, referenced log or
sequence prefix must prevent cursor advancement. Failed/no-log proposals can
advance the audit watermark without advancing the log watermark.

### Historical read path

```text
HTTP or gRPC AggregateVolumes
  -> parse exactly one selector: live, checkpoint or PIT
  -> route: local linearizable, leader-forwarded or stale-local
  -> resolve ledger name to numeric incarnation ID
  -> pin primary snapshot and current log head
  -> feature/allowlist/readiness gate
  -> wait for required history log watermark
  -> pin immutable history manifest, Pebble snapshot and leases
  -> aggregate global summaries or per-account history
  -> return result plus PointInTimeView trailer/header
```

Even without an explicit `minLogSequence`, the controller raises the requirement
to the log head observed in its primary snapshot. A successful request is
therefore claimed to reflect a coherent primary/history boundary, not an
arbitrarily old local projection.

Metadata filters add a current read-store snapshot to the historical view.
Direct address and address-prefix filters can use historical keys alone.

## State and persistence model

| State | Authority and durability |
|---|---|
| Monetary authority | Hash-chained primary `AuditEntry` plus business-intent `AuditItem` and resolved logs. |
| PIT projection | Replica-local peer Pebble DB under `<data-dir>/balance-history` by default. |
| Progress | Latest immutable manifest carries audit/log watermarks, ending audit hash, logical digest and reducer state. |
| Temporal data | Immutable runs materialize effective/insertion axes and volume/account/asset summary scopes. |
| Failure gate | Synchronously persisted `SOURCE_MISSING`, `QUARANTINED` or `REBUILDING` marker. |
| View | Cloned manifest, Pebble snapshot, generation and manifest/run/cold-cache leases. |
| Cold objects | Content-addressed parts in `<cluster>/balance-history/nodes/node-N/runs/<sha256>`. |
| Remote GC | Durable cursor, scan cycle, candidate queue, destination binding and archive mutation epoch. |

Run records, run metadata, the immutable manifest and latest pointer are written
in one Pebble batch. The batch is intentionally `NoSync`. A periodic and
shutdown WAL barrier bounds the projection suffix that may be lost; the primary
audit remains authoritative and should replay that suffix after restart.

## Concurrency model

Three production goroutines operate independently:

- one single-threaded builder/tailer;
- one verifier serialized by its guard;
- one maintenance worker that runs compaction, tiering and remote GC
  sequentially with respect to itself.

They overlap with each other and with API reads. Correctness relies on immutable
snapshots, generation invalidation, leases and the lock hierarchy:

- `mutationMu` for manifest/failure/binding/remote-GC/WAL mutations;
- `compactionMu` for one compaction;
- `waitMu` for watermark notifications;
- `leaseMu` for manifest, run and prepared-run references;
- `archiveGate` around tier/remote inventory phases.

Compaction streams outside `mutationMu`, then rechecks generation and exact
inputs before publication. Tiering uploads and verifies outside the mutation
lock, rechecks the manifest, publishes remote references while retaining local
bytes, and removes local bytes only without a run lease. Remote deletion captures
roots, releases the mutation lock during S3 deletion, then durably acknowledges
the idempotent delete.

High-value kill/interleaving points are:

1. after history publication but before WAL sync;
2. after compaction output preparation but before manifest publication;
3. after archive epoch advance but before upload;
4. after upload but before manifest CAS;
5. after remote delete but before durable acknowledgement;
6. during a pinned cold read while compaction/tiering/GC proceeds;
7. after primary rollback/restore but before the builder's next reconciliation
   tick.

## Failure, recovery and liveness

Process-local readiness starts closed. Boot is claimed to reconcile the local
manifest with a freshly read authoritative head, backfill, force the history
WAL, certify any repair and only then reopen PIT.

Rollback, hash divergence and corruption are intended to follow:

```text
Ready=false
  -> persistent fail-closed marker
  -> invalidate views and drop derived runs
  -> rebuild from audit sequence 1
  -> independent semantic replay/certification
  -> WAL barrier
  -> clear marker
  -> Ready=true
```

Important liveness characteristics:

- ordinary tailing retries at 200 ms;
- verifier defaults are 15 minutes and full replay every 96 passes, too slow
  for a short Antithesis run;
- tiering defaults to five minutes and remote GC to one hour with a 24-hour
  grace, also too slow for Antithesis;
- compaction, tiering and GC share one maintenance goroutine, so a stuck S3 call
  can delay the other maintenance operations;
- public health intentionally does not depend on PIT readiness, so a healthy
  pod is not a PIT-ready oracle;
- `WaitForLogWatermark` can wait until the caller's deadline, so PIT probes need
  short per-RPC deadlines rather than the workload's full ten-minute context.

### Confirmed code-level liveness gap

A `SOURCE_MISSING` marker written by a cold read or verifier is not observed by
an already-ready builder's process-local `sourceMissing` atomic. At an unchanged
head, the builder can keep taking its caught-up early return without certifying
or clearing the marker. Restart reloads the marker and triggers repair.

This is confirmed by code tracing but has not yet been reproduced as a running
system failure. Antithesis should remove a required cold object, observe the
fail-closed response, restore it without restarting the node, and require
recovery during a quiet period. A restart control scenario should then prove the
existing boot repair path.

## Cold storage and dependency boundaries

The same MinIO bucket contains two distinct namespaces:

- archived primary chapters, which are authoritative inputs to a rebuild;
- replica-owned content-addressed PIT runs, which accelerate/serve historical
  views.

Consequently a Ledger-to-MinIO fault can affect source replay, cold PIT reads,
tiering or remote inventory independently. Generic S3 transport failures and
proven missing/corrupt objects must remain distinguishable; the workload should
match exact error reasons rather than tolerate all gRPC `Internal` errors.

The S3 client has no custom HTTP timeout or retryer. Maintenance uses a
background context and depends on SDK/context cancellation. The chapter
`ColdReader` holds one mutex through download and ingestion, so a slow chapter
fetch can head-of-line-block other cold chapter loads on that replica.

Kubernetes MinIO currently has no PVC, unlike Compose. A MinIO pod recreation
can therefore erase authoritative archives and PIT runs. That is total object
store loss, not a recoverable service restart. Add a PVC for crash/restart
campaigns, or restrict MinIO to network/hang/throttle faults and treat object
loss as an explicit fail-closed campaign.

MinIO versioning is not enabled. This is desirable for the first remote-GC
campaign because a delete physically removes the key. Versioned-bucket lifecycle
behavior is a separate operational campaign.

## Multi-replica semantics

Each replica builds a physically independent history projection. Run IDs,
manifest versions, compaction layout, cold placement and tokens may legitimately
differ. Cross-node assertions must compare:

- monetary result;
- temporal axis and normalized requested timestamp;
- ledger incarnation;
- a sufficient/common log watermark.

They must not require identical view tokens or manifest versions. During active
writes, nodes can pin different primary heads. Quiescent checks need a barrier
and exact persisted-index gate before comparing local stale reads through the
existing per-node connections.

Leader-forwarded responses are different: routing through a follower to the
same leader should preserve the leader's result and complete PIT trailer.

## Existing test strategy and remaining value of Antithesis

Deterministic coverage is already broad:

- reducer semantics, axes, reversals and numeric identity;
- builder boot/tail, rollback, missing/corrupt audit, WAL barriers and cancel;
- real Pebble hot/cold sources and archive checksums;
- verifier replay, tampering, quarantine and certification;
- publication, compaction, pinned views, tiering, multipart cold runs, cache,
  remote GC and simulated crash windows;
- one-node PIT E2E and a three-node forwarding/convergence E2E;
- reproducible local performance harnesses.

Those tests do not provide real independent process kills, asymmetric network
partitions, S3 process faults, rolling restarts, scaling or restore while the
new projection is active. Antithesis should not duplicate byte-level corruption
matrices or latency benchmarks; it should compose real faults at the subsystem
boundaries.

## Confirmed hotspots and regression leads

### Out-of-order chapter archiving mismatch

The FSM accepts archive/confirm operations for any eligible chapter ID without
requiring an archived prefix. The PIT `HotColdSource` rejects an archived
chapter after a non-archived chapter. This code-level mismatch is confirmed;
the full-system impact has not been reproduced. A three-chapter Antithesis
scenario should archive N+1 before N, prove no partial success, then archive N
and require exact recovery.

### Existing chapter driver can create an invalid archive

`singleton_driver_chapter_close` sends `ArchiveChapter` and immediately sends
`ConfirmArchiveChapter`. The real archiver is asynchronous and confirms only
after upload/verification. The manual confirmation can win, purge hot data and
cause the archiver to abandon the now-non-ARCHIVING request. The existing
`chapter archive completed` assertion proves only the Raft confirmation, not
cold readability.

The PIT campaign must not reuse this sequence. It should request archive, wait
for the archiver-produced `ARCHIVED` state, prove a cold log/audit read, then
query PIT across the chapter.

### Empty HTTP `pit` silently selects live state

Repository tracing found that an explicitly present but empty HTTP `pit` query
parameter is treated as if the selector were absent, while the OpenAPI contract
requires an RFC3339 value. This is a confirmed deterministic adapter gap: the
request can silently execute a live aggregate rather than fail validation.

It should receive a normal HTTP regression test and fix. It is intentionally
not a cataloged Antithesis property because fault scheduling adds no meaningful
state space to parsing one request.

### Restore-ahead race hypothesis

After a primary rollback/restore but before the next builder tick, local history
and process-local readiness may still be ahead. A stale-local PIT read derives a
lower required watermark from the restored primary and could potentially open
the ahead manifest. This is a source-grounded hypothesis, not a confirmed
defect, and is a high-value timing property.

### Node namespace reuse

The PIT archive owner is derived from Raft node ID. Operator scale-down removes
the node PVC, while scale-up may reuse the same ordinal/owner namespace with a
new empty local store. The intended adoption/reclamation behavior for the old
objects is not explicit and needs a dedicated scaling property or human
decision.

## Claims to test, not established facts

The accepted design claims that:

- every successful effective/insertion response is exact at its pinned source
  watermark;
- no lag, gap or corruption silently falls back to live or partial history;
- a pinned view is immutable across publication, compaction, tiering and GC;
- logical monetary state is independent of replica layout;
- a restart loses at most a replayable asynchronous history suffix;
- remote GC never deletes current or pinned roots;
- a recreated ledger name cannot expose an earlier incarnation;
- MinIO/PIT failures do not enter or block the FSM hot path;
- metadata filters remain intentionally current-state while money is historical.

These statements are the property portfolio's inputs. Their presence in code or
documentation is not evidence that Antithesis has verified them.

## Antithesis implications

- Enable PIT in every Ledger replica; enable the cold tier in a targeted
  campaign with accelerated but configuration-valid intervals.
- Keep three Ledger replicas and a separate, faultable MinIO.
- Exclude the workload and harness control plane from ordinary data-path faults.
- Explicitly confirm tenant support for node termination and clock faults.
- Use a dedicated no-retry/per-node client for observing PIT reasons and the
  normal retrying client for final convergence.
- Maintain an independent monetary oracle keyed by accepted/log-confirmed
  effects and compare only through the response log watermark.
- Split `main` and restore/model campaigns. Antithesis chooses exactly one
  template per history; the full backup/PVC teardown/restore path currently
  exists only in `model`.
- Add small SUT-side reachability/safety assertions for transitions invisible
  over gRPC: publication, repair certification, tier phases, cold fetch, archive
  epoch invalidation and remote delete-before-ack.
- Keep deterministic performance tests as the source for write/read latency;
  virtualized fault scheduling is not a benchmark environment.

## Assumptions

- The first campaign targets the current Go server, gRPC API, operator-managed
  three-node cluster and MinIO already shipped in this repository.
- NATS and event sinks are outside the PIT data path and can be omitted or made
  non-faultable in a focused PIT campaign.
- A successful response is checked against the exact source prefix represented
  by its trailer; the oracle does not assume all client attempts committed.
- `HISTORY_BUILDING` and `HISTORY_BEHIND` are acceptable transient outcomes
  during active faults. A successful incomplete result is never acceptable.

## Open Questions

- Are node-termination and clock faults enabled for the Formance tenant?
- Should MinIO model durable object storage with a PVC, or is total object loss
  an intentional separate campaign?
- Query/verifier-originated `SOURCE_MISSING` is intended to self-heal in the
  running process; current code does not notify builder repair state, which is a
  confirmed implementation gap.
- Should transient MinIO checksum/read transport failures persist
  `SOURCE_MISSING`, or remain retryable infrastructure failures until absence is
  proven?
- Stale-local PIT is intentionally supported during follower synchronization;
  normal snapshot install is forward-only and must remain bounded by the local
  activated prefix. Administrative restore-ahead is a separate gap/property.
- Same-ordinal replacement intentionally reuses the stable Raft node owner
  namespace; content is accepted only through rebuild/digest verification.
- Should the history store keep sharing the primary 2 GiB data PVC in the
  Antithesis campaign, or receive a dedicated volume?
- Restore coverage extends the existing isolated `model` template on fresh
  PVCs, with persistent authoritative chapter archives.
- Is a small test-only diagnostic/instrumentation surface acceptable for
  manifest digest, tier and remote-GC reachability, or should all such evidence
  remain SDK-only inside the SUT?
