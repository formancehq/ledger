# remote-gc-queue-converges — Remote GC drains its durable queue after recovery

## Catalog entry

| | |
|---|---|
| **Priority** | P1 |
| **Type** | Liveness |
| **Property** | After authorized archive uploads and mutations stop, S3 is healthy, all manifest Views are closed, a fresh inventory completes for the current mutation epoch, and one full grace period elapses, a non-empty durable remote-GC queue must drain within a bounded number of successful healthy collector calls. |
| **Invariant** | Arm a checkpoint only when `Q0 > 0`, `cursor == ""`, `completedInventoryEpoch == mutationEpoch`, S3 and roots are available, all Views are drained, and `now >= freshInventoryCompletedAt + gracePeriod`. Let `I0` be that inventory's canonical object count, `D` be `DeleteLimit`, `P = I0 + 1`, and `B = P + ceil(Q0 / D)`. Assert `Reachable("pit: remote GC recovery checkpoint has a non-empty mature queue", details)`, `AlwaysOrUnreachable(!checkpointActive || healthySuccessfulCollects < B || queueObjects == 0, "pit: remote GC queue respects its healthy-pass recovery bound", details)`, and `Sometimes(checkpointActive && queueObjects == 0 && healthySuccessfulCollects <= B, "pit: remote GC queue drains after inventory grace and dependency recovery", details)`. The guarded `Sometimes` cannot pass on an always-empty queue; `Reachable` proves the recovery premise occurred; `AlwaysOrUnreachable` supplies the exact bounded-pass oracle while allowing campaigns that do not enable PIT/cold storage. |
| **Antithesis Angle** | Create orphaned cold objects, interrupt inventory pages, keep Views open, fail S3 list/delete calls, crash after a successful remote delete but before its durable acknowledgement, restart Ledger, then stop uploads, close Views, heal S3, and hold a fault-free window for the checkpoint and its `B` successful calls. |
| **Why It Matters** | Remote-GC safety prevents destructive false positives, but it does not prevent a cursor or durable candidate from wedging forever. An undrained queue leaks cold-storage capacity and can permanently block destination disable/rotation after reset. |
| **Confidence** | High in the repository-derived state-machine and bound; medium in present Antithesis executability because the durable cursor/epochs/queue and collector-call outcome are not exposed to the workload. |

**Open Questions:**

- None for the controlled, non-versioned MinIO namespace. S3 versioning's physical
  byte reclamation remains a lifecycle-policy concern, not this logical-key
  convergence property.

## Recovery premise and bounded-pass oracle

The recovery checkpoint is intentionally stronger than merely observing an old
queue gauge:

1. The workload has stopped all operations that can upload, reset, reconfigure,
   or otherwise advance the archive mutation epoch.
2. S3 list and delete operations are healthy, and remain healthy for the bounded
   episode.
3. Every manifest View has closed, so root capture is available.
4. A complete inventory begun after dependency recovery has durably stored an
   empty cursor and `completedInventoryEpoch == mutationEpoch`.
5. The workload then waits at least `gracePeriod` after that completion, with no
   clock fault. Every candidate present at the checkpoint was first observed no
   later than the completed inventory, so the entire checkpoint queue is mature.
6. `Q0`, the durable queue size after the wait, is non-zero. Empty checkpoints do
   not satisfy the liveness assertion.

For the controlled campaign, no foreign or malformed keys are inserted beneath
the replica-owned archive prefix. With a stable canonical namespace containing
`I0` objects, each non-final lexical page consumes at least one object, so at
most `P = I0 + 1` successful `Collect` calls finish one more inventory cycle.
That deliberately conservative `+1` does not assume full pages.

After those `P` calls, every still-present first-observation candidate has been
observed in a later cycle, while a first-observation candidate that vanished was
pruned at the final page. Candidates previously observed twice remain eligible
even when their remote object is now absent. Every further healthy call retires
up to `D = DeleteLimit` eligible entries, either by synchronously acknowledging
a latest-manifest root or by idempotent delete followed by synchronous
acknowledgement. Therefore

```text
B = (I0 + 1) + ceil(Q0 / D)
```

successful healthy calls suffice. The bound is conservative because each of the
`P` inventory calls also runs the collection phase and can already consume a
delete batch. At the state after call `B`, a non-zero queue is a violation.
Ticker intervals, time spent faulted, and failed calls are not counted as
successful healthy calls.

A changed archive identity or mutation epoch, a newly opened View, an S3 error,
a clock fault, or a process replacement cancels the current checkpoint rather
than extending its budget. Once recovery conditions hold again, the replacement
process rearms from a new fresh inventory and the queue's then-current durable
`Q0`. This makes the oracle honest about downtime while still checking that
restart cannot discard or permanently strand durable work.

## Restart and delete-before-ack workload

The strongest campaign composes the existing crash windows instead of testing
only a clean queue:

1. Tier enough history to create multiple pages, then reset or race a losing
   manifest CAS so unrooted canonical objects exist.
2. Complete two inventories and grace so at least one candidate is eligible.
3. Inject S3 list failures while the cursor is non-empty, and keep a historical
   View open while deletion would otherwise be eligible.
4. Let one exact-key delete succeed, pause before the Pebble `Sync`
   acknowledgement, and kill the owning Ledger process.
5. Restart it with the same Ledger volume and the same durable MinIO data. Verify
   that the persisted partial cursor/candidates are resumed; the already absent
   twice-observed object must remain queued rather than being pruned.
6. Stop uploads and mutations, close all Views, heal S3, complete a fresh
   inventory for the current epoch, wait grace, and arm the bounded checkpoint.
7. Require the queue to reach zero within `B` successful healthy calls. During
   the window, repeatedly query retained cold watermarks so convergence cannot
   be obtained by deleting a live root.

The delete-before-ack step overlaps with
`replay-remote-delete-ack-is-idempotent`; this property does not replace that
narrow replay oracle. It uses the crash residue as one member of a queue and
checks queue-wide, bounded convergence after the entire dependency set heals.

The restart leg requires MinIO object data to survive container replacement.
The current proposed topology does not give MinIO a persistent volume, so the
campaign must either add one or restrict the fault to Ledger restarts plus S3
network partitions. Losing the object store itself is not a valid GC-recovery
success.

## Evidence

- `internal/storage/balancehistorystore/remote_gc.go:154-232` serializes one
  bounded collector call, advances one inventory page, stops safely after an
  epoch restart, and then attempts the bounded candidate phase.
- `internal/storage/balancehistorystore/remote_gc.go:235-339` captures roots,
  blocks on active Views, reads no more than `DeleteLimit` eligible candidates,
  retires roots synchronously, and performs exact delete followed by a per-item
  synchronous acknowledgement.
- `internal/storage/balancehistorystore/remote_gc.go:449-500` preserves the
  durable cursor and candidates across ordinary process restart, but clears the
  cursor and partial scan counters when the binding mutation epoch changes.
- `internal/storage/balancehistorystore/remote_gc.go:503-641` durably advances
  candidates and the lexical cursor; only the final page stores
  `CompletedInventoryEpoch`, resets scan counters, and advances the cycle.
- `internal/storage/balancehistorystore/remote_gc.go:644-706` prunes an object
  that vanished after only its first observation but retains a twice-observed
  candidate, including delete-before-ack residue.
- `internal/storage/balancehistorystore/remote_gc.go:709-776` requires both a
  later observation cycle and grace before eligibility and rejects root capture
  while any manifest View is active.
- `internal/infra/coldstorage/s3.go:208-269` implements a restart-safe lexical
  cursor and idempotent exact-key deletion.
- `internal/bootstrap/balance_history_maintenance.go:35-110` starts with an
  immediate bounded pass, retries from periodic ticks, and runs tiering before
  remote GC in the single maintenance worker.
- `internal/storage/balancehistorystore/remote_gc_test.go` covers grace plus a
  second observation, cursor/candidate resumption after reopen, active-View
  blocking across reset/version ABA, vanished-first-observation pruning,
  delete-before-ack retry, archive-gate serialization, and recovery after a
  remote delete failure. These deterministic tests establish the mechanisms but
  do not provide the queue-wide Antithesis liveness oracle.
- `docs/technical/architecture/subsystems/read-path/balance-history-remote-gc.md:73-142`
  specifies the durable state, two-observation/grace protocol, cursor recovery,
  and every relevant crash window; `:171-178` specifies all-deletion blocking
  while a View is active; `:204-230` documents the existing metrics.

## Assertion and instrumentation status

No current SDK assertion covers PIT remote-GC progress. Existing OpenTelemetry
gauges expose inventory and queue totals, but the workload has no metrics
backend and the gauges do not coherently expose the durable cursor, scan epoch,
completed-inventory epoch, active View count, or successful-call counter needed
to arm and evaluate this oracle.

Add SUT-side diagnostic state at the `RemoteCollector` boundary (test build or
Antithesis instrumentation, not a public product API) with one coherent snapshot
containing:

- replica/pod process identity, archive destination identity, mutation epoch,
  scan epoch, completed-inventory epoch, cursor, and cycle;
- latest fresh-inventory completion time and canonical `inventoryObjects`;
- durable `queueObjects`, `queueBytes`, and oldest/newest candidate observation
  times (the newest time is needed to validate maturity directly);
- active manifest View count/root-capture availability;
- configured grace period, scan limit, and delete limit;
- checkpoint ID, `Q0`, `I0`, computed `B`, and successful healthy calls since
  that checkpoint; and
- whether the last exit was success, list/delete failure, active-View block,
  epoch invalidation, reconfiguration, or process replacement.

Emit the three catalog assertions from the SUT using that coherent state. Also
emit these phase signals so the workload can prove it exercised recovery rather
than only a clean run:

- `Reachable("pit: remote GC resumed a durable cursor or candidate queue after restart", details)`;
- `Reachable("pit: remote GC blocked deletion because an active view made roots ambiguous", details)`;
- `Reachable("pit: remote GC durably acknowledged an idempotent delete retry", details)`.

The successful-call counter increments only when `Collect` returns nil under an
unchanged checkpoint identity/epoch with roots available and S3 healthy. Any
invalidating event cancels the checkpoint before evaluating the bound. A
test-only failpoint immediately after successful remote delete and before the
existing Sync acknowledgement is needed to make the crash edge deterministic;
the repository already has an internal unit-test hook at exactly that boundary.

## Investigated questions

1. **Can old queue age alone arm liveness?** No. It cannot prove a complete
   inventory for the current mutation epoch or that the newest candidate has
   aged through grace. The checkpoint waits grace after a fresh completion.
2. **Does an absent candidate disappear during the next inventory?** Only when
   it was observed in a single cycle. A twice-observed candidate survives
   omission specifically so delete-before-ack can retry and acknowledge it.
3. **Do active Views merely skip rooted entries?** No. Root identity is
   version-only and reset can create ABA ambiguity, so any active View blocks
   all deletion. The bound starts only after Views drain.
4. **Can the bound include failures or restart downtime?** No. It counts only
   successful healthy calls. A restart cancels process-local accounting and
   requires a new fresh-inventory checkpoint, while durable cursor/candidates
   must survive underneath it.
5. **Why is one extra inventory bounded by `I0 + 1` calls?** In the controlled
   namespace every physical key is canonical; every non-final lexical page
   advances past at least one key. The bound does not rely on pages being full.
6. **Are current metrics sufficient?** No. They are not workload-readable and
   lack the coherent epoch/cursor/View/call-count state needed to distinguish a
   legitimate paused collector from a wedged one.

### Investigation Log

**Examined:**

- G3 in `antithesis/scratchbook/evaluation/synthesis.md`.
- SUT/topology assumptions, the current property catalog, existing assertion
  inventory, and the three related remote-GC safety/replay property files.
- The collector state machine, maintenance worker, S3 catalog/delete adapter,
  configuration defaults, architecture documentation, and focused remote-GC
  tests in this repository at `fb3f9f8334c7fbbcc27c2c211c77da36762e6aaf`.

**Found:**

- Cursor, candidates, observations, queue totals, and acknowledgements are
  durable; the maintenance worker retries and runs an immediate pass on start.
- The protocol has all mechanisms needed for eventual drain, but no integrated
  bounded queue-wide assertion after uploads, Views, S3 faults, and restarts
  cease.
- A fresh inventory plus a post-completion grace wait makes every checkpoint
  candidate mature; one additional inventory plus delete batches yields the
  conservative call bound above.
- Delete-before-ack residue is deliberately retained after later inventory and
  exact S3 deletion is idempotent, so it belongs inside the same queue bound.

**Not found:**

- Any PIT-specific SDK assertion, workload-readable durable-GC state, coherent
  recovery checkpoint, or successful-call budget.
- A persistent MinIO volume in the proposed topology sufficient for a true
  object-store-preserving container restart campaign.
- Any production backoff or permanent-error state that should exempt a healthy,
  unchanged recovery episode from making progress.

**Conclusion:**

G3 is a real liveness gap, distinct from live-root and upload/inventory safety.
Add the guarded P1 property with a SUT-owned checkpoint and bounded successful
call counter, then compose restart and delete-before-ack residue before the
fault-free recovery window. The protocol should drain the queue within
`(I0 + 1) + ceil(Q0 / DeleteLimit)` successful healthy calls; exceeding that
budget with a non-zero queue is the failure.
