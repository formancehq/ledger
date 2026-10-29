# Controlled shutdown barrier

Add the `ledger.formance.com/drain` annotation to a `Cluster` to close write
admission while its Pods keep running. The operator persists a fresh epoch in
`status.drain.phase: Preflighting`, checks every member's drain and release
support, then persists `Draining` before calling `ClusterService.BeginDrain`.
Removing the annotation during `Preflighting` clears the intent without a
release RPC because no marker has been proposed. It reports
`status.drain.phase: Drained` and a `Drained=True` condition only after every
captured member has confirmed durable application. Removing the annotation
causes the operator to release the same epoch; it clears `status.drain` after
every member durably applies that release. The RPCs require
`AUTH_SCOPE_CLUSTER_WRITE` and route to the leader. The hidden `ledgerctl`
commands are the operator's transport.

While an annotation transition is active, the operator addresses the running
StatefulSet using its live replica count, gRPC container port, and TLS mode.
Changes to the desired `Cluster` spec therefore do not redirect a release to
Pods that have not yet been updated.

The annotation requests a transition; write rejection is certified only by
`status.drain.phase: Drained`. If the annotation is removed and re-added while
release is in progress, the operator completes the old release and persists a
new epoch immediately. Writes may be admitted until the new epoch reaches
`Drained`, as during any initial annotation request.

If the shutdown is canceled or the Pods restart with their existing volumes,
the operator calls `ClusterService.EndDrain` with the same epoch. The hidden
`ledgerctl cluster end-drain` command is its transport. The release is another
Raft-ordered technical update; it clears the admission gate on each member
only when applied. `EnsureDrainReleased` waits for every captured member to
durably apply the release before reporting completion. A later shutdown uses
a new epoch.

## Ordering and admission

Before proposing the marker, the leader asks every current member for its
local `GetDrainStatus`. An old binary returns `Unimplemented`, so the request
fails closed during a rolling upgrade. The leader fences previously accepted
membership changes through Raft and holds the membership operation lock from
capability preflight until marker application. Additions, promotions, and
normal removals are blocked while a drain is active. The first `DrainMarker(epoch, members)`
technical update committed through Raft fixes the epoch and cutoff index.
Later markers cannot move the active cutoff. A retry with the same epoch
returns the first decision; a different epoch is rejected until the active
drain is released. A released epoch cannot be reused.

If the caller times out after enqueuing a marker, the local membership fence
stays closed until the marker applies or Raft definitively drops it. A timeout
alone never reopens membership admission.

The leader serializes marker enqueue with the final `Node.Propose` step used
by ordinary writes. Its local gate closes at enqueue, before any later
proposal can enter the queue. The applied marker closes admission on every
member. The FSM also rejects any order or non-marker technical update at an
index after the cutoff. A write whose proposal is ordered *before* the marker
retains its normal outcome. A request that started pre-reads but has not
entered the Raft proposal queue is not yet admitted; it can be rejected.
Business errors unrelated to drain remain possible for pre-marker writes.

The first decision is written to `ZoneClusterTransient/SubTransientDrainState`
in the same Pebble batch as the applied index. `SharedState` publishes the
gate during apply. `LastPersistedIndex` advances only after the batch commit,
so the operator cannot mistake a merely prepared marker for a durable one.
On restart or in-cluster snapshot sync, recovery reloads the marker before
admission resumes. A cross-cluster restore clears the transient zone, so a
restored backup does not inherit an operational shutdown request.

The release decision persists with its Raft index and prior epoch history. A
restart after a durable release reopens admission. A caller whose release
was not durably applied can retry the same epoch. Old, delayed release
markers cannot lift a newer epoch.

## Operator completion test

The operator retries `BeginDrain` with the same epoch, reads current leader
membership, and queries each original member's own Pod with `GetDrainStatus`.
It returns ready only when the current membership still matches the captured
set and every member reports the same epoch and cutoff, `sync_status=normal`,
and `last_persisted_index >= cutoff_index`. When the reported nonzero drain
index is covered by the captured persisted index, the local status call fsyncs
the Pebble WAL before returning. Only that qualifying response certifies the
reported index as durable. An unreachable, replaced, syncing,
or lagging member blocks shutdown. Raft's leader `Match` index is insufficient:
it does not prove the follower's Pebble batch committed.

For release, the operator verifies current membership, then checks every
original member's own Pod for an empty active drain, the same release index,
normal sync status, and a persisted index at or beyond the release. This
qualifying status response fsyncs the Pebble WAL as well. Missing members
prevent a successful all-member release acknowledgment.

The epoch stays in Cluster status across operator retries and restarts. The
operator keeps Pods running on `false` or error. It persists the `Releasing`
phase before proposing release, so a restart cannot accidentally reuse the
epoch. The leader serializes preflight with membership operations through
marker application. An emergency force removal can still change the captured
set and cause the operator's completion check to fail.

## Limits

The cutoff concerns Raft-ordered proposals, not HTTP/gRPC request start times
or client receipt of a response. A timed-out request may have committed; the
operator continues from the persisted marker. A lost member prevents a
successful all-member completion until it recovers and catches up. The
durability claim is the configured Pebble/WAL contract after the status fsync;
it does not cover destruction of the volume or storage that lies about sync.
Without a committed release, the barrier remains active after a restart.
