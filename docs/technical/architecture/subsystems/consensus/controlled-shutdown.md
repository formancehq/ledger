# Controlled shutdown barrier

The operator calls `ClusterService.BeginDrain` with a stable epoch before it
stops any Pod. The RPC requires `AUTH_SCOPE_CLUSTER_WRITE` and is routed to the
leader. `ledgerctl cluster begin-drain` is a hidden transport command for the
operator, not a user-facing shutdown workflow. The operator's
`EnsureDrainBarrier` method returns `true` only after every captured member
has confirmed local durable application. No upgrade or user trigger calls
this method yet.

## Ordering and admission

Before proposing the marker, the leader asks every current member for its
local `GetDrainStatus`. An old binary returns `Unimplemented`, so the request
fails closed during a rolling upgrade. The first `DrainMarker(epoch, members)`
technical update committed through Raft fixes the epoch and cutoff index.
Later markers cannot move the cutoff. A retry with the same epoch returns the
first decision; a different epoch is rejected.

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

The epoch must be stable across operator retries and restarts. The caller of
`EnsureDrainBarrier` owns this epoch and must leave Pods running on `false` or
error. Because the present change does not add a shutdown trigger, the
operator's future workflow must persist that intent and epoch before calling
the method. It must also keep membership changes serialized with drain
activation. The preflight proves the capabilities of the observed members;
it cannot make an unrelated, concurrent membership addition safe. An old
member introduced between preflight and marker proposal could receive a
command it cannot apply. The operator must not add or remove members while a
drain activation is in progress.

## Limits

The cutoff concerns Raft-ordered proposals, not HTTP/gRPC request start times
or client receipt of a response. A timed-out request may have committed; the
operator continues from the persisted marker. A lost member prevents a
successful all-member completion until it recovers and catches up. The
durability claim is the configured Pebble/WAL contract after the status fsync;
it does not cover destruction of the volume or storage that lies about sync.
There is no resume/un-drain operation: this barrier is for terminal shutdown.
