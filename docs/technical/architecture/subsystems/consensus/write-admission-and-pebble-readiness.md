# Write admission and Pebble readiness

## Problem

Raft quorum and Pebble readiness describe different states.

Raft can have a leader and a replication quorum while one or more voters are
still applying committed entries to Pebble. A leader can then accept writes
even though a later failure may leave no usable store from which to restore a
voter.

The failure pattern that motivated this study is a three-voter cluster:

```text
A: leader, Pebble is usable
B: voter, WAL is current but Pebble is waiting for a checkpoint
C: voter, unavailable
```

Raft still has the A+B quorum. The application must decide whether that is
enough to accept another write.

## Two different acknowledgements

Ledger uses `go.etcd.io/raft/v3` at version `v3.7.0`. The library separates
stable log writes from state-machine application when `AsyncStorageWrites` is
enabled.

```text
MsgStorageAppend
    → durable Raft WAL and HardState
    → MsgStorageAppendResp / MsgAppResp

MsgStorageApply
    → apply committed entries to the FSM and Pebble
    → MsgStorageApplyResp
```

`MsgAppResp` tells the leader that the follower has durably appended the log
entry. It does not say that the follower has applied the entry to Pebble.
`MsgStorageApplyResp` is local to the follower. Raft does not use it to count
the replication quorum.

The client acknowledgement is different again. Ledger resolves the local
proposal after the leader commits the batch to its own Pebble store. This does
not turn follower WAL acknowledgements into follower Pebble acknowledgements.

## Candidate invariants

The following invariants are distinct:

| Invariant | Meaning | Availability |
| --- | --- | --- |
| Raft quorum | A majority can replicate the WAL and commit entries | Highest |
| One external donor | The leader has at least one other voter with a usable Pebble store | Intermediate |
| Checkpointable quorum | A majority of active voters has a usable Pebble store | Lower |
| All voters checkpointable | Every voter has a usable Pebble store | Lowest |

The recovery problem requires more than Raft quorum. The target invariant for
write admission is a checkpointable quorum:

```text
checkpointable voters >= majority(active voters)
```

That gives 2/3 for a three-voter cluster and 3/5 for a five-voter cluster.
The leader counts when its own FSM is ready. A voter in `syncing`,
`out_of_sync`, snapshot recovery, or an unreachable voter does not count.

The one-donor rule is weaker. It prevents the leader from being the only
usable store, but it allows a five-voter cluster to write with only two usable
stores. It must not be described as a checkpointable-quorum guarantee.

## Joint consensus

During a joint-consensus transition, Raft requires a majority in both voter
sets. A readiness gate must apply the same rule:

```text
majority(checkpointable, incoming voters)
    AND
majority(checkpointable, outgoing voters)
```

Counting a majority over the union of both sets is not sufficient. It can count
enough nodes overall while failing to provide a majority in one of the two
configurations that Raft still uses for commitment and elections.

Learners and `LearnersNext` do not contribute to either quorum.

## Pebble readiness

The health check treats a peer as checkpointable only when all of the following
hold:

1. the peer responds to the health RPC;
2. its `SyncProgress.Status` is `normal`;
3. its Pebble persistence cursor has passed any snapshot-recovery index tracked
   by the leader;
4. the report is fresh enough for the current leadership epoch.

A snapshot entering Raft is not proof that the Pebble restore has completed.
The leader therefore marks the peer as pending as soon as it emits `MsgSnap`,
which invalidates the current write verdict before the next periodic poll. The
leader clears the pending episode only after a later peer report confirms the
persisted index and normal status.

## Why the Raft library does not remove the race

The current Raft library exposes no configuration that changes quorum
acknowledgements from WAL durability to Pebble application. The relevant
options have different purposes:

| Option | What it does | Why it is insufficient |
| --- | --- | --- |
| `AsyncStorageWrites` | Pipelines local WAL and FSM writes | Follower `MsgAppResp` still follows WAL durability |
| `CheckQuorum` | Steps down an inactive leader | Checks Raft traffic, not Pebble readiness |
| `PreVote` | Reduces disruptive elections | Does not change write acknowledgement |
| `ReadOnlySafe` | Performs a quorum read barrier | Does not persist application state on the quorum |
| `MaxUncommittedEntriesSize` | Bounds uncommitted log growth | Does not prove that Pebble can restore a voter |

Delaying `MsgAppResp` until Pebble application would create a protocol cycle:
the follower applies committed entries only after the leader commits them, but
the leader needs `MsgAppResp` to reach that commit. Eliminating this dependency
requires a new replication protocol or a second durable-application quorum.

## Remaining race

A readiness verdict can become false immediately after it is sampled:

```text
1. the leader observes a checkpointable quorum;
2. a voter fails;
3. the leader proposes a new entry.
```

The gate reduces this window by invalidating its verdict when it emits
`MsgSnap`, versioning reports with the leadership epoch, and recalculating the
quorum after recovery. It cannot guarantee that a voter will remain available
after the final check.

A strict guarantee would require follower acknowledgements after Pebble
persistence to participate in the commit condition. That would change Raft's
commit semantics and its performance profile.

## Cascading failures can make data service impossible

The checkpointable-quorum gate protects recovery capacity. It cannot create a
new Pebble store after the last usable store has failed.

Consider this sequence in a three-voter cluster:

```text
1. A is the leader and has a usable Pebble store.
2. B has the current WAL but is still restoring a checkpoint.
3. C fails.
4. A detects that the checkpointable quorum is gone and blocks new writes.
5. A fails before B completes its restore.
```

At step 5, B may still be a valid Raft voter and may hold the replicated WAL,
but no surviving voter has a usable Pebble store. Raft can still have enough
members to elect a leader or continue recovery. That does not mean that the
cluster can serve data. If B cannot complete its restore without a donor, the
cluster remains unable to serve data until a donor or another repaired voter is
available.

This is a permanent service-loss state for the current set of stores. No
election, retry, quorum check, or network-failure detector can reconstruct a
missing Pebble state. The system must repair or reintroduce a voter that can
provide the missing state.

The distinction is important:

| State | Raft status | Data service |
| --- | --- | --- |
| A is ready, B is restoring, C is down | The cluster may still have a Raft quorum | A can serve local data, but the recovery margin is lost and new writes are blocked |
| A then fails before B is ready | Raft may still have voters and WAL state | No surviving voter may be able to serve data |
| A repaired or a donor returns | Recovery can resume | Data service can be restored after Pebble readiness is confirmed |

This failure mode does not require an unsafe write. A write accepted after the
checkpointable quorum was lost would increase the amount of state that must be
recovered, but blocking that write cannot undo a failure chain that already
removed every usable store.

The gate therefore provides a bounded safety policy, not an availability
guarantee under arbitrary cascading failures:

- it refuses to add new business state when the cluster has lost its recovery
  quorum;
- it keeps Raft replication and snapshot recovery active;
- it reports the need for a repaired voter or checkpoint donor when no usable
  store remains.

Network-failure detection shortens the time before the gate closes. It does not
change this limit: once every surviving voter lacks a usable Pebble store, only
state recovery from another store can restore data service.

## Operational decision

The write gate is an availability policy above Raft. It should reject new
business writes when the configured checkpointable-quorum invariant is false,
while leaving elections, log replication, snapshot recovery, and restoration
active.

The rejection is retryable and does not claim that every acknowledged entry is
durable in Pebble on every voter. Raft WAL durability, Pebble fsync ordering,
checkpoint creation, and the admission gate remain separate guarantees.
