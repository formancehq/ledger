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
The leader therefore keeps a pending snapshot-recovery marker until a later
peer report confirms the persisted index and normal status.

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

The gate can reduce this window by versioning reports with the Raft term and
configuration, expiring reports, and rechecking leadership before admission.
It cannot guarantee that a voter will remain available after the final check.

A strict guarantee would require follower acknowledgements after Pebble
persistence to participate in the commit condition. That would change Raft's
commit semantics and its performance profile.

## Operational decision

The write gate is an availability policy above Raft. It should reject new
business writes when the configured checkpointable-quorum invariant is false,
while leaving elections, log replication, snapshot recovery, and restoration
active.

The rejection is retryable and does not claim that every acknowledged entry is
durable in Pebble on every voter. Raft WAL durability, Pebble fsync ordering,
checkpoint creation, and the admission gate remain separate guarantees.
