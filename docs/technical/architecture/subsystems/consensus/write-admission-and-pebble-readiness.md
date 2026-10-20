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

## Compaction fencing and donor selection

Compaction fencing protects a different boundary. It prevents a node from
deleting its local Raft WAL prefix beyond the Pebble index that it has
persisted. When a node is behind only within its retained WAL, it can replay
that suffix locally and does not need a checkpoint donor.

The fencing rule does not imply that every `out_of_sync` voter is unusable as a
donor. A voter can receive a newer Raft snapshot, become `out_of_sync`, and
still have a stable Pebble store that covers an older recovery target. For
example, if B needs a checkpoint at index `S_B` and C has persisted Pebble past
`S_B` before receiving a later snapshot, C can provide B with a checkpoint at
`S_B` even though C is waiting to restore its own newer snapshot.

Donor eligibility should therefore be target-aware:

```text
donor.LastPersistedIndex >= requester.WALSnapshotIndex
```

The donor must not be actively replacing its Pebble store, and its store must
remain stable while the checkpoint is created. A blanket rule that rejects all
`out_of_sync` voters is safe but can reject a valid recovery path. The current
implementation uses that conservative rule; this is a follow-up consideration
for donor selection, separate from Raft election safety.

Target-aware donors do not close every recovery gap by themselves. Consider
this schedule:

```text
1. A is the leader; B is restoring and C is unavailable.
2. A accepts entries through index S. B persists the entries in its Raft WAL,
   but neither B nor C has Pebble state covering S.
3. A publishes a Raft snapshot at S and later fails.
4. B installs that snapshot and becomes out_of_sync because its Pebble restore
   has not completed.
5. C returns with an older WAL and receives the same or a later MsgSnap from B.
```

B and C can then both have a WAL suffix after `S` while neither has Pebble state
through `S`. A donor check based on `LastPersistedIndex` correctly rejects both
nodes. No local suffix can reconstruct the missing prefix, so a donor or a
repair is still required.

This also limits what a quorum compaction fence can guarantee. Fencing only WAL
deletion is insufficient if the leader still publishes a latest Raft snapshot
above the durable quorum floor. Raft can select that snapshot when a follower
falls below the retained WAL boundary, even if entries between the quorum floor
and the snapshot index remain on disk. A strong fence must also constrain the
snapshot boundary used for follower recovery, or retain a snapshot/checkpoint
that covers every advertised recovery boundary.

The compaction fence and the write gate were evaluated as additional defenses:

| Mechanism | Protects | Does not protect |
| --- | --- | --- |
| Compaction fence | Retains local WAL history until the local Pebble state is durable | The availability of a Pebble donor after snapshot installation |
| Write gate | Stops new writes after a sampled recovery condition becomes false | A cascading failure that removes every usable Pebble state |

The fence can make the local-WAL recovery path sufficient and can reduce the
number of checkpoint transfers. The write gate only adds backpressure during
the detection window. Neither mechanism guarantees data service after the last
usable Pebble state has disappeared.

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
