# Cluster Lifecycle

This document describes the complete lifecycle of a cluster: creation, node addition, synchronization, and automatic learner promotion.

## Overview

The cluster uses a **bootstrap/join model**:

1. **One node** starts with `--bootstrap` and creates a single-node cluster (becomes leader)
2. **Other nodes** start with `--join <addr>` and join as **learners** (non-voting replicas)
3. The leader **auto-promotes** caught-up learners to full voters

This model is dynamic: nodes discover the cluster at startup, no static peer list is needed.

## Cluster Creation (Bootstrap)

The first node in the cluster must start with the `--bootstrap` flag:

```bash
ledger run \
  --node-id 1 \
  --cluster-id prod-ledger \
  --bootstrap \
  --bind-addr 127.0.0.1:7777 \
  --grpc-port 8888
```

### What Happens

1. `NewNode` detects an empty WAL (no prior state)
2. Since `--bootstrap` is set, an initial Raft snapshot is created with `Voters: [1]` (the node itself as sole voter)
3. The Raft raw node starts and immediately elects itself leader (sole voter = instant majority)
4. gRPC servers (Raft transport + service API) start and become ready

```
Empty WAL + --bootstrap
    │
    ▼
Initial snapshot: { Voters: [1], Learners: [] }
    │
    ▼
RawNode starts → self-election → Leader
    │
    ▼
gRPC servers ready → cluster operational
```

The `--bootstrap` flag must only be used **once**, on the **first node**, on its **first start**. It is mutually exclusive with `--join`.

## Adding a Node (Join as Learner)

Subsequent nodes join the cluster with `--join`, pointing to any existing cluster member's **RaftServer** address (the inter-node Raft transport port, not the external gRPC service port):

```bash
ledger run \
  --node-id 2 \
  --cluster-id prod-ledger \
  --join 127.0.0.1:7777 \
  --bind-addr 127.0.0.1:7778 \
  --grpc-port 8889
```

### Join Sequence

The join process has four phases: peer discovery, node initialization, learner registration preflight, and startup of inbound Raft traffic. Both discovery and registration go through the inter-node `ClusterBootstrapService` exposed on the RaftServer — gated by cluster-id metadata (and cluster-secret bearer when configured), without the user-JWT auth pipeline.

#### Phase 1: Peer Discovery

Before the fx application starts, `LoadConfig` connects to the address specified by `--join` and calls `ClusterBootstrapService.GetPeers` to discover all existing cluster members:

```
--join 127.0.0.1:7777
    │
    ▼
discoverPeersFromClusterWithRetry()
    │  ┌─────────────────────────────────────┐
    │  │ Retry loop:                          │
    │  │   Exponential backoff 500ms → 5s     │
    │  │   No deadline (until SIGTERM)        │
    │  │   Call GetPeers RPC                  │
    │  └─────────────────────────────────────┘
    │
    ▼
cfg.RaftConfig.Peers = [{ID: 1, Addr: "...", ServiceAddr: "...", InstanceID: <16 bytes>}]
```

The retry loop allows the joining node to wait for the bootstrap node to be ready (useful when all pods start simultaneously in Kubernetes).

**Fail-fast on a configuration mismatch (EN-1080, EN-2738).** The retry loop
treats transient conditions (peer not yet up, no leader) as retryable, but two
statuses from the target are hard configuration errors that would otherwise be
retried forever:

- `codes.Unauthenticated` — the joining node's `--cluster-secret` is missing or
  wrong. Discovery aborts immediately with a `JoinAuthError` that names the
  `--join` address and tells the operator whether to add or fix the secret.
- `codes.PermissionDenied` — the joining node's `--cluster-id` does not match
  the target cluster's. Discovery aborts immediately with a
  `JoinClusterIDError` that names the `--join` address and the rejected value.

Learner registration (Phase 3) applies the same rules.

#### Phase 2: Node Initialization

Before opening Raft state, the node reads its 16-byte `INSTANCE_ID` marker. A
new identity is generated only for a directory with no prior WAL lifecycle
artifacts. If `INSTANCE_ID` is missing while `CLUSTER_JOINED`, the WAL creation
marker, or an existing WAL/snapshot directory remains, startup fails closed:
the original marker must be restored or the WAL/PVC must be reprovisioned as a
fresh instance. This prevents a restart from retaining consensus state under a
different identity than the one known by the other cluster members.

`NewNode` detects an empty WAL and a non-empty `Peers` list:

```
Empty WAL + Peers discovered
    │
    ▼
Initial snapshot: { Voters: [1], Learners: [2] }
    │
    ▼
RawNode starts with this configuration
```

The joining node sees the existing members as voters and itself as a learner.

#### Phase 3: Learner Registration Preflight

An Fx `OnStart` hook registers a joining node when bootstrap is disabled,
discovered peers are configured, and the WAL directory has no `CLUSTER_JOINED`
marker. It runs before the Raft gRPC server accepts inbound traffic, membership
connections start, or `Node.Run` steps received messages. This ordering lets the
leader reject stale replication progress before it can send messages that the
joining node's empty log cannot accept.

The preflight dials discovered peers directly with short-lived outbound gRPC
connections; it does not require the local Raft listener or connection pool:

```
Call JoinAsLearner RPC → forwarded to leader if needed
    │
    ▼
Leader proposes ConfChangeAddLearnerNode
    │
    ▼
ConfChange committed → existing nodes update transport & service pool
    │
    ▼
Successful response or AlreadyExists → write CLUSTER_JOINED marker
```

The registration loop has no deadline of its own. It retries `Unavailable`
with backoff until success or cancellation; `AlreadyExists` is an idempotent
success, and other statuses are final. SIGTERM or interrupt cancels its dedicated
registration context, including an in-flight RPC or backoff. The application
startup context remains live so Fx can roll back hooks that already started.
The failed preflight closes its own outbound connections; it has not started a
Raft run loop. The graceful Fx notification remains queued if startup completes
while the signal is being handled.

A positive `--total-stop-timeout` starts a signal-time deadline for startup
unwind and deferred log-export finalization. Expiry cancels the private
application context and returns failure; it does not prove every hook, worker,
or resource has finished closing. Zero leaves this waiting unlimited. If startup
succeeds, normal Fx stopping retains its separate stop budget. See the
[server termination contract](cli.md#server-termination) for deadline scope and
exit status.

A signal does not suppress a fatal registration status, marker-write failure,
rollback failure, or signal-time finalization failure. Only intentional
registration cancellation followed by successful rollback and finalization
produces a successful termination.

Cancellation cannot undo a learner addition that the remote cluster already
committed. A canceled or lost response can leave remote membership without a
local marker. The next execution retries the existing registration protocol:
`AlreadyExists` permits writing the marker only while the leader's progress is
still zero; replicated progress produces the stale-membership failure described
below. A process exit alone is not evidence that remote membership was removed.

On restart, an existing `CLUSTER_JOINED` marker skips preflight. WAL presence
alone is not this gate.

The current voter quorum can commit a learner addition while its listener is
still closed. If optional-TLS probing fails at that point, the voter retains
the committed endpoint and its outbound retry loop.

#### Phase 4: Startup of Inbound Raft Traffic

After preflight succeeds, the Fx lifecycle starts the components that accept
inbound Raft traffic:

- Raft transport and service pool register the discovered peers.
- gRPC servers start (Raft transport and service API).
- The Raft node `Run()` begins the orchestrate loop.
- The ConfChange observer handles cluster membership changes.

The existing voter's retry loop can then establish streams to the learner and
deliver replication without another registration or a voter restart. Normal
catch-up and automatic promotion follow.

### ConfChange Observer

When a ConfChange is committed (adding a learner or promoting a voter), an observer on each node synchronously updates the transport and service pool. The ConfChange carries a `ConfChangeContext` with the new node's Raft and service addresses, so all nodes learn the addresses without external configuration. An `UpdateNode` that changes the Raft address refreshes the pooled connection address through `DefaultTransport.AddPeer` while preserving the existing send loop, queued messages, and the committed replacement target. This means a transient optional-TLS probe failure retains the retry owner and can recover without stranding the peer. A service-address-only change refreshes the service pool through its address-aware `AddPeer`, retaining the existing Raft connection.

### AddLearner RPC

The `AddLearner` gRPC handler on the leader:

1. Requires the target node's persisted 16-byte `INSTANCE_ID` in the request
2. Checks membership admission on the leader without changing its transport or service routing
3. Proposes a `ConfChangeV2` with `ConfChangeAddLearnerNode` (or `ConfChangeUpdateNode` for an admitted refresh); its context carries both addresses and the identity
4. Observes the committed change, updates in-memory membership and attempts to wire its transport/service endpoints before resolving the successful request

Connection/probe failures retain their existing logged, nonfatal behavior: a
successful request confirms committed membership, not peer reachability.
A rejected admission leaves the existing addresses and connections intact. The
current voters commit a learner addition without needing an acknowledgement
from that learner, so pre-registering its transport is unnecessary. Cancellation
after proposal submission does not undo a change that later commits; routing
then follows that committed change, independently of the RPC result.

If the request reaches a follower, it is transparently forwarded to the leader.
Administrative `AddLearner` and bootstrap `JoinAsLearner` are separate code
paths. Both require an identity. For `JoinAsLearner`, EN-1436's stale-progress
check rejects whenever the leader has already replicated to that node
(`Match > 0`), regardless of the supplied identity. Administrative `AddLearner`
rejects with `FailedPrecondition` only when `Match > 0` and the supplied identity
differs from the registered one. A retry with the same active identity returns
`AlreadyExists`; with `Match == 0`, a different identity refreshes the row through
`ConfChangeUpdateNode`. For stale progress, permanently retire the previous
instance, remove its membership, then retry registration; the leader-only
`remove-node --force` recovery procedure below applies when normal removal
cannot reach quorum.

## Synchronization

After joining, a node may need to catch up with the cluster's state. The synchronization process is managed through a four-state machine.

### Node Status States

| Status | Value | Description |
|--------|-------|-------------|
| `statusNormal` | 0 | Normal operation: committed entries applied directly to FSM |
| `statusSyncing` | 1 | Checkpoint fetch in progress: entries spooled |
| `statusSnapshotting` | 2 | Local snapshot creation in progress: entries spooled |
| `statusOutOfSync` | 3 | Store behind the WAL snapshot: waiting for the next donor attempt |

### State Transitions

```
                    ┌────────────────────────────────────────────┐
                    │                                            │
                    ▼                                            │
             ┌─────────────┐                                    │
  startup ──►│ statusNormal │◄──────────────────────┐           │
             └──────┬───────┘                       │           │
                    │                               │           │
        snapshot    │                    replay      │           │
        threshold   │                    complete    │           │
        reached     │                               │           │
                    ▼                               │           │
           ┌─────────────────┐               ┌──────┴──────┐   │
           │statusSnapshotting│──── done ────►│ unspool &   │   │
           └─────────────────┘               │ resume      │   │
                                             └─────────────┘   │
                                                    ▲           │
  startup ──►┌──────────────┐  donor     ┌─────────────────┐   │
  (store     │statusOutOfSync│──picked──►│  statusSyncing   │──┘
   behind)   └──────────────┘            └────────┬────────┘
                    ▲                             │
                    │   donor unavailable or       │
                    │   resynchronizing itself     │
                    └─────────────────────────────┘
```

### Startup Check

When the node starts, `NewNode` compares the store's last applied index with the WAL snapshot index (`IsStoreUpToDate`):

- **Store at or above the WAL snapshot index**: status stays `statusNormal`. Every entry the store lacks is still in the local WAL, so the node catches up by replaying it and the spool, with no checkpoint transfer.
- **Store below the WAL snapshot index**: status is set to `statusOutOfSync`. The entries between the store and the snapshot are no longer in the local WAL, so only a checkpoint from a donor can close the gap.

### Out-of-Sync State

In `statusOutOfSync`, the node keeps ticking: its Raft log is complete, so it votes and may campaign on it (see [Elections While Out of Sync](#elections-while-out-of-sync)). On the next Ready it picks a checkpoint donor, the known leader first, then the other voters in turn, never itself, transitions to `statusSyncing` and begins the checkpoint fetch. See [Donor Selection](../technical/architecture/data-flows.md#donor-selection).

### Syncing State (Checkpoint Fetch)

The synchronization fetches a Pebble checkpoint from a donor:

```
statusOutOfSync
    │
    │  Ready picks a donor (leader first)
    ▼
statusSyncing
    │
    │  Background task starts:
    │
    │  1. snapshotFetcher.GetForPeer(donorID)
    │     → gRPC-based fetcher for the donor
    │
    │  2. fsm.SynchronizeWithLeader(fetcher)
    │     → Compare lastCheckpointID vs currentCheckpointID
    │     → If behind: restoreCheckpoint()
    │        → Stream checkpoint via SnapshotService gRPC
    │        → Restore Pebble database from checkpoint
    │     → Set lastAppliedIndex = snapshotIndex
    │
    │  3. replaySpool(frozenAtIndex)
    │     → Apply spooled entries accumulated during fetch
    │
    ▼
statusNormal
```

While in `statusSyncing`:
- All new committed Raft entries are **spooled** (buffered), not applied to the FSM
- Ticks keep firing: the node votes and may campaign on its log, while ReadIndex refuses and admission waits for `leaderReady`
- `MsgTimeoutNow` messages are **rejected** (prevents forced leadership transfer to this node)

If the donor is unreachable, or refuses with `FailedPrecondition` because it is resynchronizing itself, the node falls back to `statusOutOfSync` and tries the next donor.

### Entry Spooling During Sync

The **spool** is a durable buffer that stores committed Raft entries while the node cannot apply them (during sync or snapshot creation):

```
                  statusNormal              statusSyncing
                  ───────────               ─────────────
Committed    ──► FSM.Apply()          ──► Spool.Append()
Entries                                        │
                                               │  (sync complete)
                                               ▼
                                         Spool.ReplayUntil()
                                               │
                                               ▼
                                          FSM.Apply()
                                               │
                                               ▼
                                         Spool.Prune()
```

This ensures no committed entries are lost during synchronization, regardless of how long the checkpoint fetch takes.

### Sync Completion

When the background task completes:

1. A `gatingTerminated` channel is closed
2. The `processReadies` goroutine detects this and calls `unspoolAndResume`:
   - Gets the last applied index from the store
   - Replays remaining spool entries from that index
   - Sets status to `statusNormal`
   - Prunes applied spool entries

### Elections While Out of Sync

A node whose store is behind still holds a complete, durable Raft log, and elections are decided on the log alone. The node keeps ticking in `statusSyncing` and `statusOutOfSync`: it times out, votes, may campaign, and may win. Withholding its ticks would leave the cluster leaderless whenever the voters holding the newest log are the resynchronizing ones, since no candidate with an older log can collect their votes. Only `statusInstallingSnapshot`, the short window in which the WAL snapshot is rewritten, pauses the clock.

What needs the store waits for the store instead:

1. **Write admission** waits for `leaderReady`, which completes only once the FSM has applied the new leader's no-op entry. On an out-of-sync leader that happens after its checkpoint sync and spool replay.
2. **ReadIndex** refuses while the node is syncing or out of sync.
3. **Membership changes** (`AddLearner`, `JoinAsLearner`, `PromoteLearner`, `RemoveNode`, automatic promotion) wait for the same readiness, so removed-member tombstones and instance identities are read from a current store.
4. **Event sinks and mirrors** start on `LeaderReadyEvent`.
5. **Leadership transfer** to such a node is still refused: the `MsgTimeoutNow` that `TransferLeadership` sends is dropped outside `statusNormal`. An operator cannot hand leadership to a node that is still restoring its checkpoint; it can only win an election on its own log.

An out-of-sync leader therefore serves nothing until its store is current, and it fetches its checkpoint from another voter (see [Donor Selection](../technical/architecture/data-flows.md#donor-selection)).

## Automatic Learner Promotion

The leader periodically checks whether any learner is caught up and eligible for promotion to voter.

### How It Works

On every Raft tick (default: every 100ms), if `AutoPromoteThreshold > 0`, the leader calls `checkAndPromoteLearners()`:

```
For each node in status.Progress:
    │
    ├─ Is it a learner?              → No: skip
    ├─ Is it recently active?        → No: skip
    ├─ Has it replicated anything?   → No: skip (Match == 0)
    │
    └─ Is it caught up?
       Match + AutoPromoteThreshold >= Commit  → Yes: promote
```

The **promotion condition** is:

```
prog.Match + threshold >= commitIndex
```

Where:
- `prog.Match`: the highest log index the leader knows is replicated on this learner
- `threshold`: the `--learner-promotion-threshold` flag value (default: 100)
- `commitIndex`: the current commit index of the cluster

When eligible, the leader proposes a `ConfChangeV2` with `ConfChangeAddNode`, which promotes the learner to a full voter. This ConfChange goes through normal Raft consensus.

### Configuration

```bash
# Auto-promote learners within 100 entries of the commit index (default)
--learner-promotion-threshold 100

# Disable auto-promotion
--learner-promotion-threshold 0
```

The `PromoteLearner` gRPC RPC remains available for existing clients. It is
forwarded to the leader when called on a follower. The operator CLI does not
expose manual promotion.

## Kubernetes (Operator)

The Ledger operator implements the bootstrap/join model using a StatefulSet:

```bash
# Pod 0 (index 0) → Node ID 1 → Bootstrap
if [ "$POD_INDEX" = "0" ]; then
  CLUSTER_FLAG="--bootstrap"
else
  # Pods 1..N-1 → Node IDs 2..N → Join pod-0
  BOOTSTRAP_HOST="{fullname}-0.{headless}.{namespace}.svc.cluster.local"
  CLUSTER_FLAG="--join ${BOOTSTRAP_HOST}:${RAFT_PORT}"
fi
```

- **Pod management policy**: `Parallel` (all pods start simultaneously)
- **Pod-0**: bootstraps the cluster
- **Other pods**: join via pod-0, retrying peer discovery with backoff until pod-0 answers (no deadline)
- **Auto-promotion**: controlled by `config.raft.learnerPromotionThreshold` in the Ledger CR spec
- **Node IDs**: `POD_INDEX + 1` (Pod 0 = Node 1, Pod 1 = Node 2, etc.)

### Scaling

To add nodes, update the `replicas` field in the Ledger CR:

```bash
kubectl patch ledgers.ledger.formance.com my-ledger --type=merge -p '{"spec":{"replicas":5}}'
```

New pods will join the existing cluster as learners and be auto-promoted once caught up.

## Complete Lifecycle Example

A 3-node cluster startup:

```
Time 0: Pod-0 starts with --bootstrap
    → Node 1 creates single-node cluster
    → Self-elects as leader
    → Cluster: Voters=[1], Learners=[]

Time 1: Pod-1 starts with --join pod-0:7777
    → ClusterBootstrapService.GetPeers → finds Node 1
    → Creates initial snapshot: Voters=[1], Learners=[2]
    → OnStart hook calls JoinAsLearner RPC
    → Leader adds Node 2 as learner
    → Cluster: Voters=[1], Learners=[2]

Time 2: Pod-2 starts with --join pod-0:7777
    → Same process as Pod-1
    → Cluster: Voters=[1], Learners=[2, 3]

Time 3: Nodes 2 and 3 catch up with the leader's log
    → checkAndPromoteLearners() detects Match + threshold >= Commit
    → Leader proposes ConfChangeAddNode for Node 2 → promoted
    → Leader proposes ConfChangeAddNode for Node 3 → promoted
    → Cluster: Voters=[1, 2, 3], Learners=[]

Cluster fully operational with 3 voters.
```

## Removing a Node

Nodes can be removed from the cluster using the `RemoveNode` RPC or the CLI command. This works for both voters and learners.

### Prerequisites

- The node to remove must **not** be the current leader. Transfer leadership first with `cluster transfer-leader`.
- The node must be a current member of the cluster.

### Procedure

```bash
# 1. Check cluster status to identify the node
ledgerctl cluster status

# 2. If removing a voter that is the leader, transfer leadership first
ledgerctl cluster transfer-leader <other-node-id>

# 3. Remove the node
ledgerctl cluster remove-node <node-id>

# 4. Verify the node was removed
ledgerctl cluster status

# 5. Stop the removed node process manually
```

### What Happens

1. The `RemoveNode` request is forwarded to the current leader (if sent to a follower)
2. The leader validates that the target is not itself and is a cluster member
3. The leader proposes a `ConfChangeRemoveNode` through Raft consensus
4. Once committed, all remaining nodes:
   - Apply the configuration change (node is removed from the Raft group)
   - Delete the peer row and atomically persist its mandatory removed-member tombstone
   - Close the transport connection to the removed peer
   - Remove the peer from the service connection pool
5. The serving leader waits for the committed Raft index to become durable in the FSM; there is no fixed tombstone polling timeout
6. The removed node is **not** automatically shut down; the operator must stop it manually

### Important Notes

- After removal, the removed node will no longer receive Raft messages or log entries
- The removed node should be stopped by the operator after the removal is confirmed
- Retrying an already-committed removal returns gRPC `NotFound` with reason `RAFT_NODE_NOT_IN_CLUSTER`; verify `cluster status` and treat absence as the successful postcondition
- If the caller stops waiting after the ConfChange commits but before its FSM batch is durable, the RPC returns `Unavailable` with reason `RAFT_NODE_REMOVAL_COMMITTED` and the committed Raft index. The removed identity remains blocked from rejoining while apply catches up.
- During Kubernetes scale-down, the operator checks structured `cluster status --json` for every removed ordinal, including Pending or missing replacement Pods, and again after any removal error. Pod state never proves that a stable node ID is absent. Replicas are reduced and PVC deletions are issued only after each ordinal is already absent or its removal succeeds; an unresolved membership check/removal retains replicas and PVCs. Earlier removals may already have committed and are recognized on retry. See the [operator scale-down contract](../../misc/operator/README.md#overview).
- Removing a voter reduces the cluster quorum size; ensure the remaining cluster can still form a majority
- For a 3-node cluster, removing one voter leaves a 2-node cluster where both nodes must be available for writes

### Example: Scale Down from 3 to 2 Nodes

```bash
# Starting state: 3 voters (nodes 1, 2, 3), node 1 is leader

# Remove node 3 (a follower)
ledgerctl cluster remove-node 3

# Stop the node 3 process
# (on node 3's host)
kill <node-3-pid>

# Verify: cluster now has 2 voters
ledgerctl cluster status
```

### Force-Removing a Down Node

When a node is permanently unreachable (crashed, OOMKilled, etc.) and removing it through normal Raft consensus would block due to quorum loss, use `--force` to bypass consensus:

```bash
# Starting state: 3 voters (nodes 1, 2, 3), node 1 is leader, node 3 is crashed

# Normal remove would block because {1,2} can't reach quorum=2 after node 2 is also
# down, but even removing node 3 normally requires it to be reachable for quorum.
# Force-remove bypasses consensus entirely:
ledgerctl cluster remove-node 3 --force

# Now the cluster is {1, 2} with quorum=2, both alive → cluster operational
ledgerctl cluster status
```

For a three-voter cluster that has permanently lost both followers, the same
leader-only command may be run once for each lost voter. This restores a
single-voter configuration without copying or re-bootstrapping the leader's
data directory.

This recovery is available only when the surviving node still considers itself
leader. If the sole survivor is a follower or candidate, it
cannot elect itself under the old three-voter membership and the force path
rejects the request because it is leader-only. Restore a fresh cluster from an
off-cluster backup instead. See
[Availability and Disaster Recovery](./availability-and-recovery.md#failure-scenarios).

**How it works:**

1. `ForceRemoveNode` validates the required member identity, then directly calls `rawNode.ApplyConfChange()` on the leader, bypassing the Raft log. This immediately recalculates the live quorum and can advance the live commit index before persistence.
2. The updated `ConfState` is persisted to the WAL snapshot immediately (before the peer row is deleted, so a crash between the two heals to "voter absent, orphan address" rather than "voter present, unreachable")
3. `Membership.UnregisterAndBlacklist` atomically deletes the peer row from Pebble and writes its removed-member tombstone, then drops the peer from the in-memory cache + transport + service pool in lockstep
4. After the command succeeds, Raft processing resumes with the reduced quorum.

The live etcd/raft tracker mutation in step 1 cannot be rolled back safely. If
the WAL persistence in step 2 fails, the command returns an error and the node
immediately enters a terminal state: it rejects new Raft commands and proposals,
stops its Raft tasks, and terminates. The peer cleanup in step 3 is not attempted.
Restart uses the membership in the latest durable snapshot. A failure before
the atomic snapshot-file replacement restores the previous voter set; a later
WAL-record failure can restart with the new voter set because the replacement
file matches the snapshot's existing term/index. Both outcomes are safe because
the failed process stops before executing more work. After restart, inspect the
cluster state and retry only if the target is still present and the storage
fault has been resolved.

**When to use:**

- A node is in `CrashLoopBackOff`, `OOMKilled`, or `Failed` state and won't recover before the scale-down
- The Kubernetes operator automatically uses `--force` for pods it detects as crashed during scale-down
- Removing the node through consensus would time out because the downed node prevents quorum

**Safety considerations:**

- Only use for nodes that will **never rejoin** with their old state (e.g., PVCs will be deleted)
- The force-removed node is not notified — if it somehow recovers, it will have stale cluster membership
- Cannot force-remove the leader itself
- Before force-removing any voter, permanently fence every removed member. If
  another partition can still commit, force-removal can create divergent
  histories.

## Replace a Kubernetes Node Without Changing Replica Count

Use this procedure when an operator-managed pod with ordinal greater than zero
has damaged local state but the other two voters in a three-node cluster are
healthy and synchronized. Pod 0 bootstraps with an empty WAL; do not replace
its PVCs with this procedure. Replace only the target node's WAL and data PVCs.
The surviving voters keep the
committed cluster data. Do not use this procedure to repair a lost quorum or a
leader whose state is the only surviving copy. Check the current backup and
[recovery plan](./availability-and-recovery.md) before deleting any volume.

The example assumes a Flux-managed operator release, PVC-backed `wal` and
`data` volumes, and a StatefulSet named `ledger-<cluster>`. Substitute the
actual resource names. The operator can manage other Ledger clusters in the
same Kubernetes cluster, so keep its pause as short as possible.
Run the command blocks in the same shell so the variables and `k` function
remain available.

### Inspect the Cluster and Record Volume Identities

```bash
KCTX="REPLACE_WITH_KUBERNETES_CONTEXT"
NS="REPLACE_WITH_LEDGER_NAMESPACE"
CLUSTER="REPLACE_WITH_CLUSTER_NAME"
ORDINAL=1 # Example: pod ordinal 1 is Raft node ID 2.
OP_NS="REPLACE_WITH_OPERATOR_NAMESPACE"
OP_DEPLOY="REPLACE_WITH_OPERATOR_DEPLOYMENT"
RELEASE="REPLACE_WITH_OPERATOR_HELMRELEASE"
LEADER_POD="REPLACE_WITH_SURVIVING_LEADER_POD"
OTHER_POD="REPLACE_WITH_OTHER_SURVIVING_POD"
ST="ledger-${CLUSTER}"
POD="${ST}-${ORDINAL}"
NODE_ID=$((ORDINAL + 1))
k() { kubectl --context "$KCTX" "$@"; }

k -n "$NS" get cluster "$CLUSTER"
k -n "$NS" get statefulset "$ST" -o jsonpath='{.spec.persistentVolumeClaimRetentionPolicy}'
k -n "$NS" get pod "$POD" -o custom-columns=NAME:.metadata.name,UID:.metadata.uid,STATUS:.status.phase
k -n "$NS" get pvc "wal-${POD}" "data-${POD}" -o 'custom-columns=NAME:.metadata.name,UID:.metadata.uid,PV:.spec.volumeName,STATUS:.status.phase'
k -n "$OP_NS" get helmrelease "$RELEASE" -o jsonpath='{.spec.suspend}'
k -n "$OP_NS" get deployment "$OP_DEPLOY" -o jsonpath='{.spec.replicas}'
k -n "$NS" exec "$LEADER_POD" -c ledger -- ./ledgerctl cluster status
```

Record the target pod UID, both PVC UIDs, both bound PV names, the HelmRelease
`spec.suspend` value, and the operator Deployment replica count. Confirm the
target is not the leader, the two surviving voters can form a quorum, and the
Cluster has no upgrade in progress. If the target is leader, transfer leadership
and verify the new leader before continuing. Stop if the StatefulSet retention
policy is not `Retain` for both `whenDeleted` and `whenScaled`.

### Fence the Target Pod

Pause the Flux HelmRelease before scaling its operator Deployment to zero.
Verify that another GitOps controller will not undo the pause. The operator
pause affects every Cluster it manages.

```bash
k -n "$OP_NS" patch helmrelease "$RELEASE" --type=merge -p '{"spec":{"suspend":true}}'
k -n "$OP_NS" scale deployment "$OP_DEPLOY" --replicas=0
k -n "$OP_NS" rollout status deployment "$OP_DEPLOY"
k -n "$OP_NS" get deployment "$OP_DEPLOY"
```

Delete the StatefulSet with orphan propagation. This keeps the surviving pods
running. Delete only the target pod, without `--force`, and wait for it to
disappear. Do not scale the StatefulSet down: a scale-down removes the highest
ordinal, which may be a different node or the leader.

```bash
k -n "$NS" delete statefulset "$ST" --cascade=orphan --wait=true
k -n "$NS" get pods -l "app.kubernetes.io/instance=${CLUSTER},app.kubernetes.io/name=ledger"
k -n "$NS" delete pod "$POD" --wait=true
k -n "$NS" get pod "$POD" --ignore-not-found
k -n "$NS" get pvc "wal-${POD}" "data-${POD}"
```

Before continuing, verify the target pod is absent, the two surviving pods are
ready, and both target PVCs are still bound. If the target pod is recreated,
find the controller that recreated it before changing Raft membership.

### Remove the Old Member and Its Volumes

Run normal removal from a surviving leader pod. Normal `remove-node` uses Raft
consensus; it also accepts a request through a follower. Verify the target ID
is absent from `cluster status` on both surviving pods before touching PVCs.

```bash
k -n "$NS" exec "$LEADER_POD" -c ledger -- ./ledgerctl cluster remove-node "$NODE_ID"
k -n "$NS" exec "$LEADER_POD" -c ledger -- ./ledgerctl cluster status
k -n "$NS" exec "$OTHER_POD" -c ledger -- ./ledgerctl cluster status
```

If the command reports an error after committing the removal, check status
before retrying. Treat a target ID absent from both survivors as the successful
postcondition. Do not switch to `--force` merely because normal removal failed.
The [force-removal procedure](#force-removing-a-down-node) is leader-only and
requires permanent fencing of the old member.

For protected PVCs, add the per-volume deletion annotation. Delete only the
target pod's `wal` and `data` PVCs, then confirm they are gone. With the common
`Delete` reclaim policy, Kubernetes also deletes their old PVs. If the reclaim
policy is `Retain`, leave the released PVs for separate storage cleanup; never
reattach them to the replacement node.

```bash
k -n "$NS" annotate pvc "wal-${POD}" "data-${POD}" formance.com/allow-deletion=true --overwrite
k -n "$NS" delete pvc "wal-${POD}" "data-${POD}" --wait=true
k -n "$NS" get pvc "wal-${POD}" "data-${POD}" --ignore-not-found
```

The admission policy protects selected PVCs from accidental deletion. The
annotation permits this deliberate deletion. It is not necessary to annotate
bound PVs when deleting their PVCs; the PV policy permits normal reclaim after
the PVC is gone. See [volume deletion protection](../../misc/operator/README.md#volume-deletion-protection).

### Recreate and Verify the Node

Restart the operator while the HelmRelease remains suspended. It recreates the
StatefulSet, adopts the surviving pods, creates new target PVCs, and starts the
target pod with `--join`. Wait for the target to catch up and become a voter.

```bash
k -n "$OP_NS" scale deployment "$OP_DEPLOY" --replicas=1
k -n "$OP_NS" rollout status deployment "$OP_DEPLOY"
k -n "$NS" wait --for=create "statefulset/${ST}" --timeout=120s
k -n "$NS" wait --for=condition=Ready "pod/${POD}" --timeout=180s
k -n "$NS" rollout status "statefulset/${ST}" --timeout=300s
k -n "$NS" get pvc "wal-${POD}" "data-${POD}" -o wide
k -n "$NS" exec "$POD" -c ledger -- ./ledgerctl cluster status
```

Confirm the new PVC UIDs differ from the recorded target UIDs. Confirm the
surviving PVC UIDs did not change. `cluster status` must show every expected
node as a voter, the same applied and commit indexes, normal sync, and full
replication. Check the Cluster `Ready` condition, pod restart counts, and recent
logs on all nodes for peer transport or replay errors. For a deeper data check,
use [`ledgerctl store check`](./correctness.md#store-integrity-check) and compare
application-level counts or known records with a pre-operation baseline.

If `spec.suspend` was absent before this procedure, remove the temporary
field and verify the HelmRelease becomes Ready. If it was explicitly `false`,
restore that value instead:

```bash
k -n "$OP_NS" patch helmrelease "$RELEASE" --type=json -p '[{"op":"remove","path":"/spec/suspend"}]'
k -n "$OP_NS" get helmrelease "$RELEASE"
```

If the member was removed from Raft, do not restart it with its old WAL. The
removed identity is blocked from rejoining, and the old replica has stale
membership. If replacement fails before PVC deletion, preserve the old volumes
for diagnosis without remounting them. After PVC deletion, fix the fresh join
path or recover from a verified backup if the surviving data is compromised.
Before Raft removal commits, the original PVCs can still be used to restore
the pod.

## Cluster Configuration Updates

Some configuration parameters can be changed on a live cluster via a **rolling upgrade** -- restart nodes one by one with the new CLI flags. The new leader propagates the configuration change through Raft consensus so all nodes converge deterministically.

### Mutable Parameters

| Parameter | CLI Flag | Effect of change |
|-----------|----------|-----------------|
| Cache rotation threshold | `--cache-rotation-threshold` | Controls how many Raft entries per cache generation. Lower = less memory, more Pebble reads. Higher = more memory, fewer preloads. |

### Immutable Parameters

These parameters are validated at boot and cannot be changed without `--unsafe-skip-config-validation`:

| Parameter | CLI Flag |
|-----------|----------|
| Node ID | `--node-id` |
| Cluster ID | `--cluster-id` |

### How It Works

Mutable config parameters are propagated through the Raft log to ensure all nodes apply the change at the same index. The mechanism:

1. When a node becomes **leader**, it compares its CLI config with the persisted cluster config in Pebble.
2. If they differ, it proposes an `UpdateClusterConfig` entry through Raft.
3. All nodes apply the config change at the same Raft index (deterministic).
4. The change takes effect immediately -- no restart required for the nodes that already received the Raft entry.

The leader resolves the local zero sentinel to the effective default of 1000
before comparing or serializing the desired cache rotation threshold. Omitted
and explicit-zero settings therefore produce the same positive committed
threshold; positive custom settings retain their value. This normalization
uses the same defaults as initial cache construction and does not mutate the
shared Bloom configuration. It performs constant work per readiness callback.

### Zero-default rollout and existing committed zero entries

Older binaries can propose literal zero when elected with
`--cache-rotation-threshold=0`. During a mixed-version rollout, configure every
potentially elected older leader with an explicit positive threshold, such as
1000. Use zero only after all potential leaders include proposal-side
normalization. Positive committed entries retain their existing apply semantics.

This prevention change does not recover an already committed zero entry.
Applying that entry panics at the positive-threshold guard before resetting the
cache or saving the new cluster configuration. The previous Pebble configuration
remains, while the committed zero payload remains in the Raft WAL. Correcting
startup flags or proposing a later positive value cannot bypass that earlier
entry during replay.

Historical recovery remains blocked pending a separately approved and tested
compatibility design under the [RC upgrade contract](./upgrades.md#release-requirement-from-v300-rc1).
Do not wipe state, rewrite or skip committed entries, or substitute node-local
defaults during apply/replay. A recovery design must preserve committed business
data, audit history and cluster state, and establish mixed-version behavior and
rollback boundaries before it is used.

### Rolling Upgrade Procedure

To change `--cache-rotation-threshold` from 1000 to 500 on a 3-node cluster:

```bash
# 1. Restart each follower with the new flag
#    (check cluster state to identify the leader first)
ledgerctl cluster status

# 2. Stop follower node 2, restart with new config
# 3. Wait for node 2 to rejoin as follower
# 4. Repeat for follower node 3

# 5. Transfer leadership to an updated node
ledgerctl cluster transfer-leader 2

# 6. The new leader proposes the config change via Raft
#    All nodes (including the old leader) apply it

# 7. Restart the old leader with the new flag
```

### Monitoring Convergence

Use `GetClusterState` to verify all nodes have converged to the new threshold:

```bash
ledgerctl cluster status
# Look for cluster_config.rotation_threshold in the output
```

### Transient Command Rejections During Rolling Upgrade

When a config change resets the cache (threshold changed), there is a **race window** between the admission layer (which builds preloads against the old cache state) and the FSM (which resets the cache when applying the config change).

Proposals admitted just before the config change may carry a stale `cache_epoch`. The FSM detects this and rejects the proposal with `Unavailable`, causing the client to **retry automatically** via the gRPC retry policy.

**Impact:**

- A small number of in-flight requests may experience a single retry (~1-10 ms additional latency).
- No data loss or corruption -- the retry rebuilds preloads against the fresh cache state.
- This only happens once per config change, during the brief window between the config proposal entering the Raft log and the FSM applying it.

**Mitigation:** If zero-downtime is critical, apply the config change during a low-traffic period. The rejection window is typically under 100 ms.

### What Happens to the Cache

When the rotation threshold changes, the cache boundaries shift. The FSM:

1. **Resets** the in-memory cache (Gen0 and Gen1 cleared).
2. **Purges** the persisted cache snapshot zone (0xFF) in Pebble.
3. **Increments** the cache epoch (persisted in `PersistedClusterState`).

After the reset, the preloader falls back to Pebble reads (0xF1 zone) for all cache misses. The cache rebuilds naturally as new entries are applied. This causes a temporary increase in Pebble reads until the cache is warm again.

## Related Documentation

- [Raft Consensus](../technical/architecture/subsystems/consensus/raft-consensus.md) - Raft protocol details, elections, replication
- [Data Flows](../technical/architecture/data-flows.md) - Spool mechanics, entry processing
- [Spool](../technical/architecture/subsystems/storage/spool.md) - Spool technical implementation
- [Deployment](./deployment.md) - Helm chart configuration, CLI flags
- [CLI Reference](./cli.md) - `cluster promote-learner` and other cluster commands
- [Memory Management](./memory.md) - Cache rotation threshold memory impact
- [Availability and Disaster Recovery](./availability-and-recovery.md) - Failure-domain placement and quorum-loss recovery
