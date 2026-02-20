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
ledger-v3-poc run \
  --node-id 1 \
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

Subsequent nodes join the cluster with `--join`, pointing to any existing cluster member's gRPC service address:

```bash
ledger-v3-poc run \
  --node-id 2 \
  --join 127.0.0.1:8888 \
  --bind-addr 127.0.0.1:7778 \
  --grpc-port 8889
```

### Join Sequence

The join process has four phases: peer discovery, node initialization, startup, and learner registration.

#### Phase 1: Peer Discovery

Before the fx application starts, `LoadConfig` connects to the address specified by `--join` and calls `GetClusterState` to discover all existing cluster members:

```
--join 127.0.0.1:8888
    │
    ▼
discoverPeersFromClusterWithRetry()
    │  ┌─────────────────────────────────────┐
    │  │ Retry loop:                          │
    │  │   Exponential backoff 500ms → 5s     │
    │  │   Deadline: 60 seconds               │
    │  │   Call GetClusterState RPC           │
    │  └─────────────────────────────────────┘
    │
    ▼
cfg.RaftConfig.Peers = [{ID: 1, Addr: "...", ServiceAddr: "..."}]
```

The retry loop allows the joining node to wait for the bootstrap node to be ready (useful when all pods start simultaneously in Kubernetes).

#### Phase 2: Node Initialization

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

#### Phase 3: Startup

The fx lifecycle starts all components:
- Raft transport and service pool register the discovered peers
- gRPC servers start (Raft transport + service API)
- The Raft node `Run()` begins the orchestrate loop
- The ConfChange observer is wired to handle cluster membership changes

#### Phase 4: Learner Registration

An fx `OnStart` hook detects this is a fresh start (empty WAL before `NewNode` modified it) with `--join`:

```
freshStart == true && !Bootstrap && len(Peers) > 0
    │
    ▼
Connect to first discovered peer
    │
    ▼
Call AddLearner RPC → forwarded to leader if needed
    │
    ▼
Leader proposes ConfChangeAddLearnerNode
    │
    ▼
ConfChange committed → all nodes update transport & service pool
```

On **restart** (WAL not empty), this registration step is **skipped** because the node is already a cluster member.

### ConfChange Observer

When a ConfChange is committed (adding a learner or promoting a voter), an observer on each node synchronously updates the transport and service pool. The ConfChange carries a `ConfChangeContext` with the new node's Raft and service addresses, so all nodes learn the addresses without external configuration.

### AddLearner RPC

The `AddLearner` gRPC handler on the leader:

1. Pre-registers the new peer in its local transport and service pool (so Raft messages can reach the new node immediately)
2. Proposes a `ConfChangeV2` with `ConfChangeAddLearnerNode`

If the request reaches a follower, it is transparently forwarded to the leader.

## Synchronization

After joining, a node may need to catch up with the cluster's state. The synchronization process is managed through a four-state machine.

### Node Status States

| Status | Value | Description |
|--------|-------|-------------|
| `statusNormal` | 0 | Normal operation: committed entries applied directly to FSM |
| `statusSyncing` | 1 | Checkpoint fetch in progress: entries spooled |
| `statusSnapshotting` | 2 | Local snapshot creation in progress: entries spooled |
| `statusOutOfSync` | 3 | Store behind FSM snapshot: waiting for leader discovery |

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
  startup ──►┌──────────────┐  leader    ┌─────────────────┐   │
  (store     │statusOutOfSync│──found───►│  statusSyncing   │──┘
   behind)   └──────────────┘            └────────┬────────┘
                    ▲                             │
                    │         peer unavailable     │
                    └─────────────────────────────┘
```

### Startup Check

When the node starts, `NewNode` checks whether the data store is up to date with the FSM snapshot:

- **Store up to date**: status stays `statusNormal`, spool entries are replayed
- **Store behind**: status is set to `statusOutOfSync`

### Out-of-Sync State

In `statusOutOfSync`, the node waits for a leader to be discovered. Tick processing is **suppressed** to prevent spurious elections. When a `SoftState` with a non-zero leader arrives, the node transitions to `statusSyncing` and begins the checkpoint fetch.

### Syncing State (Checkpoint Fetch)

The synchronization fetches a Pebble checkpoint from the leader:

```
statusOutOfSync
    │
    │  SoftState reveals leader
    ▼
statusSyncing
    │
    │  Background task starts:
    │
    │  1. snapshotFetcher.GetForPeer(leaderID)
    │     → gRPC-based fetcher for the leader
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
- Ticks are **suppressed** (no elections, no heartbeats)
- `MsgTimeoutNow` messages are **rejected** (prevents forced leadership transfer to this node)

If the leader is unreachable (`ErrNotAvailable`), the node falls back to `statusOutOfSync` and retries when a new leader is found.

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

### Protection Against Premature Leadership

Two mechanisms prevent a syncing node from becoming leader prematurely:

1. **Tick suppression**: During `statusSyncing` and `statusOutOfSync`, `rawNode.Tick()` is not called. Without ticks, the node cannot trigger election timeouts and will never start an election.

2. **MsgTimeoutNow rejection**: If a leader tries to transfer leadership to a syncing node via `TransferLeadership` (which sends `MsgTimeoutNow` to force an immediate election), the message is silently dropped.

These protections ensure a node that is still restoring its Pebble checkpoint will not become leader, even if it appears active from Raft's perspective.

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

# Disable auto-promotion (manual promotion only)
--learner-promotion-threshold 0
```

### Manual Promotion

Learners can also be promoted manually via the CLI:

```bash
ledgerctl cluster promote-learner <node-id>
```

Or via the `PromoteLearner` gRPC RPC. The request is forwarded to the leader if it reaches a follower.

## Kubernetes (Helm Chart)

The Helm chart implements the bootstrap/join model using a StatefulSet:

```bash
# Pod 0 (index 0) → Node ID 1 → Bootstrap
if [ "$POD_INDEX" = "0" ]; then
  CLUSTER_FLAG="--bootstrap"
else
  # Pods 1..N-1 → Node IDs 2..N → Join pod-0
  BOOTSTRAP_HOST="{fullname}-0.{headless}.{namespace}.svc.cluster.local"
  CLUSTER_FLAG="--join ${BOOTSTRAP_HOST}:${GRPC_PORT}"
fi
```

- **Pod management policy**: `Parallel` (all pods start simultaneously)
- **Pod-0**: bootstraps the cluster
- **Other pods**: join via pod-0 with 60s retry (waiting for pod-0 to be ready)
- **Auto-promotion**: controlled by `config.raft.learnerPromotionThreshold` in `values.yaml`
- **Node IDs**: `POD_INDEX + 1` (Pod 0 = Node 1, Pod 1 = Node 2, etc.)

### Scaling

To add nodes:

```bash
helm upgrade ledger-v3-poc ./misc/chart --set replicaCount=5
```

New pods will join the existing cluster as learners and be auto-promoted once caught up.

## Complete Lifecycle Example

A 3-node cluster startup:

```
Time 0: Pod-0 starts with --bootstrap
    → Node 1 creates single-node cluster
    → Self-elects as leader
    → Cluster: Voters=[1], Learners=[]

Time 1: Pod-1 starts with --join pod-0:8888
    → discoverPeersFromCluster → finds Node 1
    → Creates initial snapshot: Voters=[1], Learners=[2]
    → OnStart hook calls AddLearner RPC
    → Leader adds Node 2 as learner
    → Cluster: Voters=[1], Learners=[2]

Time 2: Pod-2 starts with --join pod-0:8888
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
   - Close the transport connection to the removed peer
   - Remove the peer from the service connection pool
5. The removed node is **not** automatically shut down; the operator must stop it manually

### Important Notes

- After removal, the removed node will no longer receive Raft messages or log entries
- The removed node should be stopped by the operator after the removal is confirmed
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

## Related Documentation

- [Raft Consensus](../dev/architecture/raft-consensus.md) - Raft protocol details, elections, replication
- [Data Flows](../dev/architecture/data-flows.md) - Spool mechanics, entry processing
- [Spool](../dev/architecture/spool.md) - Spool technical implementation
- [Deployment](./deployment.md) - Helm chart configuration, CLI flags
- [CLI Reference](./cli.md) - `cluster add-learner`, `cluster promote-learner` commands
