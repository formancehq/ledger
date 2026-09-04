# Availability and Disaster Recovery

Ledger v3 uses two separate mechanisms for operational resilience:

- **Raft replication** keeps active nodes consistent and provides automatic
  failover while a majority of voters remains available.
- **Backups** provide a portable recovery source outside the cluster failure
  domain.

Replication is not a backup. A valid Raft operation, operator mistake, or
application-level deletion is replicated to every member.

## Recommended Production Topology

The default production topology is three voters on independent persistent
volumes, spread across three failure domains in one low-latency region. It
tolerates one unavailable voter. Five voters tolerate two unavailable voters,
at the cost of additional replication traffic and quorum latency. See
[Deployment Profiles](./deployment-profiles.md#choose-the-topology-first).

Every committed mutation is synchronously replicated to a quorum. A caught-up
follower or learner can serve a default live read locally after completing a
quorum-backed `ReadIndex` barrier. This distributes normal read load, but it is
not an independently available read replica: the barrier still needs a healthy
quorum. An explicit stale read can bypass that barrier, but may return an older
local view. Independently asynchronous indexes also have different guarantees;
see
[Safety and Availability During Partitions](../technical/architecture/subsystems/consensus/raft-consensus.md#safety-and-availability-during-partitions)
and
[Linearizable Reads via ReadIndex](../technical/architecture/subsystems/consensus/raft-consensus.md#linearizable-reads-via-readindex).

## Multi-Region Options

### Three local voters plus a remote learner

A non-voting learner in a secondary region receives Raft log entries and
snapshots and, once caught up, can serve reads while it can reach the active
quorum. Because learner acknowledgements are not required to commit, its WAN
latency is outside the write quorum.

This is not automatic regional failover:

- a learner cannot vote or become leader;
- promoting it is a normal consensus operation and therefore requires an
  active leader and quorum;
- replication delay depends on the WAN, follower catch-up, and local apply
  progress and is not a declared backup RPO;
- automatic learner promotion must be disabled with
  `--learner-promotion-threshold 0`, then the intended local voters must be
  promoted manually. This is a cluster-wide control, not a per-node policy.

The current operator does not expose a first-class "secondary-region read
replica" topology. Treat a remote learner as an advanced, manually validated
deployment, not as a substitute for off-cluster backups.

### Voters stretched across regions

Voters may be placed across regions, but every write waits for a voter majority.
WAN latency therefore directly affects write latency and election behavior.

A three-voter cluster split across two regions survives loss of the region that
contains one voter, but not loss of the region that contains two. A five-voter
cluster tolerates two unavailable voters, provided the surviving placement can
still form a majority. Validate the exact region distribution, latency, and
failure modes with a production-shaped test before adopting a stretched
cluster.

For most deployments, prefer three voters across zones in one region plus
backups stored outside that region. Use a stretched topology only when its
measured latency and failure-domain behavior satisfy the customer's SLOs.

## Failure Scenarios

| Scenario | Expected behavior | Operator action |
|----------|-------------------|-----------------|
| One follower voter fails in a three-voter cluster | The remaining majority continues; the failed node catches up automatically after restart. | Replace or restart the node, then verify catch-up with `ledgerctl cluster status`. |
| The leader fails but a voter majority survives | The remaining voters elect a new leader; requests may fail transiently during election. | Let clients retry idempotently and verify the new leader. |
| A network partition leaves a majority and a minority | Only the majority side can commit writes or complete default linearizable reads. | Restore connectivity; replicas synchronize automatically. |
| Two voters are permanently lost and the surviving node still considers itself leader | Normal consensus cannot make progress. The leader can force-remove each permanently lost voter locally and continue as a one-node cluster. | Fence the lost nodes first, connect directly to the surviving leader, then use `ledgerctl cluster remove-node <id> --force` for each lost voter. Rebuild redundancy immediately. |
| Two voters are lost and the surviving node is not the leader | It cannot elect itself under the old three-voter membership, and force-remove is leader-only. | Restore a fresh cluster from an off-cluster backup. There is no supported "re-bootstrap this follower's data directory" procedure. |
| The entire cluster or its storage is lost | No replica remains. | Restore a fresh cluster from an off-cluster backup. |

The force-remove path deliberately bypasses Raft consensus. It is safe only
after every removed member has been permanently fenced and cannot run or
rejoin. Using it during a reversible partition can create two independently
writable histories. See
[Force-Removing a Down Node](./cluster-operations.md#force-removing-a-down-node).

Do not copy a surviving node's WAL or data directory and start it with
`--bootstrap`. Those directories contain node identity and Raft membership and
are not portable backup artifacts. Use the documented
[Backup and Restore](./backup-restore.md) flow when the surviving replica cannot
recover the existing cluster safely.

## Disaster-Recovery Baseline

1. Define RPO and RTO from customer requirements.
2. Run full backups periodically and incremental backups at least as often as
   the required RPO. Store them outside the cluster and region failure domain.
3. Retain the credentials, encryption/signing material, and compatible Ledger
   binary required by the restore procedure.
4. Exercise `restore validate`, a full bootstrap of production-sized data, and
   historical reads.
5. During an incident, fence the old cluster before force-removing members or
   bringing up a replacement cluster.
6. After recovery, add replacement nodes, wait for them to catch up, and verify
   that the intended voter majority has been restored.

See [Recovery Objectives](./deployment-profiles.md#recovery-objectives),
[Backup and Restore](./backup-restore.md), and
[Monitoring](./monitoring.md#alerting-recommendations).
