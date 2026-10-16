# Server Binary Upgrades

## Current v3 Support Boundary

Ledger v3 is in its release-candidate phase. Later RC builds must keep existing
persisted and audited data readable. A format change needs a safe migration;
do not require a data wipe to adopt a later RC build. Client-visible behavior
and configuration may change when the impact and upgrade steps are documented.
The service gRPC revision stays at 40; see the
[protocol policy](../technical/architecture/subsystems/api/protocol-compatibility.md#maintaining-the-revision).
Updates within the RC line must also support a direct rolling server upgrade
from every earlier published RC in that line, including when an operator skips
one or more RCs. Old and new voters must communicate and apply each committed
entry identically while they coexist. A change that cannot meet this requirement
needs a staged rollout design or must wait for a different compatibility line.

The upgrade classes below describe existing procedures. RC changes must use the
rolling class; coordinated restart and rebuild or restore are retained for
earlier development revisions and explicitly separate compatibility lines.

| Class | When it applies | Procedure |
|-------|-----------------|-----------|
| Rolling | Required between builds on the same RC line. | Restart followers one at a time, wait for each to catch up, transfer leadership to an upgraded voter, then restart the old leader. |
| Coordinated restart | Earlier development revisions or a separate compatibility line that cannot mix binaries. | Enable maintenance mode, wait for every voter to apply the maintenance barrier durably, stop every node, replace all binaries, then restart the cluster without a mixed-version window. |
| Rebuild or restore | Earlier development revisions or a separate compatibility line with incompatible persisted or backup formats. | Follow the revision-specific reset instructions and rebuild from the declared source of truth or a backup explicitly supported by the target revision. |

Before publishing an RC update, demonstrate the rolling class separately from
each earlier published RC binary to the target binary in one cluster, including
leadership transfer, writes, replay, and restart. Record the source and target
builds and the results in the release validation. If any path fails, fix the
change before release. Do not infer support from a successful single-node restart.

For upgrades from builds before `v3.0.0-rc.1`, the change-specific constraints
documented under
[Upgrading from pre-#400 clusters](./deployment.md#upgrading-from-pre-400-clusters)
and
[Upgrading across an FSM error-identity change](./deployment.md#upgrading-across-an-fsm-error-identity-change)
take precedence over this RC workflow.

## Preflight

Before any server upgrade:

1. Read the target revision's release and deployment notes for storage, backup,
   protobuf, audit-hash, and mixed-version restrictions.
2. Verify the current cluster has the expected voter count, one leader, and no
   lagging or syncing member with `ledgerctl cluster status`.
3. Complete a backup and verify that its artifacts exist outside the cluster
   failure domain. Confirm that the target revision can restore that backup
   format.
4. Confirm rollback semantics. Never start an older binary on storage already
   opened by a newer binary unless that downgrade is explicitly supported.
5. Ensure clients use stable idempotency keys for retries during leadership
   changes or transient unavailability.

## Rolling Procedure

Use this for RC updates after the mixed-version validation above passes:

1. Record the current leader and voter set with `ledgerctl cluster status`.
2. Restart one follower on the target binary.
3. Wait until it reports normal, has caught up to the leader's commit index,
   and can serve a health check.
4. Repeat for the other followers, one at a time. Never take enough voters down
   to lose quorum.
5. Transfer leadership to an upgraded, caught-up voter with
   `ledgerctl cluster transfer-leader <node-id>`.
6. Restart the former leader on the target binary and wait for it to catch up.
7. Re-run cluster status, health, representative reads and writes, and the
   required integrity checks.

For a change to replicated runtime configuration rather than the binary, use
the separate
[Cluster Configuration Updates](./cluster-operations.md#cluster-configuration-updates)
procedure.

## Coordinated Restart

When persisted storage is compatible but mixed binaries are not:

1. Enable [maintenance mode](./maintenance-mode.md) and verify writes are
   rejected.
2. Query the leader with `ledgerctl cluster status --json` and record its
   `raftStatus.commit` index after maintenance mode is visible. Call this
   barrier `C`.
3. Query every voter with
   `ledgerctl cluster status --node-id <id> --json`. Before stopping any node,
   require its sync status to be normal and
   `raftStatus.lastPersistedIndex >= C`. This proves that every voter has
   durably applied all entries through the maintenance barrier under the old
   binary. Stop or finish any cluster-internal job that can still propose work,
   then repeat the check if the leader's commit index advanced.
4. Stop all nodes before starting any target binary.
5. Replace every server binary or image.
6. Start enough voters to form quorum, then start the remaining members.
7. Verify cluster health and integrity before disabling maintenance mode.

`ledgerctl upgrade` upgrades the `ledgerctl` client binary only. It does not
upgrade Ledger servers or decide whether a server revision is storage- or
wire-compatible.
