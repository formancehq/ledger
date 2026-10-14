# Server Binary Upgrades

## Release requirement from v3.0.0-rc.1

`v3.0.0-rc.1` is the first supported v3 persisted-state baseline. Every later
v3 release, including another RC and the stable release, must provide a tested
forward upgrade from the preceding released v3 version. The upgrade must
preserve all committed business data, audit history, and the state needed to
resume the cluster. If a format changes, ship and test its migration in the
target release. A full data wipe or reconstruction from an external system is
not an acceptable release upgrade. Beta and development revisions before
`v3.0.0-rc.1` remain outside this guarantee.

Before tagging a new release, verify the upgrade using state created by the
preceding released version. Cover the affected Pebble records, Raft log and
snapshot state, backup and restore formats, audit verification, and any
rebuildable projections. Document the upgrade procedure, whether mixed binaries
are safe, and the rollback boundary in the release notes. If the target cannot
read or migrate the source state, delay the release until it can.

The target release may require a coordinated restart or migration. Use its
declared upgrade class; Kubernetes rolling updates alone do not establish
safety.

Every target revision must explicitly identify one of these upgrade classes:

| Class | When it applies | Procedure |
|-------|-----------------|-----------|
| Rolling | The target revision explicitly supports mixed binaries and the existing storage schema. | Restart followers one at a time, wait for each to catch up, transfer leadership to an upgraded voter, then restart the old leader. |
| Coordinated restart | The storage schema is compatible, but mixed binaries can apply a committed entry differently. | Enable maintenance mode, wait for every voter to apply the maintenance barrier durably, stop every node, replace all binaries, then restart the cluster without a mixed-version window. |
| Migrate or restore | Persisted or backup formats change and a rolling or coordinated restart cannot read the old state directly. | Follow the tested migration procedure or restore a backup from the preceding release into the target format. Verify the complete state before serving writes. |

If the target revision does not declare its class, stop and obtain a release
decision. Do not guess from a successful single-node restart.

The change-specific constraints currently documented under
[Upgrading from pre-#400 clusters](./deployment.md#upgrading-from-pre-400-clusters)
and
[Upgrading across an FSM error-identity change](./deployment.md#upgrading-across-an-fsm-error-identity-change)
describe development upgrades before `v3.0.0-rc.1`. Their reset instructions do
not apply to upgrades from the RC or any later release.

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

Use this only when the target revision explicitly permits mixed binaries:

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
