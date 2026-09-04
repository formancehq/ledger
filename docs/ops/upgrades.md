# Server Binary Upgrades

## Current v3 Support Boundary

Ledger v3 is pre-GA and does not yet promise persisted-storage or wire-format
compatibility between development revisions. Do not assume that an in-place,
mixed-binary rolling upgrade is supported merely because Kubernetes can perform
a rolling update.

Every target revision must explicitly identify one of these upgrade classes:

| Class | When it applies | Procedure |
|-------|-----------------|-----------|
| Rolling | The target revision explicitly supports mixed binaries and the existing storage schema. | Restart followers one at a time, wait for each to catch up, transfer leadership to an upgraded voter, then restart the old leader. |
| Coordinated restart | The storage schema is compatible, but mixed binaries can apply a committed entry differently. | Enable maintenance mode, stop every node, replace all binaries, then restart the cluster without a mixed-version window. |
| Rebuild or restore | Persisted or backup formats are incompatible. | Follow the revision-specific reset instructions and rebuild from the declared source of truth or a backup explicitly supported by the target revision. |

If the target revision does not declare its class, stop and obtain a release
decision. Do not guess from a successful single-node restart.

The change-specific constraints currently documented under
[Upgrading from pre-#400 clusters](./deployment.md#upgrading-from-pre-400-clusters)
and
[Upgrading across an FSM error-identity change](./deployment.md#upgrading-across-an-fsm-error-identity-change)
take precedence over this general workflow.

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
2. Stop all nodes before starting any target binary.
3. Replace every server binary or image.
4. Start enough voters to form quorum, then start the remaining members.
5. Verify cluster health and integrity before disabling maintenance mode.

`ledgerctl upgrade` upgrades the `ledgerctl` client binary only. It does not
upgrade Ledger servers or decide whether a server revision is storage- or
wire-compatible.
