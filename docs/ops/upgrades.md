# Server Binary Upgrades

## Release requirement from v3.0.0-rc.1

`v3.0.0-rc.1` is the first supported v3 persisted-state baseline. Every later
v3 release, including another RC and the stable release, must provide a tested
forward upgrade from the preceding released version. Preserve committed
business data, audit history, and the state needed to resume the cluster. Ship
and test a safe migration when a format changes. A full data wipe or external
reconstruction is not an acceptable release upgrade. Beta and development
revisions before `v3.0.0-rc.1` remain outside this guarantee.

During the RC line, keep the service gRPC revision at 40 and document the client
impact and upgrade steps of behavior or configuration changes. See the
[protocol policy](../technical/architecture/subsystems/api/protocol-compatibility.md#maintaining-the-revision).
RC updates are sequential: upgrade from `v3.0.0-rc.1` to `v3.0.0-rc.2`
before proceeding to `v3.0.0-rc.3`. The same rule applies to each release
line. Skipping an RC is not supported. For operator-managed clusters, the
operator compares the current StatefulSet template's Ledger image tag with
`spec.image.tag`. It rejects an unknown or non-adjacent source, or an incomplete
source rollout, before changing cluster resources. The current image must have
a canonical RC tag; an untagged digest does not establish its release version.
If the StatefulSet is temporarily absent during recreation, restore the image
tag still used by its orphaned Pods before requesting the next RC. If only PVCs
remain, restore the source StatefulSet before changing the image tag.
Outside a mandatory stopped boundary, when an RC image change also changes
volume templates, the operator completes the RC rollout before recreating the
StatefulSet for the volume change.
Old and new voters must communicate and apply each committed entry identically
while they coexist, except when the embedded operator requires a complete stop
at a hardcoded release boundary. The first such boundary is `v3.0.0-rc.2`:
the operator requires a stopped upgrade from `v3.0.0-rc.1` to `rc.2`.
RC adjacency remains mandatory. Later RCs use the normal rolling path unless
another boundary is explicitly added to the operator.
When the running version precedes a mandatory boundary, pin the target to a
canonical version tag. Moving or non-canonical tags such as `latest` cannot
prove whether the boundary is crossed, so the operator blocks the change.

Before tagging a new release, verify the upgrade with state created by the
supported source releases. Cover affected Pebble records, Raft log and snapshot
state, backup and restore formats, audit verification, and rebuildable
projections. Document the upgrade procedure and rollback boundary in the
release notes. Delay the release if the target cannot read or migrate the source
state.

The upgrade classes below describe existing procedures. The operator's
hardcoded boundary selects a stopped restart for the affected RC; other RC
updates use the rolling class. Rebuild or restore applies only to the cases
listed below.

| Class | When it applies | Procedure |
|-------|-----------------|-----------|
| Rolling | Default between adjacent RCs on the same release line. | Restart followers one at a time, wait for each to catch up, transfer leadership to an upgraded voter, then restart the old leader. |
| Operator-required stopped restart | An adjacent RC crossing a hardcoded operator boundary (`v3.0.0-rc.2`). | Enable maintenance and verify a durable barrier on every voter; retain the existing PVCs, delete the StatefulSet, wait until it and every old Pod are gone, then recreate it with the target image and the same claims. The cluster is unavailable throughout the stop. |
| Coordinated restart | A separate compatibility line that cannot safely mix binaries. | Enable maintenance mode, wait for every voter to apply the maintenance barrier durably, stop every node, replace all binaries, then restart without a mixed-version window. Preserve or migrate the released data. |
| Migrate or restore | A separate line with a changed persisted or backup format, or development state before the first RC. | Follow a tested migration or restore a backup supported by the target release. A wipe or external rebuild is permitted only for unsupported pre-RC development state. Verify the complete state before serving writes. |

Before publishing an RC update, demonstrate its selected upgrade class from the
immediately preceding RC binary to the target binary in one cluster, including
writes, replay, restart, audit verification and non-empty WAL. For a rolling
release, include leadership transfer. Record source and target builds and the
results in release validation. If the path fails, fix it before release.

## Operator-required stopped restart

At a listed boundary, the operator stores `status.upgrade` before any workload
change. It enables replicated maintenance, reads a leader commit index after
maintenance is visible, and waits until every voter reports maintenance active,
normal sync and a durable FSM cursor at or beyond that index. This barrier
ensures already committed business entries are applied under the source binary.
The operator then changes `whenDeleted` and `whenScaled` PVC retention to
`Retain`, waits for the StatefulSet controller to observe that generation,
removes the existing claims' owner references, and verifies their UIDs and bound
PV identities. This path requires existing PVC-backed WAL and data volumes;
hostPath clusters are held for a separately validated manual procedure. A
missing, unbound, deleting or replaced claim or PV blocks the upgrade.
The operator then deletes the StatefulSet with background propagation. It
waits for the StatefulSet and **all old ordinal Pods** to disappear. A delete
acknowledgement or elapsed time does not satisfy this condition. It does not
force-delete a stuck Pod. Only then may it create the target StatefulSet and
reuse the same PVC names. After all target replicas are ready, it disables
maintenance. `status.upgrade` and the `Upgrade` condition show the
phase or blocking error. The operator retries after API failures; do not remove
the status record or create a replacement workload by hand while it is active.
If the replacement StatefulSet disappears before maintenance is released, the
operator waits for its Pods to terminate, verifies the original PVC/PV identities,
and recreates it while keeping maintenance active.

Keep the requested target tag, workload spec and replica count fixed until
`status.upgrade` clears. An edit during the operation is rejected. A direct
replica-count edit to the replacement StatefulSet reports `Upgrade=Blocked`
until the pinned count is restored. If a Pod will not terminate, diagnose its
node and storage attachment, then let Kubernetes finish termination; the
operator keeps the cluster unavailable. Verify the replacement is fully ready,
then check cluster status, representative reads and writes, audit verification
and backup compatibility. The operator does not delete Raft members or volumes.

Maintenance blocks business writes before the stop; in-flight proposals after
the barrier reject at apply. Cluster-internal technical entries may continue
and must remain replay-compatible with the target. Clients must retry an
unacknowledged request with the same idempotency key. A release that changes
replay or apply semantics must prove this
with a non-empty source cluster before publishing. Rollback to an older binary
is allowed only before the target has written incompatible persisted data, and
only with a separately validated downgrade procedure. After that point, keep
the target or restore through a supported forward recovery path.

For upgrades from builds before `v3.0.0-rc.1`, the change-specific constraints
documented under
[Upgrading from pre-#400 clusters](./deployment.md#upgrading-from-pre-400-clusters)
and
[Upgrading across an FSM error-identity change](./deployment.md#upgrading-across-an-fsm-error-identity-change)
describe development upgrades before `v3.0.0-rc.1`. Their reset instructions
do not apply to upgrades from the RC or any later release.

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

## Account metadata address validation

Upgrading from a binary with unordered transaction account-metadata address
validation (including `v3.0.0-rc.1`) to deterministic smallest-address selection
requires a maintenance barrier before replacing binaries. An ordinary rolling upgrade
with writes enabled is unsafe: an old voter may select a different typed
failure, forking the authoritative audit hash. Admission on a new leader alone
cannot protect an old leader or previously committed proposals.

For an operator-managed `rc.1` to `rc.2` upgrade, use the mandatory stopped
procedure above; it supplies the maintenance barrier and never mixes binaries.
The rolling procedure below applies only where a separately validated manual
rollout is required outside that operator boundary.

1. With every voter still on the source binary, establish a healthy, consistent
   audit history and take a backup. Existing divergent history is an incident;
   this fix does not reconcile or rewrite it.
2. Enable maintenance, record leader commit index `C` after maintenance becomes
   visible, and require **every voter** to be normal with
   `raftStatus.lastPersistedIndex >= C`. Drain cluster-internal work before
   recording the final barrier. Do not upgrade an offline or lagging voter from
   pre-barrier state.
3. Keep maintenance enabled while following the rolling procedure: replace
   followers one at a time, transfer leadership, then replace the former leader.
   Preserve quorum. Do not disable maintenance or permit an administrative batch
   to disable it while any old voter remains.
4. Confirm that every current voter uses the target binary, has recovered the
   persisted maintenance state and has caught up. Only then disable maintenance
   and verify representative writes and integrity checks.

Both binaries enforce the existing replicated maintenance gate before business
order processing. In-flight business proposals after `C` therefore reject with
`MAINTENANCE_MODE` without reaching address validation or creating audit/frozen
failure records. Technical-only proposals do not execute the transaction
processor. This is a staged rolling deployment with a write pause; nodes are
never stopped together. Release validation must exercise source/target voters,
leadership transfer, rejected writes throughout coexistence, restart and writes
before and after the maintenance window. The focused FSM/recovery regression
alone does not establish that mixed-binary release validation.

A restart resumes after its durable applied cursor; it must not start from a
pre-barrier checkpoint that reexecutes ambiguous old proposals under the new
binary. A replacement voter must synchronize from a healthy post-barrier voter.
Persisted audit bytes, hashes and failure details remain unchanged. The checker
verifies stored failures and reexecutes successful proposals only. Incremental
restore copies audit evidence and reconstructs keyed failures from those stored
failure details, preserving whichever error the old cluster committed rather
than applying the new address selection to historical failures. No protobuf,
storage-format or service-protocol revision changes are required.

Rollback while maintenance is active must retain the same barrier discipline.
After reopening writes on the fixed binary, do not reintroduce an affected old
voter or downgrade without a separately validated procedure.

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
