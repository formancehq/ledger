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
while they coexist. The embedded operator can require a complete stop at a
listed release boundary, but no boundary is active for `v3.0.0-rc.3`:
`rc.2` to `rc.3` remains a rolling upgrade. `rc.2` already includes the drain
barrier needed by a future stopped upgrade. RC adjacency remains mandatory.
The `rc.1` to `rc.2` transition uses the separately validated maintenance-based
procedure below.
When the running version precedes a mandatory boundary, pin the target to a
canonical version tag. Moving or non-canonical tags such as `latest` cannot
prove whether the boundary is crossed, so the operator blocks the change.

### Preflight for Clusters using `latest`

Before upgrading to a release Operator with version admission, inspect each
Cluster's `spec.image.tag` and the Ledger image in its owned StatefulSet. The
`v3.0.0-rc.1` Helm chart used `latest` as its Ledger image default. A release
Operator rejects an opaque declared tag. If the Cluster requests an RC while
the StatefulSet still uses `latest`, the RC source check also rejects the
transition. Changing only `spec.image.tag` after the Operator upgrade does not
repair that state.

Do not start the Operator upgrade while either tag is `latest`. While the
current Operator can still reconcile the Cluster, verify the running image
digests against a published RC and pin the Cluster to that **same source RC**.
Wait for the StatefulSet rollout to converge, then verify representative
business data and audit history before upgrading to the next RC. Continue
through each adjacent RC in order; do not jump to the newest RC. If the source
cannot be identified or the current Operator already rejects the pinning step,
stop and plan a separately validated migration. Do not delete the StatefulSet
or PVCs to bypass the source check.

Before tagging a new release, verify the upgrade with state created by the
supported source releases. Cover affected Pebble records, Raft log and snapshot
state, backup and restore formats, audit verification, and rebuildable
projections. Document the upgrade procedure and rollback boundary in the
release notes. Delay the release if the target cannot read or migrate the source
state.

The upgrade classes below describe existing procedures. An explicitly listed
operator boundary selects a stopped restart for the affected RC; no current
boundary is listed, so `rc.2` to `rc.3` uses the rolling class. Rebuild or
restore applies only to the cases
listed below.

| Class | When it applies | Procedure |
|-------|-----------------|-----------|
| Rolling | Default between adjacent RCs on the same release line. | Restart followers one at a time, wait for each to catch up, transfer leadership to an upgraded voter, then restart the old leader. |
| Operator-required stopped restart | An adjacent RC crossing a listed operator boundary; none is active for `rc.3`. | Establish an operator-owned drain and verify its durable barrier on every member; retain the existing PVCs, delete the StatefulSet, wait until it and every old Pod are gone, then recreate it with the target image and the same claims. The cluster is unavailable throughout the stop. |
| Coordinated restart | A separate compatibility line that cannot safely mix binaries. | Enable maintenance mode, wait for every voter to apply the maintenance barrier durably, stop every node, replace all binaries, then restart without a mixed-version window. Preserve or migrate the released data. |
| Migrate or restore | A separate line with a changed persisted or backup format, or development state before the first RC. | Follow a tested migration or restore a backup supported by the target release. A wipe or external rebuild is permitted only for unsupported pre-RC development state. Verify the complete state before serving writes. |

Before publishing an RC update, demonstrate its selected upgrade class from the
immediately preceding RC binary to the target binary in one cluster, including
writes, replay, restart, audit verification and non-empty WAL. For a rolling
release, include leadership transfer. Record source and target builds and the
results in release validation. If the path fails, fix it before release.

## Operator-required stopped restart

At a listed boundary, the operator stores `status.upgrade` before any workload
change. It persists a drain epoch, preflights every member's drain and release
support, then commits a Raft-ordered admission cutoff. The operator waits until
every captured member reports that epoch and cutoff, normal sync, and a durable
FSM cursor at or beyond the cutoff. This ensures already committed business
entries are applied under the source binary. Deploy a drain-capable source
binary to every member before requesting this upgrade.
The upgrade state machine owns only the stop barrier, the PVC identity and
retention checks, and the decision to release ordinary reconciliation. The
ordinary StatefulSet reconciler creates the replacement and applies its pod
template and mutable fields. Both paths use the same retention policy builder;
while `status.upgrade` exists it selects `Retain`, and after the drain is
released it restores the requested policy.
While the workload is stopped, reconciliation still repairs the headless Service
needed to reach voters, the ServiceAccount, and the TLS cluster Secret and
auth-key ConfigMap needed by replacement Pods. It keeps an existing auth-key
ConfigMap until normal workload reconciliation resumes.
Keep `spec.headlessService.enabled` true for this upgrade. The operator rejects
the transition if it is disabled and preserves the Service if it is switched off
after the stopped upgrade starts.
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
reuse the same PVC names. The replacement keeps both PVC retention policies at
`Retain` throughout the active upgrade, including recovery if the replacement
disappears. After all target replicas are ready, the operator commits the drain
release and waits for every member to persist it. Normal reconciliation then
restores the requested retention policies. `status.upgrade` and the `Upgrade` condition show the
phase or blocking error. The operator retries after API failures; do not remove
the status record or create a replacement workload by hand while it is active.
If the replacement StatefulSet disappears before the drain is released, the
operator waits for its Pods to terminate, verifies the original PVC/PV identities,
and recreates it while keeping the drain active.

Keep the requested target tag, workload spec and replica count fixed until
`status.upgrade` clears. An edit during the operation is rejected. A direct
replica-count edit to the replacement StatefulSet reports `Upgrade=Blocked`
until the pinned count is restored. If a Pod will not terminate, diagnose its
node and storage attachment, then let Kubernetes finish termination; the
operator keeps the cluster unavailable. Verify the replacement is fully ready,
then check cluster status, representative reads and writes, audit verification
and backup compatibility. The operator does not delete Raft members or volumes.

The drain blocks business writes before the stop; in-flight proposals after
the cutoff reject at apply. Cluster-internal technical entries may continue
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

The Ledger Operator blocks a declared Ledger image tag newer than its own
release version, or one that cannot be compared, before changing the
StatefulSet. Upgrade the Operator first, then change the Ledger image. This
ordering check applies before an Operator-managed workload change; it does
not establish mixed-version compatibility or make a downgrade safe. Mutable
registry tags can serve different contents, which this check cannot verify.
If an Operator downgrade encounters a newer declared Ledger version,
reconciliation stops with `VersionCompatible=False` while the existing workload
is retained. Install a compatible Operator to resume reconciliation.

## Rolling Procedure

Use this only when the target release permits rolling upgrades and mixed-version
validation passes. For RC.1 to RC.2, use the [coordinated restart](#coordinated-restart)
and install the [required-downtime operator](#proposal-payload-cutover) first.

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

If the target release requires a full stop, including RC.2, follow the coordinated
restart procedure instead. The procedure below applies when rolling is permitted.

Upgrading from a binary with unordered transaction account-metadata address
validation (including `v3.0.0-rc.1`) to deterministic smallest-address selection
requires a maintenance barrier before replacing binaries. An ordinary rolling upgrade
with writes enabled is unsafe: an old voter may select a different typed
failure, forking the authoritative audit hash. Admission on a new leader alone
cannot protect an old leader or previously committed proposals.

For `rc.1` to `rc.2`, use the maintenance-based coordinated procedure below;
the published `rc.1` binary cannot participate in an operator-owned drain.
The `rc.2` to `rc.3` upgrade is rolling; the drain-based stopped-upgrade path
is available for a later release that explicitly lists a boundary.

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

### Reproducible binary rolling-upgrade validation

After integrating the harness into `release/v3.0`, run from that revision:

```bash
just test-rolling-upgrade v3.0.0-rc.1 origin/release/v3.0
```

The command takes source and target Git refs. Tags and commit SHAs must exist
locally; an `origin/<branch>` argument fetches that branch before resolving it.
The Chainsaw launcher resolves both refs to commit SHAs and records those
SHAs, Linux binary hashes, and local image IDs.
Compilation, hashing, image build, and image inspection must all succeed before
the kind cluster is created; a locally cached image cannot replace a failed build.
It requires a clean harness checkout, the target to contain its commit, and the
target to descend from the distinct source commit; it never merges or moves a
local branch.
`KEEP_WORKDIR=1` retains the extracted
trees, binaries, image IDs, failed Pod logs, and revision record for the release
validation report. `just test-rolling-upgrade v3.0.0-rc.1 <target-sha>`
rehearses a specific local commit without fetching the release branch. The
release-branch example refuses to run while `release/v3.0` lacks the integrated
harness.

The test creates a dedicated kind cluster and Chainsaw namespace. Three Pods
mount separate persistent WAL and data claims and boot from an image containing
the compiled source binary. It creates a ledger and transaction through gRPC,
then verifies the account on every voter. At the
maintenance barrier it requires each voter to be normal and durably past the
leader's recorded commit index. It replaces each follower separately, probes
ledger creation and transaction posting against the existing ledger on every
ready live voter during deletion, rejoin, and catch-up, retries inconclusive
responses in the deletion gap, and checks both rejections and
unchanged account data on every voter in stable maintenance windows. It
transfers leadership to an upgraded voter and replaces the old leader. After
all three voters run the target binary, it completes a final maintenance
window, disables maintenance, and verifies the original
account, commits another transaction, checks its readback on every voter, and
runs the audit and derived-state checker on each voter. Missing observations or
an unexpected write success fail the run.

This local test does not cover backup restore, snapshot installation, operator
rollout, or every business operation and persisted record type. It requires
Docker and Nix; the `.#upgrade` shell supplies kind, kubectl, Chainsaw, and jq.
The existing
model and restore suites and release-specific migration tests remain separate
gates. It also does not authorize rollback onto storage opened by the target.

### Operator-managed rc.2 to rc.3 rolling validation

After the current `main` has been integrated into `release/v3.0`, run the
`Release operator upgrade` GitHub Actions workflow with `release/v3.0` and
`rc2-to-rc3`, or run
the same recipe locally from a clean harness checkout:

```bash
RELEASE_BRANCH=release/v3.0 just test-release-operator-upgrade
```

The launcher fetches and pins the exact `release/v3.0` HEAD and the published
source RC selected by the transition. It requires the target to descend from
that source and to contain the harness commit from `main`. It pulls the
published source Ledger and operator images, then builds the target operator
and Ledger images from that one pinned target SHA. For the default transition,
it gives the local images canonical `v3.0.0-rc.3` tags and compiles the
operator with that version for admission;
this does not create a Git tag or publish either image. The recorded Docker
image IDs and running Pod image IDs bind the test to the exact local builds.
The launcher saves an image archive for the Docker daemon's Linux architecture
before loading Kind. Published source tags are multi-platform, but a pull may
cache only one platform; importing the incomplete index for all platforms
fails before Chainsaw starts. The selected `imageArchivePlatform` is recorded
in `revisions.txt` alongside the image IDs.
`TARGET_REF=<local-sha>` permits a local rehearsal against a commit containing
the harness and rc.2, such as a temporary integration merge; the workflow
always uses the fetched release branch HEAD. The workflow uploads revisions,
image IDs, Raft status, Chainsaw output, Kubernetes resources and Pod logs.
The harness keeps the Chainsaw namespace until diagnostics are collected, then
deletes the Kind cluster.

For the default transition, Chainsaw creates a three-voter Cluster with
published rc.2 images, commits
non-empty business data, and records Pod, PVC and Raft identities. It upgrades
the operator through Helm first, verifies the operator image and Pod changed
while Ledger Pods, PVCs and data did not. With the rc.3 operator running, it
requests a non-adjacent rc.1 downgrade and requires the RC admission error
without any Ledger Pod, image or PVC change. It then requests the local rc.3
Ledger image, observes old and new voters Ready together with at least two
Ready Pods, and requires the operator's stopped-upgrade state to stay empty.
After every Pod is replaced and every PVC retains its identity, it verifies
the original data on all voters, commits another transaction, requires the
Raft commit index to advance, and runs each voter's audit and derived-store
checker.

This scenario proves the operator-first rolling path, adjacent RC admission,
non-empty data continuity, retained PVCs, and final Raft and audit health for
the pinned rc.2 and target revisions. It does not write during mixed Ledger
versions, exhaustively compare every persisted record type, exercise snapshot
installation, backup/restore, rollback, or the dormant stopped-upgrade path.
The pinned binary-level rc.1 test and relevant model, restore and migration
suites remain separate gates.

The same harness has a separate, future `rc3-to-rc4` transition:

```bash
UPGRADE_TRANSITION=rc3-to-rc4 RELEASE_BRANCH=release/v3.0 just test-release-operator-upgrade
```

Run it only after `v3.0.0-rc.3` has been published and the PR changing the
persisted format has added `v3.0.0-rc.4` to the operator's mandatory stop
boundaries. The target operator and Ledger are still built from one exact
release-branch HEAD and tagged locally as rc.4; the harness publishes no tag
or image. This path requires the operator's drain barrier to advance past
`Draining`, observes the StatefulSet and all old Pods absent before accepting
any target Pod, and checks that the replacement StatefulSet has a new UID while
PVC UIDs persist. It restarts a target voter to exercise replay, verifies the
pre-upgrade account, commits another transaction, checks Raft progress and
runs each voter's audit/derived-state checker after drain release. It fails
against the current empty mandatory-stop list, so it is a release gate for the
format-change PR, not evidence that rc.3 itself needs a stop. A future rc.4
release still needs format-specific migration, snapshot, backup/restore and
rollback validation outside this scenario.

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

## Proposal payload expansion

The RC.1 to RC.2 transition requires the coordinated restart procedure above.
Stop every old node before starting any target node, and keep the WAL and data
volumes. The target reads historical entries and writes `ExecutionBatch` or
`SetClusterPolicyProposal` directly. Existing snapshots, backups and audit
history need no rewrite or backfill. Native policy controls persist operational
state without business audit entries. Historical policy audits remain readable
with unchanged hashes. Incremental backups preserve newer policy in a separate
manifest snapshot, including policy-only changes. No capability flag enables
this format.

This phase adds no new rollback restriction. Emitting payload-only Raft entries
is a separate, future cutover requiring cluster-wide decoder readiness and its
own rollback boundary. Do not infer permission to emit them merely because the
new binary can decode them. See the [proposal migration contract](../technical/architecture/subsystems/consensus/proposal-payloads.md).
