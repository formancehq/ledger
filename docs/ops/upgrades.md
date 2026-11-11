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
the effective requested image tag: `spec.image.tag`, or `LEDGER_IMAGE_TAG`
from the operator deployment when that field is omitted (`latest` if neither
sets it). It rejects an unknown or non-adjacent source, or an incomplete
source rollout, before changing cluster resources. The current image must have
a canonical RC tag; an untagged digest does not establish its release version.
If the StatefulSet is temporarily absent during recreation, restore the image
tag still used by its orphaned Pods before requesting the next RC. If only PVCs
remain, restore the source StatefulSet before changing the image tag.
Outside a mandatory stopped boundary, when an RC image change also changes
volume templates, the operator completes the RC rollout before recreating the
StatefulSet for the volume change.
Old and new voters must communicate and apply each committed entry identically
while they coexist. The embedded operator requires a complete stop at the
`v3.0.0-rc.4` boundary because RC.3 cannot execute native proposal payloads
and encodes [mirror asset mappings](#mirror-asset-mappings) differently.
`rc.3` includes the drain barrier used for this upgrade. RC adjacency remains mandatory.
The `rc.1` to `rc.2` transition rolls one voter at a time with a maintenance
barrier that pauses writes; it does not stop the whole cluster.
The `rc.2` to `rc.3` transition remains rolling.
Every explicit-target `UpdatePreparedQuery` order must wait until the stopped
`rc.3` to `rc.4` upgrade completes. Even when the target value is unchanged,
the new processor records target fields in the audit log, so old and new voters
would produce different audit bytes for the same order. A changed target also
changes replicated query state. The release containing these updates must list
and validate the mandatory stopped boundary before clients submit them. The
operator drain path is in place; [PR #2404](https://github.com/formancehq/ledger/pull/2404)
coordinates that boundary. Until `rc.4` is active, clients must not submit
explicit-target updates; use delete plus create for target changes.
When the running version precedes a mandatory boundary, pin the target to a
canonical version tag. Moving or non-canonical tags such as `latest` cannot
prove whether the boundary is crossed, so the operator blocks the change.
Likewise, the operator blocks a canonical target at or past a boundary when the
running image has no canonical version tag, such as `latest` or a digest.

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

The upgrade classes below describe existing procedures. The operator boundary
at `rc.4` selects a stopped restart from `rc.3`; rebuild or restore applies
only to the cases listed below.

| Class | When it applies | Procedure |
|-------|-----------------|-----------|
| Rolling | Default between adjacent RCs on the same release line. | Restart followers one at a time, wait for each to catch up, transfer leadership to an upgraded voter, then restart the old leader. |
| Operator-required stopped restart | An adjacent RC crossing a listed operator boundary; `rc.3` to `rc.4` crosses one. | Establish an operator-owned drain and verify its durable barrier on every member; retain the existing PVCs, delete the StatefulSet, wait until it and every old Pod are gone, then recreate it with the target image and the same claims. The cluster is unavailable throughout the stop. |
| Coordinated restart | A separate compatibility line that cannot safely mix binaries. | Enable maintenance mode, wait for every voter to apply the maintenance barrier durably, stop every node, replace all binaries, then restart without a mixed-version window. Preserve or migrate the released data. |
| Migrate or restore | A separate line with a changed persisted or backup format, or development state before the first RC. | Follow a tested migration or restore a backup supported by the target release. A wipe or external rebuild is permitted only for unsupported pre-RC development state. Verify the complete state before serving writes. |

Before publishing an RC update, demonstrate its selected upgrade class from the
immediately preceding RC binary to the target binary in one cluster, including
writes, replay, restart, audit verification and non-empty WAL. For a rolling
release, include leadership transfer. Record source and target builds and the
results in release validation. If the path fails, fix it before release.

An accepted received snapshot may replace a node's Raft WAL directory while
retaining the historical `raft/etcd/` path and WAL record format. If a process
stops between the two directory renames, restart it with the version that
started the replacement so startup can complete the directory recovery. Once
`raft/etcd/` is present again, the preceding RC can read the completed WAL.

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
and must remain replay-compatible with the target. Clients normally retry an
unacknowledged request with the same idempotency key. An RC.3 batch containing
a targetless `UpdatePreparedQuery` is an exception after the RC.4 upgrade: an
identical retry fails `PREPARED_QUERY_TARGET_REQUIRED` before the retained
outcome lookup. Adding `target` changes the request hash and conflicts with a
retained key. Before submitting any replacement work, use `ledgerctl audit list`
with an exact `idempotency_key` filter and inspect the prepared query and any
other affected business state. Reconcile whether the original batch committed
and its outcome; if the evidence is incomplete, keep the outcome unresolved
and do not issue a replacement batch that could duplicate its effects. A release
that changes replay or apply semantics must prove this
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

## Operator workload-hash migration

Install the updated CRD and upgrade the Operator alone before changing storage
or ServiceAccount inputs. Keep the current Cluster declaration unchanged and
wait for a valid hash association on the current owned StatefulSet before
applying those edits. StatefulSet metadata `ledger.formance.com/pod-config-hash`
and installed Pod-template `ledger.formance.com/spec-hash` must both be
nonempty, and StatefulSet metadata `ledger.formance.com/pod-config-spec-hash`
must equal that installed Pod hash. Marker presence alone is insufficient.
The association records the actual retained hash, including historical hashes;
it survives recreation without depending on StatefulSet UID or generation.
This permits automatic exact reconstruction even when a released hash includes
an earlier declared storage size that never reached its claim templates, subject
to a successful StatefulSet Update preview with equal admitted templates.

If combined edits already produce `HashMigrationRequired`, temporarily restore
only those concurrent declared inputs to their authentic previous declaration,
wait for the valid association, then reapply the edits. The previous declaration
must be known from trusted records; live claim templates and the current
ServiceAccount cannot establish it. The PVC reconciler runs independently and
may already have accepted growth. Do not shrink PVCs or revert live growth;
restoring declared inputs does not undo accepted capacity or CSI effects. If
the prior declaration is unavailable or reconstruction still fails, recover the
missing evidence and leave the ambiguous migration blocked. Strict historical
proof, described below, is an alternative that retains the concurrent edits.
ServiceAccount reconciliation can already have written requested annotations
before a migration error blocks the Pod-template update. That error does not
undo those annotations or refresh existing Pods' admission inputs.

Released StatefulSets can carry the full-spec Pod hash without a valid
canonical association. Missing or broken association uses verified legacy
migration and does not alone request a storage-only rollout. For this migration
without explicit proof, the operator first normalizes the desired Pod template
using the installed hash through a dry-run Update of the existing StatefulSet
and compares it with the live template. Equal admitted templates preserve the
installed hash automatically only after exact released-hash reconstruction using
the current nonstorage inputs and retained claim settings. A difference requests
the normal canonical-hash rollout, even for a reconstructible hash; upgrading to
the adjacent RC does not require historical proof just because the explicit image
tag or the operator's implicit `LEDGER_IMAGE_TAG` changed. Configuration and
TLS changes follow the same rule. Existing RC admission, TLS sequencing and
storage rollout gates remain in force. A dry-run error reports
`ClaimTemplatesReady=False/AdmissionCheckFailed` before canonical metadata or
recovery intent/journal writes. This StatefulSet Update preview does not execute
Pod Create admission or certify actual IAM/runtime access.

Equal templates without exact reconstruction cannot establish whether external
admission inputs such as ServiceAccount annotations changed. Only this ambiguous
no-proof case reports
`ClaimTemplatesReady=False/HashMigrationRequired` without advancing workload
hashes or starting storage replacement. If the unchanged-input procedure above
does not establish history, follow the
[historical proof procedure](../../misc/operator/README.md#released-workload-hash-migration)
using the admitted historical spec, the released `applyDefaults` values and
that deployment's `LEDGER_IMAGE_REPOSITORY`/`LEDGER_IMAGE_TAG`. Retain omissions
where they entered the released hash. Explicitly supplied malformed, foreign,
stale or inconsistent proof remains `HashMigrationInvalid` even when the desired
template differs. Correct that proof or deliberately remove it before retrying
the no-proof path; a rollout cannot bypass supplied proof validation. Do not
reset hash annotations or reconstruct Ledger business data to bypass this
operational metadata check.

Only a valid association gives canonical metadata precedence over leftover
proof; a trusted canonical mismatch still requests the normal rollout. Normal
and preparatory Raft scale-down writes record both metadata hashes atomically
after final Pod-template resolution, using its actual retained hash. The
preparatory write retains the old replica count before Raft exec. If the Raft
operation then fails, retry uses that durable pair instead of rejecting the
original proof against the updated Pod hash. This write does not prove rollout
or Raft completion. Verify the subsequent rollout,
membership removal, replica count and retained storage separately.
The replacement snapshot also records both metadata hashes before encoding and
digesting. Existing immutable journals are never edited to add the association.

The updated CRD must also retain the whole claim-template recovery intent:
`sourceUID`, full `payloadSHA256`, digest-derived `configMapName` and the optional
`configMapUID` binding. Preparing status is committed and verified before the
immutable journal is created; an old CRD that prunes these fields blocks the
operation before orphan deletion. Protected status authorizes exact journal
bytes, and nil status never adopts a standalone ConfigMap. Do not downgrade
the Operator while this pointer exists. The feature is unpublished, so there is
no migration of unauthenticated old feature pointers. Journal version `1`,
Ledger data, Raft, audit and service protocol versions stay unchanged. See the
[durable recovery contract](../technical/architecture/subsystems/operator/storage-reconciliation.md#durable-template-replacement)
for pointer CAS, absence-abort and late-journal handling.

After recovery completes, a released Operator can retain unknown metadata while
changing the Pod hash. The new Operator detects the broken association and
repeats verified migration. For a new-Operator configuration A followed by
released-Operator ServiceAccount B and then desired A, an opaque equal template
without usable history must report `HashMigrationRequired`. Authentic B history
instead establishes the required rollout to A. Stale A metadata cannot certify
B's installed hash. The old binary can itself request a storage-hash rollout;
the new controller adds none when exact legacy reconstruction succeeds and the
StatefulSet Update preview admits an equal template.

Failed attempted intent/journal preparation uses memory rate control per
Cluster namespace/name, retaining only its latest UID and full prepared payload
digest: an initial two-second hold increases exponentially to 30 seconds.
Before a new Preparing condition or intent CAS, the controller checks the hold. Watch events cannot
bypass it, churn preparation conditions, increment its attempt or slide its
deadline. Safe mutable-workload/protection work during the hold still requires
no durable attempt in that pass and direct APIReader proof of the same
nonterminating Cluster UID with no recovery pointer. The controller requests
a positive `RequeueAfter`, retaining an earlier positive requeue from normal
work, without a preparation error; other actual errors and normal availability
status remain visible. A Preparing absence-abort
still requests a two-second requeue. Neither request guarantees execution at
that cadence. Mutex-protected state keeps one entry per namespace/name. New
UID/digest resets the hold; successful start or Completed-pointer clear removes
matching state. Safe Preparing absence-abort retains the hold; deletion or
`NotFound` removes the entry.
Ten minutes without a failed attempt expires it after its retry deadline, with
no cardinality cap or active-entry eviction. Cleanup scans scale with recently
failed Cluster count. Restart loses it. This rate-control memory never authorizes recovery or changes journal trust.

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
validation passes. For RC.1 to RC.2, follow the
[maintenance-barrier rolling procedure](#account-metadata-address-validation).
For RC.3 to RC.4, use the [operator-required stopped restart](#operator-required-stopped-restart).

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

## Metadata-limit replay recovery

Upgraded leaders can recover a retained keyed outcome when a retry's original
metadata exceeds a subsequently tightened committed policy. The retry must
still pass authentication, signatures, structural validation and write-readiness
gates. A matching live key returns the historical success or definitive
failure; live different content returns conflict; absent or expired keys execute under the policy at apply and
record any definitive metadata-limit refusal in the audit chain. Maintenance and drain still refuse writes.

Public admission now forwards safely prepared requests to the existing FSM
handlers instead of rejecting metadata policy locally. Fresh refusals, including
unkeyed requests, are audited; a retained refusal still replays after policy is
loosened. Replays replicate the ordinary business payload to the WAL without
adding another audit entry. Transport caps remain the proposal-size backstop.
This is an admission-only correction: old and new voters interpret committed
entries identically. It changes no Raft, persisted, backup, audit or wire format,
requires no data migration and keeps service protocol revision 40. During an
otherwise supported rolling upgrade, a leader running the old binary may still
return the limit rejection; transfer leadership to an upgraded voter to obtain
the corrected recovery behavior. Clients should retain the original key and
payload when recovering an ambiguous earlier attempt. The existing retention
window and transport caps still apply; this adds no guarantee after expiry.
See [the mechanism and cost boundary](../technical/architecture/subsystems/admission/idempotency.md#replay-after-metadata-limits-tighten).

## Account metadata address validation

RC.1 to RC.2 uses the maintenance-barrier rolling procedure below: writes pause,
but voters are replaced one at a time and quorum remains available. RC.2 to RC.3
remains rolling. RC.3 to RC.4 uses the
[operator-required stopped restart](#operator-required-stopped-restart).

Upgrading from a binary with unordered transaction account-metadata address
validation (including `v3.0.0-rc.1`) to deterministic smallest-address selection
requires a maintenance barrier before replacing binaries. An ordinary rolling upgrade
with writes enabled is unsafe: an old voter may select a different typed
failure, forking the authoritative audit hash. Admission on a new leader alone
cannot protect an old leader or previously committed proposals.

The published `rc.1` binary cannot participate in an operator-owned drain.

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

Run it against a release branch whose operator lists `v3.0.0-rc.4` as a
mandatory stop boundary. The target operator and Ledger are still built from
one exact release-branch HEAD and tagged locally as rc.4; the harness publishes no tag
or image. This path requires the operator's drain barrier to advance past
`Draining`, observes the StatefulSet and all old Pods absent before accepting
any target Pod, and checks that the replacement StatefulSet has a new UID while
PVC UIDs persist. It restarts a target voter to exercise replay, verifies the
pre-upgrade account, commits another transaction, checks Raft progress and
runs each voter's audit/derived-state checker after drain release. It fails
against an operator without the rc.4 boundary, so it is a release gate for the
format-change PR, not evidence that rc.3 itself needs a stop. The rc.4 release
still needs format-specific migration, snapshot, backup/restore and rollback
validation outside this scenario.

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

## Proposal payload cutover

The RC.3 to RC.4 transition requires the operator-owned stopped restart above.
Stop every old node before starting any target node, and keep the WAL and data
volumes. The target reads historical entries and writes `ExecutionBatch` or
`SetClusterPolicyProposal` directly. Existing snapshots, backups and audit
history need no rewrite or backfill. Native policy controls persist operational
state without business audit entries. Historical policy audits remain readable
with unchanged hashes. Incremental backups preserve newer policy in a separate
manifest snapshot, including policy-only changes. No capability flag enables
this format.

The first native payload appended to the WAL closes the rollback window for
RC.3. Restarting RC.3 on that storage requires a separately validated recovery
procedure. See the [proposal migration contract](../technical/architecture/subsystems/consensus/proposal-payloads.md).

## Change-specific notes

### Mirror asset mappings

`MirrorSourceConfig.asset_mappings` (field 6) is an additive optional field on
a message stored in `LedgerInfo` and in the audited `CreatedLedgerLog`. Existing
mirrors have an empty list, and their persisted state and audit hashes remain
valid. No migration is required.

Creating a mirror with `assetMappings` is not mixed-version safe. A voter
without the field keeps it as an unknown field, and the deterministic order
encoding appends unknown fields after the `http`/`postgres` source, while a
voter that declares field 6 writes it before the source. The two voters
therefore derive different `serialized_order` bytes, audit hash-chain entries
and proposal hashes for the same committed `CreateLedger` entry, and their
audit chains diverge from it. A leader running a release without asset-mapping
support also ignores the field and ingests assets verbatim. The operator
therefore lists `v3.0.0-rc.4` as a mandatory stop boundary, so no pre-`rc.4`
member runs alongside an `rc.4` member. A cluster not managed by the operator
must drain and stop every member before starting any `rc.4` member. See
[asset mappings](../technical/architecture/subsystems/events-mirror/mirror.md#asset-mappings).

A worker that finds assets v3 cannot represent among a mirror's stored volumes
stops ingesting that mirror after the upgrade. Delete the mirror ledger and
recreate it with the needed mappings.

## Repairing historical transaction address indexes

A builder fix can preserve transaction-address history for purged EPHEMERAL
volume cells without changing primary business data, audit records, Raft entries,
or the protocol revision. Existing READY address indexes may still omit rows
written by an older builder. Restarting a server resumes its committed cursor;
it does not revisit those rows. Repeating `CreateIndex` for an existing index
returns a conflict and does not repair it.

After upgrading **every replica** to the fixed binary, explicitly rebuild each
configured transaction address index affected by the omission. An older replica
would replay the same repair with the old exclusions and could still return
incomplete results. Record the configured indexes before starting. For each
configured type (`address`, `source-address`, `destination-address`), commit a
drop and then a fresh create, for example:

```bash
ledgerctl indexes drop --ledger my-ledger --type address
ledgerctl indexes create --ledger my-ledger --type address
ledgerctl indexes list --ledger my-ledger
```

Use the same drop/create pair for the other affected configured types; do not
create indexes that were deliberately absent. The committed drop removes the
index definition; each replica removes local readiness when its builder folds
that log, retaining the additive address-role buckets. Recreate allocates a fresh local version above the old high-water and
replays retained transaction logs together with applied-proposal history to add
missing rows. Each replica promotes its rebuilt index only after catch-up.
Existing valid rows remain; this procedure repairs omissions and does not remove
incorrect extra mappings.

Plan for query unavailability: address filters can fail while the index is
absent or BUILDING, and replicas complete independently. Confirm READY on every
replica that serves the workload, then verify representative funding and draining
queries for all repaired roles. Writes may continue; normal catch-up includes
new committed history. If interrupted after drop, the index remains absent until
creation succeeds. If interrupted during backfill, startup resumes its persisted
cursor; the index remains unavailable until local promotion completes.

For operator-managed indexes, coordinate through the owning `Ledger.spec.indexes`
configuration and its normal reconciliation procedure. Manual deletion or
replacement of operator-owned indexes is outside the ownership contract; do not
race a manual drop/create against reconciliation. See
[operator index ownership](../technical/architecture/subsystems/indexer/operator-ownership.md).

Existing frozen query checkpoints keep their original physical read-index
copies. Repairing live indexes does not add omitted historical mappings to those
copies. Create fresh query checkpoints after repair when complete historical
address search is required. A main-store backup restore into a fresh read store
instead replays retained history with the installed builder; address mappings are
rebuildable local projections, not additional audit-bound business state.

## Connector URL input policy

Before creating PostgreSQL mirrors or NATS/ClickHouse sinks on upgraded servers,
convert PostgreSQL keyword DSNs to `postgres://` or `postgresql://` URLs and add
an explicit `nats://`, `tls://`, `ws://` or `wss://` scheme to every NATS server.
ClickHouse inputs must use `clickhouse://`, `http://` or `https://` URL form.
Malformed URL shapes return safe validation failures before Raft. PostgreSQL
mirror admission accepts any explicit scheme and does not parse driver options;
the PostgreSQL worker validates supported schemes/options and requires explicit
TLS before installing IAM credentials. Invalid driver settings can now be
persisted and fail during source construction rather than creation. The required
IAM region remains an admission check.
Existing persisted configurations, audit history and backups remain valid;
workers continue to consume their original bytes. Public reads fully mask legacy
PostgreSQL keyword DSNs. During a rolling upgrade an older leader can still
admit a non-URL input; FSM application is unchanged on all voters. Service
protocol revision 40 remains unchanged and no state migration is required.
