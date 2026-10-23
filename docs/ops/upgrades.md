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
When an RC image change also changes volume templates, the operator completes
the RC rollout before recreating the StatefulSet for the volume change.
Old and new voters must communicate and apply each committed entry identically
while they coexist. A change that cannot meet this requirement needs a staged
rollout design or must wait for a different compatibility line.

Before tagging a new release, verify the upgrade with state created by the
supported source releases. Cover affected Pebble records, Raft log and snapshot
state, backup and restore formats, audit verification, and rebuildable
projections. Document the upgrade procedure and rollback boundary in the
release notes. Delay the release if the target cannot read or migrate the source
state.

The upgrade classes below describe existing procedures. RC changes must use the
rolling class; coordinated restart and rebuild or restore are retained for
earlier development revisions and explicitly separate compatibility lines.

| Class | When it applies | Procedure |
|-------|-----------------|-----------|
| Rolling | Required between adjacent RCs on the same release line. | Restart followers one at a time, wait for each to catch up, transfer leadership to an upgraded voter, then restart the old leader. |
| Coordinated restart | A separate compatibility line that cannot safely mix binaries. | Enable maintenance mode, wait for every voter to apply the maintenance barrier durably, stop every node, replace all binaries, then restart without a mixed-version window. Preserve or migrate the released data. |
| Migrate or restore | A separate line with a changed persisted or backup format, or development state before the first RC. | Follow a tested migration or restore a backup supported by the target release. A wipe or external rebuild is permitted only for unsupported pre-RC development state. Verify the complete state before serving writes. |

Before publishing an RC update, demonstrate the rolling class from the
immediately preceding RC binary to the target binary in one cluster, including
leadership transfer, writes, replay, and restart. Record the source and target
builds and the results in the release validation. If the path fails, fix the
change before release. Do not infer support from a successful single-node restart.

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
an earlier declared storage size that never reached its claim templates.

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
migration and does not alone request a storage-only rollout. The operator first
attempts exact reconstruction using the current nonstorage inputs and retained
claim settings. If it cannot reconstruct the hash and no proof is supplied, it
compares the admitted desired Pod template using the installed hash with the
live template. A difference requests the normal rollout; upgrading to the
adjacent RC does not require historical proof just because the explicit image
tag or the operator's implicit `LEDGER_IMAGE_TAG` changed. Configuration and
TLS changes follow the same rule. Existing RC admission, TLS sequencing and
storage rollout gates remain in force.

Equal templates cannot establish whether external admission inputs such as
ServiceAccount annotations changed. Only this ambiguous no-proof case reports
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
the new controller adds none when exact legacy reconstruction succeeds.

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
