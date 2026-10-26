# Ledger Operator

Kubernetes operator for deploying and managing high-availability [Formance Ledger](https://github.com/formancehq/ledger) instances using Raft consensus.

## Overview

The Ledger Operator manages `Cluster` and `EventSink` custom resources to automate the lifecycle of distributed ledger clusters and event delivery on Kubernetes. It handles:

- **StatefulSet management** with Raft-based consensus (odd replica counts)
- **Persistent storage** for WAL and data volumes
- **Observability** with OpenTelemetry traces, Prometheus metrics, and Pyroscope profiling
- **Security** with TLS, OIDC authentication, and Ed25519 response signing
- **Backups** to S3-compatible backends, with [recoverable Job provisioning](../../docs/ops/backup-restore.md#scheduling-with-the-kubernetes-operator) and sibling-run exclusion
- **Event sinks** reconciled into Ledger's Raft-replicated runtime configuration
- **Credentials** for application-level access control

During StatefulSet scale-down, every removed ordinal must satisfy the Raft
membership removal postcondition (EN-1999). A replacement Pod can be Pending,
missing, or have no container status while its stable node ID is still a voter;
none of those Pod states proves absence from Raft. Skipping such an ordinal can
leave a one-replica StatefulSet with a two-voter membership and no write quorum.

The operator first updates the StatefulSet template while retaining the current
replica count, then transfers leadership to node 1 (pod-0). Pod health selects
only the removal mode: crashed or missing Pods use the existing force-removal
path first. Other Pods, including Pending or running Pods without a crash
indicator, use normal consensus removal in descending ordinal order. Before
**each** removal it queries structured
`ledgerctl cluster status --json` and requires a leader response. Force removal
uses `--node-id 1` to read the retained leader's local membership without a
quorum-backed route. An already-absent node is not removed again. A present node
must be removed successfully; if `remove-node` returns an error, the operator
queries membership again and continues only when the target is absent. This
recovers committed removals whose response was lost, without matching CLI error
substrings.

Only after all removed ordinals satisfy that postcondition does the operator
persist the reduced StatefulSet replica count and issue PVC deletions. The
StatefulSet update requests Pod termination; it does not wait for termination to
finish. If a membership check or removal fails, replicas and PVCs are retained,
although the template update and earlier membership removals may already have
succeeded. A later reconciliation checks membership again and skips ordinals
already removed.

## Ledger target identity

A `Ledger` CR's `spec.name` and `spec.clusterRef` are immutable from creation,
including while Pending. Kubernetes rejects changes to either field before the
controller can create, promote, reconcile indexes, or delete against a different
target. Correct a target by deleting and recreating the CR. Index updates and
mirror-to-normal promotion remain supported without changing the target.
The generated CRDs, including the Helm CRD chart, enforce these CEL transition
rules in the API server; exported JSON schemas alone do not enforce transitions.

Before installing these rules on an existing deployment, stop all operator
reconcilers and inventory existing Ledger CRs. Verify each stored name and
cluster reference against the intended external ledger before resuming the
operator. Recover any drifted target **before** installing the new CRDs: the
rules freeze the currently stored values and will reject corrections afterward.
Include Pending CRs in this inventory: a timed-out creation or failed status
write may have left an external ledger even when the CR never became Ready.
`status.appliedSpecHash` cannot reconstruct the original target; if that target
is unknown, keep reconciliation stopped until external records establish it.
Do not delete a drifted CR as a recovery step with the old controller, because
deletion uses its currently stored target.

Admission immutability fixes the target name and Cluster reference, not the
incarnation of a Cluster recreated under the same name or ownership of an
already-existing external ledger. Ledger deletion retains its best-effort
behavior: it attempts external deletion and then removes the finalizer even
when endpoint resolution or execution fails; the external ledger may remain.
This change does not add controller binding state or alter Ledger storage,
audit records, backup formats, or service APIs.

## Declarative ledger indexes

For a Ledger with `spec.indexes`, reconciliation creates missing indexes in
separate batches tagged with the Ledger CR UID through the existing idempotency
key. It reconstructs `status.appliedIndexes` from successful audit creation
records matching the current index creation dates. A crash, lost CLI response,
or failed status update therefore does not permanently lose attribution.
Strict creation conflicts are surfaced and retried without adopting the index.

Operator-created indexes must be managed through the Kubernetes spec. External
indexes are not adopted; replacements already visible during attribution are
excluded. Manual replacement between verification and the unconditional drop
can still be deleted: this race is explicitly accepted, and no atomic deletion
precondition is provided. See the [audit attribution contract](../../docs/technical/architecture/subsystems/indexer/operator-ownership.md).

## TLS disable recovery

Removing `spec.tls`, or setting `tls.enabled: false` while clearing its Secret
reference, drives the existing cluster through `optional` before `disabled`.
The operator first persists `status.tlsRollout` (mode, certificate Secret name
and CA key), then changes or recreates the StatefulSet using those references.
If that status write fails, the workload remains unchanged. Desired spec is
not rewritten. A replacement controller can resume even after orphan deletion
has removed the StatefulSet; retained mode prevents plaintext bootstrap while
TLS peers still exist.

The retained reference remains until the disabled template has converged
(current StatefulSet generation observed, all desired replicas updated and
ready). A failed workload or cleanup write leaves durable recovery state for
retry. Keep the externally owned certificate Secret until that convergence
and rollout-state cleanup; the operator never deletes it. The separately
managed inter-node `cluster-secret` is removed after the disabled template
has dropped its reference. External authentication continues to require TLS.
New backup Jobs resolve TLS mode and certificate/CA references from the same
owned StatefulSet template, so removing desired TLS cannot strip trust from a
Job targeting the optional phase. Missing workloads retry provisioning; foreign
owners are rejected. Existing Jobs retain their own persisted templates.
In-pod ledgerctl calls from Ledger and EventSink reconciliation also retry
without exec when the StatefulSet is absent, then read its mode after recreation.
See [TLS migration](../../docs/ops/tls-migration.md) for phases, interruption
boundaries and recovery guidance.

## Pyroscope credentials

`spec.monitoring.pyroscope.authTokenFrom` and `basicAuthPasswordFrom` accept
`{name, key}` references to Secrets in the Cluster namespace. The operator
renders required `valueFrom.secretKeyRef` entries only when profiling is enabled;
it never copies those credential bytes into the Cluster or Pod template.
Plaintext `authToken` and `basicAuthPassword` fields are not supported.
Reference changes trigger a rollout; rotating Secret contents requires a Pod
restart. See [profiling deployment](../../docs/ops/deployment.md#pyroscope-continuous-profiling)
for examples and missing-reference behavior.

## Health check interval

`spec.health.interval` accepts a Go duration that parses to a value greater
than zero, such as `30s` or `1m30s`. Omitting it, or setting an empty string,
uses the server's `30s` default. Invalid, zero, negative, overflowing, and
sub-nanosecond values that round to zero stop reconciliation with
`ConfigValid=False`, reason `ValidationFailed`, and phase `Error` before workload
creation or updates. Existing workloads remain unchanged; correcting the field
resumes reconciliation. This validation runs during reconciliation, so the CR
may be accepted by Kubernetes and its generated cluster identity may already
have been persisted before validation fails.

`spec.extraEnv` retains its override precedence. The server validates the final
selected interval at startup, including values supplied through that override.

## Backup scheduling

Backup schedules preserve their completion cursors in `Backup.status` before
pruning run history. Zero retention therefore preserves scheduling across
operator restarts. See the [scheduling contract](../../docs/technical/architecture/subsystems/backup/operator-scheduling.md)
and [backup operations guide](../../docs/ops/backup-restore.md#scheduling-with-the-kubernetes-operator).

Static S3 credentials use `spec.destination.s3AccessKeyIdFrom` and
`spec.destination.s3SecretAccessKeyFrom`, each referencing a Secret name/key in
the Backup namespace. The kubelet injects them into the Job environment; the
operator stores no credential values in Backup or workload specs. See
[S3 credentials for operator backups](../../docs/ops/backup-restore.md#s3-credentials-for-operator-backups)
for runtime delivery, missing-key behavior and ambient authentication.

## Custom Resources

| Resource | Scope | Description |
|----------|-------|-------------|
| `Cluster` | Namespaced | Main resource - deploys a ledger cluster |
| `EventSink` | Namespaced | Configures a runtime event sink for a Cluster |
| `Credentials` | Cluster | Cluster-level API credentials |

## Quick Start

### Prerequisites

- Kubernetes cluster with `StatefulSetAutoDeletePVC` enabled (default since
  1.27, stable since 1.32); a tested minimum version is not established.
  PVC/PV deletion protection needs `ValidatingAdmissionPolicy` (stable since
  1.30); see [Volume Deletion Protection](#volume-deletion-protection).
- Helm 3
- [Nix](https://nixos.org/) (optional, for development)

### Install the Operator

```bash
helm dependency build misc/operator/helm/operator
helm install ledger-operator misc/operator/helm/operator \
  --namespace ledger-system \
  --create-namespace
```

### Deploy a Ledger Cluster

If `spec.clusterID` is omitted, the operator writes a random UUID into the
Cluster resource before creating workloads. That committed value is shared by
every replica and survives reconciliation and pod restarts. Set `clusterID`
explicitly only when attaching data whose persisted identity must be reused;
Ledger rejects an ID mismatch on boot. A deleted Cluster recreated with fresh
storage receives a new ID. Inspect `kubectl get cluster my-ledger -o
jsonpath='{.spec.clusterID}'` to discover the resolved value.

```yaml
apiVersion: ledger.formance.com/v1alpha1
kind: Cluster
metadata:
  name: my-ledger
spec:
  replicas: 3
  image:
    repository: ghcr.io/formancehq/ledger
    tag: latest
  podAntiAffinity:
    enabled: true
    type: hard
    topologyKey: kubernetes.io/hostname
  pebble:
    memTableSize: 256Mi
    cacheSize: 1Gi
  # Cache and bloom parameters are part of the Raft-replicated ClusterConfig.
  # Editing them triggers a rolling restart of the StatefulSet; convergence
  # is deterministic via applyClusterConfig (cache reset + bloom rebuild) and
  # bounded by one election cycle after the last pod restarts.
  cache:
    rotationThreshold: 1000
  # Supplied fpRate must be finite and strictly between 0 and 1.
  # Omit it for the default; explicit zero, NaN and infinities are invalid.
  bloom:
    volumes:
      expectedKeys: 100000000
      fpRate: "0.01"
    ledgerMetadata:
      expectedKeys: 1000000
      fpRate: "0.001"
    preparedQueries:
      expectedKeys: 1000000
      fpRate: "0.001"
  persistence:
    wal:
      size: 5Gi
    data:
      size: 10Gi
  resources:
    requests:
      cpu: "2000m"
      memory: 4Gi
    limits:
      cpu: "4000m"
      memory: 4Gi
  # The operator derives GOMEMLIMIT from this memory limit. The default ratio
  # is 90; set it explicitly here so the resource policy is visible.
  goMemLimitRatio: 90
```

### Configure a NATS Event Sink

Create an `EventSink` in the same namespace as its referenced `Cluster`. The
resource name is the Ledger sink name. Create the NATS JetStream stream first;
its subjects must include the topics derived from the configured prefix
(`<topic>.<ledger>.<event-type-lowercase>`). For this example, the stream should
capture `ledger.events.>`:

```yaml
apiVersion: ledger.formance.com/v1alpha1
kind: EventSink
metadata:
  name: primary
spec:
  clusterRef:
    name: my-ledger
  nats:
    url: nats://nats.default.svc.cluster.local:4222
    topic: ledger.events
  format: json
```

A ready to apply example is in
[`config/samples/ledger_v1alpha1_eventsink.yaml`](config/samples/ledger_v1alpha1_eventsink.yaml).

The operator applies creation and edits through Ledger's replicated runtime
API without restarting the StatefulSet. `status.conditions` reports
reconciliation state; `status.cursor` and `status.error` expose delivery
progress and the current delivery error. `Delivering=Unknown` means Ledger has
not yet reported a sink status or the cursor remains zero, even
when its configuration is synced. Startup and delivery errors set
`Delivering=False`; cursor progress without a recorded error sets
`Delivering=True`. Filtered or internal logs can advance the cursor without
external delivery, so this condition alone does not prove a publish. The
operator adds a finalizer and removes the runtime sink before the `EventSink`
can be deleted. A same-name sink that this resource does not own is left
untouched.

The published Ledger image includes NATS sink support. When
`Cluster.spec.networkPolicy.enabled` restricts egress, allow TCP access to the
NATS service with `Cluster.spec.networkPolicy.additionalEgress`. Do not embed
credentials in the NATS URL: Kubernetes custom resource specs are not secret
storage.

## Helm Values

| Key | Default | Description |
|-----|---------|-------------|
| `image.repository` | `ghcr.io/formancehq/ledger-operator` | Operator image |
| `image.tag` | `latest` | Operator image tag |
| `ledgerImage.registry` | `ghcr.io` | Default ledger image registry |
| `ledgerImage.name` | `formancehq/ledger` | Default ledger image name |
| `ledgerImage.tag` | `latest` | Default ledger image tag |
| `replicaCount` | `1` | Operator replicas |
| `leaderElection` | `true` | Enable HA leader election |
| `watchNamespace` | `""` | Namespace to watch (empty = all) |
| `pvcProtection.enabled` | `true` | Install the cluster-scoped ValidatingAdmissionPolicy that blocks accidental deletion of ledger PVCs/PVs (requires Kubernetes >= 1.30). On by default; set `false` to opt the cluster out. Arming only — a ledger is selected via `spec.persistence.deletionProtection` (also on by default) |
| `pvcProtection.allowDeletionAnnotation` | `formance.com/allow-deletion` | Annotation key whose value `true` opts a volume out of deletion protection |
| `pvcProtection.additionalExemptServiceAccounts` | `[]` | Extra ServiceAccount usernames (`system:serviceaccount:<ns>:<name>`) exempt from the policies — sibling operator releases managing protected ledgers, or managed workload/GitOps controllers |

## Declarative Storage Reconciliation

Enfuce needs to manage the capacity and IOPS/throughput profile of existing
Ledger volumes through GitOps, without replacing volumes or running a custom
Argo Job. The [internal request](https://numary.slack.com/archives/C03TZRKCCM6/p1792764941343909)
identified two RC.1 limits: explicit `size`/profile edits did not patch existing
PVCs, and claim templates retained the old settings for future replicas.
Automatic expansion alone did not satisfy that request because it waits for a
usage threshold. The declarative contract below applies independently of that
optional policy.

For PVC-backed `spec.persistence.wal` and `spec.persistence.data`:

| Setting | Existing PVCs | StatefulSet claim templates |
| --- | --- | --- |
| `size` | Minimum storage request, including when `autoExpansion` is absent or disabled. Requests only increase; a larger existing request/capacity is preserved. | Configured minimum for future claims. A lower configured value retains the previous, larger template minimum. |
| Nonempty `volumeAttributesClassName` | Request the profile on each existing PVC in place through CSI. | Use the requested profile for future claims. |
| `autoExpansion` | Optional utilization-based growth, above the configured minimum. | Does not change templates or the Cluster spec. |
| `storageClass` / `accessMode` | Migration of an existing claim is unsupported. | Template recreation does not migrate existing volumes to another class or access mode. |

An edit to an existing Cluster can use the following shape. This example
assumes `gp3-expandable` was explicitly configured at creation; retain the
existing access mode and use a profile installed for its CSI driver:

```yaml
spec:
  persistence:
    data:
      storageClass: gp3-expandable # Keep the originally configured class.
      accessMode: ReadWriteOnce
      size: 150Gi
      volumeAttributesClassName: ledger-high-throughput
```

If `storageClass` was omitted or empty, retain that declaration; the effective
class assigned to an existing PVC can differ because Kubernetes defaulted it.
Explicitly naming that same effective PVC class is also accepted, but does not
rewrite the retained template class or migrate the claims.

For example, increasing `size` from `100Gi` to `150Gi` requests at least `150Gi`
on existing claims. A claim already grown automatically to `200Gi` stays at
least `200Gi`. A new claim starts from the template minimum, not another
replica's automatic growth; an enabled automatic policy may subsequently grow
that claim as part of group convergence. Reducing a configured minimum does not
shrink PVCs or lower an existing template minimum, and does not cause repeated
recreation.
`autoExpansion.maximumSize` limits automatic decisions only; it does not cap an
explicit GitOps capacity request. Keep the cap at least as large as live
requests if automatic growth should remain available.

### Storage prerequisites and limits

Capacity increases require the PVC's StorageClass to permit expansion
(`allowVolumeExpansion: true`) and a CSI driver, resizer and filesystem that
support the requested growth. See [Kubernetes PVC expansion](https://kubernetes.io/docs/concepts/storage/persistent-volumes/#expanding-persistent-volumes-claims).
An accepted PVC request is not proof that the block device and mounted
filesystem have finished expanding.

Profile changes require the Kubernetes distribution to expose and enable
`VolumeAttributesClass`, a class for the volume's CSI driver, and CSI support
for volume modification with the required sidecars. IOPS/throughput parameters
and supported transitions are driver-specific; see
[Kubernetes VolumeAttributesClasses](https://kubernetes.io/docs/concepts/storage/volume-attributes-classes/).
The operator patches the PVC; it does not call AWS APIs or modify EBS directly.
Missing support, rejected requests and reported CSI failures remain visible
through storage conditions and PVC status. Clearing a previously set profile
is unsupported and is surfaced instead of silently removing it. Select another
supported nonempty class to request a profile change.

This feature does not shrink or replace PVCs/PVs, migrate StorageClasses/access
modes, or change hostPath paths and per-ordinal layout. HostPath volumes have
no PVC to expand or modify. Existing retention and deletion-protection policies
remain applicable; do not disable them to complete a size/profile edit.

A difference in declared `storageClass` or `accessMode` does not block
size/profile template alignment: the replacement copies the existing template
class and access modes unchanged. Live PVC validation separately reports a
genuine unsupported migration as `StorageUnsupported` on the affected storage
condition without changing those PVC fields. Mutable workload reconciliation
and independently safe growth on owned PVCs continue under their normal gates.
Unsupported profile clearing or incompatible claim layouts remain separate
limits; they are not class/access-mode migration support.

The Pod restart hash excludes WAL/data `size`, `volumeAttributesClassName`,
`storageClass` and `accessMode`. Declaration changes to those fields alone do
not restart Pods. HostPath wiring and ServiceAccount configuration remain
rollout inputs. In particular, adding, replacing or removing ServiceAccount
annotations still requests the normal Pod rollout: Pod admission can consume
those annotations even when the StatefulSet's other Pod-template bytes are
unchanged. Updating the ServiceAccount alone does not refresh admission inputs
in existing Pods; verify the rollout and resulting runtime access separately.

Admission webhooks must be available and support the dry-run requests used to
normalize the desired Pod template, check replacement creation and, when
needed, compare a legacy workload hash. Before starting recovery, a dry-run
Update applies the desired Pod template to a copy of the existing StatefulSet.
The controller compares that admitted template with the live template, so API
defaults such as probe timeout and success threshold do not falsely report an
unfinished rollout.
The admission result must preserve the requested replacement specification.
Normalization and legacy-hash preview errors report
`ClaimTemplatesReady=False/AdmissionCheckFailed` on the Cluster.
A dry-run rejection, timeout or mutation mismatch can block that reconcile
pass before replacement. Inspect the reported API/webhook error, restore
webhook availability and dry-run support, and correct any conflicting mutation
or missing API feature before retrying. There is no fallback that skips this
check or rolls Pods to bypass it.

### Released workload hash migration

The canonical StatefulSet metadata `ledger.formance.com/pod-config-hash` is
trusted only when it is nonempty, the installed Pod-template
`ledger.formance.com/spec-hash` is nonempty, and StatefulSet metadata
`ledger.formance.com/pod-config-spec-hash` equals that installed Pod hash.
This association records the actual retained hash, including a historical
full-spec hash. It does not depend on StatefulSet UID or generation and survives
recreation. Missing or broken association uses the verified legacy migration
below; its absence alone does not request a storage-only rollout.

For legacy migration without explicit proof, the operator first normalizes the
desired Pod template using the installed hash through a dry-run Update of the
existing StatefulSet and compares it with the live template. If they differ, it
requests the normal rollout with the canonical hash. If they are equal, it
automatically preserves the installed hash only when exact released full-spec
reconstruction succeeds using the current nonstorage configuration and retained
claim settings. Some released workloads have a hash from an earlier storage
declaration that no longer matches their templates. Image, configuration and
TLS changes can therefore proceed without historical proof, including an image
tag supplied by the operator's `LEDGER_IMAGE_TAG` when `spec.image.tag` is
omitted. Sequential RC and TLS rollout gates still apply.

If those templates are equal, external admission inputs such as ServiceAccount
annotations may still have changed. An unknown source then reports
`ClaimTemplatesReady=False/HashMigrationRequired`, without advancing either
workload hash or starting storage replacement. This ambiguous case needs
verified historical inputs. An explicitly supplied invalid proof reports
`HashMigrationInvalid` even if the desired template differs; the no-proof
comparison does not bypass proof validation.
Dry-run errors report `ClaimTemplatesReady=False/AdmissionCheckFailed` before
canonical metadata or recovery intent/journal writes. The StatefulSet Update
preview does not execute Pod Create admission or certify actual IAM/runtime
access; verify those effects separately.

Upgrade the CRD and Operator before changing storage or ServiceAccount inputs.
Keep the existing Cluster declaration unchanged during that upgrade and wait
for the valid canonical/installed-hash association above on the current owned
StatefulSet before applying those edits. Marker presence alone is insufficient.
This gives exact reconstruction the unchanged inputs it needs, including when
a released hash reflects a storage edit that never reached the old claim
templates.

If combined edits already report `HashMigrationRequired`, temporarily restore
only those concurrent declared inputs to their authentic previous declaration,
wait for the valid association, then reapply the edits. Use this route only when
that previous declaration is known; do not guess it from live claim templates
or the current ServiceAccount. The PVC reconciler runs independently and may
already have accepted growth. Never shrink PVCs or revert accepted live growth
as part of this procedure; restoring declared inputs does not undo capacity or
CSI effects. If the prior declaration is unavailable or exact reconstruction
still fails, recover the historical evidence and keep the ambiguous migration
blocked.

ServiceAccount reconciliation can already have written the requested
annotations before `HashMigrationRequired` or `HashMigrationInvalid` blocks the
Pod-template update. That error does not undo the ServiceAccount write or
refresh admission inputs in existing Pods.

Strict historical proof is the alternative when retaining the concurrent edits.
A supplied invalid proof must be corrected, or deliberately removed before
retrying the no-proof path; neither route bypasses its validation.

Supply the Cluster annotation `ledger.formance.com/legacy-spec-hash-proof` as
one JSON object with these fields:

| Field | Required value |
| --- | --- |
| `version` | `1` |
| `sourceUID` | UID of the current StatefulSet whose Pod-template hash is being migrated. |
| `oldSpecHash` | Exact installed Pod-template `ledger.formance.com/spec-hash`. |
| `spec` | Complete historical effective `ClusterSpec` that the released operator hashed, with its actual `applyDefaults` values, historical image environment values and omissions retained as hashed. |

Reconstruct the effective spec using the released operator's
[`applyDefaults`](internal/controller/cluster_controller.go), starting with the
admitted spec used for that rollout. For the supported
released implementation, it fills these zero or unset fields:

| Field | Value when omitted |
| --- | --- |
| `image.repository` | Historical nonempty `LEDGER_IMAGE_REPOSITORY`, otherwise `ghcr.io/formancehq/ledger`. |
| `image.tag` | Historical nonempty `LEDGER_IMAGE_TAG`, otherwise `latest`. |
| `image.pullPolicy` | `IfNotPresent` |
| `replicas` | `3` |
| `bindAddr` | `0.0.0.0:7777` |
| `grpcPort` | `8888` |
| `httpPort` | `9000` |
| `walDir` | `/data/raft` |
| `dataDir` | `/data/app` |
| `serviceAccount.create` | `true` |
| `service.type` | `ClusterIP` |

Use the image environment values from the operator deployment that produced
the installed hash. Do not substitute the current deployment's values or fill
other fields from current rendering defaults. In particular, retain omitted
storage quantities and access modes where they were omitted in the hashed spec,
even if the resulting claim templates contain defaults.

Run the commands from the repository root after creating the working directory:

```bash
mkdir -p build/operator-storage-hash
```

Use this procedure:

1. Recover the trusted historical effective spec from the rollout's records,
   original admitted configuration, the released `applyDefaults` implementation
   and that deployment's `LEDGER_IMAGE_REPOSITORY`/`LEDGER_IMAGE_TAG` values.
   Save that spec object alone as
   `build/operator-storage-hash/historical-effective-spec.json`. Preserve the
   historical quantities, omitted fields and ServiceAccount configuration
   exactly as hashed. The current CR, live ServiceAccount and current defaults
   cannot establish unknown historical values. If that history is unavailable,
   keep the migration blocked and recover the missing evidence.
2. Read the current owned StatefulSet and preserve its UID and installed hash:

   ```bash
   kubectl -n <namespace> get statefulset <statefulset-name> -o json \
     > build/operator-storage-hash/source-statefulset.json
   ```

3. Build the proof from those two records. `jq` is provided by the repository's
   Nix environment:

   ```bash
   jq -cn \
     --slurpfile source build/operator-storage-hash/source-statefulset.json \
     --slurpfile historic build/operator-storage-hash/historical-effective-spec.json \
     '{version: 1, sourceUID: $source[0].metadata.uid,
       oldSpecHash: $source[0].spec.template.metadata.annotations["ledger.formance.com/spec-hash"],
       spec: $historic[0]}' \
     > build/operator-storage-hash/proof.json
   ```

4. Review the complete proof, then put its compact JSON value in the Cluster's
   `metadata.annotations["ledger.formance.com/legacy-spec-hash-proof"]` through
   GitOps. This is a metadata annotation; no public CRD schema changes.
5. Let reconciliation retry. It verifies the version, source UID and installed
   hash, then recomputes the released hash from `spec` without applying current
   defaults. A malformed, stale, foreign or inconsistent proof reports
   `HashMigrationInvalid`. No caller-supplied canonical hash is accepted.
6. Verify the valid canonical/installed-hash association on the current
   StatefulSet and that no migration failure remains, then
   remove the proof annotation from GitOps. Verify storage recovery, any Pod
   rollout and effective runtime access separately.

The controller recomputes the canonical hashes of both historical and current
specs. It preserves the installed Pod hash only when these match and an
admission-normalized comparison also establishes equal Pod templates. A changed
canonical runtime input, such as a ServiceAccount annotation, uses the desired
Pod hash and requests the normal rollout. Only a valid association gives
canonical metadata precedence over leftover proof; a trusted canonical
mismatch still requests the normal rollout. Remove proof after success to
avoid stale recovery input.

Normal and preparatory Raft scale-down writes record both metadata hashes
atomically after final Pod-template resolution. The association stores that
template's actual hash, including a preserved historical hash. The preparatory
write keeps the previous replica count before Raft exec. If exec then fails,
the template and valid pair remain durable: the next pass uses the pair
instead of rejecting the original proof against the changed Pod hash. The pair
does not establish completed rollout or membership removal;
the normal Raft postcondition still precedes replica reduction.
Do not force migration by editing or resetting either StatefulSet hash
annotation, substituting current ServiceAccount values for history, or deleting
recovery state or storage.

### Claim-template recovery sequence

Install the updated Cluster CRD before upgrading the controller. The controller
verifies that Kubernetes persists its recovery pointer; an older schema that
prunes it blocks storage recreation safely.
Finish the Operator rollout before requesting storage edits, so an older
controller cannot take over an active recovery. Do not downgrade the Operator
while `status.claimTemplateRecovery` is present.
After recovery completes, a released Operator can preserve unknown hash
metadata while changing the installed Pod hash; the new Operator detects the
broken association and repeats verified legacy migration. For example, a
new-Operator configuration A followed by released-Operator ServiceAccount B and
then desired A must not preserve B's Pod hash by trusting A's stale marker.
If exact reconstruction fails and the admitted templates are equal, it reports
`HashMigrationRequired` without proof; authentic B history requests the rollout
to A. The old binary can itself roll a storage hash. The new controller adds
no storage-only rollout when exact reconstruction succeeds and the StatefulSet
Update preview admits an equal template.

Before upgrading, disable external Jobs/controllers that mutate the same PVCs
or StatefulSet and inventory the Cluster's configured minima/profiles against
live storage. Reconciliation now applies those existing fields on upgrade,
without requiring a new spec edit: an existing larger minimum can immediately
request growth. Adopt the intended nonempty profile in the CR when a Job
previously managed it; an omitted class does not authorize clearing a live one.

Kubernetes does not permit an in-place edit to StatefulSet claim templates.
For a size/profile template change, the operator uses the following sequence:

1. Complete any current rollout before starting storage recreation. A combined
   scale-up may wait until template recovery so new claims use the new settings.
   During a combined image/configuration and
   storage edit, the interim rollout retains the old claim templates and Pod
   storage wiring. Sequential RC upgrade checks still apply.
2. Build the replacement specification in memory with the new claim templates
   and the existing Pod template, replica count and selector. A storage-only
   edit therefore does not request a Pod rollout.
3. Prepare independent copies of the source, replacement and retained storage
   identities. After source/Pod/binding checks and admission preview, record
   both canonical and installed-hash association metadata on the replacement
   using its actual retained Pod hash. Encode the complete snapshot once.
   Compute a domain-separated SHA-256 over the
   Cluster UID, namespace, fixed journal base and exact JSON bytes. The journal
   name is `<StatefulSet-name>-storage-recovery-<first-32-digest-hex>`.
   Both hashes are therefore saved before encoding/digesting; the operator does
   not edit an existing immutable journal to add them.
4. Before creating the ConfigMap, compare-and-swap nil
   `status.claimTemplateRecovery` to Preparing: required `sourceUID`, full
   lowercase `payloadSHA256` and `configMapName`, without `configMapUID`.
   Require the same nonterminating Cluster UID and prepared generation, with
   no other operation. Verify the API preserved the whole intent.
5. Create the immutable Cluster-owned journal with the exact encoded bytes.
   An existing name is never overwritten: reuse requires fresh full preparation,
   durable matching intent and direct owner/nontermination/immutability/full
   digest verification. Decode only authenticated bytes and validate journal
   version, source UID and workload/claim structure before binding the UID.
   The decoded source UID must match protected status.
6. Bind the observed journal UID by compare-and-swap from that exact Preparing
   pointer and verify persistence. This is Active. Directly reread the journal
   and repeat name/owner/nontermination/immutability/full-digest authentication
   against the bound UID before dependencies, deletion or creation. Then delete
   the old StatefulSet with UID/resourceVersion preconditions and orphan
   propagation, retaining its Pods, claims and data.
7. Recreate the StatefulSet from the authenticated snapshot. Verify its ownership,
   operation identity and exact saved specification, its controller's observed
   generation, all expected Pods' adoption and the retained PVC/PV UIDs and
   bindings. Compare-and-swap the exact Active pointer to Completed before
   deleting the journal, then clear only that expected Completed pointer.
   This ordering makes a lost cleanup response recoverable.
8. Resume deferred scale, image, TLS and other workload changes after recovery.

`ClaimTemplatesReady=False/PreparationFailed` identifies a preparation error.
After preparation hands control to recovery, failures retain
`ClaimTemplatesReady=False/RecoveryFailed`, including a failed initial
orphan-delete or replacement-create attempt. The journal and protected pointer
remain available for retry.

Protected Cluster status authorizes the journal. A ConfigMap ownerReference,
name, structure or digest supplied only by the ConfigMap cannot authorize it.
Each active resume repeats authentication before consuming its bytes or deriving
dependencies. Every pointer transition compares the whole expected previous
pointer and uses optimistic resourceVersion; a conflict waits for fresh
observations rather than overwriting a newer operation. The internal journal
remains version `1`. This feature is unpublished, so missing source/digest/name
fields fail closed without a retroactive authentication migration. Ledger data,
Raft, audit and service protocol versions are unchanged.

A nil pointer never adopts a standalone ConfigMap and its presence alone does
not block normal reconciliation. Late unbound journals have no workload
authority. Different plans derive different names; reusing identical bytes
still requires fresh preparation, durable matching intent and exact checks.
Unbound journals may remain for Cluster garbage collection; their separate
cleanup does not authorize workload recovery.

If Preparing has no bound UID and its named journal is directly `NotFound`,
the operator can clear only that exact valid pointer after direct API reads
prove the same nonterminating Cluster UID and the expected Cluster-controlled,
nonterminating StatefulSet with the saved source UID. Readiness, source
generation and later CR validity do not matter to this absence-abort. It reads
no recovery dependencies or old claims, writes no workload and deletes no
journal. Successful clear ends the pass with a requested two-second requeue;
this is advisory scheduling, not a guaranteed execution cadence. Missing,
terminating, replaced or foreign sources and uncertain reads fail closed.
UID binding and abort compare the same Preparing pointer, so only one wins;
a journal arriving after abort remains unbound. A missing Active journal is a
hard error and never permits initial creation.

Completed requires a bound journal UID. Cleanup checks owner and UID, deletes
with UID/resourceVersion preconditions and clears only the exact Completed
pointer. It needs no payload, immutability or dependency reconciliation, so
damaged payload bytes do not prevent cleanup. A foreign UID still fails.

After deletion, completion proves controller continuity; it does not require
`ReadyReplicas` or `UpdatedReplicas`. All expected Pods must exist, be
nonterminating and be controlled by the replacement, even if they are unready
or restarting. Cleanup can therefore finish with zero Ready replicas and allow
normal workload repair and authorization rollout to proceed. The source
workload must still be ready before deletion starts. Recovery completion does
not certify workload availability or effective authorization in serving Pods.
An expected Pod that is missing after recreation reports
`ClaimTemplatesReady=False/AwaitingAdoption` with its name and a short requeue.
The journal and pointer remain until that Pod exists, is nonterminating and is
controlled by the replacement. Other Pod-read errors remain recovery errors.
This wait does not bypass StatefulSet `OrderedReady` creation: an unready
lower ordinal can still delay creation of a missing Pod and later normal repair.

Every fresh reconciliation checks for recovery state before other workload
mutations. While recovery is active, later desired scale, image and TLS edits
do not replace the saved workload specification. Recovery resumes from the
durable snapshot after a controller restart, including when no StatefulSet is
present. PVC reconciliation and CSI completion are separate effects; this is
not one atomic transaction across the StatefulSet and all volumes.
If the original StatefulSet changes generation or becomes unready before
orphan deletion, the operator first verifies that the same source UID is still
present without a deletion timestamp. It marks the obsolete journal completed,
cleans it up, then returns to normal workload repair and replanning. A failed
delete precondition therefore does not freeze a later image/configuration
repair. Once deletion starts, the saved snapshot remains authoritative; an
absent source, terminating source or replacement UID cannot use this escape.

| Failure boundary | Residual state and next action |
| --- | --- |
| Preparation fails before any durable status-intent write attempt, and the direct API proves the same nonterminating Cluster UID has no recovery pointer | `PreparationFailed` is set for the failed attempt. The operator keeps the old templates, continues safe mutable-workload and volume-protection work under their normal gates, then returns the preparation error for retry. Standalone ConfigMaps do not authorize or block this continuation. |
| Intent write, journal write or UID bind fails, including rejection or a lost response | No storage-recreation delete is issued and normal mutable-workload work stops in this pass. Preparing, the journal or Active may already be durable. Previously accepted PVC edits can still exist. Retry after fixing the API/permission error. |
| Preparing survives, but the named journal is directly absent | Only the same controlled nonterminating source UID permits exact-pointer CAS clear. The pass reads no dependencies/old claims and requests a two-second requeue. Missing/replaced/terminating source or read uncertainty remains blocked. |
| Snapshot persists, delete fails or its response is lost | The snapshot remains; the old StatefulSet can still exist or be terminating. Recovery checks observed identity/state before continuing. |
| Original StatefulSet changes generation or becomes unready before deletion | The same nonterminating source UID must still exist. The operator marks the old journal completed, cleans it up and returns to normal repair/replanning. |
| Old StatefulSet is absent, replacement creation fails | The snapshot, orphaned Pods and retained claims remain. Restore the dependency or permission and let recovery recreate from the snapshot. |
| Replacement exists, its generation is not yet observed or expected Pods are missing or not adopted | `AwaitingAdoption` keeps the snapshot and retries while other workload mutations wait. Readiness alone does not block cleanup. |
| Replacement differs from the saved specification or operation identity | `RecoveryFailed` retains the journal. Inspect and correct the mismatch as described below; recovery does not force adoption. |
| Completed cleanup fails or its delete response is lost | The completed pointer and journal can both remain, including with unready Pods. Retry owner/UID/resourceVersion-fenced cleanup; missing or corrupt payload does not block it. Clear only the expected pointer before later workload edits. |
| A PVC patch or CSI operation fails after other claims changed | Some claims can already contain larger requests or the new requested profile. Retry and inspect each claim; reverting Git does not undo those effects. |

A failed status-intent or journal write can have committed before its response
was lost, or still commit after a later read reports `NotFound`. The freeze
starts at the first attempted durable status-intent write, before ConfigMap
creation. Every later error ends that pass without normal mutable-workload
continuation, even for rejected/no-commit writes. Typed errors preserve that
phase and the original cause. Only failures before that attempt can use the
direct APIReader fence: no pointer on the same nonterminating Cluster UID.
An existing pointer, changed identity or uncertain read blocks continuation.
Neither a nil cached/in-memory pointer nor post-write `NotFound` proves that
preparation had no durable effect. A standalone ConfigMap alone is irrelevant.

Failed attempted intent/journal preparation also starts an in-memory retry hold
for that Cluster namespace/name, UID and full prepared payload digest: initially
two seconds, increasing exponentially to a 30-second cap. Before a new Preparing
condition or intent CAS, the operator checks the hold. Watch events cannot
bypass it. Waiting neither changes preparation conditions nor writes an intent,
increments the attempt or moves its deadline. Normal status updates can still
report actual workload availability.

During a hold, safe mutable-workload and volume-protection work with old
templates requires the direct APIReader same-UID/nonterminating/nil-pointer
fence and no durable intent/journal attempt in that pass. It requests a positive
`RequeueAfter`, retaining an earlier positive requeue from normal work, without
a preparation error; another actual error is still reported. Mutex-protected state keeps one entry per namespace/name and
only its latest UID/digest; a new UID/digest resets the hold. Successful start
or Completed-pointer clear removes matching state; safe Preparing absence-abort
retains the hold. `NotFound` and deletion remove the entry. Ten minutes without
a failed
attempt expires an entry after its retry deadline. There is no cardinality cap
or active-entry eviction; cleanup scales with recently failed Cluster count.
A restart also loses it. This is rate control, not security authority, and it
leaves the Preparing absence-abort contract unchanged. Failed or lost responses
from attempted durable preparation still freeze normal mutable work in their
own pass.

The snapshot is control-plane recovery state, not a volume backup. Do not edit
or delete it manually to unblock a rollout, and do not delete the Cluster, PVCs
or PVs as a storage-reconciliation recovery step. If a same-name foreign
resource or an invalid snapshot blocks recovery, preserve the snapshot and
resolve the ownership/error before resuming.

### Dependencies and authorization during recovery

After active-pointer and exact-byte authentication, recovery keeps the saved
Pod template, TLS/auth wiring, replica count and claim names authoritative.
When the saved template mounts the auth-key ConfigMap,
recovery refreshes it from current Credentials without deleting it. No
matching Credentials produces an explicit empty key set; unresolved matching
keys retain the existing fail-safe carry-forward rules. Cluster-secret
maintenance runs only when the saved template requires the Secret and retains
the existing token. Volume-protection reconciliation uses the saved workload
geometry with the current deletion-protection toggle. Recovery protection
checks saved PVC/PV UIDs and bindings and fences label patches with
UID/resourceVersion preconditions. Newer invalid auth, TLS
or replica fields do not change those dependency inputs.
Recovery reads current Credentials and their referenced Secrets directly from
the API, rather than relying on cached key-distribution observations.
Independent auth-key ConfigMap and cluster-secret writes also retain the live
Cluster's `spec.additionalLabels`.

These dependency writes do not change the saved Pod template. Their errors are
returned together with any recovery error, while recovery itself can still
restore the StatefulSet. Services, NetworkPolicy and other normal workload
resources wait for the recovery gate to clear.

`AuthKeysRolloutDeferred=True/StorageRecovery` exposes that key projection and
running-Pod authorization can differ. It clears only after the recovery pointer
is gone and a normal StatefulSet reconciliation succeeds; clearing it does not
certify that all Pods have completed the resulting rollout. The current key
loader rejects an empty key set on new starts: after all matching Credentials
are removed, a restarted
Pod that retains the saved key-file wiring can fail startup. An empty mounted
ConfigMap does not disable authentication or make the saved workload accept
unauthenticated requests.

Ledger loads Ed25519 keys and their authorization metadata once at boot.
Refreshing the ConfigMap is **not effective revocation in already running
Pods**. Key removal, scope reduction and rotation still require the subsequent
normal rollout after recovery and cleanup. An adopted Pod that is unready after
an OOM or empty-key startup failure does not keep that cleanup gate closed.
Missing Pods, unobserved controller generation or identity/adoption failures
still block recovery; resolve those failures while preserving the journal.
Let the subsequent normal rollout finish, then verify the intended authorization
on every serving Pod. ConfigMap convergence and journal cleanup alone do not
close that runtime security gap.

### Diagnosing a blocked replacement

1. Inspect `status.claimTemplateRecovery`, its source UID, full digest and
   ConfigMap name/UID, the immutable
   journal payload, `ClaimTemplatesReady`, events and controller errors. Preserve
   this evidence before changing the environment.
2. Compare the live StatefulSet's UID, owner, operation annotation and complete
   spec with the saved replacement. Inspect the retained Pods, PVC/PV UIDs,
   claim references and bindings; names alone do not establish continuity.
3. Correct admission/webhook configuration or restore the trusted replacement
   specification exactly where Kubernetes permits it. A foreign owner, changed
   identity or immutable mismatch requires an operator-led recovery that
   preserves the committed snapshot and storage identities; the controller
   does not force adoption or substitute newly provisioned volumes.
4. Let recovery retry, then verify observed controller generation, adoption,
   retained storage identities and automatic journal cleanup before expecting
   deferred workload and authorization rollouts. Verify their readiness and
   effective authorization separately.

Do not delete the journal, clear its status pointer or delete claims to force a
fresh creation. An immutable or identity mismatch has no automatic emergency
override. During the StatefulSet absence, aggregate workload status can show
`Pending`, `Ready=False` or `0/N` while retained Pods still run; inspect those
Pods directly and use `ClaimTemplatesReady` for recovery progress.

Deleting the Cluster is not a recovery action. During the orphan window, Pods
can have lost their StatefulSet owner, so normal Cluster garbage collection
must not be assumed to remove every surviving Pod. If Cluster deletion is
intentional, inventory surviving Pods and claims and follow the configured
retention/deletion-protection policy. Real garbage collection during this
window requires a running Kubernetes environment to validate.

### Storage progress and rollback

`Cluster.status.conditions` separates these operations:

- `WALStorageReady` and `DataStorageReady` report existing PVC request and
  observed capacity/profile convergence, including pending or failed CSI work.
- `ClaimTemplatesReady` reports the claim-template recreation/recovery state.
  A ready template does not certify completed CSI modification on every claim.
- `WALVolumeExpansionReady` and `DataVolumeExpansionReady` report the optional
  utilization-based expansion policy.

Compare each PVC's `spec.resources.requests.storage` with `status.capacity`,
and inspect `Resizing` / `FileSystemResizePending`. For profiles, compare
`spec.volumeAttributesClassName` with `status.currentVolumeAttributesClassName`
and inspect `status.modifyVolumeStatus`, PVC events and CSI diagnostics. A
stored requested class alone does not prove its IOPS/throughput were applied.
Check mounted capacity and retained Ledger data when validating the target
driver; a fake client or a template-only test cannot prove those outcomes.

A safe rollback keeps all accepted capacity increases: neither a Git revert
nor disabling automatic expansion shrinks a volume or lowers an existing
template minimum. Revert a profile only by requesting a nonempty class whose
reverse transition the CSI driver supports. Let an active recovery finish from
its snapshot before applying subsequent workload edits. A CR revert does not
cancel a CSI operation already accepted by Kubernetes.

## Automatic Volume Expansion

The operator can grow PVC-backed WAL and data volumes before the Ledger reaches
its disk-full write gate. Expansion is strictly opt-in and does not require a
Prometheus server: every five minutes a separate controller evaluates the live
PVC group. When the group is ready for a new capacity decision, it executes
`ledgerctl cluster disk-usage --json` inside each Ledger pod, uses the highest
valid utilization for the volume kind, and patches every replica's PVC to the
same target size. Replica calls run in parallel, and WAL and data share the same
response so the command executes at most once per pod and reconciliation. It
skips pod exec while PVCs are converging, resizing, or in
cooldown. Each measurement carries validity, server-computed age, last
successful observation time, and an optional diagnostic error. The operator
rejects the selected volume when collection failed, the sample is older than
one minute, its observation time is missing, or total capacity is zero. The
replica set is read from the live StatefulSet rather than the Cluster spec, so
a rejected Cluster update cannot leave a running replica outside the expansion
group. The measurement command expands `GRPC_PORT` inside each target pod, so
an invalid Cluster update or a partial rollout cannot redirect it to a port
that pod does not serve. TLS mode comes from the live StatefulSet.

```yaml
apiVersion: ledger.formance.com/v1alpha1
kind: Cluster
metadata:
  name: production
spec:
  persistence:
    data:
      size: 100Gi
      storageClass: gp3-expandable
      autoExpansion:
        enabled: true
        thresholdPercent: 70
        targetPercent: 55
        minimumIncrement: 10Gi
        maximumSize: 2Ti
        cooldown: 8h
```

`maximumSize` is mandatory. The other enabled-policy defaults are 70% trigger,
55% target, 10 GiB minimum increment, and an eight-hour cooldown. The cooldown
cannot be shorter than six hours. The selected StorageClass must exist and set
`allowVolumeExpansion: true`; for EKS this is normally an EBS CSI StorageClass
using `ebs.csi.aws.com` or `ebs.csi.eks.amazonaws.com`.

Utilization-based automatic growth only patches live PVC storage requests.
Automatic growth never changes the Cluster's `size` or StatefulSet claim
templates; the separate declarative reconciliation above updates templates
from the configured minimum. Neither path changes PV capacity or AWS resources
directly. CSI performs the underlying volume and filesystem expansion.
Shrinking and hostPath expansion are unsupported.

All PVCs of one kind converge to the largest requested size before another
capacity decision is made, unless that request exceeds the current
`maximumSize`. In that case the operator emits `VolumeExpansionUnsupported`
and leaves the group unchanged until the cap is raised to at least the largest
live request. This cap does not reject the separate explicit `size` request;
it only blocks automatic decisions until the cap is raised. A resize already
in progress and the cooldown both block further automatic growth. The PVC
annotations
`ledger.formance.com/last-expansion-at` and
`ledger.formance.com/last-expansion-target` make retries and operator restarts
idempotent. The timestamp drives cooldown; a malformed or future timestamp is
rejected as invalid evidence rather than allowing an external annotation edit
or clock skew to suppress expansion indefinitely. The target annotation is
diagnostic only.

Troubleshooting surfaces:

- Cluster conditions: `WALVolumeExpansionReady` and
  `DataVolumeExpansionReady`, with a reason describing the latest state.
- Cluster events: `VolumeExpansionRequested`, `VolumeExpansionPending`,
  `VolumeExpansionMeasurementFailed`, `VolumeExpansionUnsupported`, and
  `VolumeExpansionLimitReached`.
- Operator metrics: `ledger_operator_volume_usage_ratio`,
  `ledger_operator_volume_usage_sample_age_seconds`,
  `ledger_operator_volume_requested_bytes`,
  `ledger_operator_volume_expansions_total`, and
  `ledger_operator_volume_expansion_errors_total`.
- PVC status/conditions: compare `spec.resources.requests.storage` with
  `status.capacity` and inspect `Resizing` / `FileSystemResizePending`.

If measurements are incomplete and no reachable replica crosses the threshold,
the operator does not assume the cluster is healthy: it emits a warning and
retries after one minute. A reachable replica over threshold may still trigger
the safe cluster-wide expansion. Last-known bytes from an invalid or stale
sample are diagnostic only and never trigger a resize.
Usage ratio and sample-age series exist only for fresh samples used by the most
recent reconciliation; skipped or invalid measurement paths clear them. Metrics
labelled by a Cluster are retired when expansion is disabled or the Cluster is
deleted, and scaled-down pod labels are removed on the next reconciliation.

## Volume Deletion Protection

Protection is **on by default**: a freshly deployed ledger is protected without
any extra configuration, and both layers below must be explicitly turned off to
opt out. Deletion protection has three independent layers, so the choice of
*which* ledgers are protected lives with the ledger owner (per-CR), while
*whether the mechanism exists at all* stays a cluster-admin decision:

1. **`pvcProtection.enabled` (Helm value, cluster-admin consent).** Installs two
   cluster-scoped `ValidatingAdmissionPolicy` objects (`failurePolicy: Fail`) that
   reject `DELETE` of selected ledger PVCs/PVs. **On by default** but **requires
   Kubernetes >= 1.30** (ValidatingAdmissionPolicy GA). On an older cluster the
   chart detects that the `ValidatingAdmissionPolicy` kind is absent and **skips
   these objects** so the default install/upgrade still succeeds (it prints a
   NOTES warning); a Cluster with `deletionProtection: true` then reports the
   runtime `DeletionProtectionInactive` warning because no policy acts on its
   volumes. `helm template` run offline uses Helm's built-in capability list, so
   pass `--api-versions admissionregistration.k8s.io/v1/ValidatingAdmissionPolicy`
   to force-render the policy there. Installing the policy does **not** protect
   anything on its own — the policy bindings only select volumes carrying the
   `ledger.formance.com/deletion-protection: enabled` label.

   The policy is a **cluster-wide singleton** — enable `pvcProtection.enabled` on
   **at most one** operator release per cluster. The cluster-scoped policy objects
   have fixed, release-independent names, so a second release with
   `pvcProtection.enabled=true` fails its `helm install`/`upgrade` with an ownership
   conflict by design, rather than installing a second policy that would cross-apply
   to and block legitimate deletes on the first release's volumes. **Because the value
   now defaults to `true`, in a multi-release cluster you must set
   `pvcProtection.enabled=false` on all but one release** — otherwise the second
   install fails. In a multi-release cluster where *other* releases also manage
   ledgers with `deletionProtection: true`, list those releases' operator
   ServiceAccounts in `pvcProtection.additionalExemptServiceAccounts` on the owning
   release, so their operators' scale-down deletes are not blocked by the singleton
   policy.
2. **`spec.persistence.deletionProtection` (per-Cluster, default `true`).**
   Protected by default: the operator stamps that label on the ledger's PVCs and
   their bound PVs, so the cluster policy selects them. Set it explicitly to `false`
   to opt out — the label is removed and protection is lifted. This is versioned
   alongside the ledger and toggleable without a `helm upgrade`.
3. **`formance.com/allow-deletion=true` annotation (per-volume override).** A
   protected volume can still be deleted on purpose by annotating it first.

To delete a protected volume on purpose:

```bash
kubectl annotate pvc <name> formance.com/allow-deletion=true --overwrite
kubectl delete pvc <name>
```

If a Cluster sets `deletionProtection: true` while no cluster-scoped protection
policy is installed on the cluster, the label is still stamped but no policy acts on it;
the operator surfaces this as a `DeletionProtectionInactive` warning event and status
condition on the CR rather than silently leaving the volumes unprotected. The operator
detects this by probing for the policy's `ValidatingAdmissionPolicyBinding` directly, so
the condition stays correct in a multi-release cluster: a sibling release with
`pvcProtection.enabled=false` whose ledgers are protected by the owning release's
singleton policy is **not** falsely warned.

Exemptions: the operator ServiceAccount (its own raft scale-down deletes) and the
kube-controller-manager garbage collector are exempt, so the StatefulSet
`retentionPolicy: Delete` path (`persistence.retentionPolicy.whenScaled` /
`whenDeleted=Delete`) continues to work with protection enabled.

No other identity is exempt. A workload/GitOps controller (ArgoCD, Flux, Velero
restore, etc.) that deletes a protected `Cluster` **and** its PVCs/PVs in a
single managed teardown runs under its own ServiceAccount, so once
`pvcProtection.enabled=true` those deletes are blocked just like a manual one. To
allow such a teardown, either annotate the volumes with the allow-deletion key
first (as above) or, for a recurring controller, add its ServiceAccount username to
`pvcProtection.additionalExemptServiceAccounts` (full form
`system:serviceaccount:<namespace>:<name>`), which appends it to the policies'
`matchConditions` exemptions.

The PV policy only protects **Bound** PVs (volumes holding live ledger data). Once
a PVC is deleted — which itself goes through the PVC policy above — its PV becomes
`Released` and the reclaim path proceeds normally: with `persistentVolumeReclaimPolicy:
Delete` (the default for most cloud StorageClasses) the PV controller / CSI
external-provisioner deletes the volume without being blocked, and with `Retain` an
admin can delete the orphaned `Released` PV directly. Deleting a live, Bound PV by
hand is still rejected unless it carries the allow-deletion annotation. (A PV that is
orphaned in the `Released` state keeps the protection label it last held, because the
operator only reconciles the label on live PVCs; this is harmless since the policy
guards Bound PVs only.)

## kubectl Plugin

The `kubectl-ledger` plugin provides a CLI for managing Cluster resources.

### Installation

**From source (use Go 1.27.3; module language minimum Go 1.26.0):**

```bash
go build -o $(go env GOPATH)/bin/kubectl-ledger ./cmd/kubectl-ledger
```

Or using `just`:

```bash
just install-plugin
```

Once installed, kubectl discovers it automatically:

```bash
kubectl ledger --help
```

### Commands

```
kubectl ledger list [-A]                  # List all Clusters
kubectl ledger get <name>                 # Show detailed status
kubectl ledger create <name>              # Create a new Cluster (interactive)
kubectl ledger delete <name> [-y]         # Delete a Cluster
kubectl ledger scale <name> --replicas=5  # Scale replicas (must be odd)
kubectl ledger restart <name>             # Rolling restart
kubectl ledger logs <name>                # Stream pod logs
kubectl ledger portforward <name>         # Port-forward to a pod
kubectl ledger config view <name>         # View configuration
kubectl ledger config edit <name>         # Edit configuration
kubectl ledger explain [field.path]       # Explore the CRD schema
kubectl ledger credentials list           # List cluster credentials
kubectl ledger credentials create <name>  # Create credentials with API key
kubectl ledger credentials get-key <name> # Retrieve credentials API key
kubectl ledger version                    # Print version info
```

### Examples

```bash
# List all ledger services across namespaces
kubectl ledger list -A

# Inspect a specific service
kubectl ledger get my-ledger

# Explore CRD schema for Raft configuration
kubectl ledger explain spec.raft

# Create with schema-driven field overrides
kubectl ledger create my-ledger \
  --set replicas=5 \
  --set image.repository=ghcr.io/formancehq/ledger \
  --set image.tag=latest \
  --set podAntiAffinity.enabled=true \
  --set podAntiAffinity.type=hard \
  --set resources.requests.cpu=2000m \
  --set resources.requests.memory=4Gi \
  --set resources.limits.cpu=4000m \
  --set resources.limits.memory=4Gi \
  --set persistence.wal.size=10Gi \
  --set persistence.data.size=50Gi

# Scale up
kubectl ledger scale my-ledger --replicas 7

# Rolling restart
kubectl ledger restart my-ledger -y
```

Plain `kubectl rollout restart statefulset/<name>` also works: the operator
preserves the `kubectl.kubernetes.io/restartedAt` annotation kubectl stamps on
the pod template instead of reverting it on the next reconcile. Prefer
`kubectl ledger restart`, which records the restart on the Cluster CR itself
(`spec.podAnnotations`) and therefore survives StatefulSet recreation.

**Exception:** if the Cluster CR itself sets `kubectl.kubernetes.io/restartedAt`
in `spec.podAnnotations`, the CR value is authoritative and the operator reverts
a direct `kubectl rollout restart` stamp on the next reconcile — the rollout
stops after the first pod. In that configuration, restart through the CR only
(`kubectl ledger restart`, or bump the annotation value in the CR).

## Development

### Setup

The project uses [Nix](https://nixos.org/) for reproducible development environments:

```bash
# Enter the dev shell (automatic with direnv)
nix develop

# Or manually
nix develop --impure
```

### Build & Test

```bash
just build          # Build operator binary
just test           # Run tests
just generate       # Regenerate CRDs, JSON Schema, RBAC, and Helm chart
just pre-commit     # Run all checks (generate + tidy + build)
just build-plugin   # Build kubectl plugin
just install-plugin # Install kubectl plugin to $GOPATH/bin
```

### JSON Schema

`just generate` also writes standalone JSON Schema files under `config/crd/schemas/`:

| File pattern | Contents |
|--------------|----------|
| `<group>_<version>_<kind>.json` (e.g. `ledger.formance.com_v1alpha1_cluster.json`) | Full CRD resource schema (`openAPIV3Schema`) for that version — top-level `apiVersion`/`kind`/`metadata`/`spec`/`status` |
| `<group>_<version>_<kind>.spec.json` | `spec` fields only, at the schema root — for validating a spec fragment on its own (e.g. Helm `values.yaml` shaped like a CR's `spec`) |
| `<group>.json` (e.g. `ledger.formance.com.json`) | Dispatcher schema: routes a manifest to the full resource schema matching its `apiVersion`/`kind`, via JSON Schema draft-07 `if`/`then` |

The group prefix avoids collisions when these files sit alongside schemas from unrelated CRDs/operators (kind names like `Cluster` or `Backup` are common). Unlike `config/crd/bases/*.yaml` — one file per CRD, group+plural named, with no version in the name because a single CRD can serve several versions from that one file — each exported schema here describes exactly one version's shape, so the version belongs in its name.

Example for a full manifest, with VS Code / Red Hat YAML:

```yaml
# yaml-language-server: $schema=../config/crd/schemas/ledger.formance.com_v1alpha1_cluster.json
apiVersion: ledger.formance.com/v1alpha1
kind: Cluster
metadata:
  name: my-ledger
spec:
  replicas: 3
```

Use the `.spec.json` variant only when validating a bare spec fragment (its root has no `spec` property, so pointing it at a full manifest validates nothing useful).

For a directory containing manifests of several kinds, point `$schema` at the group dispatcher instead of picking a per-kind schema:

```yaml
# yaml-language-server: $schema=../config/crd/schemas/ledger.formance.com.json
apiVersion: ledger.formance.com/v1alpha1
kind: Backup
metadata:
  name: my-backup
spec:
  clusterRef: my-cluster
  destination:
    driver: s3
    s3:
      bucket: example-bucket
```

The dispatcher declares `$schema: draft-07` for its own `if`/`then` branches, while each branch's `$ref` target stays draft-04 — validators resolve a `$ref` using the referenced document's own declared draft, so mixing the two is safe.

Every object with a fixed property set rejects unknown properties (`additionalProperties: false`), so a typo like `replicass` is flagged by the editor instead of being silently pruned the way the Kubernetes API server would prune it. Map-type fields (e.g. `additionalLabels`) keep their value schema and still accept arbitrary keys; opaque fields (`metadata`, and anything marked `x-kubernetes-preserve-unknown-fields`) are left unconstrained. Each full resource schema also requires and constrains `apiVersion`/`kind` to the exact values for that kind, so `{}` or a manifest for an unrelated kind is rejected. A field marked `x-kubernetes-list-type: set` (e.g. `EventSink`'s `eventTypes`) gets `uniqueItems: true`, so a manifest with a duplicate entry is rejected the way Kubernetes' own `ValidateListSetsAndMaps` would reject it. A `required` field that also carries a `default` (e.g. `Backup`'s `destination.driver`, defaulted to `s3`) is dropped from `required`: Kubernetes applies CRD defaulting before validating required fields, so a manifest omitting it is still valid to apply.

**Known limitation — CEL rules are not enforced.** `x-kubernetes-validations` (CEL) rules are preserved in the exported schema for reference but are not translated into portable JSON Schema constraints, which draft-04 validators ignore. For example, `Ledger`'s Postgres mirror source requires exactly one of `passwordFrom`/`awsIamAuth`, and requires `sslMode` to be TLS-enabled when `awsIamAuth` is set (otherwise the SigV4 bearer token would travel in cleartext) — both enforced by CEL in `api/v1alpha1/ledger_crd_types.go`, neither enforced by `ledger.formance.com_v1alpha1_ledger.spec.json`. A fragment violating either rule still validates cleanly against the exported schema. Kubernetes admission (CEL) remains the authoritative check for these rules; treat the exported schemas as catching structural/typo errors, not business-rule violations.

Regenerate after changing CRD types in `api/v1alpha1/`. These files are also attached to each tagged [GitHub release](https://github.com/formancehq/ledger/releases) alongside `openapi.yml`.

### Project Structure

```
cmd/
  operator/          # Operator entrypoint
  kubectl-ledger/    # kubectl plugin
api/v1alpha1/        # CRD type definitions
internal/controller/ # Reconciliation logic
chart/               # Helm chart
config/
  crd/bases/         # Generated CRD manifests
  crd/schemas/       # Generated JSON Schema (full resource + spec-only)
  rbac/              # Generated RBAC rules
  samples/           # Example custom resources
```

## License

Proprietary - Formance
