# Ledger Next Operator

Kubernetes operator for deploying and managing high-availability [Formance Ledger](https://github.com/formancehq/ledger) instances using Raft consensus.

## Overview

The Ledger Operator manages `Cluster` and `EventSink` custom resources to automate the lifecycle of distributed ledger clusters and event delivery on Kubernetes. It handles:

- **StatefulSet management** with Raft-based consensus (odd replica counts)
- **Persistent storage** for WAL and data volumes
- **Observability** with OpenTelemetry traces, Prometheus metrics, and Pyroscope profiling
- **Security** with TLS, OIDC authentication, and Ed25519 response signing
- **Backups** to S3-compatible backends, with [recoverable Job provisioning](../../docs/ops/backup-restore.md#scheduling-with-the-kubernetes-operator) and sibling-run exclusion
- **Cold storage** archival to S3-compatible backends
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

## Pyroscope credentials

`spec.monitoring.pyroscope.authTokenFrom` and `basicAuthPasswordFrom` accept
`{name, key}` references to Secrets in the Cluster namespace. The operator
renders required `valueFrom.secretKeyRef` entries only when profiling is enabled;
it never copies those credential bytes into the Cluster or Pod template.
Plaintext `authToken` and `basicAuthPassword` fields are not supported.
Reference changes trigger a rollout; rotating Secret contents requires a Pod
restart. See [profiling deployment](../../docs/ops/deployment.md#pyroscope-continuous-profiling)
for examples and missing-reference behavior.

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
| `Ledger` | Namespaced | Declarative logical ledger and indexes |
| `Backup` | Namespaced | Scheduled backup configuration |
| `BackupRun` | Namespaced | Individual backup execution |

All six kinds retain their names and use `ledger-next.formance.com/v1alpha1`.
The Pebble operator continues to own `ledger.formance.com/v1alpha1`.

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
# Set immutable candidate tags from the RocksDB build/release you have verified.
# Both images must contain this revision's RocksDB code and next API identity.
: "${ROCKSDB_OPERATOR_TAG:?Set the pinned RocksDB operator candidate tag}"
: "${ROCKSDB_LEDGER_TAG:?Set the pinned RocksDB Ledger candidate tag}"
helm dependency build misc/operator/helm/operator --skip-refresh
helm install ledger-next-operator misc/operator/helm/operator \
  --namespace ledger-next-system \
  --create-namespace \
  --set watchNamespace=ledger-next \
  --set image.repository=ghcr.io/formancehq/ledger-operator \
  --set-string image.tag="$ROCKSDB_OPERATOR_TAG" \
  --set-string ledgerImage.tag="$ROCKSDB_LEDGER_TAG"
```

Install this as a **new release alongside** the existing Pebble operator; do not
upgrade the old release or replace its CRDs. The operator runs in
`ledger-next-system`; create the separate `ledger-next` benchmark namespace
before applying workloads there. `watchNamespace=ledger-next` limits namespaced
reconciliation, while `Credentials` remain cluster-scoped. The CRD dependency is
`ledger-next-operator-crds` (disable with `ledger-next-operator-crds.create=false`
only when those next CRDs are already installed).

The image repository stays `ghcr.io/formancehq/ledger-operator`. No
`ledger-next-operator` image repository is assumed to exist. Candidate tags must
be supplied explicitly: no published RocksDB candidate is asserted here. Pin
both operator and Ledger tags to immutable build versions and record the resolved
digests for the benchmark; `latest` chart defaults are not benchmark pins.
For a local E2E build, the explicit operator candidate is
`ledger-next-operator:e2e` with `image.pullPolicy=Never`, not a published image.

Uninstall runs a pre-delete hook that deletes **all**
`credentials.ledger-next.formance.com` across the cluster while the next operator
can process finalizers. This group-wide cleanup is independent of
`watchNamespace` and does not delete Pebble Credentials. Coordinate uninstall
with other next releases sharing these cluster-scoped credentials.

### Deploy a Ledger Cluster

```yaml
apiVersion: ledger-next.formance.com/v1alpha1
kind: Cluster
metadata:
  name: my-ledger
  namespace: ledger-next
spec:
  replicas: 3
  image:
    repository: ghcr.io/formancehq/ledger
    tag: "<pinned-rocksdb-ledger-tag>"
  clusterID: default
  podAntiAffinity:
    enabled: true
    type: hard
    topologyKey: kubernetes.io/hostname
  rocksdb:
    memTableSize: 256Mi
    cacheSize: 1Gi
  # Cache and bloom parameters are part of the Raft-replicated ClusterConfig.
  # Editing them triggers a rolling restart of the StatefulSet; convergence
  # is deterministic via applyClusterConfig (cache reset + bloom rebuild) and
  # bounded by one election cycle after the last pod restarts.
  cache:
    rotationThreshold: 1000
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
apiVersion: ledger-next.formance.com/v1alpha1
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
not yet reported a sink status, even when its configuration is synced. The
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
| `pvcProtection.allowDeletionAnnotation` | `ledger-next.formance.com/allow-deletion` | Annotation key whose value `true` opts a volume out of deletion protection |
| `pvcProtection.additionalExemptServiceAccounts` | `[]` | Extra ServiceAccount usernames (`system:serviceaccount:<ns>:<name>`) exempt from the policies — sibling operator releases managing protected ledgers, or managed workload/GitOps controllers |

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
   `ledger-next.formance.com/deletion-protection: enabled` label.

   The next policies use `ledger-next-volume-protection-pvc` and
   `ledger-next-volume-protection-pv`, with next label and annotation keys. They
   coexist with Pebble's policies without selecting its volumes.

   The policy is a **cluster-wide singleton per API group** — enable `pvcProtection.enabled` on
   **at most one** next operator release per cluster. The cluster-scoped policy objects
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
3. **`ledger-next.formance.com/allow-deletion=true` annotation (per-volume override).** A
   protected volume can still be deleted on purpose by annotating it first.

To delete a protected volume on purpose:

```bash
kubectl annotate pvc <name> ledger-next.formance.com/allow-deletion=true --overwrite
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

The `kubectl-ledger` binary name remains unchanged. Built from this checkout,
it targets the next API group, including `Cluster` and `Credentials` resources.
Keep the existing Pebble CLI separately if you need to manage both groups.

### Installation

**From source (requires Go 1.26+):**

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
  --set image.tag="$ROCKSDB_LEDGER_TAG" \
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
just generate       # Regenerate CRDs, RBAC, and Helm chart
just test-helm-render # Check policy API capability gating
just test-helm-coexistence # Lint and check next installation isolation
just pre-commit     # Run all checks (generate + tidy + build)
just build-plugin   # Build kubectl plugin
just install-plugin # Install kubectl plugin to $GOPATH/bin
```

### Project Structure

```
cmd/
  operator/          # Operator entrypoint
  kubectl-ledger/    # kubectl plugin
api/v1alpha1/        # CRD type definitions
internal/controller/ # Reconciliation logic
helm/operator/       # ledger-next-operator Helm chart
helm/crds/           # ledger-next-operator-crds Helm chart
config/
  crd/bases/         # Generated CRD manifests
  rbac/              # Generated RBAC rules
  samples/           # Example custom resources
```

## License

Proprietary - Formance
