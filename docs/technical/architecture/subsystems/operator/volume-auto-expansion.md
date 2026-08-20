# Automatic PVC Expansion

## Purpose

Ledger rejects new writes once WAL or data filesystem usage crosses its
configured high-water mark. Automatic PVC expansion provides headroom before
that safety gate is reached without making Ledger, Prometheus, or application
code responsible for cloud storage mutations.

The mechanism is implemented by a dedicated `VolumeExpansionReconciler` in the
existing Ledger v3 Operator. It is separate from `ClusterReconciler`: the latter
owns declarative workload resources and Cluster status, while volume expansion
owns only live PVC storage requests and two bookkeeping annotations.

## Interface and prerequisites

The policy is opt-in under a PVC-backed WAL or data `VolumeSpec`. A hard
`maximumSize` is required. Trigger, target utilization, minimum increment, and
cooldown have safe defaults but remain configurable. The StorageClass must set
`allowVolumeExpansion: true` and its CSI driver must support online expansion.

The feature rejects hostPath and cold-cache policies. Cold cache is bounded and
evictable rather than an authority-bearing dataset, so growing it indefinitely
would hide a cache-retention problem.

## Measurement path

No Prometheus server is required. On every enabled Cluster reconcile interval,
the operator runs the existing command below in each expected pod:

```text
ledgerctl cluster disk-usage --json
```

Pod exec deliberately reuses the Ledger container's cluster token, TLS CA,
headless DNS identity, and transport mode. The JSON response carries local WAL
and data used/total bytes. The operator evaluates the highest utilization seen
across replicas. A failed measurement cannot turn an otherwise-below-threshold
sample into a healthy verdict; it causes a warning and a short retry. If any
reachable replica is already over the threshold, expansion can proceed despite
another failed sample.

## Decision and convergence

For a volume kind, all live StatefulSet replica PVCs form one expansion group.
The group size comes from `StatefulSet.spec.replicas`, not from the Cluster spec:
if an invalid Cluster update is rejected while the previous StatefulSet keeps
running, every live replica remains covered. Before consulting usage, the
reconciler repairs a partially-applied prior request by raising every smaller
PVC request to the largest request already present, provided that request does
not exceed the current `maximumSize`. This makes a crash or Kubernetes patch
failure self-healing without propagating a request that violates a subsequently
lowered policy cap or an external PVC edit.

When the threshold is crossed, the new capacity is:

```text
ceilGiB(max(maxUsedBytes / targetUtilization,
            largestCurrentRequest + minimumIncrement))
```

The result is capped at `maximumSize`. The reconciler never shrinks a volume.
It waits while any PVC is unbound, while requested capacity exceeds reported
capacity, while Kubernetes reports `Resizing` or `FileSystemResizePending`, and
until the group cooldown expires.

One patch changes both `spec.resources.requests.storage` and:

- `ledger.formance.com/last-expansion-at`
- `ledger.formance.com/last-expansion-target`

The Cluster spec and StatefulSet VolumeClaimTemplates remain unchanged: their
`size` is the initial/minimum request, not mutable operational state. Kubernetes
CSI performs the cloud-volume and filesystem resize; the operator has no AWS
client or cloud credentials.

## Failure modes and operations

- Unsupported or absent StorageClass: emit `VolumeExpansionUnsupported`, do not
  patch PVCs, retry after one minute.
- Missing/unbound PVC or active resize: emit `VolumeExpansionPending` and retry.
- Incomplete usage with no observed threshold crossing: emit
  `VolumeExpansionMeasurementFailed` and retry.
- Maximum reached while usage remains high: emit
  `VolumeExpansionLimitReached`; operators must increase the explicit cap or
  reduce data growth.
- A live PVC request above the current `maximumSize`: emit
  `VolumeExpansionUnsupported` and leave the group unchanged; operators must
  raise the cap to at least the largest live request before convergence resumes.
- Partial patch: the largest PVC request becomes the recovery target on the
  next pass when it remains within `maximumSize`; cooldown does not prevent
  convergence.

Structured logs record the decision, current/target/max bytes, highest usage,
and failed measurement count. Kubernetes events provide the human-facing state.
The operator's existing metrics endpoint exports usage ratios, requested bytes,
decision counters, and error counters; scraping these metrics is optional and
does not participate in the control loop.

## EKS validation

Use an expandable `gp3` StorageClass and a three-replica staging Cluster. Fill
one data filesystem past the trigger, verify all three PVC requests converge,
then wait for PVC `status.capacity` and `df` inside every pod to reflect the new
size. Pods must remain Ready, writes must not reach the Ledger disk gate, and no
second request may be issued during cooldown.
