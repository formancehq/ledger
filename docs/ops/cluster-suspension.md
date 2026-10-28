# Operator Cluster suspension

## Requirement and scope

Global Stack disable must stop Ledger workloads without deleting the
`ledger.formance.com/v1alpha1` `Cluster`. The parent Stack controller can retain
that CR and set `spec.disabled: true`; clearing the flag restores the retained
cluster. This requirement was validated with Maxence in the
[Stack disable discussion](https://numary.slack.com/archives/C06CG0618JU/p1793187342401379).
Individual module disable through Membership still deletes the module CR.
This mechanism changes only the Ledger repository's Operator. It does not add
global suspension support to another controller.

`spec.disabled` is an optional boolean. Absent or `false` preserves normal
reconciliation. `true` requests suspension. `spec.replicas` remains the configured
Raft topology, with its existing minimum of one; zero is only an internal
StatefulSet replica target during suspension.

```yaml
apiVersion: ledger.formance.com/v1alpha1
kind: Cluster
metadata:
  name: my-ledger
spec:
  replicas: 3
  disabled: true
```

## Retained resources

| Resource or state | Suspension behavior |
| --- | --- |
| Cluster CR, UID and `spec.clusterID` | Retained. An omitted cluster ID is generated and persisted even for an initially disabled Cluster. |
| StatefulSet | Retained at zero replicas. Its current Pod template, image, storage wiring and authorization references stay unchanged. |
| Ledger Pods | Terminated through the StatefulSet replica target. No Raft member is removed. |
| WAL/data PVCs and PVs | Retained, including when the configured `whenScaled` or `whenDeleted` retention policy is `Delete`. No scale-down PVC deletion runs. |
| HostPath data | Left in place. The existing requirement to schedule onto the correct host still applies. |
| Services, Ingresses, DNS endpoints, NetworkPolicies and ServiceAccounts | Retained without normal reconciliation. Existing addresses do not imply an available Ledger service. |
| Auth ConfigMaps, generated inter-node Secret and referenced external Secrets | Retained. Cluster-side auth/TLS reconciliation waits. External Secret owners can still rotate or delete their Secrets. |
| Credentials CRs and distributed credential Secrets | Credential matching and distribution continue; disabling a Cluster does not remove it from a Credentials selector. |
| Backup and BackupRun CRs, completion cursors and result summaries | Retained. Scheduling, new Job provisioning, result interpretation and history pruning wait. |
| Existing backup Jobs | Incomplete owned Jobs are suspended; Kubernetes stops their active Pods. Completed Jobs and existing backup objects remain. |
| Ledger and EventSink CRs | Runtime mutations and deletion cleanup wait, with finalizers and reconciliation cursors retained. |
| Volume expansion state | Retained. Disk measurement and expansion reconciliation wait. |

Suspension is a cold stop, not an export or a new bootstrap identity. It does
not alter business data, audit records, Raft entries, snapshot/backup formats or
the gRPC protocol. It cannot restore data that was already lost before the
request, replace missing external Secrets, or preserve an unbacked ephemeral
storage source.

## Stop sequence and recovery

Suspension runs before normal configuration, upgrade, selector, credentials and
TLS gates. A desired configuration error therefore does not prevent stopping
an existing workload.

1. The Cluster controller suspends incomplete backup Jobs whose BackupRun
   ownership and persisted endpoint identify this Cluster. New Jobs also retain
   the Cluster UID. Changing a parent reference does not retarget an existing
   execution.
2. The Cluster controller saves the current StatefulSet replica count in
   `ledger.formance.com/suspended-replicas` and sets StatefulSet PVC
   `whenScaled` retention to `Retain` in one durable update, preserving the
   source `whenDeleted` policy. It keeps replicas
   unchanged in that update.
3. The Cluster controller waits until the StatefulSet controller observes that
   generation and removes ordinal-Pod garbage-collection owner references
   from the retained claims. StatefulSet owners remain valid for
   `whenDeleted: Delete` because suspension retains the StatefulSet.
4. The Cluster controller sets the StatefulSet replica target to zero.
5. The Cluster controller waits for Ledger Pods and active backup Job Pods to
   disappear before reporting `Suspended`.

Each completed API write remains after a later error. Failed writes leave the
saved topology and retained resources available for retry. The controller
requeues during suspension, so restarts, delayed garbage-collection updates and
late workload creation can converge without another spec edit. If an enabled
reconciliation already in flight restores `whenScaled: Delete`, suspension
repairs it without replacing the saved replica count, then repeats controller
observation and claim-ownership checks before scaling. It never uses
the ordinary Raft scale-down/removal path for suspension.

An initially disabled Cluster receives its stable cluster ID, but does not
create a StatefulSet or other normal Cluster resources. Its first enable runs
normal provisioning.

## Resume sequence

1. The caller sets `spec.disabled: false`, or removes the field.
2. The Cluster controller repairs the auth ConfigMap from current Credentials,
   applies current authorization settings/key wiring to the retained template,
   and honors the existing unresolved-key fail-safe before starting any Pod.
   This prevents stale revoked keys/scopes from reactivating and repairs a
   missing Operator-owned ConfigMap. A requested `restore: true` to `false`
   handoff also updates the startup command and probes before startup: a
   finalized restore cannot boot in restore mode again. All other template
   configuration, including image, TLS transport and storage, stays retained.
   The controller then restores the saved replica count with the retained PVCs
   and `whenScaled` retention still set to `Retain`.
3. The Cluster controller waits for that StatefulSet generation to be observed
   and for all saved replicas to become ready.
4. The Cluster controller resumes only backup Jobs that it marked with
   `ledger.formance.com/cluster-suspended`, then removes the saved replica cursor.
5. The Cluster controller resumes normal reconciliation against the current
   desired spec. Normal validation, RC predecessor checks, TLS transitions,
   credentials gates, storage changes and Raft-aware scale-down apply again.

The saved count is the actual pre-pause StatefulSet target, rather than a
replacement for `spec.replicas`. If the desired replica count changes while
paused, the old topology first returns so a subsequent ordinary scale-down can
remove members through consensus. Desired image and TLS edits wait until that retained topology is ready.
Entering restore mode also waits; leaving restore mode is the explicit
pre-start handoff described above. Suspension does not complete
a restore, skip an RC upgrade, or rewrite the persisted startup identity.

Backup Job suspension stops containers; it does not checkpoint their process
memory or undo an already completed remote write. Resume can start another
container attempt in the retained Job. Existing backup recovery and result
capture contracts still apply. A Job already suspended by another actor is
left suspended on resume.

## Status and operational limits

| Phase | Meaning |
| --- | --- |
| `Suspending` | Stop requested; retention, workload shutdown or Job shutdown is still converging. |
| `Suspended` | No Ledger Pods or active owned backup Job Pods remain. `Suspended=True`, reason `WorkloadsStopped`. |
| `Resuming` | The retained topology is being restored before normal reconciliation. |

`Ready=False` throughout these phases. `Suspended=False` uses
`StoppingWorkloads` or `RestoringTopology` during transitions; normal enabled
reconciliation reports reason `Enabled`. `readyReplicas` remains the observed
StatefulSet value and can temporarily lag Pod deletion. Dependent CR status
can retain its last result while reconciliation waits; it is not a current
availability signal. The Cluster's `Ready` condition supplies that signal.

Do not delete the StatefulSet, its suspension annotation or the retained PVCs
to force progress. If an interrupted orphan/recreate operation leaves Ledger
Pods without a StatefulSet, suspension fails closed: the controller cannot
prove claim retention and does not delete those Pods. A foreign-owned
StatefulSet is also rejected. Resolve the ownership/recreation problem first.

The default StatefulSet has no `preStop` hook. Ledger shutdown attempts a
best-effort leadership transfer, then requests bounded task shutdown even if
that transfer fails. Ordered termination therefore does not require keeping a
leader or quorum until the final Pod stops. The default `/readyz` checks that
the local Raft loop started, independently of election/quorum, so OrderedReady
can launch enough retained voters for a cold-start election. `CLUSTER_JOINED`
in each retained WAL prevents those nodes from joining as new identities.

Resume depends on the retained Pods becoming ready. Custom probes that demand
a full quorum can block an ordered cold start; retain the documented
quorum-independent readiness behavior. Missing images, volumes, Secrets or
capacity can also keep the Cluster in `Resuming`. Fix the dependency rather
than changing its identity or discarding data.

A spec edit is not an atomic fence against a reconciliation already in flight
or an external writer. A late Pod or Job can briefly appear after disable;
repeated reconciliation stops it. Wait for the observed suspension state and
inspect Pods before treating the Stack as stopped. Repeated disable/enable
requests remain recoverable through the durable StatefulSet cursor.

Deleting a Cluster remains deletion, including its normal garbage-collection
and retention behavior. Suspension does not redefine individual module
deletion, credential deletion, or externally managed resource lifecycles.

## Regression evidence

The Operator's `cluster-suspension` Chainsaw scenario creates an initially
disabled Cluster, enables three TLS-enabled voters, commits a sentinel
transaction, then performs two suspension/resume cycles. It configures
`whenScaled: Delete` with volume deletion protection off. It compares all six
PVC UIDs, Cluster/StatefulSet identity, inter-node Secret and certificate
identity, and the exact non-empty transactions from each local voter
projection after restart. It also runs store integrity checks and commits a
new transaction after each resume to prove write quorum recovered. Finally it
selects `whenDeleted: Delete`, suspends again, deletes the Cluster and verifies
that garbage collection removes all six claims.

Run it on the disposable Kind harness described in
[Operator development](../../misc/operator/README.md#development):

```bash
cd misc/operator
nix develop --command just e2e-cluster-up
nix develop --command chainsaw test --config e2e/chainsaw-test.yaml \
  --test-dir e2e/tests/cluster-suspension --kube-context kind-ledger-e2e
```

The scenario requires a working Docker daemon. Unit/fake-client checks verify
resource write ordering and retry behavior; they do not prove real StatefulSet
garbage collection, persistent data recovery or Raft cold-start availability.
