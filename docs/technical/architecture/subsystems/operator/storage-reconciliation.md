# Declarative storage reconciliation

## Requirement and ownership

Tenants need to manage volume capacity and IOPS/throughput through their GitOps
Cluster manifest, without a separate Job patching PVCs and recreating their
StatefulSet. Previously the Operator only applied these settings at creation;
automatic expansion changed live capacity but did not implement explicit edits.
The requested contract preserves claims, bound volumes and Ledger data.

`spec.persistence.wal/data.size` is a minimum. Reconciliation can increase a
request, including without automatic expansion, but cannot shrink a claim or
lower an existing template floor. Nonempty `volumeAttributesClassName` changes
request in-place CSI modification; clearing a nonempty class, StorageClass
migration and access-mode migration are unsupported. The existing hostPath
layout transition remains a separate mechanism.

Size/profile alignment copies the existing template's StorageClass and access
modes unchanged even when the desired declaration differs. An omitted or empty
template class can coexist with a defaulted effective live PVC class: retaining
that declaration or explicitly naming the same effective class does not migrate
the template or claims, and does not block size/profile alignment. Live PVC
validation separately diagnoses a genuine migration as `StorageUnsupported` on
the affected storage condition and never patches PVC class/access modes.
Mutable workload reconciliation and independently safe owned-PVC growth
continue under their normal gates. Unsupported profile clearing and incompatible
claim layouts remain separate limits.

The existing `VolumeExpansionReconciler` is the sole Operator writer of live PVC
requests and profiles. Explicit minima do not wait for usage, cooldown or the
automatic cap. Automatic growth still follows its own policy, never rewrites
templates, and pauses when live requests exceed its cap. The CSI driver owns
physical modification; the Operator does not call AWS APIs.

## Pod restart inputs

The canonical workload hash excludes WAL/data `size`,
`volumeAttributesClassName`, `storageClass` and `accessMode`, whose declarations
do not change Pod mounts or references. HostPath wiring and ServiceAccount
configuration remain hashed. Adding, replacing or removing ServiceAccount
annotations must request the normal rollout, including alongside a storage
edit: admission can consume those annotations at Pod creation. Equality of the
other admitted Pod-template bytes alone cannot establish unchanged runtime
inputs. A differing trusted canonical workload hash must not be suppressed by
a generic admission-equality fallback. Actual Pod rollout and effective runtime
access require separate verification.

Canonical metadata is trusted only as a valid association: StatefulSet metadata
`ledger.formance.com/pod-config-hash` is nonempty, the installed Pod-template
`ledger.formance.com/spec-hash` is nonempty, and StatefulSet metadata
`ledger.formance.com/pod-config-spec-hash` equals that installed Pod hash.
The association records the actual retained Pod hash, which can be historical;
it need not equal the canonical hash. Both hash resolution and compatibility
preservation use this rule. Neither marker presence alone nor StatefulSet UID
or generation establishes the association, which must survive recreation.

Missing or broken association returns to verified legacy migration below.
It does not by itself request a storage-only rollout. A released Operator can
preserve unknown metadata while changing the Pod hash, invalidating the pair.
For example, after canonical ServiceAccount configuration A, a released
Operator can install B and leave A's metadata intact. Returning to desired A
must not certify the installed B hash as A. If exact reconstruction fails and
the admitted templates are equal, missing proof reports `HashMigrationRequired`;
authentic history for B instead establishes the required rollout to A.

Released full-spec hashes can migrate automatically through exact
reconstruction from current nonstorage inputs and retained claim settings,
including the supported omitted-field variants. Equal installed/desired hash
strings alone do not prove released-spec reconstruction. If reconstruction fails
and no proof is supplied, the controller substitutes the installed hash in a
copy of the desired Pod template and normalizes it through admission dry-run Update.
If that admitted template differs from the live template, it returns the
desired canonical-hash template for a normal rollout. This covers image,
configuration and TLS changes, including an omitted image tag whose effective
`LEDGER_IMAGE_TAG` changed. Sequential RC, TLS and storage gates remain active.

Equal admitted templates cannot establish unchanged external admission inputs:
an earlier storage declaration may be unrecoverable and ServiceAccount
annotations may have changed. Only this ambiguous no-proof branch reports
`ClaimTemplatesReady=False/HashMigrationRequired` without advancing canonical
metadata or starting replacement. An explicitly supplied proof must be
validated before a rollout; differing templates do not bypass invalid proof.

Upgrade the CRD and Operator with the Cluster declaration unchanged before
storage or ServiceAccount edits, then wait for the valid association above.
If combined edits require history, restoring only their authentic previous
declared inputs can permit exact reconstruction before reapplying the edits.
Do not infer unavailable history or reverse accepted live PVC growth. PVC
reconciliation is independent. The ServiceAccount resource can also already
reflect the requested annotations before a hash migration error; blocking the
template does not roll back that write or refresh existing Pods' admission
inputs. Strict historical proof remains the alternative;
correct invalid supplied proof or deliberately remove it before the no-proof
path. See the [upgrade procedure](../../../../ops/upgrades.md#operator-workload-hash-migration).

The Cluster annotation `ledger.formance.com/legacy-spec-hash-proof` supplies
one JSON object with `version: 1`, `sourceUID`, `oldSpecHash` and the complete
historical effective ClusterSpec in `spec`. The source UID and installed
Pod-template hash must match the current StatefulSet. The decoder rejects
unknown fields and malformed/trailing JSON. Recomputing the released full-spec
hash from the historical spec must reproduce the installed value; current
defaults are never applied to that history. Wrong version/identity/hash or
inconsistent history reports `HashMigrationInvalid`. Historical values must
reproduce the admitted spec after the released `applyDefaults`, including image
repository/tag defaults from that deployment's `LEDGER_IMAGE_REPOSITORY` and
`LEDGER_IMAGE_TAG`. Preserve other omissions, such as storage quantities and
access modes, as hashed; rendered template defaults are not spec history.
Current CR/ServiceAccount state is not a substitute. This is operational
metadata, with no public CRD schema or Ledger persisted-data format change.

After validation, the controller recomputes both canonical hashes. A difference
requests the desired rollout, including a ServiceAccount annotation change.
Equality permits preservation of the old hash only after the admitted desired
template also equals the live template. A dry-run error remains
`AdmissionCheckFailed`. Only a valid canonical/installed-hash association takes
precedence over leftover proof; a trusted canonical mismatch requests new Pods.
Missing or broken association cannot bypass proof validation. Remove the proof
after success. Never force migration by resetting either workload hash. See the
[historical proof procedure](../../../../../misc/operator/README.md#released-workload-hash-migration).

## PVC actuation and completion

The controller reads the live StatefulSet replica count and establishes claim
ownership from exact owner UIDs or an owned Pod referencing the retained claim.
Every optimistic patch attempt rereads the claim and workload, verifies identity
and ownership, and retains requests above its target. Volume modification also
validates the bound PV's claim reference and the class's CSI driver. Stable VAC
API reads are preferred, with a controlled beta fallback for older clusters.

`WALStorageReady` and `DataStorageReady` distinguish accepted requests from
`status.capacity`, resize errors, `currentVolumeAttributesClassName` and
`modifyVolumeStatus`. They are separate from automatic-expansion conditions,
workload `Ready`, and `ClaimTemplatesReady`. Both status writers preserve the
other writer's fields/conditions across optimistic conflicts. A normal check
uses a five-minute interval; pending/error paths retry sooner. Work scales with
the live replica count and the two volume kinds, not Ledger business cardinality.

## Durable template replacement

Kubernetes claim templates are immutable. For same-name capacity/profile edits,
`ClusterReconciler` copies the observed StatefulSet and changes only supported
claim settings. It preserves selectors, replica count and the complete Pod
template, including image, TLS references and restart annotations. It completes
the existing rollout first; a combined scale-up can wait for the new templates.
Historical spec hashes follow the verified migration above; the canonical
workload hash and its installed-hash association live on StatefulSet metadata
so storage edits do not request Pod restarts. After final Pod-template
resolution, normal and preparatory Raft scale-down writes record
`ledger.formance.com/pod-config-hash` and
`ledger.formance.com/pod-config-spec-hash` atomically with that template. The
association uses its actual Pod hash, including a preserved historical hash.
The preparatory write retains the previous replica count before any Raft exec.
If that API write fails, Raft work does not start. If Raft exec fails after the
write,
the prepared template and valid pair remain durable. The next pass uses
that pair instead of revalidating historical proof against the changed Pod
hash, then retries the normal membership-removal path before reducing replicas.
The pair is not evidence of rollout or membership completion.
Before comparing desired and existing Pod templates, an admission dry-run
Update normalizes the desired template on a copy of the existing StatefulSet.
Semantic equality of the admitted and live templates includes API defaults;
defaulted probe timeouts/success thresholds do not block recovery initiation.

The controller performs these durable steps in order:

1. Prepare independent copies of the source and replacement, source
   UID/generation and retained PVC/PV identities. Verify the stable source,
   owned Pods and bound claim references. Dry-run creation of the replacement
   must preserve its settings, including VAC.
2. Record the canonical hash and association to the replacement's actual retained
   Pod hash on that in-memory replacement, then encode the complete journal
   payload once. Compute a domain-separated SHA-256
   over the Cluster UID, namespace, fixed journal base and those exact JSON
   bytes, then append the first 32 hex characters to the journal base for the
   ConfigMap name. The pointer retains and verifies the full lowercase digest.
   The pair is therefore part of the saved replacement before encoding and
   digesting; an existing immutable journal is never edited to add it.
   The internal journal format remains version `1`.
3. Compare-and-swap nil `status.claimTemplateRecovery` to a Preparing intent
   containing `sourceUID`, `payloadSHA256` and `configMapName`, without a
   `configMapUID`. Require the same nonterminating Cluster UID, the generation
   used for preparation and no other operation. Verify that the API retained
   the whole intent before any journal write. An old CRD that prunes fields
   rejects this operation before orphan deletion.
4. Create the immutable Cluster-owned ConfigMap with those exact bytes. On
   `AlreadyExists`, reread it directly and require the expected owner, name,
   nontermination, immutability and full digest; never overwrite it. Reuse
   requires the fresh preparation and durable matching intent above. Decode
   only authenticated bytes, then validate the journal version, source UID and
   workload/claim structure before binding. The decoded source UID must match
   the protected pointer.
5. Compare-and-swap that exact Preparing pointer to Active by binding the
   observed ConfigMap UID, and verify durable preservation. Directly reread
   the journal after binding and repeat name/owner/nontermination/immutability/
   full-digest authentication against the bound UID before dependencies,
   deletion or recreation. A missing or changed journal stops that pass.
6. Request orphan deletion with source UID/resourceVersion preconditions. The
   original Pods, claims and volumes remain; the StatefulSet object is absent
   between successful deletion and recreation.
7. Recreate from the authenticated journal, with fresh metadata and an operation
   annotation outside the Pod template. A lost create response is recognized from the
   replacement identity/annotation and saved specification.
8. Verify the exact saved spec, ownership and operation identity of the
   nonterminating replacement, its observed generation, all expected Pods'
   adoption and retained PVC/PV identities. Compare-and-swap the exact Active
   pointer to Completed before deleting the journal, then compare-and-swap the
   expected Completed pointer to nil.

The digest domain is `ledger.formance.com/claim-template-recovery/v1`. NUL
bytes separate it, the Cluster UID, namespace and fixed
`resourceName(cluster.Name) + "-storage-recovery"` base from the exact payload.
The ConfigMap cannot choose a different domain or digest authority.

These phases are inferred from the pointer: Preparing has no ConfigMap UID,
Active has a bound UID, and Completed has that UID plus `completed: true`.
Every transition compares the whole expected previous pointer and uses an
optimistic resourceVersion. A conflict returns for fresh observations; retry
must never refresh and overwrite a newer operation or clear its pointer.

Protected Cluster status authorizes the payload. A ConfigMap's ownerReference,
name, structure or self-declared digest cannot supply that authority. Every
active resume repeats the status/ConfigMap authentication before using decoded
content. A nil pointer never adopts a standalone ConfigMap, and its presence
alone must not block normal reconciliation. A late unbound journal has no
workload authority. Different payloads use different digest-derived names;
identical payloads can reuse a ConfigMap only after fresh preparation, durable
intent and exact verification. Unbound journals can remain for Cluster garbage
collection; their separate cleanup cannot authorize workload recovery.

The complete feature is unpublished. Pointers without the required source,
digest and name are rejected; there is no retroactive authentication migration.
This change does not increment journal, Ledger data, Raft, audit or service
protocol versions.

Preparing can remain after an interrupted intent or journal write. If direct
API reads find the named ConfigMap exclusively `NotFound`, the controller may
compare-and-swap that exact valid Preparing pointer to nil only while the same
Cluster UID is nonterminating and the expected StatefulSet is Cluster-controlled,
nonterminating and has the saved source UID. Source generation, readiness and
the latest desired spec do not enter this absence-abort decision. It reads no
old claims or recovery dependencies, mutates no workload and deletes no journal
in that pass. After a successful clear, it returns handled with a two-second
requested requeue, not a guaranteed two-second execution cadence. Missing,
terminating or replaced sources and uncertain reads fail
closed. A concurrent UID bind and this clear compare the same Preparing pointer:
only one can win. A journal that appears after the clear remains unbound.

Completed requires a bound ConfigMap UID. Cleanup checks its Cluster owner and
that UID, uses the live journal resourceVersion for deletion, then clears only
the expected Completed pointer. It does not decode the payload, require
immutability or reconcile dependencies, so damaged payload bytes cannot strand
completed cleanup. A different journal UID still
fails closed. `NotFound` after deletion supports retry after a lost response.

The post-delete completion gate establishes controller continuity, without
requiring `ReadyReplicas` or `UpdatedReplicas`. It requires
`status.observedGeneration >= metadata.generation`. Each expected Pod must exist,
be nonterminating and be controlled by the replacement. Zero Ready replicas,
OOM restarts or an empty-key startup failure do not alone retain the journal;
cleanup returns to the normal repair and rollout path. Missing/unadopted Pods
or an unobserved generation still block completion. Source rollout readiness
remains required before initiating deletion. Workload availability and effective
runtime authorization remain separate post-rollout checks.
An expected Pod's `NotFound` after recreation sets
`ClaimTemplatesReady=False/AwaitingAdoption`, names the missing Pod and returns
the recovery requeue interval without completing or cleaning up the operation.
Other read errors remain `RecoveryFailed`. Restoring the expected Pod still
requires nontermination and exact replacement ownership before completion.
The requeue does not bypass `OrderedReady` Pod creation; an unready lower
ordinal can still delay a missing ordinal and the return to normal repair.

In Kubernetes v1.32, the StatefulSet controller initializes
[`ObservedGeneration` before processing Pod readiness](https://github.com/kubernetes/kubernetes/blob/v1.32.0/pkg/controller/statefulset/stateful_set_control.go#L497-L505)
and [attempts to persist a returned status even after a processing error](https://github.com/kubernetes/kubernetes/blob/v1.32.0/pkg/controller/statefulset/stateful_set_control.go#L102-L107).
Observed generation therefore does not certify readiness; adoption and retained
storage identities supply the separate continuity checks above.

Fresh reconciliations read the pointer directly from the API and authenticate
its active journal before current-spec validation, RC admission or other
workload mutations. Later edits
cannot overwrite its saved image, TLS or membership-related replica count. A
nonterminating source with the original UID but a changed generation or an
unready rollout safely invalidates the old plan before reading obsolete claims.
The controller persists the completed marker, cleans up the journal/pointer,
then returns to normal repair and replanning. Once orphan deletion starts, the
saved workload remains authoritative; a terminating source, absent source or
replacement UID cannot use this pre-delete escape.

A failed journal/pointer write does not authorize orphan deletion. Failed deletion
leaves the journal and possibly the source; failed recreation leaves the journal
and orphaned Pods/claims. Adoption failure retains recovery state and freezes
later workload edits. The completed marker handles a lost cleanup response.
A missing active journal or changed identity is an explicit error, never initial
creation or permission to provision replacement storage. PVC/CSI effects are
independently durable and are not rolled back by a failed template operation.

Immediately before the first actual status-intent `Status.Patch`, preparation marks its
write phase as attempted and retains it through journal creation. Any later
error preserves that phase and its original cause; the caller returns
without normal mutable-workload continuation. A subsequent `NotFound` cannot
exclude a delayed server-side commit and cannot relax that boundary.
Rejected writes and failures that committed nothing still end that pass.
Only a failure before any durable intent-write attempt enters the direct API
fence. A nil recovery pointer on the same nonterminating Cluster UID, verified
through the APIReader, permits safe mutable-workload and volume-protection reconciliation
with the old templates. The pass retains its failed preparation condition and
returns its error after those attempts. Concurrent status writers can race
condition observation. A recovery pointer, changed identity or failed
fence read prevents continuation. Nil cached or in-memory status is never
sufficient; an unbound ConfigMap alone supplies neither authority nor a block.

Repeated failed intent/journal preparation uses a mutex-protected in-memory
retry gate with one entry per Cluster namespace/name, retaining only its latest
UID and full prepared payload digest. After a failed attempted durable
intent/journal phase, including response loss, the hold starts at two seconds
and increases exponentially to 30 seconds. Preparation computes the exact
payload digest first, then checks the gate before publishing Preparing or
attempting a new intent CAS. Repeated watch
events during the same hold neither retry that phase, change its conditions,
increment the attempt nor move the deadline. `RequeueAfter` requests scheduling;
it does not establish a fixed reconcile cadence.
Failed pointer writes are recorded once at the write boundary; preparation
records other failures after successful intent. Scheduled waits record no
failure. Failed Completed journal deletion also records a failure.

A scheduled hold requests a positive requeue, retaining an earlier positive
requeue from normal work, without a preparation error. Because that pass
attempted no durable intent/journal write, safe normal
mutable-workload and volume-protection work can proceed only through the same
direct APIReader same-UID/nonterminating/nil-pointer fence above. Another actual
error remains visible. Normal availability/status updates can still occur;
the gate suppresses retry-intent and preparation-condition churn, not truthful
workload status. A new UID or payload digest resets the hold. Successful start
or Completed-pointer clear removes state matching that UID/digest; safe
Preparing absence-abort does not reset it. Deletion or `NotFound` forgets the
entry. Ten minutes without a failed attempt expires it after its retry deadline.
There is no cardinality cap or active-entry
eviction; cleanup scans scale with recently failed Cluster count. A controller
restart loses the hold. This memory is rate control, never journal authority,
and does not change Preparing absence-abort guards.

This journal is operational Kubernetes state, outside Raft, audit and Ledger
backup formats. Install the CRD, finish the new Operator rollout and disable
competing storage Jobs before using this behavior. Do not downgrade a controller
with active recovery. After completion, a released Operator can retain unknown
hash metadata but change the Pod hash, which breaks its association on return
to the new controller. That old binary can itself request a storage-hash rollout;
the new controller adds none when exact legacy reconstruction succeeds.
See the [operations contract](../../../../../misc/operator/README.md#claim-template-recovery-sequence)
for upgrade effects, rollback and failure handling.

## Independent dependencies and operational recovery

After authenticating the active pointer and exact journal bytes, then validating
the decoded journal format and saved workload, recovery
refreshes auth-key projection from current Credentials when the saved Pod
template mounts that ConfigMap, without deleting it. No matching Credentials
produces an explicit empty key set; unresolved matching keys retain their
fail-safe handling. Required cluster-secret maintenance retains the existing
token and uses the saved TLS/auth wiring. Volume protection uses saved replicas
and claim names with the current protection toggle. Later invalid auth/TLS or
replica edits do not select these dependency inputs. Dependency failures are
joined with the recovery error after its attempt; they do not prevent
StatefulSet restoration.
These operations preserve the saved Pod template. Services, NetworkPolicy and
other normal resource reconciliation remain behind the recovery gate.
Credentials and their referenced Secrets are read directly from the API for
this projection, without relying on cached distribution observations. Auth-key
ConfigMap and cluster-secret reconciliation copy the live Cluster's
`spec.additionalLabels` as independent dependency metadata.

`AuthKeysRolloutDeferred=True/StorageRecovery` identifies deferred application
to running Pods. The normal controller removes it only after the recovery
pointer is nil and a normal StatefulSet pass succeeds. Removal is not evidence
that the resulting rollout has reached every Pod. The current startup key
loader rejects an empty key set; after all matching Credentials are removed,
a restarted Pod using the saved key-file wiring can fail startup. Empty
projection does not disable auth.

Ledger loads Ed25519 keys and authorization metadata once at boot. A refreshed
ConfigMap does not prove key revocation, scope reduction or rotation in running
Pods. Effective changes require the normal subsequent rollout, which waits for
recovery and cleanup. An adopted but unready Pod does not block journal cleanup
or a later repair. A missing Pod or failed identity/adoption/generation check
still needs recovery before that rollout; dependency projection does not
resolve the runtime security gap.

Desired-template normalization, replacement-create and legacy-hash comparison
dry-runs require available webhooks with dry-run support. Admission must
preserve the full replacement spec, including supported volume attributes.
Rejection, timeout or differing replacement admission output must remain an
explicit failure; no unsafe
fallback may skip the preview or introduce a storage-only Pod rollout. Restore
webhook/API prerequisites or correct conflicting mutations before retrying.
Normalization and legacy-hash preview failures must publish
`ClaimTemplatesReady=False/AdmissionCheckFailed` on the actual Cluster, including
when workload reconciliation uses an effective TLS configuration copy.

A mismatched replacement remains `RecoveryFailed`. Inspect the immutable
journal/pointer, live spec and operation annotation, owner/UID identities and
retained PVC/PV bindings. Correct admission or restore the exact trusted saved
specification where Kubernetes permits it, preserving storage identities.
Immutable or identity mismatches need an operator-led recovery; journal/claim
deletion and forced adoption are not recovery mechanisms. See the
[replacement diagnosis steps](../../../../../misc/operator/README.md#diagnosing-a-blocked-replacement).

The workload status can temporarily show `Pending`, `Ready=False` or `0/N`
during StatefulSet absence despite surviving Pods. Inspect those Pods and
`ClaimTemplatesReady` separately. Cluster deletion during the orphan window
can leave Pods outside Cluster owner-based garbage collection; it is not a
recovery action or proof of complete workload cleanup.

## Evidence boundaries

Unit evidence must cover identity, partial writes, response loss, process
replacement, historical hashes, scale-down and CSI status interpretation.
Hash regressions must cover valid pairs, missing/broken pairs, retained
historical hashes, leftover proof and the A-to-released-B-to-A transition, both
without proof and with authentic B history. Keep released RC/TLS fixtures free
of both modern annotations. Cover deterministic retry timing, reset, ten-minute
expiry, one-entry-per-name retention and concurrent access. A bounded
normal-manager test must verify real ResourceQuota denial and count Cluster
events/status changes and intent/clear transitions during watch traffic, retain
storage and prove progress after removing the denial. Concurrent status writers
can race diagnostic sampling; a stable observed `PreparationFailed` condition
is not required to prove this rate limit. Unit tests separately verify waiting
preserves conditions and permits fenced protection work. Envtest publishes
quota usage and simulates GC/adoption explicitly; its mutable-work case proves
scale-up without Raft or a StatefulSet controller.
Do not infer zero status writes globally from the retry gate.
API-server evidence must cover the normal manager path with defaulted probes,
admission, durable status, dry-run preservation and actual Kubernetes writes.
Completion cases must include adopted Pods with zero Ready/Updated replicas
and a later normal repair, plus negative unobserved-generation and unadopted-Pod
cases. Envtest does not run garbage collection, the StatefulSet controller,
kubelet or CSI. Pod adoption, mounted capacity, effective EBS profiles and
sentinel Ledger data still require a disposable workload/CSI environment.
Missing-Pod tests must assert `AwaitingAdoption`, a short requeue and retained
journal/pointer, then restore that ordinal and verify guarded completion and
cleanup. Trust tests must reject authority derived from standalone ConfigMaps,
missing digest/source, changed bytes/UID/owner, mutable or terminating journals
and stale-pointer CAS.
Exercise intent response loss, CRD pruning, `AlreadyExists` verification,
Preparing absence-abort versus binding, and late unbound journals without
dependency or workload mutation. Completed cleanup must tolerate payload
corruption while rejecting a foreign UID. Preparation-failure tests must
distinguish errors before the first durable status-intent attempt from every
later failure. An attempted write returning
an error must block normal work even with `NotFound`; include delayed/unknown
outcomes, lost journal/pointer responses, uncertain API reads and stale cached
absence. Only the earlier phase plus a directly verified same-UID nonterminating
Cluster with no operation permits normal work. Report executed trust checks
separately from this evidence contract.
