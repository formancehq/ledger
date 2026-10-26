# Operator reconciliation durability evidence contract

The [manifest](operator-reconciliation-durability.json) defines a reusable audit
of the Ledger Kubernetes control plane in `misc/operator`. The operational need
is to establish whether repeated or interrupted reconciliation preserves the
intended cluster, credentials, volumes and backup/restore progress. Existing
Raft, persistence and lifecycle domains do not own the Kubernetes transition
between those effects and the CR state that drives the next reconciliation.
This domain adds scope and evidence requirements to the existing native workflow;
it adds no runner, product behavior or deployment guarantee.

For EventSink status, follow the Ledger sink-status response through parsing
and condition publication. A startup or delivery error is `Delivering=False`;
cursor zero without an error remains `Unknown` until cursor progress exists.
An empty error by itself does not establish successful delivery.
Filtered or internal logs can advance the cursor without external delivery;
`Delivering=True` therefore does not establish a successful publish by itself.

## Reachability and proof

For each hypothesis, establish all of the following from the audited SHA:

1. **Entry and identity:** an admitted CR, supported spec edit, deletion, child
   event or restart; namespace/name/UID/generation and external operation identity
   where relevant. Inspect API defaults, CEL/schema validation and manager setup.
2. **Execution:** the real reconcile path, watches/predicates/requeues, ownership
   guards, RBAC and required external components. Fake clients do not establish
   API validation, garbage collection, StatefulSet adoption, CSI expansion or
   controller-runtime event behavior.
3. **Failure boundary:** the actual in-memory mutation order, API-server writes,
   Ledger/object-store commits, status writes and cleanup. Enumerate the reachable
   persisted prefix at interruption; do not describe an intended end state as if
   it were an atomic transaction.
4. **Recovery:** reconstruct a fresh reconcile from persisted observations,
   including stale-cache reads, conflicts, missed acknowledgements and replacement
   UIDs. Identify the guard or retry source that either repairs the prefix or
   leaves a material violation after dependencies recover.
5. **Impact and falsification:** cite exact code locations, the established
   invariant, affected outcome, existing protections and test gap. Supply a
   specific reproduction plan with a trigger, failure point and observable
   postcondition that distinguishes the claimed branch from another failure.

No existing test or green suite proves convergence by itself. Conversely, a
missing test, finalizer, watch or retry is not a defect without a reachable
failure mechanism. A permanently unavailable dependency is not evidence of a
controller bug; show incorrect success, destructive behavior, or failure to
recover when the prerequisite is restored. Separate controller restart from
Ledger process restart and cluster-level availability from recovery of the
intended PVC/node. Record inspected areas and untested boundaries in residual
risk rather than claiming exhaustive coverage.

## Ledger target admission boundary

Ledger `spec.name` and `spec.clusterRef` are immutable from CR creation through
API-server CEL transition validation. Include Pending resources and updates
combined with promotion or index changes: an unknown creation outcome must not
permit retargeting. API-server rejection must leave the stored target unchanged;
deleting that CR must select the original ledger and leave distinguishable
external ledgers in either Cluster untouched. Fake clients and exported JSON
schemas do not enforce this transition boundary.

For upgrades, distinguish newly admitted edits from target drift already stored
before these rules were installed. The operator must be stopped while existing
CRs, including ambiguous Pending creations, are inventoried and recovered before
installing the rules. The applied hash cannot recover original identity, and
installing immutability freezes the current values. Do not treat rejection of
future edits as retrospective identity recovery, external ownership proof, or
fencing against a Cluster recreated under the same name. Ledger cleanup remains
best-effort after target selection; endpoint/exec failure can leave the external
ledger after finalizer removal.

## Backup Secret delivery boundary

Backup S3 static authentication uses same-namespace Secret key references.
Reconciliation reserves the BackupRun as Running before persisting the Job; kubelet
credential resolution happens later. A missing required Secret/key prevents
container startup, so Running does not establish delivery or backup success.
Subsequent container starts resolve current Secret values; existing process
environments do not change on Secret updates. Check these transitions separately
from confidentiality, which is owned by `sensitive-data-exposure-boundaries`.

## TLS disable recovery boundary

Removing `spec.tls`, or setting `enabled: false` while clearing `secretName`,
is a supported disable request. Optional mode must retain the previous
certificate Secret and CA references. Trace `status.tlsRollout` persistence
before StatefulSet mutation or orphan deletion, then reconstruct a fresh
reconciler from each persisted prefix. A failed recovery-status write must
leave the workload untouched. A missing StatefulSet with retained rollout state
is interrupted reconciliation, not evidence of a new plaintext bootstrap.
New backup Jobs must resolve mode and certificate/CA references from one
current owned StatefulSet template, rather than mixing observed mode with
already-cleared desired TLS. Missing workloads during recreation must cause
retry without creating a plaintext Job; foreign ownership must reject creation.
In-pod ledgerctl calls from Ledger and EventSink reconciliation must also
return a retryable lookup error before exec while the StatefulSet is absent,
then read the recreated workload mode on retry. Cover both callers separately
and exercise missing-then-recreated optional, required and disabled workloads.
An existing Job's persisted template remains its configuration source. Prove a
real missing-workload failure followed by successful provisioning after owned
workload recovery, and preserve prior backup error/response-loss regressions.
The certificate Secret remains user-owned and available until disable finishes;
its lifetime differs from the operator-managed inter-node bearer Secret.

Prove both omitted TLS and a cleared certificate reference through the real
StatefulSet reconcile path. Interrupt after recovery-state persistence and
after orphan deletion, replace the reconciler, and assert optional-mode
certificate/CA continuity, the recovered mode, unchanged desired spec and final
disabled-template convergence before recovery-state cleanup. Selecting a
disabled template or reporting `tlsMigrationPhase: disabled` alone does not
establish that every Pod has rolled. Use non-empty pre/post transaction data
as the availability/data oracle in the TLS-disable Chainsaw fixture. Fake
clients establish API-write ordering, not StatefulSet adoption, admission,
rolling-update behavior or real peer/CLI connectivity; disclose those limits.

## Sequential RC and storage transition boundary

For an RC target, trace the running image rather than assuming the desired CR
is the predecessor. Reject a skipped RC or an unfinished source rollout before
mutating the StatefulSet. When an adjacent RC change coincides with an immutable
WAL/data claim-template change, the interim image rollout must retain the old
Pod storage volumes and mounts as well as the old claim templates. Only after
the RC rollout converges may orphan/recreate install the requested storage
layout. Test both hostPath-to-PVC and PVC-to-hostPath against a Kubernetes API
server: a fake client does not validate the interim Pod template. Reconstruct
an interrupted rollout with a fresh reconciler, and inspect retained Pods or
PVCs when the StatefulSet is absent so source verification cannot be skipped.

## Completed backup result observation

For a successful owned Job, distinguish result acquisition from interpretation.
Pod-list failures, temporarily absent Pods and fallback log open/read errors must
preserve the nonterminal reservation and retry across fresh reconciler instances.
A malformed acquired summary or failed Job remains terminal. Inspect diagnostic
write failures and repeated errors as separate persisted prefixes: no completion
or successful summary is certified before the result status write succeeds.

Use the result-read and fallback-log recovery regressions as evidence oracles:
verify actual fail-then-success acquisition, unchanged Job identity, final literal
summary values, sibling exclusion and parent full-summary/incremental eligibility.
With zero history retention, the run must survive pending acquisition and can be
pruned only after terminal accounting is recorded. These fake-client and HTTP
fixtures do not prove Kubernetes garbage collection, kubelet execution or live
manager backoff. Historical terminal failures are not automatically reopened.
## Declarative storage and claim-template recovery boundary

The [declarative storage contract](../../../misc/operator/README.md#declarative-storage-reconciliation)
supports GitOps capacity/profile edits for existing PVC-backed WAL and data
volumes. It adds new durable transitions and evidence oracles under the
[audit maintenance triggers](../contributing/ai-audit.md#maintaining-audit-contracts);
updating this contract does not authorize an audit run.

Distinguish two nondecreasing minima. An existing PVC request must remain at
least its previous request and the configured `size`, preserving already larger
capacity. A claim-template request must remain at least its previous template
minimum and the configured `size`. Automatic PVC growth must not flow back into
the Cluster spec or template. For example, template `100Gi`, auto-grown PVC
`200Gi`, configured `150Gi` must produce a `150Gi` template and retain the
`200Gi` claim. Prove explicit growth with automatic expansion absent/disabled,
a floor decrease, repeated reconciliation, and scale-out. An explicit floor
above `autoExpansion.maximumSize` remains an explicit request; that cap and the
automatic cooldown only constrain automatic decisions.

A nonempty `volumeAttributesClassName` requests an in-place CSI modification.
Trace admission, class/driver prerequisites, the PVC patch and subsequent CSI
status independently. Unsupported clearing must remain visible without
silently clearing the existing class. Existing StorageClass/access-mode
migrations are unsupported, and this feature leaves hostPath paths/layout
unchanged. `WALStorageReady` / `DataStorageReady` represent existing claim
convergence; `ClaimTemplatesReady` represents claim-template recovery. Inspect
status-writer ownership and conflict retries so these conditions cannot erase
each other or substitute a template result for actual volume convergence.

StorageClass/access-mode declaration differences must not prevent size/profile
template alignment: copy the existing template class and access modes unchanged.
Include a template with no explicit class whose PVCs received the default class:
making that same effective PVC class explicit must allow both PVC and template
size/profile progress without migrating either class. Distinguish the omitted
declaration from the effective live class in examples. Live PVC validation must
report a genuine migration as `StorageUnsupported` on the affected storage
condition without mutating class/access modes. Prove mutable workload and
independently safe capacity progress separately, retaining UID, ownership,
binding and no-shrink guards. Unsupported VAC clearing and incompatible claim
layouts retain their separate failure contracts.

For size/profile template changes, establish this actual ordering and enumerate
each persisted prefix:

1. Finish the current workload rollout with existing claim templates and Pod
   storage wiring. A combined scale-up may wait for the new template settings.
   Normalize the desired Pod template through admission dry-run Update of a
   copy of the existing StatefulSet before comparing it with the live template.
   API defaults, including probe timeout/success threshold, must not create a
   permanent false `WaitingForRollout`.
2. Build a replacement specification in memory that changes the claim settings
   while preserving the previous complete Pod template, replica count and
   selector. Storage-only edits must not request a Pod rollout.
3. Prepare independent source/replacement copies, source UID/generation and
   retained identities under the source/Pod/binding/admission guards. After
   admission preview, record the canonical hash and its association to the
   actual retained Pod hash on the replacement before encoding/digesting.
   Never edit an existing immutable journal to add the association. Encode
   the complete payload once. Compute a domain-separated SHA-256 over Cluster
   UID, namespace, fixed journal base and exact JSON bytes; append its first
   32 hex characters to the base for the ConfigMap name.
4. Before ConfigMap creation, compare-and-swap nil
   `status.claimTemplateRecovery` to Preparing with required `sourceUID`, full
   lowercase `payloadSHA256` and `configMapName`, without `configMapUID`.
   Require the same nonterminating Cluster UID, the prepared generation and no
   other operation. Verify API preservation of the whole intent; stale CRDs
   that prune fields must fail before orphan deletion.
5. Create the immutable Cluster-owned journal with exact encoded bytes.
   `AlreadyExists` can only reuse a direct-read owner/nonterminating/immutable
   journal with the expected full digest after fresh preparation and matching
   durable intent. Never overwrite it. After direct full authentication,
   decode and validate the journal version, source UID and workload/claim
   structure. Require decoded source UID to match protected status before bind.
6. Bind the observed UID by exact Preparing-to-Active compare-and-swap and verify
   persistence. Directly reread the journal and repeat name/owner/nontermination/
   immutability/full-digest authentication against the bound UID before any
   dependencies, deletion or creation. Missing or changed journals stop the pass.
7. Orphan-delete the old StatefulSet, retaining Pods and claims.
8. Recreate from the authenticated snapshot. Verify exact spec/operation identity, ownership,
   nontermination, observed controller generation, all expected Pods' adoption
   and retained PVC/PV identities. Compare-and-swap the exact Active pointer to
   Completed before deleting the journal, then clear only the exact Completed
   pointer. Cover lost cleanup responses. Later workload changes wait for cleanup.

Protected Cluster status supplies authority; a ConfigMap ownerReference, name,
structure or self-declared digest cannot. Recheck active authentication on
every resume before consuming content or deriving dependencies. Required
source/digest/name fields have no retroactive authentication migration because
the entire feature is unpublished. The journal stays at version `1`; Ledger
data, Raft, audit and service protocol versions do not change.

All pointer transitions compare the whole expected previous pointer and use
optimistic resourceVersion. A conflict must trigger fresh observations rather
than refresh-and-overwrite a newer operation. Nil status neither adopts a
standalone ConfigMap nor blocks normal work because of it. Late unbound journals
have no workload authority; different payloads derive different names. Reusing
identical payloads still needs fresh full preparation and durable matching
intent. Unbound journals may remain for Cluster garbage collection, with
cleanup separate from recovery authority.

Preparing means UID absent and `completed: false`. Exercise direct named-journal
`NotFound`: only a valid Preparing pointer, same nonterminating Cluster UID and
expected Cluster-controlled nonterminating source StatefulSet with the saved
UID permit compare-and-swap clear. Readiness, source generation and current CR
validity are irrelevant. Assert no recovery dependencies, old-claim reads,
workload writes, journal deletion or normal continuation in this pass; successful
clear returns handled and a requested two-second requeue, not a guaranteed
execution cadence. Missing, terminating, replaced or foreign sources and uncertain reads fail closed. Race bind against abort:
only one exact-pointer compare-and-swap can win. Deliver a late ConfigMap after
abort and prove it remains unbound.

Failed attempted intent/journal preparation, including response loss, must be
rate-controlled by mutex-protected memory state with one entry per Cluster
namespace/name, containing only its latest UID and full prepared payload digest.
The initial two-second hold increases exponentially to a 30-second cap. Compute
the exact digest, then check the hold before a Preparing condition or new intent
CAS. Repeated watches during the same hold must not change its deadline/attempt,
write intent or churn preparation conditions. Record failed pointer writes once
at the write boundary, including response loss; preparation records other
failures after successful intent. A scheduled wait records no failure. Failed
Completed journal deletion also records a failure. New UID/digest resets the
hold; successful start or Completed-pointer clear removes matching state. Safe
Preparing absence-abort retains the hold; deletion/`NotFound` removes the entry.
Ten minutes
without a failed attempt expires the entry after its retry deadline. There is
no cardinality cap or eviction of active entries; cleanup scans scale with
recently failed Cluster count. Restart loses this rate control, which never
authorizes journal consumption.

Use deterministic time for timing, reset, cleanup and concurrency tests. A
bounded normal-manager envtest must verify real ResourceQuota admission denial
and count Cluster events/status changes and actual intent/clear transitions
during watch traffic. Require bounded retries, no orphan delete while denied
and actual progress after removing the denial. Include
response-loss failures. During a scheduled hold, safe mutable-workload and
volume-protection work with old templates requires no durable attempt in that
pass and the direct APIReader same-UID/nonterminating/nil-pointer fence. Return
a positive `RequeueAfter`, retaining an earlier positive requeue from normal
work, and nil preparation error unless another actual error occurs. Normal availability/status updates can still be valid;
do not claim that the hold suppresses all status writes. Preserve Preparing
absence-abort and every post-attempt continuation fence. This bounded fixture
does not authorize denial/load tests on a user cluster. Concurrent status
writers can race `PreparationFailed` sampling; actual intent and abort
transitions prove the retry bound. Unit tests separately verify condition
preservation and fenced protection. The manager fixture publishes quota usage
and simulates GC/adoption/observed generation explicitly; safe scale-up is
mutable-work evidence, not Raft or StatefulSet-controller evidence.

Absence oracles must check nil protected status plus an actual prefix-filtered
ConfigMap list or recorded ConfigMap Create attempts. A `NotFound` for the old
fixed journal name proves nothing about digest-derived journals. Cleanup uses
the captured real key. Preserve all preflight/destructive-boundary checks,
source UID/full-digest checks and retained PVC/PV assertions.

Completed requires a bound UID. Cleanup checks journal owner/UID and uses
UID/resourceVersion deletion preconditions without reading payload, requiring
immutability or reconciling dependencies. Corrupt bytes must not strand
completed cleanup; a different UID must still fail. A lost delete response can
resume from `NotFound` and clear only the expected Completed pointer. Keep
these controls separate from active missing-journal failure.

Post-delete completion proves controller continuity and must not require
`ReadyReplicas` or `UpdatedReplicas`. Require
`status.observedGeneration >= metadata.generation`. Expected Pods must exist,
be nonterminating and be controlled by the replacement; an unobserved
generation, missing or unadopted Pod still blocks completion. Readiness of the
source before deletion remains required. Kubernetes v1.32 sets
[`ObservedGeneration` before readiness processing](https://github.com/kubernetes/kubernetes/blob/v1.32.0/pkg/controller/statefulset/stateful_set_control.go#L497-L505)
and [attempts to persist returned status despite processing errors](https://github.com/kubernetes/kubernetes/blob/v1.32.0/pkg/controller/statefulset/stateful_set_control.go#L102-L107).
Do not substitute controller observation for workload availability or runtime
authorization evidence.
After recreation, an expected Pod's `NotFound` must report
`ClaimTemplatesReady=False/AwaitingAdoption` with its name and a short requeue,
retaining the journal and pointer. Other read errors remain recovery failures.
The missing Pod must exist, be nonterminating and be controlled by the exact
replacement before completion; this classification does not relax continuity.

Keep these regression triggers independent of the existing response-loss and
pre-delete cancellation cases:

- Drive a storage size/profile edit through the normal envtest manager with
  API-defaulted probes and a settled source. Assert journal creation and the
  UID-fenced orphan-delete request. Calling `startClaimTemplateRecovery`
  directly does not exercise the initiation guard. Envtest cannot complete real
  GC or StatefulSet adoption; any fixture status/owner changes must be disclosed.
- Complete a faithful replacement with an observed generation and all expected
  adopted Pods at zero Ready/Updated replicas. Cover OOM/unready and empty-key
  startup failure states. Persist completion, clean up the journal/pointer,
  then execute a real subsequent normal workload repair/auth-template change.
  Assert final template bytes and stable storage identities; do not claim that
  the repaired workload is running or authorization is effective from this alone.
- Retain recovery state for an unobserved generation and, independently, a
  missing, terminating or unadopted expected Pod. A failed identity/binding
  check must also preserve the journal. Zero readiness is the positive control,
  not the failure oracle for these negative cases.
  For a missing Pod, assert `AwaitingAdoption`, its name and the requeue, then
  restore that ordinal and verify guarded completion and cleanup. Keep
  non-NotFound API errors as separate `RecoveryFailed` regressions.
- Make the defaulted effective PVC class explicit while increasing size or
  selecting a nonempty profile. Assert template progress with the original
  class/access modes and retained PVC/PV identities. Separately request a real
  migration and verify its storage diagnostic without field migration.
- Change live `spec.additionalLabels` during active recovery and assert that
  independent auth-key ConfigMap and cluster-secret writes retain those labels
  without replacing the saved Pod template or cluster-secret token.

Recognize hashes created by the released full-spec algorithm before excluding
PVC size/profile inputs. Preserve the existing Pod hash when only storage has
changed, using the separate StatefulSet workload hash after template recreation.
An actual workload edit must still change the Pod template and roll out.
Trust canonical metadata only when StatefulSet
`ledger.formance.com/pod-config-hash` and installed Pod-template
`ledger.formance.com/spec-hash` are nonempty and StatefulSet metadata
`ledger.formance.com/pod-config-spec-hash` equals that actual installed Pod hash.
Both hash resolution and compatibility preservation must share this rule.
Missing/empty/broken pairs use verified legacy migration and never force a
storage-only rollout merely for losing the association. The association stores
the actual retained hash, including historical full-spec hashes; UID and
generation cannot substitute for it because it must survive recreation.
Limit canonical claim exclusions to WAL/data size, profile, StorageClass and
access mode; retain HostPath wiring and ServiceAccount configuration as rollout
inputs. In particular, adding, replacing or removing ServiceAccount annotations
must still request new Pods even when the other admitted template bytes are
equal. A differing trusted canonical metadata hash must not enter a generic
admission-equality fallback. Cover modern and released sources, with and
without simultaneous storage edits, through both focused unit and normal
manager paths. Verify the rollout and effective admission/runtime inputs in a
real workload separately from template-hash evidence.

Operational guidance must recommend upgrading the CRD and Operator with the
Cluster declaration unchanged, then waiting for the valid association before
storage or ServiceAccount edits. If combined edits require history, temporarily
restore only their authentic previous declared inputs, wait for the pair and
reapply. This route requires known history; it cannot promise recovery from an
unavailable declaration. PVC reconciliation is independent and accepted live
growth must not be reversed. Keep strict proof as the alternative when retaining
the edits. Invalid supplied proof must be corrected or deliberately removed
before the no-proof path; never bypass its validation.
Marker presence alone must not be presented as sufficient. The ServiceAccount
resource may already reflect requested annotations before a migration error;
that error does not undo the write or refresh admission inputs in existing Pods.

After completed recovery, cover canonical configuration A, a released Operator
installing ServiceAccount B/full Pod hash B while retaining unknown metadata,
and then desired A under the new Operator. Without usable history and with
opaque equal admitted templates, assert `HashMigrationRequired`, never silent
certification of B as A. Authentic B proof must request the actual A rollout and
publish a valid pair. Also break the association where exact reconstruction is
possible and prove no added storage-only rollout after a successful StatefulSet
Update preview admits an equal template. An old binary can itself
roll its storage hash; that is separate from the new controller's decision.
Downgrade remains forbidden during active journal recovery. Cover missing,
empty and broken pairs, retained historical hashes and shared trust call sites.

For a released hash with no proof, normalize the desired template with the
installed hash through admission dry-run StatefulSet Update before accepting
exact reconstruction. A differing admitted template must
request the normal canonical-hash rollout for image, configuration and TLS
changes, including an omitted tag supplied by a changed `LEDGER_IMAGE_TAG`.
Equal admitted templates permit automatic preservation of the installed hash
only after exact released-hash reconstruction. A dry-run failure must publish
`ClaimTemplatesReady=False/AdmissionCheckFailed` before canonical metadata or
recovery intent/journal writes. This preview does not execute Pod Create
admission or certify actual IAM/runtime access. The real StatefulSet Update
still passes admission; the full Pod template revision determines rollout.
Preserve sequential RC admission and TLS/storage rollout ordering. Only equal
templates without exact reconstruction leave external admission inputs ambiguous
and require explicit history through the Cluster annotation
`ledger.formance.com/legacy-spec-hash-proof`: one JSON object with `version: 1`,
current StatefulSet `sourceUID`, installed Pod-template `oldSpecHash` and the
complete historical effective `ClusterSpec` in `spec`. Verify exact version,
UID/hash and recomputed released full-spec hash before trusting those inputs.
Do not apply current defaults or infer historical ServiceAccount values from
current resources. The history must reproduce the admitted spec after the
released `applyDefaults`, including that deployment's historical
`LEDGER_IMAGE_REPOSITORY` and `LEDGER_IMAGE_TAG` values. Preserve omissions,
including storage quantities and access modes, where they entered the hash;
rendered claim-template defaults are not historical spec values.
No supplied canonical-hash assertion may replace recomputation.

For valid proof, equal historical/current canonical hashes still require an
admission-normalized equal template to preserve the installed Pod hash. A
nonstorage canonical difference must roll. Missing required history reports
`HashMigrationRequired` without canonical metadata advancement; malformed,
foreign, stale or inconsistent proof reports `HashMigrationInvalid`.
Explicit invalid proof must still fail when the admitted template differs;
the no-proof rollout branch cannot bypass supplied proof validation. Known
exact legacy reconstruction remains automatic after a successful Update preview
admits an equal template. Only a valid canonical/installed-hash association takes
precedence over leftover proof and must still trigger
later real rollouts. Equal installed/desired hash strings alone do not prove
released reconstruction. Cover successful proof cleanup from GitOps and stale
proof after canonical migration, UID replacement and later ServiceAccount edits. Do not
treat a manual hash reset or arbitrary current spec as historical evidence.

Cover genuine released sources with no canonical metadata or proof, including
the original RC and TLS regression fixtures; remove both modern metadata
annotations when constructing them. Use the actual released hash of
the historical effective spec. Request the adjacent RC with both an explicit
tag and an omitted tag whose `LEDGER_IMAGE_TAG` changes; also exercise a
configuration change and a TLS transition. Assert the normal desired template
is written and storage recreation waits for workload convergence. Retain the
equal-template `HashMigrationRequired` counterexample and invalid-proof
rejections. A synthetic canonical marker must not replace the released-source
trigger. Unit and API-server/normal-manager tests establish requested writes,
not real mixed-version Raft communication or completed rollout.

Add recognized released-hash regressions with no canonical association or proof.
Assert that an equal admitted template preserves the installed hash and records
the valid canonical association, while a differing admitted template returns
the desired canonical-hash template. Reject the Update preview and assert
`AdmissionCheckFailed` on the actual Cluster, unchanged StatefulSet template and
hash metadata, no recovery pointer/journal writes and no orphan deletion.
Exercise the real StatefulSet reconciliation caller, including its effective
TLS copy, so a resolver-only assertion cannot certify the error/status and
mutation boundary. Restore admission and prove a real retry succeeds. Retain
opaque equal-template `HashMigrationRequired`, invalid explicit proof with
equal and differing templates, and valid-association precedence regressions.
These checks cover StatefulSet Update admission; they do not establish changes
to Pod Create webhooks, completed Pod rollout or actual IAM/runtime access.

For a released source with valid history, combine a ServiceAccount change and
scale-down. The preparatory StatefulSet update must resolve the hash and commit
both canonical and installed-hash association metadata atomically after final
prepared Pod-template resolution, using the actual retained Pod hash,
retaining the old replica count before the first Raft side effect. Reject that
API write and assert no Raft call. Separately let it commit, fail the first
Raft exec, and retry with a fresh reconciler: the template/valid pair must remain
durable, the original proof must not become `HashMigrationInvalid`, and the
next pass must actually retry Raft before reducing replicas. Preserve claims
and the existing membership postcondition guards. Test a real fail-then-success
sequence and final replica count; a marker assertion alone does not prove Raft
or runtime rollout completion.
Apply the same pair/template atomicity oracle to normal writes and to the
replacement snapshot before marshal/digest, without modifying existing
immutable journals.

Focused source regression anchors are:

- [`canonical_hash_binding_test.go`](../../../misc/operator/internal/controller/canonical_hash_binding_test.go)
  and [`canonical_hash_binding_integration_test.go`](../../../misc/operator/internal/controller/canonical_hash_binding_integration_test.go):
  `TestCanonicalHashBindingReleasedWriterRequiresHistoricalProof` and
  `TestCanonicalHashBindingReleasedWriterThroughManagerIntegration` cover
  A-to-released-B-to-A with strict proof and the resulting valid pair.
- [`claim_template_retry_test.go`](../../../misc/operator/internal/controller/claim_template_retry_test.go):
  deterministic timing/reset/retention and concurrent-worker cases,
  `TestClaimTemplateRetryHoldDoesNotWriteOrChangeConditions` and
  `TestClaimTemplateRetryIntentResponseLossIsRateLimited` cover the memory gate.
- [`claim_template_retry_integration_test.go`](../../../misc/operator/internal/controller/claim_template_retry_integration_test.go):
  `TestClaimTemplateRetryQuotaThroughManagerIntegration` bounds real
  ResourceQuota-denied intent/abort retries, permits mutable scale-up and
  resumes authenticated recovery after quota removal. GC, adoption and
  generation observation are simulated; the shared manager has no Clientset
  for Raft or protection evidence.
- [`storage_workload_test.go`](../../../misc/operator/internal/controller/storage_workload_test.go):
  `TestStorageReleasedHashRCUpgradeExplicitTag`,
  `TestStorageReleasedHashRCUpgradeEnvironmentTag` and
  `TestStorageReleasedHashWorkloadChangesWithoutProof` cover genuine released
  hashes without canonical metadata or proof. The ServiceAccount counterexample
  is `TestStorageReleasedHashServiceAccountChangeStillRequiresProof`.
- [`storage_reconciliation_integration_test.go`](../../../misc/operator/internal/controller/storage_reconciliation_integration_test.go):
  `TestStorageReleasedHashWorkloadRolloutThroughManagerIntegration` and
  `TestStorageReleasedHashServiceAccountRequiresProofThroughManagerIntegration`
  exercise accepted template writes and the equal-template proof gate through
  the registered manager and real API server.
- [`claim_template_recovery_test.go`](../../../misc/operator/internal/controller/claim_template_recovery_test.go):
  `TestClaimTemplateRecoveryScaleDownProofSurvivesPreparatoryWrite` observes the
  valid pair/template before the first Raft call, fails that call, then resumes
  with a fresh reconciler through membership removal and final replica count.

Keep the original released-source triggers in
[`rc_upgrade_test.go`](../../../misc/operator/internal/controller/rc_upgrade_test.go)
and
[`reconcile_tls_rollout_test.go`](../../../misc/operator/internal/controller/reconcile_tls_rollout_test.go)
alongside these additions. Test presence is not an execution result. Fake exec
does not prove Raft connectivity; envtest does not run StatefulSet controllers,
kubelets or CSI.

Recovery intent is operational control-plane state, not business/audit data or
a volume backup. A fresh process must resume it before current-spec validation
or other workload mutations. Cover an absent StatefulSet, changed or invalid
desired spec, and later scale/image/TLS edits; none may replace the saved
workload mid-recovery. Do not generalize this size/profile snapshot guarantee
to the historical hostPath/PVC layout-transition path.

A failed snapshot write must prevent the storage-recreation delete. A snapshot
may coexist with the old StatefulSet after a failed/lost delete response, with
orphaned Pods after a failed creation, or with a replacement during delayed
generation observation/adoption and failed cleanup. Recovery must identify the
observed incarnation before mutating it and retain sufficient intent for the
next process. Include foreign or malformed ConfigMaps, replaced same-name
resources, stale observations, ownership mismatches and cleanup retries. Later desired
scale/image/TLS edits stay deferred during those recovery prefixes.
After preparation failure, safe normal workload/protection progress first
requires proof that no durable status-intent write was attempted in that pass.
The boundary is the first durable status-intent write attempt, before `Create`:
the implementation marks that phase immediately before status persistence and
retains it through journal creation and typed errors with their original cause.
The caller must
return on that marker even if the API currently reports `NotFound`: an earlier
write can still commit after the read. Only errors before that phase may use
direct APIReader proof that the same nonterminating Cluster UID has no recovery
pointer. Standalone ConfigMaps alone must neither authorize nor block safe
continuation. Keep the failed preparation condition and return the
error after those safe attempts. Cover rejected/no-commit status-intent writes,
including failure before any CMCreate, subsequent journal `NotFound`, lost
journal/pointer responses, stale cached absence, uncertain
reads and changed Cluster identity as blocking controls. Prove safe
mutable-workload/protection progress with old templates after an earlier
read/dry-run failure, then a real preparation retry after the prerequisite is
restored.
Before orphan deletion, a changed generation or an unready original
StatefulSet may invalidate the saved plan. Prove the same source UID is still
present without a deletion timestamp, persist the obsolete journal's completed
marker, clean it up, then allow normal workload repair and replanning. Include
a delete precondition rejected once followed by an unready source with unchanged
generation and a real image/configuration repair. Verify this is a durable
fail-then-success path, not an endless `WaitingForRollout` loop. A terminating
source, absent source or replacement UID cannot discard the only recovery
intent; once deletion starts, the snapshot remains authoritative.

Trace the independent recovery-dependency path after trusted journal decoding.
It must derive auth/TLS wiring, replicas and claim names from the saved
StatefulSet while using current Credentials and the current protection toggle.
Newer invalid auth/TLS/replica fields must not redirect those inputs. Projection
must read current Credentials and their referenced Secrets directly from the
API; include stale-cache distribution observations in the evidence. Auth-key
and cluster-secret metadata must retain live `spec.additionalLabels`, while
later desired workload fields stay excluded from the saved dependency inputs.
Auth-key projection must never delete a ConfigMap mounted by the snapshot:
no matching Credentials writes an explicit empty key set without stale revoked entries;
unresolved matching keys keep their fail-safe handling. Cluster-secret
maintenance runs only when required by the saved workload and retains the
existing token. Volume protection uses saved workload geometry and verifies
the journal's PVC/PV UIDs and binding before mutation. UID/resourceVersion
patch preconditions must fence replacement or rebinding after the read. Include
changed-identity and read/patch race cases without touching foreign volumes.
Dependency errors must remain visible without blocking the recovery attempt, and none of
these writes may alter the saved Pod template. Services, NetworkPolicy and
other normal resource updates remain deferred until recovery clears.

Projection and runtime authorization are separate evidence gates. Ed25519 keys
and authorization metadata are loaded once at boot; a refreshed ConfigMap does
not establish revocation, scope reduction or rotation in running Pods. The
`AuthKeysRolloutDeferred=True/StorageRecovery` condition reports that boundary.
Remove it only after the recovery pointer is gone and a normal StatefulSet
pass succeeds. Assert it survives pending auth and failed normal passes;
removal does not certify that all serving Pods completed the resulting rollout.
The current startup loader rejects an empty key set, so no-match projection can
prevent a restarted Pod with saved key-file wiring from starting; it does not
disable authentication. Include this failure prefix in runtime evidence.
The normal rollout waits for recovery and cleanup. An adopted Pod that is
unready after an OOM or empty-key startup failure must not keep that gate closed.
Missing/unadopted Pods, unobserved generation or failed identities still require
recovery before normal repair. Verify the eventual rollout and authorization
on serving Pods separately. Do not claim projection or journal cleanup alone
solves the runtime security gap, or invent an automatic emergency override.

Cover unavailable or dry-run-incompatible admission webhooks, rejected desired
Pod-template normalization/legacy-hash previews and changed replacement
admission output. Normalization and legacy-hash errors must publish
`ClaimTemplatesReady=False/AdmissionCheckFailed` on the actual Cluster even when
TLS uses an effective configuration copy. Preview failures must remain visible
without skipping validation or falling back to a storage-only Pod rollout.
After fixing the prerequisite, prove a real retry converges. A replacement
spec/operation-identity mismatch must stay
`RecoveryFailed` with the journal and storage intact. Operational recovery
inspects the journal/pointer, live spec, ownership/UIDs and retained bindings,
then corrects admission or restores the exact trusted specification where
Kubernetes permits it. Immutable/identity mismatches require an operator-led
recovery; deleting the journal/pointer or claims and forced adoption are not
supported escape paths.

The initial preparation-to-resume handoff must preserve the resume condition.
Inject an orphan-delete or replacement-create failure during that first call,
assert `RecoveryFailed` rather than `PreparationFailed`, retain the bound
journal/pointer and original error through the durable-write fence, then prove
a fresh reconciler retries the same operation successfully. Preparation errors
before handoff still report `PreparationFailed`; retry accounting stays single.
Also fail the initial uncached Cluster refresh after journal creation, before
authentication or binding. It must report `RecoveryFailed`, retain the exact
Preparing pointer and journal, and attempt no dependency or workload mutations.
A fresh reconciler must then bind that journal and progress normally. Install
error handling before the refresh and its identity checks so these early
failures cannot retain `Recreating`.

An optimistic PVC-lock regression must place a larger concurrent request after
the authoritative read and before the patch. Observe the actual stale-patch
conflict and subsequent reread, then assert the larger request, UID and
annotations survive without expansion annotations from the rejected patch.
Keep the separate case where the larger request already exists before the
direct read. Removing the optimistic lock must fail the conflict regression.

Do not equate aggregate `Pending`, `Ready=False` or `0/N` during StatefulSet
absence with actual retained-Pod loss. Inspect the Pods and recovery condition.
Intentional Cluster deletion during the orphan window must account for Pods
outside normal owner-based garbage collection. It is not a recovery action and
does not prove complete workload cleanup. Real GC and adoption evidence require
running Kubernetes controllers; envtest cannot establish either guarantee.

PVC patches are independently durable and may partially succeed before any
template/CSI failure. A spec revert must not shrink accepted storage or imply
cancellation of a profile change. Supported profile rollback selects another
nonempty class whose reverse transition the driver supports. Manual deletion
of the snapshot, PVCs or PVs is not a supported recovery mechanism.

Use fake clients only for call ordering and persisted-object selection. An API
server proves admission and StatefulSet immutability; orphan deletion/adoption
needs running Kubernetes controllers. Real CSI evidence must separately compare
PVC request/capacity, resize conditions, requested/current profile,
`modifyVolumeStatus`, mounted capacity and driver-specific effective profile.
Assert stable PVC UIDs/PV binding and nonempty sentinel data on each intended
volume through interruptions. A Kind fixture without the required CSI driver
does not prove EBS expansion, IOPS/throughput application or filesystem growth.
Report executed checks separately from proposed dynamic scenarios.

## Severity

Use the native P0–P3 scale with demonstrated impact and explicit preconditions:

| Priority | Domain application |
| --- | --- |
| P0 | Established catastrophic data loss/corruption or trust-boundary bypass in a reachable supported transition. Do not infer this from a scary resource deletion alone. |
| P1 | High-impact wrong or duplicated durable effects, lost required recovery state, or sustained cluster unavailability after dependencies recover. |
| P2 | Bounded reconciliation/reliability failure or materially false status/operational guidance with a concrete consumer or recovery consequence. |
| P3 | Minor correctness impact supported by evidence; style, optional refactoring and performance-only suggestions remain excluded. |

P0/P1/P2 require the native concrete-path evidence standard. Unclear acceptance
criteria, absent guarantees or conflicting authoritative sources become audit
questions, not invented defects. Severity reflects the product defect, not
whether this tooling-only manifest PR should merge. First-pass findings remain
hypotheses until independently challenged.

## Ownership and deduplication

One finding represents one root cause and required correction, even if several
controllers, failure prefixes or visible symptoms demonstrate it. Use stable ids
`operator-reconciliation-durability/<root-cause-name>`; avoid line numbers, run
identifiers, severities and changing symptom counts in the suffix. Compare with
other findings and available prior audit artifacts by mechanism and correction,
not merely title. Do not claim backlog deduplication unless that backlog was
actually inspected, and do not publish to Jira as part of this audit.

| Adjacent domain | Boundary |
| --- | --- |
| `configuration-startup-contracts` | Owns static admitted/defaulted Cluster-spec-to-server args/environment value translation, parameter validation and consumer wiring. This domain retains workload selection, repeated reconciliation and durable rollout/rotation transitions. Deduplicate shared symptoms by root cause and required correction. |
| `raft-membership-leadership` | Owns Raft safety and internal membership algorithms. This domain owns the controller's membership postcondition checks and ordering with replicas/PVCs. |
| `persistence-restore-replay` | Owns checkpoint, replay and restore data correctness. This domain owns restore-mode/CLI/volume handoffs and backup Job/result/CR orchestration. |
| `idempotency-retries-partial-failures` | Owns service/API retry semantics. This domain owns reconciliation identity and interpretation of ambiguous Kubernetes/exec/Job outcomes. |
| `concurrency-lifecycle-shutdown` | Owns internal goroutine, cancellation and shutdown ownership. This domain owns persisted control-plane progress across controller replacement. |
| `process-boundary-recovery` | Owns Ledger process exit and reopening local durable state. This domain owns which workload, configuration and PVC the operator selects or recreates. |
| `test-reachability-enforcement` | Owns test discovery and CI enforcement. Test gaps here support a concrete runtime finding; they are not separate coverage-only findings. |

Follow calls outside manifest paths when necessary to prove a control-plane
failure, loading only the relevant subsystem contract. If the root cause is
entirely within an adjacent domain, record the boundary in inspected areas or
residual risk instead of issuing a duplicate finding here. Independent
controller and service defects may remain separate when they require distinct
corrections; explain their dependency.

## Contract limits

- Read current CRDs, API comments and implementation alongside the operator
  README and deployment docs. Historical deployment snippets can name an old CR
  or chart path. Contradicted documentation does not prove current behavior;
  record material drift and resolve intent before assigning a product defect.
- `Cluster.spec.restore` selects a server mode. It does not imply a Restore CR
  or automatic download/finalize transaction. Identify the actor for each step.
- PVC minimum/profile reconciliation is in scope, including existing claims,
  automatic-growth coexistence and durable size/profile template recovery.
  A size field or accepted profile patch alone does not promise completion on
  every storage driver or permit shrinking/replacement. Distinguish unsupported
  prerequisites from false success or failed recovery after they are restored.
- BackupRun history limits retain Kubernetes execution records, not S3 objects.
  Explicit volume-retention choices, admission exemptions and intermediate TLS
  modes must be assessed against their actual contracts.
- Rollback means compensating an interrupted operation or reverting a supported
  desired-state edit. Upgrades from `v3.0.0-rc.1` onward must preserve data;
  safe arbitrary binary downgrade is outside scope.
- Dynamic scenarios in the manifest are optional evidence strategies, not
  authorization to mutate an external cluster or add a reproducer during an
  audit. The native audit and challenge remain read-only. Report diagnostics
  actually executed separately from proposed reproductions.

### Pyroscope reference lifecycle (EN-2061)

`authTokenFrom` and `basicAuthPasswordFrom` refer to externally managed Secrets
in the Cluster namespace. Reference edits change the desired Pod template and
trigger the existing rollout mechanism. In-place Secret content rotation requires
an explicit Pod restart; the operator does not read or own these credentials.
Missing referenced Secrets/keys prevent container startup. Evaluate transitions
against that contract rather than assuming automatic content-based rollouts.
Credential-byte disclosure is owned by `sensitive-data-exposure-boundaries`.

## Execution and publication

After reviewing the manifest, commit it with a clean worktree. The trusted outer
workflow runs:

```bash
bash scripts/ai-audit operator-reconciliation-durability
bash scripts/ai-audit-challenge <audit-result.json>
```

Keep the same exact HEAD for both passes. Raw and qualified artifacts stay under
ignored `build/ai-audit/`; they are separate from the manifest PR. Inspect the
qualified report before reporting confirmed counts. Jira publication requires
separate explicit authorization and is not part of either pass.
