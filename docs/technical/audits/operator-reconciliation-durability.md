# Operator reconciliation durability evidence contract

The [manifest](operator-reconciliation-durability.json) defines a reusable audit
of the Ledger Kubernetes control plane in `misc/operator`. The operational need
is to establish whether repeated or interrupted reconciliation preserves the
intended cluster, credentials, volumes and backup/restore progress. Existing
Raft, persistence and lifecycle domains do not own the Kubernetes transition
between those effects and the CR state that drives the next reconciliation.
This domain adds scope and evidence requirements to the existing native workflow;
it adds no runner, product behavior or deployment guarantee.

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
- PVC growth is in scope, including existing claims and orphan/adoption windows.
  A size field alone does not promise live expansion on every storage driver or
  permit shrinking. Unclear support is a question; a demonstrated destructive
  transition still qualifies as a finding.
- BackupRun history limits retain Kubernetes execution records, not S3 objects.
  Explicit volume-retention choices, admission exemptions and intermediate TLS
  modes must be assessed against their actual contracts.
- Rollback means compensating an interrupted operation or reverting a supported
  desired-state edit. It does not require wire/storage compatibility between
  unreleased v3 revisions or safe arbitrary binary downgrade.
- Dynamic scenarios in the manifest are optional evidence strategies, not
  authorization to mutate an external cluster or add a reproducer during an
  audit. The native audit and challenge remain read-only. Report diagnostics
  actually executed separately from proposed reproductions.

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
