# Integrity verifier evidence and ownership

The [native manifest](integrity-verifier-soundness.json) audits whether the
checker detects corruption within its promised scope and preserves the evidence
through its consumers. This is a reusable verifier contract: the question is
whether a corrupted projection can pass verification, not whether every possible
transaction or restore operation is correct. It introduces no runner, schema,
product guarantee or authorization to run an audit.

## Evidence contract

For each candidate record the exact audited SHA, production entry point, primary
snapshot and optional peer snapshot, persisted keyspace/field, corruption,
authoritative witness, verification pass and interval, comparison direction,
callback events, terminal error and caller-visible outcome. Establish why this
state is within the promised scope using [AGENTS.md](../../../AGENTS.md), the
[checker documentation](../architecture/subsystems/checker/) and
[Audit-Bound vs Technical State](../architecture/audit-vs-technical-state.md).
A missing capability whose intended semantics remain unclear is an audit
question, not a defect inferred from a preferred design.

A post-persistence corruption fixture need not be producible through admitted
commands: detecting such corruption is the verifier's purpose. Its initial
healthy state and the claimed consumer path must nevertheless be realistic.
Distinguish a corrupt projection with intact evidence from an attacker replacing
both truth and projection. The ClusterID-derived hash key is not an independent
secret or external anchor. Do not invent detection guarantees for coordinated
history truncation or complete history replacement without retained evidence.

P0/P1/P2 require the concrete failure path and impact prescribed by the native
[audit contract](../contributing/ai-audit.md). Each candidate remains a hypothesis
until the independent [challenge](../contributing/ai-audit-challenge.md) at the
same SHA tries to disprove it. Preserve stable finding identities and keep one
root cause across multiple affected passes or callers.

## Production source map

| Boundary | Sources and required evidence |
| --- | --- |
| Pass selection and verdict | `internal/application/check/checker.go`: trace `Check`, every comparison and early exit. Inventory current calls, not a frozen count from the documentation table. `Check` returns execution errors and emits corruption through its callback; progress and a nil return are not alone a healthy verdict. |
| Source chain and bounds | `verifyAuditHashChain`, `newProposalBoundaryReader`, `internal/query/audit.go`, `log.go`, `applied_proposal.go`, `internal/infra/state/audit_envelope.go`, `batch.go`, `machine.go`: identify entry/item/log/proposal key ranges and cardinalities, hash inputs and the verified source of replay inputs. Include first/last inclusivity and iterator terminal errors. |
| Reconstruction | `checker.go`, `replay_store.go`, `internal/domain/replay/`, `internal/domain/processing/processor*.go`: establish order-to-log-to-projection provenance. Inspect producer code to establish semantics and correlated defects, without treating producer agreement as an independent oracle. |
| Projection coverage | Inventory volumes, metadata, transactions and post-commit volumes, reversions, exclusions, idempotency outcomes, indexes, mirror position, numscript content/latest pointers, ledger presence/schema/account types/boundaries/references, signing, cluster policy and query checkpoints from actual calls. Map each field to expected-source and stored-row scans, including deletion and unmatched rows. This discovery list must be refreshed on the audited SHA. |
| Storage boundary | `internal/storage/dal/`, `internal/infra/attributes/`, `internal/storage/readstore/`, `reverse_map_orphans.go`: primary snapshot and cursor semantics; only the documented reverse-map exception reads peer data for a verdict. Broad globs are counterevidence sources, not a general storage or index audit. |
| Result consumption | `internal/adapter/grpc/server_bucket.go` (`CheckStore`), `server_restore.go` (`CheckRestore`), `cmd/ledgerctl/store/check.go`, `bootstrap.go`: trace events, transport errors and final decisions. A staged store has no peer readstore; it cannot certify peer contents. Restore correctness itself belongs to the persistence domain. |
| Existing evidence | `internal/application/check/*_test.go`, `internal/domain/replay/*_test.go`, relevant `tests/e2e/business/` and `tests/e2e/cluster/`: record assertions actually reached, not test names or line coverage as proof. |

At the discovery base, `Check` continues other work after some chain-break
callbacks while signing and policy mark their folds incomplete. The prose that
a chain break stops all downstream work must not replace inspection of this
control flow. Likewise the documented pass table omits several implemented
comparators. These are interpretation cautions, not product findings created by
this manifest.

## Scope refinements

- Primary-store coverage is the floor of invariant #8. Classify each exemption
  with its actual informational role or wired discard/rebuild lifecycle.
  Rebuildable in principle, replicated, or byte-equal across nodes is not proof.
  Compare map-bearing projections logically; do not require byte identity.
- Persisted bloom blocks are rebuilt on backup/restore and bloom configuration
  changes, but loaded on normal restart. That lifecycle distinction preserves
  their documented restart integrity gap rather than granting a blanket cache
  exemption. Prepared queries, maintenance mode and the cluster-config hash
  algorithm are also known gaps, not approved exemptions.
- The reverse-map pass checks presence/key validity against registered indexes
  and audited ledger liveness, not row values. It pins the peer before the main
  snapshot. Aligned cursors permit oracle-dependent verdicts; lag or no peer
  prevents full verification, malformed keys need no oracle, and ahead is
  reported according to the current contract. Nonzero concurrent versions are
  valid during rewrites. `IndexVersionState`, other peer index contents and
  `usagestore` remain outside main-store checking.
- Deletion and retention qualify comparisons. In particular, idempotency
  eviction is permitted and an audit record does not alone prove its outcome
  row must still exist. Establish the active retention contract before proposing
  a missing-row check; refresh EN-1827 work. Signing state has no TTL and must
  fold its complete supported history, including proposal-local cascade rules.
- Inspect empty/failure-only histories and maximal bounds, but do not reintroduce
  chapters, cold-store baselines, archival windows or pre-v3 compatibility that
  were removed. A partial fold can emit a mismatch/incomplete signal and stop;
  the contract does not demand every corruption be enumerated after failure.

## Ownership and deduplication

Choose the owner by the missing protection and required correction. Shared
files and one corrupted store do not justify duplicate issues.

| Owner | Boundary |
| --- | --- |
| `integrity-verifier-soundness` | Missing or unsound verifier pass/field/range, unchecked source dependency, incomplete/error signal lost within checker-specific consumption. A faulty producer can be counterevidence or a trigger, but the finding must establish an independently missing checker protection. |
| `accounting-invariants` | Business outcomes, conservation, posting/amount semantics and deterministic application. If producer and checker share a defect, this domain owns only a distinct detection gap with an independent witness; cross-reference the accounting root cause rather than cloning it. |
| `persistence-restore-replay` | Durability, snapshots, backup/delta reconstruction and restore parity, including corruption introduced there. Here a restored store is only an input to the verifier; a restore loss already owned elsewhere remains there. |
| `read-consistency-projections` | Query/index content correctness, GC and per-replica detect/drop/rebuild. Only the existing reverse-map checker exception belongs here; do not expand it to full peer validation. |
| `idempotency-retries-partial-failures` | Outcome retention/reuse and retry semantics. This domain checks the applicable frozen-outcome verification, not a new TTL policy. |
| `api-boundary-contracts` | Generic RPC/CLI adaptation and transport error handling. Checker-specific loss of the distinction between mismatch, incomplete and healthy is examined here, but a shared adapter defect remains API-owned. |
| `authentication-authorization-boundaries`, `configuration-startup-contracts` | Access, redaction, secret handling, configuration precedence and persisted identity validation. Here signing/policy rows and hash inputs are verifier sources, not a new authorization or startup audit. |
| `concurrency-lifecycle-shutdown`, `process-boundary-recovery`, `raft-membership-leadership` | General cancellation/lifetime, process recovery and consensus safety. Snapshot ordering or cancellation matters here only for a concrete verification verdict; do not launch those broader campaigns. |
| `test-reachability-enforcement` | Test collection and CI enforcement. A verifier test's actual assertion is local evidence here, not a new repository-wide test audit. |

### Discovery snapshot and refresh

Discovery on 2026-09-11 used remote `release/v3.0` commit
`12f48ef84e7a82c2a9e96784dd069bd9bf320f36`. This is provenance, not the future audit
target. Before a later audit the trusted outer workflow refreshes PRs, tickets,
current manifests and available qualified results and supplies that context to
the provider. A provider remains a read-only leaf and never starts audit,
challenge or Jira orchestration itself.

- [PR #1912](https://github.com/formancehq/ledger/pull/1912), EN-1526, was open:
  audit-only histories, a log-value zero sequence bypass, deleted log tails,
  audit-derived log bounds and incomplete verification handling are existing
  work. Read its current diff and regression fixtures, not just its initial body,
  before treating a related hypothesis as novel. Earlier PRs #1666 and #1719
  are historical versions of the same work, not additional findings.
- [PR #1884](https://github.com/formancehq/ledger/pull/1884), EN-1945, was merged:
  chapters/cold storage were removed. Historical archival checker fixes such as
  #1854 and EN-1903/#1828 are not evidence of a current reachable archival path.
- EN-1514 / EN-1323 own the documented peer index-content integrity gap.
  EN-1458 reverse-map checking, EN-1515 signing checking and EN-1550 mirror
  boundary checking are existing protections; EN-1513 removed the duplicate
  durable mirror cursor. Do not convert these historical defects into new ones.
- Prepared-query, persisted-bloom restart, maintenance-mode and cluster-config
  hash-algorithm gaps are already described in Audit-Bound vs Technical State.
  Transaction log `inserted_at` / `updated_at` verification is also a documented
  gap associated with EN-1854's source-date change. These are deduplication
  subjects even when the accessible documentation does not name a unique ticket;
  do not invent an issue ID or silently recategorize them as exemptions.
- [PR #1907](https://github.com/formancehq/ledger/pull/1907), EN-1827, was open
  for frozen idempotency expiry. Refresh its actual status and scope before
  auditing retained-outcome windows. EN-1860/#1926 sequence exhaustion checks
  were merged and are existing counterevidence for boundary hypotheses.

For each overlap record the existing ticket/PR/finding identity and whether the
candidate is the same root cause, a regression of its protection, or a distinct
mechanism. A regression needs current evidence; a new surface alone is not a
new defect. If external records are unavailable, say so and leave novelty
unresolved instead of asserting a fresh finding.

## Falsifiable diagnostics and proof limits

Start with a healthy fixture through production apply, retain its audit witness,
and specify a controlled corruption on an isolated copy. Observe the typed
finding (including identity/aggregation where relevant), method error and final
consumer outcome. Run a matched healthy control. A crash, fixture setup error or
unrelated mismatch is not proof of the intended detector. Pairwise projection
corruption needs an oracle independent of both values; producer/checker equality
through a shared helper alone does not supply one.

The manifest's suggested probes cover missing/extra/changed/malformed rows,
range boundaries, failed and mixed proposals, incomplete folds and error
propagation after a valid prefix. They are proposals for a separately authorized
audit, not executed evidence. Providers may run existing focused tests or
permitted temporary diagnostics under native read-only rules; tracked
reproducers and product fixes belong to a later engineering task. Use pinned
Nix tools and repository-managed shared caches, isolated stores and deterministic
synchronization. Do not mutate live stores or bypass gates. Record selected
tests, skips, infrastructure requirements and the precise assertion reached.

For this manifest-only PR, validate the JSON with the existing `scripts/ai-audit`
manifest predicate and check every scope glob and related-document path, without
invoking its provider. Run `bash scripts/agent-check` and
`AI_REVIEW_BASE_SHA=<exact-base-sha> bash scripts/agent-check-pr`. These validate
the artifact and candidate; they do not establish checker soundness. Report the
actual commands/results in the PR separately from these proposed probes. No
product audit or Jira publication is authorized by adding these files.
