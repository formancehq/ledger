# Configuration startup contracts audit

This domain follows a server parameter from supported input to the component
that consumes it. Its reusable delta is value semantics: provenance, precedence,
parsing, defaults, validation, wiring and local versus replicated authority.
These can fail on an ordinary first startup without process death, restart or
reconciliation retries. Extending `process-boundary-recovery` would conflate
that chain with the independent evidence needed for a real OS process boundary.

## Evidence contract

For each candidate, establish the complete chain:

1. A supported CLI/environment input or an admitted/defaulted Cluster spec.
2. The selected source, parser, explicit/unset state and normalization order.
3. The validator and the specific documented constraint or consumer precondition.
4. The bootstrap constructor/options and actual component that use the value.
5. A concrete incorrect result and a feasible regression plan, including the
   existing protection or test that does not cover this path.

The manifest paths are entry points, not permission for a general audit of all
code in those directories. Follow additional callees only to establish this
chain, loading their authoritative subsystem contract. Inspect `main.go` and
resolve the exact go-libs version from `go.mod`/`go.sum` when following binding;
a test calling `LoadConfig` directly cannot prove production environment binding.
Environment-only values need their own precedence evidence. There is no implied
server configuration-file layer or universal precedence between every CR field,
`extraEnv`, shell argument and direct environment read.

The contract anchors are deployment's Configuration and Configuration Safety
Checks sections; `cmd/server` flag comments and sentinel/default tests;
`bootstrap.Config.Validate`, delegated consumer validators and persisted-config
checks; current operator API/CRD/defaulting and `buildEnvVars`; and
`deterministic-fsm.md` sections 3.4–3.6. Different defaults are not defects merely
because they differ: prove a supported input violates its intended meaning.

Keep the existing unsafe identity/TTL override distinct from non-bypassable
schema rejection and the explicit restore-mode branch. Trace in-memory mutation,
write order and residual persisted bytes on each error; validation rejection does
not imply no prior writes. Existing historical backfill branches are evidence to
inspect, not authorization to add v3 compatibility shims or migrations.

TTL and the query-checkpoint limit currently use committed `ClusterPolicy`;
node-local flags describe desired values. Cache/bloom settings also have a
replicated `ClusterConfig` handoff. The older admission-only checkpoint-limit
example in section 3.4 must be read alongside the specific section 3.6 policy
contract and current code. Historical deployment snippets likewise may use an
old CR shape. Conflicting sources, unspecified empty/zero semantics, or an
unclear strict-versus-soft limit become audit questions, not asserted defects.

## Ownership and exclusions

| Domain | Ownership |
| --- | --- |
| `configuration-startup-contracts` | Static parameter resolution/translation, validation predicate, consumer wiring and desired-versus-applied authority. |
| `process-boundary-recovery` | Real exec, signal delivery, exit status, listener/readiness sequencing, hard death, local durable reopen and residual-state recovery. Incorrect value resolution/validation belongs to `configuration-startup-contracts`; correct rejection hidden by process exit/readiness belongs to `process-boundary-recovery`. |
| `operator-reconciliation-durability` | Selection/recreation of workloads, TLS/credential rotations, retries, rollout and durable CR/Job/Secret/PVC transitions. Static admitted-spec-to-args/env value translation belongs to `configuration-startup-contracts`; temporal convergence belongs to `operator-reconciliation-durability`. |
| `concurrency-lifecycle-shutdown` | Goroutine ownership, cancellation, Stop/join and startup unwind resource ordering. |
| `persistence-restore-replay` | Generic durability, checkpoints, restore data correctness and replay. This domain only traces configuration authority and validation; it does not prove crash recovery. |
| `raft-membership-leadership` | Raft/membership/leadership algorithms. This domain checks parameter delivery to those components. |
| Other accounting/read/idempotency/test domains | Business semantics, query correctness, retry outcomes, test discovery and CI enforcement. A test gap supports a configuration finding rather than a separate coverage-only finding. |

Exclude standalone `ledgerctl` profiles/keychain precedence, generic auth/security
review, performance tuning, new dependencies, compatibility designs and the
operator work tracked as EN-1999 through EN-2002. Auth/TLS configuration value
validation is in scope; authorization algorithms and rotation workflows are not.
Do not turn a broad path into a second audit of those neighboring mechanisms.

Deduplicate by root cause and required correction, even when a parameter causes
both startup and later runtime symptoms. Use stable finding ids
`configuration-startup-contracts/<root-cause-name>`. When the correction belongs
entirely to a neighbor, record the boundary in inspected areas or residual risk
instead of issuing a duplicate. Compare available prior artifacts and active PRs;
do not claim backlog deduplication without actually inspecting that backlog.

## Execution boundary

The dynamic checklist contains optional existing tests and proposed reproductions.
Report which checks actually ran. Use the pinned Nix toolchain, native temporary
fixtures and separate root/operator modules. Environment-mutating cases require
isolation; server fixtures use leased listeners and bounded lifecycle cleanup.
No live cluster mutation, tracked test additions or product edits occur during
the read-only audit. The manifest uses the existing native JSON contract without
new launcher fields or dependencies.

After manifest review/merge, the trusted outer workflow can separately run
`bash scripts/ai-audit configuration-startup-contracts`, then
`bash scripts/ai-audit-challenge <audit-result.json>` at the same clean HEAD.
Provider workers remain leaves. First-pass findings remain hypotheses until
challenged; Jira preview/publication and product fixes require separate tasks
and explicit authorization. Creating this manifest runs neither audit campaign.
