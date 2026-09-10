# Authentication and authorization audit evidence contract

The [manifest](authentication-authorization-boundaries.json) defines a reusable
inspection of who may enter each API operation and whose identity is captured
when a mutation is admitted. The API boundary domain explicitly excludes auth
internals; configuration and operator domains cover value wiring and rollout,
not the resulting per-request authorization decision. This domain fills that
gap using the existing native runner and schema.

## Preparation and execution gate

This change prepares the domain only. **Do not run the product audit as part of
manifest preparation or merely because this PR merges.** Wait for the active
auth work to stabilize, refresh the source inventory and deduplication snapshot,
and obtain a separate decision to execute. This is an operational instruction;
the existing runner does not implement a new manifest scheduling field or gate.

At that later point the trusted outer workflow selects an exact clean HEAD,
reads [ai-audit](../contributing/ai-audit.md), runs the manifest-scoped audit,
then independently qualifies its raw report at the same HEAD under
[ai-audit-challenge](../contributing/ai-audit-challenge.md). Providers remain leaf
workers and never invoke launchers themselves. Publication to Jira, reproducer
implementation and product fixes remain separate authorized actions. No finding
is confirmed merely because the first pass emitted it.

## Evidence required

Each hypothesis must establish this complete chain:

1. A registered method/path or fully qualified RPC and its actual server mode.
2. The supplied credential, anonymous mapping, trust source and effective scopes.
3. The installed guard and any request-dependent scope selection, including
   when a streaming request is first decoded and when protected work begins.
4. The established expected decision and a source for it, including exemptions.
5. The downstream call or attribution capture actually reached and the concrete
   unauthorized effect, protected read, wrong denial or lost/spoofed attribution.
6. A reproduction plan with exact input, guard crossed, distinguishing response
   and backend observation, plus the existing guard/test that fails to cover it.

Pair failures with an authorized control using otherwise valid input. A 401/403
from an unrelated layer, a protocol-version rejection, a nonexistent ledger or
a stream that never receives its first message does not prove the intended guard.
No backend call after an auth rejection is narrower than rollback after an
accepted request; cancellation and ambiguous acknowledgements do not prove
noncommit. Helper-only synthetic contexts do not prove wire reachability.

Use the native severity/confidence standard. P0/P1/P2 require concrete evidence,
not a missing test, a missing interceptor or generic security guidance. If the
policy is absent or authoritative sources conflict, retain an **audit question**.
A valid scope name does not imply an ACL for each ledger, a tenant model, a
mandatory JWT subject, a revocation service or a refresh-token contract.

The broad adapter globs locate registrations, implementations and fixtures;
they do not authorize a general security review of every endpoint. Follow
outside callers only far enough to prove or disprove an in-scope mechanism,
loading the routed documentation. Resolve library behavior from the version in
`go.mod`/`go.sum` rather than assuming JWT helper semantics from their names.

## Source-backed contracts

These anchors describe the preparation base. Reconcile current code, tests and
authoritative docs at the later audited SHA. In particular, historical line
numbers, algorithm/source descriptions and RPC counts in `auth.md` or tickets
are not an inventory of the current executable surface.

| Contract | Established rule and source anchors | Limits |
| --- | --- | --- |
| A — Installed coverage | `api/auth.md` Authorization enforcement; `internal/adapter/http/handler.go` registration and middleware; `internal/adapter/grpc/server_bucket.go`, `server_cluster.go` and generated service descriptors; `internal/bootstrap/module.go` service registration. Walk both unary and streaming methods, including dynamic index scopes. | No global auth interceptor exists on the preparation base. Its absence is not itself a defect; guards are manual. Do not freeze a method count or require EN-1955 annotations before they land. |
| B — Credential states | `api/auth.md` Wire shape, Error mapping, Anonymous access and Dev-mode bypass; `grpc_auth.go:Authenticate`, `http_middleware.go:HTTPAuthMiddleware/RequireScope`, `scopes.go`. | Configured anonymous writes and auth disabled are deliberate modes. Non-Bearer headers currently count as absent. HTTP public bypasses include `/health`, `/livez`, `/readyz`, `/_info`; `/clusterz` and pprof need their own route/handler inspection. gRPC Discovery, health and reflection must be inventoried independently. |
| C — JWT trust | `grpc_auth.go:validateToken`, `composite_keyset.go`, `ed25519_keys.go` and their tests; `api/auth.md` Token validation and OIDC discovery. Signature/expiration, issuer on the applicable OIDC path and configured static-key scope/god restrictions precede privilege grants. | EN-1926 supplies an approved audience decision but its PR is not on this base. Do not assume the existing algorithm-based source selection is the intended final contract or require an audience for deployment-dedicated static keys. |
| D — Batch scopes | `request_scope.go:RequiredScopeForRequest`, `request_scope_exhaustiveness_test.go`, `http/handlers_bulk.go:serveBulk` and `grpc/server_bucket.go:Apply`; `admission/signing.md` distinguishes JWT auth from batch signatures. | Check every known Request and nested LedgerRequest variant. The unknown/malformed fallback requires OpsWrite and still reaches ordinary validation; it is not an unconditional reject. An unparsable signed payload defers to authoritative signature verification, so a failed peek alone does not prove bypass. |
| E — Client/internal boundary | `grpc_auth.go:Authenticate` nonempty cluster-secret fast path; `grpc/server_bucket.go:adoptForwardedSnapshotIfTrusted`; `grpc/raft_auth.go` and `bootstrap/module.go`; `bootstrap/module_restore.go` and `api/grpc-api.md` restore lifetime. | A supplied snapshot is rejected unless the context is cluster-internal, including when auth is disabled. HTTP has no cluster-secret fast path. Raft is separately secret-protected with its documented empty-secret mode; when the secret is configured, its health RPCs are protected too (`raft_auth_test.go`). Service health/protocol exemptions do not bypass Raft authentication. Restore has a separate service set, is intentionally not JWT-authenticated, and defaults to loopback; do not assert it can never be configured otherwise. |
| F — Caller capture | `api/auth.md` Caller identity; `grpc/client_bucket.go:Apply` captures the follower snapshot, `server_bucket.go` adopts it, `auth/caller_snapshot.go:ResolveCallerSnapshot` selects system actor, forwarded snapshot, then local claims, and `application/admission/admission.go` attaches it to the proposal. `commands/system_caller.go` names system principals. | Current snapshots can be nil without claims, including permitted anonymous/auth-disabled requests. Preserve established authenticated context rather than inventing a total principal union or universal rejection of absent forwarded attribution. Those stronger guarantees belong to EN-1955. Attribute according to verified current source rules; documentation prose alone does not prove which source tag code selects. |
| G — Frozen attribution | `auth/caller_snapshot.go:buildCallerSnapshot` forbids re-deriving permissions from the snapshot; `api/auth.md` says it is not re-evaluated downstream; `checker/audit-chain.md` binds caller data; `fsm/deterministic-fsm.md` section 3.4 forbids node-local auth decisions in committed apply. | Trace the proposal and hash-binding seam only. Generic hash/checker completeness, persisted encoding and cross-cluster restore parity have other owners. Token expiry after admission is not a requirement to invalidate committed work. |

Paths in the manifest exist at preparation time. New policy/attribution packages
or changed response types introduced by the pending PRs must be discovered and
reflected in the manifest before the later audit, not silently assumed present.
The Raft-plane inventory includes `rafttransportpb`, `snapshotpb` and
`clusterbootstrappb` descriptors and their source protos. Bootstrap registers
these services on `RaftServer`; `internal/infra/node/transport.go` implements
the transport service, while snapshot/bootstrap implementations are already
covered by `internal/adapter/grpc/**`. Inspect these paths only for registration,
unary/streaming coverage and the installed authentication boundary, not transport
algorithms, snapshot correctness or membership semantics. Health/reflection
descriptors supplied by dependencies still require separate inventory from the
actual server registration and pinned dependency versions.

Tests under `tests/e2e/cluster/` are supporting authentication/forwarding evidence,
not authorization for a repository-wide E2E or lifecycle campaign.

## Ownership and exclusions

Deduplicate by **root cause and required correction**, not endpoint count,
transport, symptom or ticket title. Use stable finding ids
`authentication-authorization-boundaries/<root-cause-name>`. If a correction is
entirely owned elsewhere, record that boundary in inspected areas/residual risk
instead of emitting a duplicate. Independent mechanisms may remain separate
only with evidence of their distinct corrections.

| Neighbor or workstream | Boundary |
| --- | --- |
| `api-boundary-contracts` | Owns decoding, representation, public error sanitization, protocol revision, generic context/cancellation and stream framing. This domain owns auth decisions and loss/spoofing of authenticated caller context; wire status is evidence, not a second error-format finding. |
| `configuration-startup-contracts` | Owns auth/TLS input provenance, defaults, startup predicates and consumer wiring. This domain starts from an established effective auth configuration and checks per-request validation/authorization. A missing audience value caused solely by flag/env wiring belongs there; incorrect token acceptance despite a correctly wired policy belongs here. |
| `operator-reconciliation-durability` | Owns Credentials/Secret distribution, key rotation, TLS transitions and repeated rollout convergence. Do not audit Kubernetes RBAC, secret storage or reconciliation here. |
| `accounting-invariants` | Owns business effects, balancing, amounts and deterministic business processing. This domain uses protected dispatch as an auth observation and does not re-prove accounting. |
| `idempotency-retries-partial-failures` | Owns batch atomicity, retry identity and commit/acknowledgement ambiguity. Per-element scope enforcement belongs here; rejected authorization must not be confused with rollback after acceptance. |
| `persistence-restore-replay` and checker work | Own audit encoding/integrity, replay, storage and restore completeness. This domain follows which caller reaches audit capture. A captured correct caller later lost by restore is a persistence cause. |
| `concurrency-lifecycle-shutdown`, `process-boundary-recovery`, `raft-membership-leadership` | Own resource lifetime, hard restart, consensus, quorum and routing algorithms. This domain checks credentials at their exposed seams, without re-auditing those mechanisms. |
| `read-consistency-projections`, `test-reachability-enforcement` | Own query visibility/consistency and repository-wide test enforcement. An auth test gap supports a demonstrated auth mechanism; it is not another test-wiring finding. |
| Event delivery and mirror ingestion domains prepared in parallel | Own sink delivery and source ingestion semantics. Remote-provider credentials, driver behavior, acknowledgements, checkpoints and retries are excluded here. This domain checks who may configure/read those resources only under established scope contracts. |
| EN-1632/1634/1635 credential projections | Public secret redaction is a distinct confidentiality workstream, not implicit authorization policy. Known sink/mirror/audit/log disclosure causes and their typed projection/storage changes are excluded from this campaign. Follow response boundaries only to classify overlap; no general secret scan, redaction redesign or new access-control requirement. |

Batch signature cryptography, signing-key lifecycle, cryptographic primitive
analysis, generic dependency vulnerabilities, CLI keychain/profile storage,
provider availability, denial-of-service budgets and speculative hardening are
excluded. Follow signature verification only to establish the existing signed
batch precedence or trusted system-actor exception. Do not assume equal HTTP,
service gRPC, Raft and restore policies merely because they share helpers.

## Deduplication snapshot to refresh

Preparation inspected Jira status/descriptions and current GitHub PR metadata on
**2026-09-10**, against `release/v3.0` base
`f76499637a5af0d92a2cfabbdacf8247256fa4bb`. The existing manifest inventory and a
GitHub search found no equivalent authentication/authorization domain. This is
not a claim of exhaustive historical backlog or prior artifact coverage.

| Existing work | Observed state and handling |
| --- | --- |
| [EN-1926](https://formance-team.atlassian.net/browse/EN-1926), [PR #1962](https://github.com/formancehq/ledger/pull/1962) | Jira in review; PR open. Approved per-deployment OIDC audience, string/array matching and static-key exception based on successful static verification. OIDC Ed25519 tokens still require issuer/audience checks; algorithm or colliding `kid` alone cannot grant the exception. Requalify the landed implementation later, without reopening the known missing-audience or source-selection cause. |
| [EN-1417](https://formance-team.atlassian.net/browse/EN-1417) | Jira in review. Typed auth state and shared batch authorization are refactor work, not current mandatory APIs. Ticket/auth-title PR searches did not identify a dedicated active implementation PR; refresh rather than interpreting status as proof of delivery. [PR #1454](https://github.com/formancehq/ledger/pull/1454) is the merged EN-1250 bulk scope repair; [PR #1642](https://github.com/formancehq/ledger/pull/1642) is the merged EN-1506 Request scope classification repair. |
| [EN-1954](https://formance-team.atlassian.net/browse/EN-1954), [PR #1952](https://github.com/formancehq/ledger/pull/1952) | Jira in review; PR open. Removes five dedicated audited mutation RPCs in favor of Apply. Inventory whichever methods actually remain at the audited SHA. [PR #1888](https://github.com/formancehq/ledger/pull/1888) already merged the EN-1950 authenticated-context repair on those methods. |
| [EN-1955](https://formance-team.atlassian.net/browse/EN-1955) | Jira backlog; searched PR results did not identify a standalone implementation PR. Proposed exhaustive policy annotations/interceptors, a total principal representation and pre-proposal attribution rejection are future work dependent on EN-1954. Do not turn their absence or known footguns into new findings. Reconcile the ticket's dependency intent with the actual delivered stack before execution. |
| [EN-1632](https://formance-team.atlassian.net/browse/EN-1632), [EN-1634](https://formance-team.atlassian.net/browse/EN-1634), [EN-1635](https://formance-team.atlassian.net/browse/EN-1635) | All in review. Open stack [#1977](https://github.com/formancehq/ledger/pull/1977) → [#1979](https://github.com/formancehq/ledger/pull/1979) → [#1970](https://github.com/formancehq/ledger/pull/1970) covers sensitive projection, structured operational credentials and public audit views. Alternative [#1963](https://github.com/formancehq/ledger/pull/1963) remains open. These are existing confidentiality causes, including raw/signed audit evidence, not fresh auth findings. |

The trusted outer preparation must refresh issue assignment, PR state and actual
merged code, collect available prior audit/challenge artifacts and record missing
deduplication evidence before any later run. Pass the relevant context to the
read-only worker. A PR description states intent, not proof that its behavior is
on the audited HEAD. Lack of access to tickets/artifacts becomes residual risk,
not a false claim that no duplicate exists.

## Permitted diagnostics and limits

The manifest names existing fixtures as starting points. Inspect current test
names and feature tags before running them, use the pinned Nix/native validation
environment, and keep JWT keys, tokens, issuer/JWKS servers and transport fixtures
synthetic and local. Probe actual router/service chains with valid protocol
metadata; capture backend calls, first stream receive/terminal status and caller
snapshots. Exercise both denied and allowed controls. No live credentials,
external clusters, real identity-provider probes, destructive restore operations
or operator mutations are required.

Existing tests and allowed temporary diagnostics may run in a later read-only
pass; tracked reproducer changes are separate engineering work. Report exactly
which checks ran, their observations and untested boundaries. Proposed cases,
green CI and exhaustive helper mappings do not by themselves prove runtime auth
coverage. Manifest shape/path validation during preparation starts no provider
and produces no product audit report.
