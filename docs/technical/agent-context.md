# Agent Context Routing

This document maps code areas and task types to the **minimum authoritative documentation** an AI agent should load before making changes. The goal is to keep the always-loaded prompt small while preserving access to deep subsystem knowledge.

## Documentation authority

Use documentation according to this order:

1. `AGENTS.md` — repository-wide guardrails and workflow.
2. `docs/technical/**` — authoritative engineering architecture and contributor documentation.
3. `docs/ops/**` — authoritative operational behavior and CLI/deployment documentation.
4. `docs/sales/**` — product-facing material; useful for product context, **not** an engineering specification.
5. `docs/drafts/**` — experimental/future designs; **non-authoritative unless the task explicitly targets that RFC/design**.

When current code and authoritative documentation disagree, stop treating the documentation as proof of behavior. Inspect the implementation and flag the documentation drift as part of the task.

`docs/technical/agent-reference-legacy.md` is a temporary snapshot of the former monolithic `AGENTS.md`. It exists to preserve details during the context refactor. It is not required reading and must not override current subsystem documentation.

## Routing by code area

| Code / change area | Read before editing |
|---|---|
| `internal/application/admission/**` | `docs/technical/architecture/subsystems/admission/`, `docs/technical/architecture/subsystems/fsm/` when proposal coverage/order semantics are involved |
| `internal/domain/processing/**` | `docs/technical/architecture/subsystems/fsm/`; this package executes business orders on the FSM path, so determinism, coverage/preload, cache, rollback/skip, and order-immutability rules apply |
| `internal/infra/state/**` | `docs/technical/architecture/subsystems/fsm/`, especially deterministic FSM / coverage / preload docs |
| `internal/infra/plan/**`, `internal/infra/preload/**` | `docs/technical/architecture/subsystems/fsm/` |
| `internal/infra/cache/**`, `internal/infra/attributes/**`, `internal/infra/bloom/**` | `docs/technical/architecture/subsystems/attributes/` and relevant FSM docs |
| `internal/application/check/**`, `internal/domain/replay/**` | `docs/technical/architecture/subsystems/checker/`, `docs/technical/architecture/audit-vs-technical-state.md` |
| `internal/storage/dal/**`, `internal/storage/wal/**`, `internal/storage/spool/**`, `internal/storage/pebblecfg/**` | `docs/technical/architecture/subsystems/storage/` |
| `internal/storage/readstore/**`, `internal/application/indexbuilder/**` | `docs/technical/architecture/subsystems/indexer/`, `docs/technical/architecture/subsystems/read-path/` |
| `internal/storage/usagestore/**` | relevant usage-builder/subsystem docs plus `docs/technical/architecture/audit-vs-technical-state.md` when integrity/rebuild semantics change |
| `internal/application/ctrl/**`, `internal/query/**` | `docs/technical/architecture/subsystems/read-path/` |
| `internal/adapter/grpc/**` | `docs/technical/architecture/subsystems/api/`, `docs/technical/contributing/api-comparison.md` |
| Snapshot transfer (`internal/adapter/grpc/file_streaming.go`, `internal/adapter/grpc/server_snapshot.go`, `internal/application/ctrl/file_fetcher.go`, `internal/application/ctrl/file_receiver.go`, `internal/application/ctrl/snapshot_fetcher.go`) | `docs/technical/architecture/subsystems/storage/follower-sync.md`, `docs/technical/contributing/testing.md`; also read `docs/technical/architecture/subsystems/api/auth.md` when changing the RPC trust boundary |
| `internal/adapter/http/**`, `openapi.yml` | `docs/technical/architecture/subsystems/api/`, `docs/technical/contributing/api-comparison.md` |
| `internal/infra/node/**`, `internal/infra/transport/**`, `internal/infra/membership/**` | `docs/technical/architecture/subsystems/consensus/` |
| `internal/infra/coldstorage/**`, `internal/infra/receipt/**`, `internal/application/backup/**` | `docs/technical/architecture/subsystems/chapters/`, relevant `docs/ops/` backup/restore docs |
| `internal/infra/backup/**` | `docs/technical/architecture/subsystems/chapters/backup.md`, `docs/technical/architecture/subsystems/chapters/incremental-restore-contract.md`, `docs/ops/backup-restore.md` |
| `internal/application/events/**`, `internal/application/mirror/**` | `docs/technical/architecture/subsystems/events-mirror/` |
| Numscript runtime/library | `docs/technical/architecture/subsystems/scripting/`, `docs/technical/contributing/numscript.md` |
| `misc/proto/**`, generated protobuf code | `docs/technical/contributing/protobuf.md` |
| `cmd/ledgerctl/**` | `docs/ops/cli.md`, `docs/technical/contributing/conventions.md` |
| `internal/bootstrap/**` | `docs/technical/architecture/overview.md`, relevant subsystem docs, and `docs/ops/deployment.md` for persisted/config behavior |
| tests only | `docs/technical/contributing/testing.md` plus the subsystem documentation for the behavior under test |
| contributor/build tooling | `docs/technical/contributing/getting-started.md`, `docs/technical/contributing/development.md`, `docs/technical/contributing/conventions.md` as relevant |
| `scripts/aicampaign/**`, `scripts/ai-campaign*` | `docs/technical/contributing/ai-campaign.md`, `docs/technical/contributing/testing.md`; also `docs/technical/contributing/product-technical-traceability.md` for distributed coordination semantics |
| agent Go cache / validation isolation tooling | `docs/technical/contributing/agent-module-download-cache.md`, `docs/technical/contributing/ai-worktree-isolation.md`, `docs/technical/contributing/testing.md` |

If a touched production area has no matching row, do not assume that no subsystem rules apply. Identify its callers/callees or owning subsystem first, then load that subsystem's documentation before editing.

## Routing by task type

### Architecture exploration

Start with:

- `docs/technical/architecture/overview.md`
- the matching subsystem README
- `docs/technical/architecture/data-flows.md` only for cross-subsystem request/read/sync flows

Do not load every architecture document up front.

### Significant technical decisions

Before choosing or materially changing an API semantic, persistence/cache/consistency strategy, retry/idempotency mechanism, dependency, distributed-system mechanism, compatibility strategy, major abstraction, subsystem boundary, or other meaningful complexity, read:

- `docs/technical/contributing/product-technical-traceability.md`;
- the owning subsystem documentation;
- the authoritative product/operational evidence referenced by the task, when available.

Establish the chain from product/operational need to observable requirement before selecting the implementation mechanism. Do not infer undocumented product intent from the implementation you would prefer. If the need or requirement cannot be established from accessible evidence, surface the missing decision instead of inventing it.

Mechanical maintenance, generated refreshes, narrow test-helper changes, and behavior-preserving renames do not require artificial product rationale.

### Persistence or integrity changes

Always determine which class the state belongs to before implementation:

- audit-bound business truth;
- primary-store projection;
- peer secondary/rebuildable state;
- operational/informational state.

Read:

- `docs/technical/architecture/audit-vs-technical-state.md`
- `docs/technical/architecture/subsystems/checker/`
- the storage/FSM subsystem docs touched by the change

A new primary-store projection requires checker coverage or an explicitly justified documented exemption.

If a persisted value can change after a full checkpoint, also read
`docs/technical/architecture/subsystems/chapters/incremental-restore-contract.md`.
Classify it as preserved, rebuilt, or deliberately discarded during a
cross-cluster restore, then exercise the classification with a non-empty
post-checkpoint delta. This applies to updates and deletion cascades as well as
new projections.

### FSM / proposal changes

Read the FSM subsystem documentation before coding. Explicitly check:

- determinism;
- proposal coverage/preload completeness;
- coverage-gate usage;
- order business-payload immutability;
- cache generation/eviction rules;
- hot-path storage capabilities.

### Configuration affecting writes or Raft apply

Before adding a flag, environment variable, startup setting, or version-dependent default that can influence a write, read:

- `docs/technical/architecture/subsystems/admission/`;
- `docs/technical/architecture/subsystems/fsm/deterministic-fsm.md`, especially the node-local configuration boundary;
- `docs/ops/deployment.md` when deployment or upgrade behavior changes.

Decide explicitly whether the value only gates proposal admission or changes the outcome of a committed entry. Node-local configuration may do the former; it must never do the latter. If concurrent admissions can exceed a local operational limit, document whether that soft-limit behavior is intentional rather than silently presenting it as a strict FSM invariant.

### Deep correctness audits

Use the native deep-audit workflow when the request seeks latent correctness defects across an immutable repository state in a reusable domain, such as persistence/restore/replay, Raft membership/leadership, accounting invariants, idempotency/partial failures, read consistency, or concurrency/lifecycle.

1. Check `docs/technical/audits/` for a matching domain manifest.
2. If a manifest exists, read `docs/technical/contributing/ai-audit.md` and run `bash scripts/ai-audit <domain>`. Keep the manifest as the campaign scope instead of replacing it with an ad-hoc audit prompt.
3. Qualify the structured result with `bash scripts/ai-audit-challenge <audit-result>` after reading `docs/technical/contributing/ai-audit-challenge.md`. The challenge pass independently tries to disprove every finding; a first-pass finding is not a confirmed engineering defect.
4. Treat Jira publication as a separate, later step. `scripts/ai-audit-jira` consumes a qualified report, and publication requires explicit authorization; neither the audit nor challenge pass creates issues.
5. For cross-session coordination, read
   `docs/technical/contributing/ai-campaign.md` and use
   `scripts/ai-campaign`. Campaign state is a local projection; it never
   replaces GitHub, Jira, Git, or native audit provenance.

If no manifest matches, first decide whether the requested correctness scope is durable and reusable. If it is, create and review a manifest before running the native audit. If it is not, keep the work as a task-specific investigation rather than mechanically creating a new audit domain.

Do not use `ai-audit` for:

- pull-request review, which follows the AI code review and re-review workflow below;
- diagnosis or implementation of one concrete known bug;
- routine performance analysis, benchmark work, CI timing analysis, or diagnosis of one tooling failure, unless the work is intentionally being promoted into a reusable correctness domain.

`ai-audit` runs a read-only, manifest-scoped provider pass and binds its structured report to an exact clean `HEAD`; `ai-audit-challenge` independently qualifies that report at the same clean `HEAD`. Their contracts prohibit code fixes, commits, pushes, comments, issue creation, and GitHub metadata changes; structured reports are their only intended writes.

### AI code review and re-review

Read `docs/technical/contributing/ai-review.md` before reviewing a pull request or reviewing fixes to previous findings.

For a significant technical decision, also read `docs/technical/contributing/product-technical-traceability.md` and use the documented need/requirement as review intent. If that chain is missing or conflicting, do not infer it from implementation details; surface a human-decision question.

Load the subsystem documentation for the behavior changed by the PR, but do not preload unrelated documentation. Findings must follow the review contract: concrete evidence, explicit severity and blocking status, no style-only noise, and a compressed final decision. On re-review, start from the current HEAD and classify previous findings as fixed, still valid, or outdated before reporting new issues.

### API changes

Read the API subsystem docs and update the compatibility/spec material required by `AGENTS.md`:

- `docs/technical/contributing/api-comparison.md`
- `openapi.yml` for HTTP surface changes

### CLI changes

Read `docs/ops/cli.md` and keep it synchronized with flags/commands/behavior. Regenerate demos when applicable.

### Protocol Buffers

Read `docs/technical/contributing/protobuf.md`, then run `just generate-proto` immediately after modifying proto definitions.

### Refactors and shared-symbol changes

Use GitNexus impact analysis when available before changing shared/high-fan-in symbols or cross-subsystem contracts. Local helpers inside an already-understood component do not require independent blast-radius analysis unless the scope grows.

## Context budget rule

Prefer **progressive disclosure**:

1. repository guardrails;
2. one subsystem's docs;
3. targeted code/context;
4. additional cross-cutting docs only when impact analysis or implementation requires them.

If an agent has loaded unrelated sales material, drafts, or multiple subsystem deep-dives without a task-specific reason, it is carrying too much context.
