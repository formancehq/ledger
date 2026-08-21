# AI Agent Instructions

This file is the always-loaded entry point for AI agents working on Ledger v3. Keep it short. Detailed technical knowledge belongs in `docs/`; load only the documentation relevant to the task.

## Release status — v3 is unreleased

**CRITICAL:** Ledger v3 has not been released and state is wiped on pushes to `release/v3.0`. There is no backward-compatibility burden between v3 development revisions.

- Do not add migrations, compatibility shims, version guards, or fallback paths for old v3 wire/storage formats.
- When removing protobuf fields, delete them and realign field numbers sequentially. The EN-1551 cleanup removed every existing `reserved` declaration from `misc/proto/`; do not reintroduce any.
- Read `docs/technical/contributing/protobuf.md` before changing `.proto` files.

## Configuration safety checks

Critical persisted configuration such as node/cluster identity and storage schema is validated on boot. Do not weaken or bypass those checks without an explicit task requiring it. Read `docs/ops/deployment.md` and the relevant `internal/bootstrap/**` code before changing persisted/config-validation behavior.

## How to load context

1. Start with this file only.
2. Identify the code areas the task will touch.
3. Read the matching documentation from `docs/technical/agent-context.md`.
4. Read additional docs only when the task, code, or impact analysis shows they are relevant.
5. Treat `docs/drafts/**` as non-authoritative unless the task explicitly asks to implement a draft/RFC.
6. Treat `docs/sales/**` as product material, not an engineering specification.

Do not preload the whole documentation tree. Prefer the smallest authoritative context that covers the change.

<a id="invariants"></a>
## Non-negotiable architecture invariants

These are guardrails, not complete explanations. Before changing the affected subsystem, read the linked documentation.

1. **Cache consistency:** the in-memory cache must not diverge between nodes for the same applied index.
2. **Deterministic FSM:** Raft apply must produce identical results on every node. No randomness, wall-clock-dependent behavior, or node-local state in the FSM apply path. Node-local configuration — including flags, environment variables, startup configuration, and version-dependent defaults — may gate admission, but must never affect how a committed entry is applied. See [the configuration boundary](docs/technical/architecture/subsystems/fsm/deterministic-fsm.md#34-node-local-configuration-and-rolling-upgrades).
3. **No Pebble reads in the FSM hot path:** apply reads through the cache/command contract; do not introduce storage-read capabilities into the hot path.
4. **Main-store writes are capability-restricted:** new `OpenWriteSession` call sites require an explicitly justified lifecycle path and must satisfy `.golangci.yaml` enforcement.
5. **Cache entries are not individually evicted:** eviction happens through generation rotation; do not delete individual cache entries outside that mechanism.
6. **FSM reads require declared coverage/preload:** every cache-keyed FSM read must be authorized by the proposal's declared `plan.Coverage`; never widen the read horizon inside apply.
7. **Impossible states fail loudly:** a branch that is unreachable by contract must surface an invariant failure rather than silently `return nil` or `continue`.
8. **Audit is business truth:** the audit chain is authoritative. New persisted primary-store projections must be checker-verified or have an explicitly documented valid exemption.
9. **Never bypass the coverage gate:** use the scoped/gated cache APIs in order/TU handlers; never read the parent registry/key store directly from the FSM path.
10. **Accepted orders are immutable until audit capture:** do not mutate business payloads, including indirectly through aliased maps/slices/messages. Only the explicitly technical order fields are outside business-intent hashing.
11. **Incremental restore parity:** every committed effect intended to survive a cross-cluster restore must be preserved by the checkpoint or reconstructed from the exported delta. Changes to persisted state, audited orders, or deletion cascades must classify their restore behavior and prove it with a non-empty post-checkpoint delta. See [the incremental restore contract](docs/technical/architecture/subsystems/chapters/incremental-restore-contract.md).

Required reading for FSM/cache/preload work:
- `docs/technical/architecture/subsystems/fsm/`
- `docs/technical/architecture/subsystems/attributes/`

Required reading for persisted projections/checker work:
- `docs/technical/architecture/subsystems/checker/`
- `docs/technical/architecture/audit-vs-technical-state.md`
- `docs/technical/architecture/subsystems/chapters/incremental-restore-contract.md` when the projection changes during audited apply or another lifecycle path covered by incremental backup

The pre-refactor, fully expanded instruction set is retained temporarily at `docs/technical/agent-reference-legacy.md` for migration safety. It is **reference material, not automatically loaded context**. If a rule in this file or current subsystem documentation conflicts with that legacy snapshot, the current file/subsystem documentation wins.

## Engineering conventions

Use `docs/technical/contributing/` as the canonical source for development conventions.

Key rules that apply broadly:

- Keep one file per command and one file per HTTP handler.
- Do not introduce global variables for flags; use structs.
- Do not ignore errors. Handle them explicitly, or use `_ = ...` with a justification comment when intentional.
- Keep a struct's methods colocated with the struct; extract composed sub-types rather than scattering methods across files.
- Prefer existing repository patterns and DRY solutions over parallel abstractions.
- Every CLI invoked by repository scripts or documented as a contributor prerequisite must be provided by the Nix development environment and pinned through `flake.lock`; do not add host-only CLI dependencies.
- Build artifacts belong under `build/`, never the repository root.
- JSON properties use camelCase.
- In tests, do not use `time.Sleep`; prefer `require.Eventually` or deterministic synchronization.
- Unit tests should use `t.Parallel()` where supported by existing test conventions.
- Do not hand-roll mocks for mockgen-managed interfaces; regenerate generated mocks after interface changes.

Before changing Protocol Buffers, tests, Numscript, or contributor workflow, read the matching document listed in `docs/technical/agent-context.md`.

## Documentation maintenance

Documentation is part of the change when behavior, architecture, interfaces, CLI, or APIs change.

- New technical mechanism/subsystem/non-obvious invariant: update the matching `docs/technical/architecture/` subsystem documentation and its README.
- API endpoint change: update `docs/technical/contributing/api-comparison.md`; update `openapi.yml` for HTTP changes.
- CLI behavior/flag/command change: update `docs/ops/cli.md`; regenerate demo GIFs when applicable.
- Interface/behavior change: update relevant code comments.
- Documentation is written in English.

Do not use `docs/drafts/**` as evidence of current behavior unless the task explicitly references that design.

## Definition of done

A task is not complete when the code merely looks correct. Before handing work to another agent or a human reviewer:

1. finish the requested implementation and documentation;
2. run the canonical baseline validation with `bash scripts/agent-check`;
3. run tests appropriate to the touched subsystem and risk level;
4. inspect the final diff for unrelated or generated changes;
5. perform a self-review against the task, loaded subsystem docs, and the invariants above;
6. report unresolved concerns instead of silently weakening or skipping a check.

For broad or high-risk changes where the root-module unit suite is appropriate, run `bash scripts/agent-check-full`. Do not run the full suite mechanically for every local documentation or narrowly scoped change when targeted validation is sufficient.

### Canonical validation commands

```bash
# Required baseline for code changes
bash scripts/agent-check

# Baseline + full root-module unit test suite
bash scripts/agent-check-full
```

`scripts/agent-check` runs the existing `just pre-commit` pipeline, compiles all root-module packages with `GOROOT= go build ./...`, and runs `git diff --check`. It deliberately does not choose task-specific tests for you.

After modifying `.proto` files, run `just generate-proto` immediately.
After changing a `//go:generate mockgen` interface, run `go generate ./...`.
Run tests appropriate to the touched subsystem; see `docs/technical/contributing/testing.md` for tagged/full suites and Docker requirements.

## Impact and code intelligence

Use GitNexus when available to understand unfamiliar code and assess cross-component impact. The detailed GitNexus workflow and skill routing remain in `.claude/skills/gitnexus/`.

Use impact analysis before high-blast-radius changes such as:
- public/shared interfaces and domain types;
- FSM handlers and persistence formats;
- cross-subsystem contracts;
- renames/refactors of shared symbols;
- symbols known to have high fan-in.

For a local implementation change inside an already understood component, do not multiply context/tool calls solely to analyze every helper independently. Re-run change-impact analysis before committing when the affected scope is unclear or broader than planned.

## Human escalation

Do not ask for human input when the choice is local, reversible, covered by an existing pattern, or determined by tests/documentation.

Escalate when:
- product behavior or acceptance criteria are genuinely ambiguous;
- a non-negotiable invariant would need to change;
- authoritative sources conflict;
- a new external dependency or subsystem is required;
- persisted/audited semantics or security boundaries would materially change;
- completing the task would require weakening a validation/check.

When escalating, compress the decision:

```text
DECISION REQUIRED
Context: <why the repository cannot decide this>
Options: <A / B>
Recommendation: <preferred option and why>
Risk if wrong: <short consequence>
```

## Completion summary

Return a concise result rather than a transcript of the work:

```text
RESULT: PASS | PASS WITH CONCERNS | BLOCKED
Risk: LOW | MEDIUM | HIGH
Validation: <checks/tests run>
Blocking findings: <count + summary>
Non-blocking findings: <count + summary>
Human decision required: YES | NO
Docs updated: <paths or N/A>
```
