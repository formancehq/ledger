# AI review / fix loop

The review loop automates only the mechanical handoff between an implementation agent and a reviewer. It does **not** grant an agent permission to broaden scope, change product behavior, weaken repository invariants, or resolve genuine design choices without a human.

Use the PR launcher, which creates and mechanically binds the required
candidate worktree:

```bash
bash scripts/ai-pr-loop 1732
```

The underlying commands remain provider-agnostic. Direct `review-loop`
invocation is a low-level launcher API and must supply every binding flag
documented in [AI PR worktree isolation](ai-worktree-isolation.md); it never
falls back to the caller's cwd.

Each invocation creates a persistent, unique directory below `build/ai-review-loop/`. Review and fix payloads from concurrent or subsequent runs therefore never share file names. The selected directory is printed when the loop starts.

## PR-oriented launcher

For the common case of an existing GitHub pull request, use the higher-level launcher:

```bash
bash scripts/ai-pr-loop 1732
```

A full PR URL is accepted as well. The launcher:

- resolves the current repository and PR metadata through `gh`;
- requires an open same-repository PR and currently rejects fork/cross-repository heads;
- fetches the current target-branch tip and the PR head ref, requires the fetched head to equal the GitHub-reported head SHA, and requires the fetched target tip to still equal the GitHub-reported base SHA before running agents;
- creates a detached linked worktree outside the primary checkout, under a unique sibling `.<repo>-ai-worktrees/pr-<number>.<run>/worktree` directory;
- writes an immutable PR/worktree binding outside that candidate, starts the
  orchestrator with `env -C` in the candidate, and requires the mechanical
  binding gate before every reviewer, fixer, and validator;
- runs the [legitimacy triage](ai-pr-triage-loop.md) before any technical review, using the `scripts/ai-pr-triage` adapter from a detached worktree pinned at the verified base SHA, so the PR under review cannot supply the policy that authorizes its own review;
- accepts a triage result only when its `base_sha` and `head` equal the SHAs the launcher fetched and verified, and continues into technical review only on a `KEEP` decision;
- passes the verified base commit SHA to `review-loop`, so a later update of the shared remote-tracking ref cannot change the reviewed delta;
- runs the standard Codex review + Claude fix composition against the PR base;
- runs repository validation locally before accepting any approval; no GitHub
  Actions status participates in the decision;
- never checks out or edits the primary checkout, and fails with
  `ROOT_MUTATION_DETECTED` if its HEAD, branch, or status changes while a
  subprocess runs;
- preserves the isolated worktree automatically when fixes remain so the resulting diff can be inspected manually;
- removes a clean temporary worktree after the loop unless `--keep-worktree` is supplied, together with the run directory that holds its triage result, known-findings ledger, and isolated validation caches.

Each invocation owns a unique worktree and never reuses or removes another invocation's directory. Without `--push`, the launcher performs no commit or push and only reports `READY_FOR_HUMAN_REVIEW`, `HUMAN_DECISION_REQUIRED`, `LEGITIMACY_REJECTED`, `BASE_UPDATE_REQUIRED`, or an orchestration error.

`LEGITIMACY_REJECTED` is emitted when triage returns `REJECT`; a `QUESTION` decision stops the run with `HUMAN_DECISION_REQUIRED`. Both stop before technical review and exit `2`. A triage provider error or target mismatch fails closed as an orchestration error. Every triage outcome is advisory: the launcher never closes, comments on, or otherwise changes the PR.

### Stale PR base

GitHub's reported base SHA describes the PR snapshot and may legitimately lag behind the current target branch. Reviewing against a historical base can miss semantic conflicts introduced since the PR was opened, so the launcher compares the freshly fetched target-branch tip with that reported base SHA before creating any worktree:

When the checkout is shallow and the first ancestry check is negative, the launcher unshallows history for the exact fetched target SHA and retries the check. A shallow boundary therefore cannot by itself turn normal target advancement into a rewrite/divergence result. Failure to restore that history is an orchestration error.

- identical: the reviewed base is current and the run continues into triage;
- the reported base is still an ancestor of the tip: the target branch advanced since the PR snapshot, so the launcher prints both SHAs, reports `AI_PR_LOOP_RESULT: BASE_UPDATE_REQUIRED`, and exits `3`. Synchronize the PR with its target branch — merge or rebase, push the PR head — and rerun the launcher;
- the reported base is not an ancestor of the tip: the target branch was rewritten or diverged, reported as `AI_PR_LOOP_RESULT: ERROR (target base rewritten or diverged)` with exit `1`;
- the reported base cannot be resolved even after an explicit fetch: an orchestration error with exit `1`.

Only the first case reaches an agent; every other case stops before triage, so no legitimacy triage, technical review, fix, or validation runs.

### Guarded publish mode

Publishing reviewed fixes is explicit and opt-in:

```bash
bash scripts/ai-pr-loop 1732 --push
```

`--push` does not weaken the review boundary. If the first bounded loop reaches `READY_FOR_HUMAN_REVIEW` with a dirty worktree, the launcher:

1. creates a second detached worktree at the exact verified base SHA, builds the policy engine in that base's pinned Nix environment, and uses only the base-pinned reviewer, fixer, and validation tools; the PR under review cannot supply the tooling that authorizes its own publication;
2. stages the complete isolated fix set and creates one local `fix: address AI review findings` candidate commit;
3. runs a second **review-only** pass on that exact clean commit, with no fixer configured;
4. refuses publication unless that candidate commit is approved as-is, `HEAD` still equals its immutable SHA, and the worktree remains clean;
5. verifies that the remote PR branch still points to the exact head SHA observed before any agent ran;
6. checks that the candidate is a descendant of that original head;
7. pushes the candidate SHA, rather than the mutable `HEAD` name, to the PR branch with an explicit lease on the original head SHA.

The lease acts as an atomic compare-and-swap guard. The candidate update is still a normal fast-forward, but the push is rejected if another actor updates or rewrites the PR branch between the initial metadata lookup and publication. A failed candidate review, moved remote head, dirty post-review worktree, or failed push preserves the candidate worktree for inspection and never overwrites the remote branch.

If the bounded loop produces no fixes, `--push` reports `NO_CHANGES` and does not create a commit. The launcher never comments on GitHub, resolves review threads, marks a PR ready, or merges it.

## Loop states

The orchestrator emits exactly one terminal state:

- `READY_FOR_HUMAN_REVIEW` — the reviewer returned `APPROVE` and no blocking findings remain.
- `AUTO_FIX_REQUIRED` — blocking findings are all explicitly auto-fixable, but no fix command was supplied.
- `HUMAN_DECISION_REQUIRED` — the reviewer requested human judgment, at least one blocking finding is not auto-fixable, or the bounded loop exhausted its pass budget.

The default pass budget is three review passes. Increase it only deliberately; repeated disagreement between author and reviewer is a signal to escalate, not to spend tokens indefinitely.

## Reviewer command contract

For every pass, the orchestrator sets:

- `AI_REVIEW_PASS` — 1-based pass number;
- `AI_REVIEW_RESULT` — file path where the reviewer must write its JSON result;
- `AI_REVIEW_HEAD` — exact commit SHA being reviewed;
- `AI_REVIEW_WORKTREE_FINGERPRINT` — SHA-256 fingerprint of that commit, staged and unstaged diffs hashed separately, and all non-ignored untracked file contents outside the selected state directory;
- `AI_REVIEW_CHANGE_TARGET` — path to the immutable JSON description of the committed comparison and included worktree scope;
- `AI_REVIEW_PREVIOUS_RESULT` — previous pass JSON path on re-review only.

`--base` is required. The orchestrator resolves that explicit ref once at loop startup and never asks a reviewer to infer a base from branch names, remotes, or history. For each pass it computes the merge base against the current HEAD and writes this target:

```json
{
  "kind": "BASE_COMPARISON",
  "base_ref": "origin/release/v3.0",
  "base_sha": "89abcdef0123456789abcdef0123456789abcdef",
  "merge_base_sha": "0123456789abcdef0123456789abcdef01234567",
  "head": "fedcba9876543210fedcba9876543210fedcba98",
  "worktree_scope": {
    "staged": true,
    "unstaged": true,
    "untracked": true
  },
  "worktree_present": {
    "staged": false,
    "unstaged": true,
    "untracked": false
  },
  "untracked_paths": []
}
```

The committed review range is exactly `merge_base_sha..head`. `worktree_scope` declares that staged, unstaged, and non-ignored untracked changes are also part of every reviewed state; `worktree_present` records which categories existed when that pass began. `untracked_paths` is the sorted, repository-relative manifest of the untracked files included in the fingerprint. Reviewer adapters must use this manifest instead of independently enumerating untracked files, so loop state stored inside a non-ignored repository directory cannot leak into the review scope. The orchestrator rejects a reviewer command that changes either the worktree or the target description.

The reviewer must follow [ai-review.md](ai-review.md) and write:

```json
{
  "decision": "REQUEST_CHANGES",
  "head": "0123456789abcdef",
  "worktree_fingerprint": "f5e6d7c8b9a00123456789abcdef0123456789abcdef0123456789abcdef0123",
  "residual_risk": "MEDIUM",
  "human_decision_context": "",
  "previous_findings": [],
  "findings": [
    {
      "id": "stable-short-id",
      "severity": "P1",
      "blocking": true,
      "auto_fixable": true,
      "title": "Preserve the committed-entry ordering",
      "location": "internal/infra/node/example.go:123",
      "evidence": "The changed path acknowledges before the durable commit.",
      "impact": "A crash can acknowledge work that was not persisted.",
      "resolution": "Acknowledge only after the durable commit succeeds."
    }
  ]
}
```

The reviewer must copy `AI_REVIEW_HEAD` and `AI_REVIEW_WORKTREE_FINGERPRINT` into `head` and `worktree_fingerprint` after reviewing that exact state. The orchestrator rejects a mismatch and also rejects a review command that changes the worktree while reviewing. This binds an approval to uncommitted fixes as well as to the current commit.

`previous_findings` is required. It is empty on pass one. On every later pass it must classify every finding from `AI_REVIEW_PREVIOUS_RESULT` exactly once as `FIXED`, `STILL_VALID`, or `OUTDATED`, with a non-empty reason. A `STILL_VALID` finding must remain in `findings` under the same stable id; a `FIXED` or `OUTDATED` finding must not. The orchestrator validates these relationships rather than relying on provider prose.

`auto_fixable` means the repository already determines the intended result and the change is local/reversible. It must be `false` when resolving the finding requires a product choice, changes an invariant, selects between conflicting authoritative sources, broadens subsystem scope, or otherwise requires human judgment.

The orchestrator rejects unknown JSON fields, missing required fields (including both boolean flags), inconsistent reviewer output such as `APPROVE` with blocking findings, and `REQUEST_CHANGES` without any blocking finding.

## Codex review adapter

`scripts/ai-review-codex` is the first provider-specific reviewer adapter. It keeps provider invocation separate from the `review-loop` policy/state machine.

`ai-pr-loop` composes this adapter automatically. A low-level composition must
also satisfy the worktree-binding contract; the following fragment is not a
standalone unbound invocation:

```bash
bash scripts/review-loop \
  --pr "$EXPECTED_PR_NUMBER" \
  --worktree "$EXPECTED_WORKTREE" \
  --expected-head "$EXPECTED_HEAD" \
  --trusted-root "$TRUSTED_ROOT_CHECKOUT" \
  --binding-file "$AI_WORKTREE_BINDING_FILE" \
  --validation-run-dir "$VALIDATION_RUN_DIR" \
  --git-guard /trusted/base/scripts/ai-git-guard \
  --base origin/release/v3.0 \
  --review-cmd 'bash scripts/ai-review-codex'
```

The adapter:

- validates the environment variables supplied by `review-loop`;
- consumes the exact committed range and worktree scope from `AI_REVIEW_CHANGE_TARGET` instead of asking Codex to infer a base;
- runs `codex exec` in an ephemeral, read-only sandbox;
- runs with an isolated temporary `HOME` and `CODEX_HOME`, copying only `auth.json` when present so authentication remains available without personal config, rules, plugins, skills, hooks, memories, or session state;
- passes `--ignore-user-config`, `--ignore-rules`, and disables hooks, plugins, apps, memories, multi-agent execution, and skill search explicitly;
- supplies the repository review contract and current review target in the prompt;
- exposes the prior structured review only as an external untrusted-data file on re-review; its JSON strings are never concatenated into the trusted prompt;
- uses `scripts/codex-review.schema.json` with Codex structured output;
- writes the final JSON directly to `AI_REVIEW_RESULT`.

Codex CLI 0.147 exposes `--base`, `--commit`, and `--uncommitted` on `codex exec review`, but that subcommand does not expose the full `--sandbox`/working-directory surface used by this adapter. The adapter therefore keeps `codex exec` for structured output and read-only execution, and supplies the orchestrator-resolved merge-base range as an exact Git command in the trusted prompt. It does not ask Codex to reproduce base-selection policy.

The schema intentionally requires every output field. Fields that are semantically optional use an empty string when unused (for example `human_decision_context` on an ordinary approval and `location` when no useful file/symbol location exists). The Codex CLI has no single flag that disables every extension source. The temporary homes remove personal sources, while repository `AGENTS.md`, repository documentation, repository-scoped skills, built-in Codex behavior, and administrator-managed `/etc/codex` policy remain visible by design. Process-level environment and OS credential storage also remain available. The orchestrator remains responsible for validating the reviewed HEAD/worktree fingerprint and for deciding whether the result is ready, auto-fixable, or requires a human.

The adapter performs no fixes and has no GitHub write behavior.

## Fix command contract

When every blocking finding is auto-fixable, the orchestrator writes only those blockers to a JSON file and sets:

- `AI_REVIEW_FINDINGS` — path to that blocker-only JSON;
- `AI_REVIEW_RESULT` — path to the review that produced them;
- `AI_REVIEW_PASS` — pass being fixed.

The fix agent must:

1. change only what is needed to resolve the supplied findings;
2. preserve the PR's stated scope and repository contracts;
3. avoid opportunistic refactors;
4. report/exit non-zero rather than weakening a check or guessing through an ambiguity.

The blocker payload and originating review result are immutable inputs. The orchestrator snapshots both files before invoking the fixer and rejects the pass if either file's contents change or can no longer be read.

After a successful fix command, the orchestrator runs
`bash scripts/agent-check-pr` by default. It runs the baseline checks and unit
tests, then selects additional local gates from the complete
base-to-worktree diff: E2E tests for production Go paths, scenario tests for
scenario/Numscript paths, Schemathesis for the HTTP/OpenAPI surface, and
operator tests for the operator module. Changes to FSM, admission, Raft,
cluster membership, cache/preload, primary persistence, snapshot/restore, or
the model-checker harness additionally run the three-node
`just test-model-cluster 180` gate with rolling restarts. These are local
correctness profiles selected for the changed architecture, not a mirror of
GitHub Actions jobs. A validation failure stops the loop immediately. Only
after validation succeeds does it invoke the reviewer again.

The same local validation runs after an `APPROVE` decision and before
`READY_FOR_HUMAN_REVIEW` is emitted. The orchestrator fingerprints the
workspace again and rejects readiness if a validator changed the state that the
reviewer approved. It passes the immutable review-base SHA to the validator as
`AI_REVIEW_BASE_SHA`; in guarded publish mode, the validator itself comes from
the base-pinned trusted tool worktree. Its `agent-just` wrapper also loads the
base-pinned `justfile` with dotenv loading disabled while setting the reviewed
PR worktree as the recipe working directory. A PR therefore cannot replace a
required recipe with a no-op. Script-backed gates are split the same way:
Schemathesis and model-checker runners come from the trusted base, while their
server build, OpenAPI input, and other reviewed sources come from the candidate
worktree. Base-pinned shell adapters used only for publication are invoked
explicitly through `bash` and need to be readable, non-symlink regular files; their
executable bit is not an additional trust boundary. The model driver/oracle is
also base-pinned. The loop does not query
or wait for GitHub Actions checks.

## Claude Code fix adapter

`scripts/ai-fix-claude` is the first provider-specific fixer adapter. It deliberately exposes a narrower autonomous surface than an interactive Claude Code session.

Use it together with the Codex reviewer:

```bash
bash scripts/review-loop \
  --pr "$EXPECTED_PR_NUMBER" \
  --worktree "$EXPECTED_WORKTREE" \
  --expected-head "$EXPECTED_HEAD" \
  --trusted-root "$TRUSTED_ROOT_CHECKOUT" \
  --binding-file "$AI_WORKTREE_BINDING_FILE" \
  --validation-run-dir "$VALIDATION_RUN_DIR" \
  --git-guard /trusted/base/scripts/ai-git-guard \
  --base origin/release/v3.0 \
  --review-cmd 'bash scripts/ai-review-codex' \
  --fix-cmd 'bash scripts/ai-fix-claude'
```

The adapter:

- requires only the blocker payload, originating review result, and pass number provided by `review-loop`;
- requires those two state files to resolve inside the current worktree; the default `build/ai-review-loop` state directory satisfies this boundary, while a custom external `--state-dir` is rejected by this adapter;
- passes those JSON files by path and explicitly treats their contents as untrusted data rather than interpolating finding text into the trusted prompt;
- runs Claude Code non-interactively in `--safe-mode`, which disables user/project hooks, plugins, skills, MCP servers, custom agents, and other discovered customizations while preserving normal authentication;
- uses `--strict-mcp-config`, disables slash commands and session persistence, and exposes exactly the built-in `Read`, `Edit`, `Write`, and `Grep` tools through `--tools`; `Glob` is omitted because Claude Code 2.1.202 does not reliably apply project-relative `Read` rules to external path discovery;
- uses `--permission-mode dontAsk` with project-relative `Read(/**)` and `Edit(/**)` approval, so repository inspection and edits succeed while parent, sibling-worktree, and other external filesystem access fails closed without an interactive prompt;
- explicitly denies `Bash`, `WebFetch`, `WebSearch`, and edits to `.git` as defense in depth, so the fixer cannot run tests, mutate Git/GitHub state, or access the network through those tools;
- instructs Claude to make only finding-traceable edits and to avoid guessing through product/design/invariant ambiguity;
- leaves all validation to the orchestrator's mandatory local
  `bash scripts/agent-check-pr` gates before re-review and final readiness.

Safe mode deliberately disables automatic instruction discovery, so the trusted prompt explicitly tells Claude to read the repository's `AGENTS.md` and only the relevant subsystem documentation. Administrator-managed policy still applies and may further restrict the invocation, but repository/user settings and extensions are not loaded. The adapter does not use `--dangerously-skip-permissions`. Authentication and the installed Claude Code binary remain machine prerequisites.

The adapter does not commit, push, comment on GitHub, resolve threads, or decide whether a fix is accepted. A successful CLI exit only means the fix pass completed; `review-loop` verifies that the blocker payload and originating review result were not modified, validates the resulting worktree, and asks the reviewer to determine whether findings are actually resolved.

## Exit codes

`review-loop`:

| Code | Meaning |
|---|---|
| `0` | `READY_FOR_HUMAN_REVIEW` |
| `1` | orchestration/reviewer/fixer/validation error |
| `2` | `HUMAN_DECISION_REQUIRED` |
| `3` | `AUTO_FIX_REQUIRED` but no `--fix-cmd` was supplied |

`ai-pr-loop` has its own exit semantics. It forwards the bounded loop's status, but adds pre-triage outcomes of its own, so its codes must not be read with the `review-loop` meanings above:

| Code | Meaning |
|---|---|
| `0` | `READY_FOR_HUMAN_REVIEW`, including the `--push` results `NO_CHANGES` and `PUSHED` |
| `1` | orchestration error, including a rewritten or diverged target branch |
| `2` | `HUMAN_DECISION_REQUIRED`, `LEGITIMACY_REJECTED`, or a `--push` refusal caused by a candidate review needing human judgment, a moved remote head, or a rejected push |
| `3` | `BASE_UPDATE_REQUIRED` — the target branch advanced past the PR base, or, in `--push` mode, the review-only candidate pass returned `AUTO_FIX_REQUIRED` |

Because one launcher exit code can cover several outcomes, automation must key on the emitted `AI_PR_LOOP_RESULT` / `AI_PR_LOOP_PUSH_RESULT` line and use the exit status only as a success/failure signal.

Before review, `ai-pr-loop` invokes the base-pinned
`ai-pr-publication-preconditions` helper for legitimacy triage, exact-head
known-finding collection, shared bugfix-intent classification, and bugfix
evidence validation. `ai-pr-adopt-candidate` invokes that same helper; adoption
changes which implementation bytes enter exact review, not the publication
policy that authorizes them. Target advancement is classified as
`BASE_UPDATE_REQUIRED` before helper lookup. A helper genuinely missing from the
selected complete base-pinned graph is a `TOOLING_ERROR`, not a human decision.

## Safety boundary

`review-loop` itself never posts comments, resolves threads, pushes commits, or merges pull requests. The higher-level `ai-pr-loop` also remains read-only with respect to GitHub by default. Its explicit `--push` mode may create a local candidate commit and update only the existing same-repository PR head branch under the verified lease described above; it still never comments, resolves threads, changes PR metadata, or merges.
### Hermetic validation environments

Git worktree isolation alone is insufficient for candidate validation. Each
run must use a unique temporary validation directory and isolate `HOME`,
`GOCACHE`, `GOMODCACHE`, `GOPATH`, `TMPDIR`, `XDG_CACHE_HOME`, and the
`golangci-lint` cache. The canonical `agent-validation-env` wrapper creates
these directories and records `VALIDATION_RUN_ID`; `ai-pr-loop` and
`ai-pr-adopt-candidate` invoke validation through it. This keeps generated
files and absolute paths from one concurrent run out of another run's caches.

Those caches live inside the run's dedicated `validation/` directory, disjoint
from both Git worktrees, so `ai-pr-loop` reclaims the whole
run directory recursively when the run ends without `--keep-worktree`. It does
so only after both owned worktrees are gone: a preserved worktree — inspectable
fixes or a failed removal — keeps its run directory and its caches.

### Candidate normalization

Candidate publication normalizes the candidate before assigning its final SHA.
The trusted base-pinned `just pre-commit` recipe runs in the isolated validation
environment; any resulting tree changes are logged, checked with `git diff
--check`, committed as normalization, and rechecked. This repeats to a bounded
fixpoint (three passes by default). Only the converged SHA is reviewed and
published. A candidate supplied to `ai-pr-adopt-candidate` is immutable: if it
needs normalization, adoption refuses rather than silently changing the SHA.
