# AI review and fix loop

The PR workflow automates the mechanical handoff between an implementation
agent and a technical reviewer. It does not grant authority to broaden scope,
change product behavior, weaken repository invariants, or resolve genuine
design choices without a human.

For an existing same-repository pull request, run from an up-to-date checkout
of its target branch:

```bash
bash scripts/ai-pr-loop 1732
```

The launcher accepts a PR number or URL. `--keep-worktree` preserves its
temporary candidate worktree. `--push` permits a guarded update of the existing
PR branch; the launcher never comments, resolves threads, changes PR metadata,
closes, or merges.

## Ordinary path

The launcher:

1. resolves the open PR and fetches its exact target and head;
2. stops with `BASE_UPDATE_REQUIRED` if the target advanced beyond the PR's
   recorded base;
3. creates separate candidate, trusted-tool, and validation directories;
4. validates required bugfix evidence when the PR is classified as a bugfix;
5. collects current unresolved GitHub findings once;
6. invokes the technical reviewer with the exact base SHA, candidate state,
   task/PR context, and the GitHub findings as untrusted context;
7. runs proportional `agent-check-pr` validation before readiness; and
8. in `--push` mode, revalidates the target and remote head and publishes only
   with an exact `--force-with-lease`.

A straight-through PR therefore makes no legitimacy-provider call, no
known-finding reconciliation-provider call, and one technical-review provider
call. If that review requests an auto-fix, the loop performs the fix, focused
validation, and a new review. Changing bytes invalidates the older review.

The current bounded review/fix state machine remains in place for this PR.
Linearizing that implementation and collapsing its remaining guards is deferred
to the follow-up review-loop simplification.

## Exact review target

`review-loop` requires an explicit `--base` and a mechanically bound candidate
worktree. For each pass it records:

- the resolved base SHA and merge base;
- the current HEAD;
- staged, unstaged, and untracked scope; and
- a SHA-256 fingerprint of the complete reviewed state.

The reviewer copies the exact head and fingerprint into its structured result.
The orchestrator rejects a mismatch or any reviewer mutation of the worktree.
The review result contains the decision, risk, previous-loop finding
classifications, current actionable findings, and classifications for every
supplied GitHub finding. See [AI review](ai-review.md) and
[Known GitHub PR findings](ai-pr-known-findings.md).

## Fix and validation behavior

Only blocking findings whose `auto_fixable` flag is true enter the automated
fix path. A product choice, invariant change, conflicting authoritative source,
or scope expansion produces `HUMAN_DECISION_REQUIRED`.

After a fix, the base-pinned `agent-check-pr` policy runs before re-review. Its
successful exact-state result may be reused only within the same live
`review-loop` process when all candidate, tool, environment, binding, and
validation inputs are unchanged. This receipt is in memory; it is not persisted
or trusted after interruption.

Validation after an approval must leave the approved worktree unchanged. The
loop does not query or wait for GitHub Actions; CI remains the authoritative
broad, clean validation boundary before merge.

## Recovery

Git is the recovery mechanism. Preserve the branch or worktree, then start a
clean invocation and inspect its branch, commits, diff, PR state, and current
target. Run fresh focused evidence and validation, collect current GitHub
findings, and obtain a fresh exact technical review before a leased push.

No review or validation receipt from an interrupted invocation is reusable.
There is no candidate-adoption command or recovery state machine.

## Worktree and publication safety

The candidate worktree is distinct from the primary checkout and is bound to
the expected PR and SHA before reviewer, fixer, or validator subprocesses. The
primary checkout is checked for accidental mutation at those boundaries. See
[AI PR worktree isolation](ai-worktree-isolation.md) for the low-level binding
contract.

In `--push` mode, the launcher commits reviewed fixes when necessary, requires
a clean reviewed candidate, verifies that it descends from the original PR
head, refreshes the target and remote head, and pushes with:

```text
--force-with-lease=refs/heads/<head>:<original-head>
```

Any target advance requires manual synchronization followed by fresh evidence,
validation, finding collection, and exact review.

## Exit results

`review-loop` emits `READY_FOR_HUMAN_REVIEW`, `AUTO_FIX_REQUIRED`, or
`HUMAN_DECISION_REQUIRED`. `ai-pr-loop` additionally emits
`BASE_UPDATE_REQUIRED` and guarded-push results. Automation should key on the
emitted result line rather than treating an exit code as a complete status
protocol.
