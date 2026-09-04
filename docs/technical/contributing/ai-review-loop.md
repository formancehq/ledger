# AI final review flow

The PR workflow performs one validation and one exact technical review. It does
not fix reviewer findings, retry a malformed review, or carry review state into
another invocation.

For an existing same-repository pull request, run from an up-to-date checkout
of its target branch:

```bash
bash scripts/ai-pr-loop 1732
```

The launcher accepts a PR number or URL. `--keep-worktree` preserves its
temporary candidate worktree. `--push` permits a guarded publication of the
exact approved candidate; the launcher never comments, resolves threads,
changes PR metadata, closes, or merges.

## Linear path

The launcher:

1. resolves the open PR and fetches its exact target and head;
2. stops with `BASE_UPDATE_REQUIRED` if the target advanced beyond the PR's
   recorded base;
3. creates separate candidate, trusted-tool, and validation directories;
4. validates required bugfix evidence when the PR is classified as a bugfix;
5. runs proportional `agent-check-pr` validation exactly once;
6. collects current unresolved GitHub findings exactly once;
7. invokes the technical reviewer once with the exact base SHA, candidate SHA,
   complete diff/worktree fingerprint, task/PR context, bugfix evidence, and
   GitHub findings as untrusted context; and
8. after approval, performs one adjacent final target/head refresh and, in
   `--push` mode, publishes only with an exact `--force-with-lease`.

The happy path therefore has one validation call, one reviewer-provider call,
and no fixer-provider call. A validation failure stops before collection and
review. `FINDINGS` preserves the candidate worktree and stops without another
validation or provider call. After an engineer changes the candidate, the next
ordinary invocation validates and reviews the new exact commit from scratch.

## Exact review target

`review-loop` requires an explicit `--base` and a mechanically bound candidate
worktree. After validation it records:

- the resolved base SHA and merge base;
- the exact candidate HEAD;
- staged, unstaged, and untracked scope; and
- a SHA-256 fingerprint of the complete reviewed state.

The reviewer copies the exact head and fingerprint into its structured result.
The orchestrator rejects a mismatch, malformed output, provider failure, or any
reviewer mutation of the candidate or immutable finding snapshot as
`REVIEW_FAILED`. The only valid review decisions are `APPROVE` and `FINDINGS`.

## Findings and recovery

The workflow never invokes `ai-fix-claude` or another fixer. Findings are the
handoff to the engineering agent or user. Preserve the branch/worktree, apply
and commit the fixes, then invoke the ordinary flow again.

Git is the recovery mechanism. A new invocation establishes the current target,
runs fresh bugfix evidence and proportional validation, collects current GitHub
findings, and obtains a new exact final review. No review or validation state is
reused across invocations.

## Worktree and publication safety

The candidate worktree remains distinct from the primary checkout and is bound
to the expected PR and SHA before collector, validator, or reviewer subprocesses.
One outer root-integrity runner checks the primary checkout before and after the
complete linear validation/findings/review run.
See [AI PR worktree isolation](ai-worktree-isolation.md) for the low-level
binding contract.

In `--push` mode, the launcher requires a clean reviewed candidate, verifies
that it descends from the original PR head, refreshes the target and remote
head, and pushes with:

```text
--force-with-lease=refs/heads/<head>:<original-head>
```

Any target advance requires manual synchronization followed by a fresh linear
invocation.

## Exit results

`review-loop` emits `APPROVE`, `FINDINGS`, `VALIDATION_FAILED`, or
`REVIEW_FAILED`. `ai-pr-loop` maps findings to `NOT_READY`, preserves the
candidate worktree, and additionally emits target-freshness and guarded-push
results. Automation should key on the emitted result line rather than treating
an exit code as the complete status protocol.
