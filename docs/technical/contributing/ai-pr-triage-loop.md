# PR legitimacy gate in `ai-pr-loop`

`ai-pr-loop` runs the advisory legitimacy triage before the expensive review/fix loop.

The triage policy and adapter are always loaded from the exact verified PR base commit, never from the proposed head. The launcher binds triage to the same immutable base/head SHAs that will be reviewed.

## Outcomes

- `KEEP`: continue into the normal Codex review / Claude fix / validation loop.
- `QUESTION`: stop before review with `AI_PR_LOOP_RESULT: HUMAN_DECISION_REQUIRED`.
- `REJECT`: stop before review with `AI_PR_LOOP_RESULT: LEGITIMACY_REJECTED`. This is advisory only; no PR is closed or commented on.

A triage provider error or target mismatch fails closed and does not start technical review.

The triage result is persisted in the launcher run directory and its path is printed for inspection.

## Trust boundary

The PR head may not choose or modify the triage policy that authorizes its own review. `ai-pr-loop` therefore uses a base-pinned trusted triage adapter for every invocation, not only guarded publish mode.

`ai-pr-triage` accepts optional launcher-provided expected base/head SHAs. It still resolves current GitHub metadata itself, but refuses to run if those SHAs no longer match. This turns a concurrent PR update into a safe restart instead of triaging one state and reviewing another.

The base-pinned triage adapter may predate that binding, so `ai-pr-loop` does not rely on the tool enforcing it: before accepting any decision, the launcher requires the result's `base_sha` and `head` to equal the SHAs it fetched and verified. A result describing another PR state is a target mismatch and stops the run.

The triage provider itself starts with `env -C` in the workflow-created trusted
worktree, never in the primary checkout. Immediately before launch, the adapter
checks the trusted top-level and base HEAD. `EXPECTED_WORKTREE` and
`EXPECTED_HEAD` identify the read-only PR evidence worktree and actual PR head;
`AI_WORKTREE_PATH` and `AI_WORKTREE_EXPECTED_HEAD` independently bind the
provider cwd to the trusted worktree and target-base HEAD. `TARGET_BASE_SHA` and
`PR_HEAD_SHA` expose those identities without overloading either one. The
adapter uses the shared, resident full-content rootguard around Codex and
reports `ROOT_MUTATION_DETECTED` on any change. The same process owns both
snapshots; scope and observation frequency match the technical review
root-protection contract.
