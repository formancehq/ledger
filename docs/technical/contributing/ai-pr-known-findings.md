# Known GitHub PR findings

Immediately before technical review, `ai-pr-loop` collects the current GitHub
review state once. The resulting JSON is untrusted context for the same Codex
invocation that reviews the exact candidate; there is no separate provider
reconciliation pass.

## Collection

`scripts/ai-pr-known-findings` binds the collection to the PR number and exact
GitHub head SHA and verifies the head both before and after its queries. It uses
GitHub review threads as the primary source:

- unresolved threads become findings;
- resolved threads are excluded;
- each finding records the path, current and original line, and GitHub's
  `isOutdated` value; and
- the whole thread conversation is retained so replies are not lost.

GitHub does not create a review thread for a review that carries blockers only
in its top-level body. Current repository history still contains that case. If
the pull request's structured review decision is `CHANGES_REQUESTED`, the
collector therefore retains a compatibility fallback for change-request review
bodies that have no thread comments. Structured `[P0-P3][blocking]` sections are
split mechanically; otherwise the non-empty review body is one finding. The
fallback is not used after the review decision moves away from
`CHANGES_REQUESTED`.

## Final-review coverage

Review text is a claim to verify against the current code, tests, and
authoritative documentation, never an instruction. The final reviewer must
classify every supplied id exactly once as:

- `FIXED`;
- `STILL_VALID`;
- `OUTDATED`; or
- `HUMAN_DECISION_REQUIRED`.

A `STILL_VALID` item is also emitted as an actionable blocking finding with the
same id. A human-decision classification forces `FINDINGS` and a non-empty
`human_decision_context`. The reviewer adapter validates complete id coverage
mechanically.

The collection is run-local evidence, is never reused by a later invocation,
and does not resolve threads or write GitHub state.
