# AI review / fix loop

The review loop automates only the mechanical handoff between an implementation agent and a reviewer. It does **not** grant an agent permission to broaden scope, change product behavior, weaken repository invariants, or resolve genuine design choices without a human.

Run it through the repository-pinned environment:

```bash
bash scripts/review-loop \
  --review-cmd '<review command>' \
  --fix-cmd '<fix command>'
```

The commands are intentionally provider-agnostic. They may wrap Codex, Claude, another local agent, or a test double.

Each invocation creates a persistent, unique directory below `build/ai-review-loop/`. Review and fix payloads from concurrent or subsequent runs therefore never share file names. The selected directory is printed when the loop starts.

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
- `AI_REVIEW_WORKTREE_FINGERPRINT` — SHA-256 fingerprint of that commit, all tracked changes, and all non-ignored untracked file contents outside the selected state directory;
- `AI_REVIEW_PREVIOUS_RESULT` — previous pass JSON path on re-review only.

The reviewer must follow [ai-review.md](ai-review.md) and write:

```json
{
  "decision": "REQUEST_CHANGES",
  "head": "0123456789abcdef",
  "worktree_fingerprint": "f5e6d7c8b9a00123456789abcdef0123456789abcdef0123456789abcdef0123",
  "residual_risk": "MEDIUM",
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

`auto_fixable` means the repository already determines the intended result and the change is local/reversible. It must be `false` when resolving the finding requires a product choice, changes an invariant, selects between conflicting authoritative sources, broadens subsystem scope, or otherwise requires human judgment.

The orchestrator rejects unknown JSON fields, missing required fields (including both boolean flags), inconsistent reviewer output such as `APPROVE` with blocking findings, and `REQUEST_CHANGES` without any blocking finding.

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

After a successful fix command, the orchestrator runs `bash scripts/agent-check` by default. A validation failure stops the loop immediately. Only after validation succeeds does it invoke the reviewer again.

## Exit codes

| Code | Meaning |
|---|---|
| `0` | `READY_FOR_HUMAN_REVIEW` |
| `1` | orchestration/reviewer/fixer/validation error |
| `2` | `HUMAN_DECISION_REQUIRED` |
| `3` | `AUTO_FIX_REQUIRED` but no `--fix-cmd` was supplied |

## Safety boundary

This script deliberately does not post comments, resolve threads, push commits, or merge pull requests. GitHub write automation belongs in a later integration layer after this local state machine has proven stable.
