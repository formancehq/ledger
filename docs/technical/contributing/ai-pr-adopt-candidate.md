# Guarded AI PR candidate adoption

`ai-pr-adopt-candidate` is the recovery path for candidate bytes that already
exist in a local commit after an interrupted or exhausted fix loop. It is not a
review bypass or a second, weaker publication workflow.

Adoption reuses the implementation commit, not the trust receipts produced by
the interrupted run. An old exact review, validation result, pre-commit receipt,
same-run validation receipt, or bot approval never authorizes adoption. The
launcher re-establishes the relevant publication guarantees from scratch for
the exact candidate SHA and never runs a fixer.

## Preconditions

- the PR is open and same-repository;
- the target branch still equals the PR's GitHub-reported base SHA;
- the remote PR head still equals the GitHub-reported head SHA;
- the candidate commit exists locally;
- the candidate is a strict descendant of that PR head.

If the target advanced, return `BASE_UPDATE_REQUIRED`. This rule applies even
when the target and candidate changed disjoint paths. If the PR head moved or
the candidate is not a descendant, refuse adoption.

After those identity checks, adoption runs the same base-pinned publication
precondition helper as `ai-pr-loop`. The helper:

- performs legitimacy triage and continues only for `KEEP`;
- snapshots qualifying blocking GitHub findings for the exact verified PR head;
- uses the normal bugfix-intent rules and enforces `DISCOVERY`, `BEFORE_FIX`,
  `AFTER_FIX`, and conditional baseline classification through
  `ai-bugfix-gate`;
- records fresh run-local outputs and refuses to reuse pre-existing outputs.

The exact-candidate review then runs through
`ai-review-known-findings`. Every snapshotted finding is reconciled against the
candidate even when the fresh base review does not rediscover it. A
`STILL_VALID` blocker or unresolved human decision prevents approval.

## Policy comparison

| Gate | Normal `ai-pr-loop` | Adoption | Should adoption require it? | Why? |
|---|---|---|---:|---|
| PR/worktree binding | Immutable manifest and isolated worktree | Same binding for the supplied SHA | Yes | Review and validation must target the intended PR and commit. |
| Base/head freshness | Verified before the loop; guarded head lease before push | Verified before adoption; guarded head lease before push | Yes | A stale target or moved PR head invalidates the review target. |
| Legitimacy triage | Base-pinned shared precondition | Base-pinned shared precondition | Yes | Recovery does not establish that the change is legitimate. |
| Known findings | Fresh exact-head snapshot | Fresh exact-head snapshot | Yes | A prior explicit blocker cannot disappear through non-rediscovery. |
| Known-finding reconciliation | Every review pass | Exact-candidate review | Yes | Every known finding needs an evidence-based current classification. |
| Bugfix intent | Shared metadata classifier | Same classifier | Yes | A bugfix must not be relabeled by choosing another launcher. |
| `DISCOVERY` | Required for bugfixes | Required for bugfixes | Yes | Existing-work classification is a publication precondition. |
| `BEFORE_FIX` | Required for bugfixes | Required for bugfixes | Yes | Adoption cannot bypass reproduction evidence. |
| `AFTER_FIX` | Required for bugfixes | Required for bugfixes | Yes | The claimed fix still needs passing after-fix evidence. |
| Baseline classification | Required when validation failure makes it applicable | Same conditional requirement | Yes, when applicable | Candidate attribution must remain explicit. |
| Candidate normalization | Post-fix validation plus exact-SHA replay | Exact-SHA validation owns the single pass | Yes | Adoption preserves bytes and therefore refuses if validation rewrites them. |
| Exact review | Final candidate commit is reviewed | Supplied candidate commit is reviewed | Yes | No earlier review receipt is inherited. |
| Validation | Runs for the exact approved candidate | Runs for the exact approved candidate | Yes | No earlier validation receipt is inherited. |
| Push binding | Clean reviewed SHA plus force-with-lease | Same descendant and lease checks | Yes | Publication must be an atomic update of the verified PR head. |

The engineering implementation/fix loop is intentionally not rerun. That work
already produced the candidate bytes and is the only part adoption recovers.

## Trusted-tool rollout

The shared precondition helper follows the same rollout rule as the
known-finding toolchain: a PR that introduces or changes base-pinned policy is
reviewed and published by the preceding target-branch toolchain. It cannot use
its candidate copy to authorize itself. After the change lands, PRs whose
historical base predates the helper must synchronize with the target branch and
rerun from scratch. The launcher reports `BASE_UPDATE_REQUIRED` before resolving
the helper whenever the target has advanced. If the helper is genuinely absent
from the selected, synchronized base-pinned graph, the result is `TOOLING_ERROR`,
never `HUMAN_DECISION_REQUIRED`. There is deliberately no fallback to the
candidate worktree. See
[Known external PR findings](ai-pr-known-findings.md#trusted-tool-rollout).

## Trust boundary

The candidate must not provide the precondition policy, triage adapter,
known-finding collector/reconciler, bugfix gate, reviewer, validator, or
review-loop binary that authorizes its own publication. Those tools are
loaded/built from a detached worktree at the exact verified PR base SHA.

The candidate is reviewed in a detached clean worktree. The review pass has no fixer. Local validation runs only after approval, through the base-pinned `agent-check-pr` path.

The startup base comparison cannot authorize a long-running adoption by itself.
Adoption re-fetches and compares the exact target again after validation and
before exact review, after review before that evidence can authorize approval,
and immediately before a guarded push. These
checks all compare with the immutable base established at startup; they never
move that expected base forward.

Adoption uses the same immutable PR/worktree manifest, explicit `env -C`
process cwd, root snapshot, cross-PR guard, Git mutation guard, and disjoint
validation directory described in
[AI PR worktree isolation](ai-worktree-isolation.md). It has no legacy path that
reviews the candidate from the checkout that launched adoption.

## Publication

Without `--push`, a successful run ends with `APPROVED_NOT_PUSHED`.

With `--push`, publication is allowed only if:

1. the target still equals the startup base immediately before exact review;
2. the exact candidate SHA was approved and validated;
3. the target still equals the startup base before that exact review becomes
   publication-authorizing evidence;
4. review/validation left its worktree clean and did not change HEAD;
5. exact-state validation leaves the candidate clean and unchanged;
6. the remote PR head still equals the original verified head;
7. the candidate still descends from that original head;
8. the target still equals the startup base immediately before push;
9. `git push --force-with-lease=<head-ref>:<original-head>` succeeds.

The force-with-lease is only a compare-and-swap guard. The candidate itself must be a normal descendant of the existing PR head.

If any last-mile target check observes an advance, adoption reports the
expected base, observed base, and candidate SHA, preserves the candidate
worktree, and returns `BASE_UPDATE_REQUIRED` without approving or pushing.
Synchronization is manual and must be followed by a fresh trust run. A review
of the pre-synchronization candidate is never reused.

## Non-goals

Candidate adoption does not:

- reconstruct or resume Claude state;
- trust old review artifacts;
- skip re-review or validation;
- resolve merge conflicts;
- synchronize a stale target branch;
- modify GitHub reviews/comments/threads;
- merge a PR.
