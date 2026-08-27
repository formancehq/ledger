# Guarded AI PR candidate adoption

`ai-pr-adopt-candidate` is the recovery path for a candidate commit that already exists locally after an interrupted or exhausted fix loop.

It does **not** trust a claim that the candidate was previously validated. It re-establishes the publication guarantees from scratch for the exact candidate SHA and never runs a fixer.

## Preconditions

- the PR is open and same-repository;
- the target branch still equals the PR's GitHub-reported base SHA;
- the remote PR head still equals the GitHub-reported head SHA;
- the candidate commit exists locally;
- the candidate is a strict descendant of that PR head.

If the target advanced, return `BASE_UPDATE_REQUIRED`. If the PR head moved or the candidate is not a descendant, refuse adoption.

## Trust boundary

The candidate must not provide the policy, reviewer, validator, or review-loop binary that authorizes its own publication. Those tools are loaded/built from a detached worktree at the exact verified PR base SHA.

The candidate is reviewed in a detached clean worktree. The review pass has no fixer. Local validation runs only after approval, through the base-pinned `agent-check-pr` path.

Adoption uses the same immutable PR/worktree manifest, explicit `env -C`
process cwd, root snapshot, cross-PR guard, Git mutation guard, and disjoint
validation directory described in
[AI PR worktree isolation](ai-worktree-isolation.md). It has no legacy path that
reviews the candidate from the checkout that launched adoption.

## Publication

Without `--push`, a successful run ends with `APPROVED_NOT_PUSHED`.

With `--push`, publication is allowed only if:

1. the exact candidate SHA was approved and validated;
2. review/validation left its worktree clean and did not change HEAD;
3. the remote PR head still equals the original verified head;
4. the candidate still descends from that original head;
5. `git push --force-with-lease=<head-ref>:<original-head>` succeeds.

The force-with-lease is only a compare-and-swap guard. The candidate itself must be a normal descendant of the existing PR head.

## Non-goals

Candidate adoption does not:

- reconstruct or resume Claude state;
- trust old review artifacts;
- skip re-review or validation;
- resolve merge conflicts;
- synchronize a stale target branch;
- modify GitHub reviews/comments/threads;
- merge a PR.
