# AI PR worktree isolation

Every AI operation associated with a pull request is bound to an explicit Git
worktree before a provider process starts. This protects cooperative agents
from ordinary wrong-worktree, wrong-PR, and wrong-SHA mistakes.

## Directory roles

- The primary checkout is used for PR metadata, fetches, and worktree
  lifecycle. It may already be dirty, but agents must not change it.
- The candidate worktree is the unique detached worktree for the verified PR
  head. Reviewers and validation run there.
- `VALIDATION_RUN_DIR` is a unique non-worktree directory for temporary review
  and validation state. Shared Go and lint caches remain outside the candidate
  and validation directories as documented in [Local validation](local-validation.md).

`ai-pr-loop` creates
`.<repo>-ai-worktrees/pr-<pr>.<run>/{worktree,trusted-tools,validation}`. The
trusted-tools worktree is pinned to the verified PR base. The review-loop state
also lives in the run directory, outside the candidate.

## Identity contract

Git and three child environment variables are the identity authority:

```text
EXPECTED_PR_NUMBER
EXPECTED_WORKTREE
EXPECTED_HEAD
```

Before a reviewer, findings collector, or validator runs, `review-loop`
verifies that:

1. its process cwd, Git top-level, and canonical candidate path match;
2. candidate `HEAD` equals the expected SHA;
3. candidate and primary checkout are distinct worktrees in the same repo; and
4. validation and review-state directories are disjoint from protected roots.

There is no duplicate binding file or second set of identity environment
variables. `EXPECTED_HEAD` is candidate identity; it is not overloaded with a
target-base SHA.

## Root protection

The standalone `scripts/rootguard` runner is the single root-integrity
implementation. `ai-pr-loop` wraps the complete linear validation, findings,
and review child with one before snapshot and one after snapshot. The after
snapshot runs after every normal child result, including validation failure,
review failure, malformed output, and `FINDINGS`.

Each snapshot records the primary checkout's HEAD and branch, hashes porcelain
status plus staged and unstaged tracked diffs, and hashes the paths, modes, and
contents of non-ignored untracked files reported by Git. It uses six Git
processes. An untracked nested repository is hashed as the directory boundary
reported by Git without traversing its contents. Ignored paths, ignored build
artifacts, shared caches, and ignore configuration are not enumerated or
hashed.

This is a cooperative accidental-mistake boundary. It does not try to detect a
primary-checkout mutation restored exactly before the final snapshot, intercept
Git commands, or defend against deliberate same-user helper replacement. CI,
branch protection, required human review, and the exact publication lease are
the authoritative merge boundary.

## Launcher inventory

| Launcher/path | Provider cwd | Worktree owner |
| --- | --- | --- |
| `ai-pr-loop` final review | unique PR candidate | `ai-pr-loop` |
| `review-loop` | required `--worktree` | caller |
| `ai-review-codex` | bound candidate | caller |
| `agent-check-pr` | bound candidate | caller |

The normal entry point is `ai-pr-loop`. A low-level caller invoking
`review-loop` directly must create the candidate and validation directories and
provide:

```text
--pr
--worktree
--expected-head
--base
--trusted-root
--validation-run-dir
```

There is no fallback to the caller's cwd and no mode that authorizes the
primary checkout as the candidate.

## Recovery

Preserve an interrupted branch or worktree and inspect it with ordinary Git. A
new invocation establishes the current target, runs fresh evidence and
validation, collects current GitHub findings, and requires a fresh exact
technical review before a leased push. No historical review or validation
state is imported.
