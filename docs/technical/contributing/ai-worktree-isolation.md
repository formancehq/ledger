# AI PR worktree isolation

Every AI operation associated with a pull request is bound to an explicit Git
worktree before a provider process starts. This protects cooperative agents
from ordinary wrong-worktree, wrong-PR, and wrong-SHA mistakes.

## Directory roles

- `TRUSTED_ROOT_CHECKOUT` is the primary checkout used for PR metadata,
  fetches, and worktree lifecycle. It may already be dirty, but agents must not
  change it.
- `CANDIDATE_WORKTREE` is the unique detached worktree for the verified PR
  head. Reviewers and validation run there.
- `VALIDATION_RUN_DIR` is a unique non-worktree directory for temporary review
  and validation state. Shared Go and lint caches remain outside all three
  directories as documented in [Local validation](local-validation.md).

`ai-pr-loop` creates
`.<repo>-ai-worktrees/pr-<pr>.<run>/{worktree,trusted-tools,validation}`. The
trusted-tool worktree is pinned to the verified PR base. The review-loop state
also lives in the run directory, outside the candidate.

## Binding contract

The launcher writes an immutable run-local binding:

```json
{
  "version": 1,
  "expectedPrNumber": 1771,
  "candidateWorktree": "/absolute/path/to/worktree",
  "expectedHead": "0123456789abcdef0123456789abcdef01234567",
  "trustedRootCheckout": "/absolute/path/to/primary-checkout"
}
```

Before a reviewer, findings collector, or validator runs, `review-loop` verifies that:

1. its process cwd, Git top-level, and canonical candidate path match;
2. candidate `HEAD` equals the expected SHA;
3. flags, environment, and binding identify the same PR;
4. candidate and primary checkout are distinct worktrees in the same repo;
5. validation and review-state directories are disjoint from protected roots;
6. the binding file is unchanged; and
7. the primary checkout still has its original HEAD, branch, status, content,
   and ignore configuration.

The provider receives:

```text
EXPECTED_PR_NUMBER
EXPECTED_WORKTREE
EXPECTED_HEAD
AI_WORKTREE_PR
AI_WORKTREE_PATH
AI_WORKTREE_EXPECTED_HEAD
```

For ordinary review/validation, both worktree/head pairs name the same
candidate. `EXPECTED_HEAD` is candidate identity; it is not overloaded with a
target-base SHA.

## Root protection and Git guard

The shared `scripts/internal/rootguard` implementation snapshots the primary
checkout before and after provider and validator boundaries. It covers tracked,
staged, untracked, and ignored paths plus ignore configuration, hashes file
contents in-process, records symlink targets without following them, and fails
closed on enumeration or read errors.

The validation directory puts a Git wrapper first on agent `PATH`. It allows a
small read-only command set in the protected primary checkout and rejects
mutating or unknown commands there. Candidate Git operations and independent
fixture repositories remain available where the workflow permits them.

This is an accidental-mistake boundary, not an OS sandbox against deliberate
same-user bypass. The before/after snapshot catches persistent root mutation;
GitHub review, CI, branch protection, and human review remain the durable
authorities.

## Launcher inventory

| Launcher/path | Provider cwd | Worktree owner |
| --- | --- | --- |
| `ai-pr-loop` final review | unique PR candidate | `ai-pr-loop` |
| `review-loop` | required `--worktree` | caller |
| `ai-review-codex` | bound candidate | caller |
| `agent-check-pr` | bound candidate | caller |

The normal entry point is `ai-pr-loop`. A low-level caller invoking
`review-loop` directly must create the candidate and validation directories and
provide all binding flags:

```text
--pr
--worktree
--expected-head
--trusted-root
--binding-file
--validation-run-dir
--git-guard
```

There is no fallback to the caller's cwd and no mode that authorizes the
primary checkout as the candidate.

## Recovery

Preserve an interrupted branch or worktree and inspect it with ordinary Git.
A new invocation establishes the current target, runs fresh evidence and
validation, collects current GitHub findings, and requires a fresh exact
technical review before a leased push. No historical review or validation
state is imported.
