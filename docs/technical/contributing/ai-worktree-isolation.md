# AI PR worktree isolation

Every AI operation associated with a pull request is bound to an explicit Git
worktree before a provider process starts. Prompt instructions are secondary;
the launcher enforces the boundary mechanically.

## Directory roles

The workflow keeps three directory roles separate:

- `TRUSTED_ROOT_CHECKOUT` is the primary checkout returned first by `git
  worktree list`. It is control-plane state only: PR metadata lookup, fetches,
  and creation/removal of workflow-owned worktrees. Its initial HEAD, branch,
  and complete porcelain status may already be dirty, but must remain byte-for-
  byte unchanged while agents run.
- `CANDIDATE_WORKTREE` is a unique detached worktree created for one PR and one
  expected HEAD. Reviewers, fixers, and PR validation run there. It must never
  equal `TRUSTED_ROOT_CHECKOUT`.
- `VALIDATION_RUN_DIR` is a unique non-worktree directory for `HOME`, Go caches,
  `TMPDIR`, the golangci-lint cache, and the Git guard wrapper. It must neither
  equal nor contain either checkout, and neither checkout may contain it.

`ai-pr-loop` uses
`.<repo>-ai-worktrees/pr-<pr>.<run>/{worktree,trusted-tools,validation}`.
`ai-pr-adopt-candidate` uses the corresponding
`adopt-<pr>.<run>/{candidate,trusted-tools,validation}` layout.

## Binding contract

The workflow writes an immutable, control-owned JSON binding outside the
candidate worktree:

```json
{
  "version": 1,
  "expectedPrNumber": 1771,
  "candidateWorktree": "/absolute/path/to/worktree",
  "expectedHead": "0123456789abcdef0123456789abcdef01234567",
  "trustedRootCheckout": "/absolute/path/to/primary-checkout"
}
```

It invokes `review-loop` with the same PR, paths, and SHA as explicit flags and
uses `env -C <candidate>` for the orchestrator process. Immediately before
every reviewer, fixer, or validator subprocess, `review-loop` verifies:

1. its process cwd, `git rev-parse --show-toplevel`, and the canonical
   candidate path are identical;
2. `git rev-parse HEAD` equals the expected SHA;
3. the manifest, flags, and environment all identify the same PR;
4. candidate and trusted root are different worktrees in the same Git common
   directory;
5. the validation directory is disjoint from both worktrees;
6. the binding file is unchanged and outside the candidate worktree.

Only then does it emit `WORKTREE_BINDING_GATE=PASS` and start the subprocess
with Go's `Cmd.Dir` set to the candidate. A path/root mismatch emits
`WORKTREE_BINDING_GATE=FAIL`; using the primary checkout emits
`ROOT_CHECKOUT_AS_CANDIDATE_FORBIDDEN`; a PR mismatch emits
`CROSS_PR_WORKTREE_CONTAMINATION`. All stop the run.

The subprocess receives both contracts explicitly:

```text
EXPECTED_PR_NUMBER
EXPECTED_WORKTREE
EXPECTED_HEAD
AI_WORKTREE_PR
AI_WORKTREE_PATH
AI_WORKTREE_EXPECTED_HEAD
```

Codex and Claude adapters re-check them, add the expected values to the trusted
prompt, and use `env -C "$EXPECTED_WORKTREE"` for the provider process. An
agent's `pwd` self-check is defense in depth, not the authorization mechanism.

## Root protection and Git guard

Before the first subprocess, `review-loop` captures `ROOT_HEAD`, `ROOT_BRANCH`,
and the complete `ROOT_STATUS`. It compares the same values immediately before
and after every reviewer, fixer, and validator. Any difference emits
`ROOT_MUTATION_DETECTED` and stops without reset, clean, checkout, or other
automatic recovery. An already-dirty root is supported because equality, not
cleanliness, is the invariant.

The validation directory contains a `git` wrapper at the front of agent
`PATH`. It allows read-only Git commands, but rejects these mutations when they
target `TRUSTED_ROOT_CHECKOUT`: `switch`, `checkout`, `add`, `commit`, `merge`,
`rebase`, `cherry-pick`, `reset`, `clean`, `restore`, and branch
creation/deletion/move/copy. Workflow worktree mutations bypass that agent
`PATH`; agent-issued `git worktree add/remove/move/prune/repair/lock/unlock` are
always rejected as unregistered child worktrees.

Detached worktrees for baseline comparison, read-only experiments, and
`BEFORE_FIX` reproduction remain valid when the workflow creates them. The
triage adapter is the model: it creates and owns separate trusted-policy and
untrusted-head worktrees, explicitly binds the read-only Codex cwd to the
trusted one, and removes both on exit. An agent cannot silently replace its
primary candidate worktree with another worktree.

## Launcher inventory

The table describes the enforced state after this mechanism. “Created by
workflow” identifies the component that owns creation; lower-level adapters
consume the binding rather than create another candidate.

| Launcher/path | `CURRENT_CWD_SOURCE` | `WORKTREE_CREATED_BY_WORKFLOW` | `CWD_EXPLICITLY_BOUND` |
|---|---|---:|---:|
| `ai-pr-loop` technical review/fix | unique PR candidate; `env -C` plus review-loop flags | YES | YES |
| `ai-pr-loop` legitimacy triage | workflow-owned base-pinned trusted worktree; head is separate read-only evidence | YES | YES |
| `ai-pr-adopt-candidate` | unique detached worktree at the supplied candidate SHA | YES | YES |
| `review-loop` | required `--worktree`; shell wrapper uses `env -C`, Go uses `Cmd.Dir` | NO, caller owns it | YES |
| `ai-review-codex` | `EXPECTED_WORKTREE`, re-verified against Git top-level | NO, caller owns it | YES |
| `ai-review-known-findings` base review | candidate via `ai-review-codex` | NO, caller owns it | YES |
| `ai-review-known-findings` reconciliation | base-pinned trusted child worktree, read-only | YES, outer PR workflow | YES |
| `ai-fix-claude` | `EXPECTED_WORKTREE`, re-verified against Git top-level | NO, caller owns it | YES |
| `agent-check-pr` | candidate via review-loop `Cmd.Dir`, with its own secondary gate | NO, caller owns it | YES |
| `ai-audit`, `ai-audit-challenge`, `ai-audit-jira` | exact clean audit HEAD or publication directory under their separate contracts | N/A: not PR candidate workflows | YES where a provider is used; read-only |

Before this enforcement, the high-level launchers created worktrees but relied
on shell `cd`; `review-loop` left `Cmd.Dir` unset, so a direct launch from the
primary checkout made that checkout available as the agent cwd. The regression
fixture records this as `BEFORE_FIX: ROOT_CWD_AVAILABLE_TO_AGENT`. The fixed
fixture requires `WORKTREE_BINDING_GATE=PASS` and `ROOT_UNCHANGED=PASS`.

## Low-level review-loop invocation

`ai-pr-loop` and `ai-pr-adopt-candidate` are the supported entry points. A test
or another trusted launcher invoking `review-loop` directly must first create
the candidate and validation directories plus the binding JSON, then provide
all binding flags:

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
