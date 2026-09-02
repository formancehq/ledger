# AI PR worktree isolation

Every AI operation associated with a pull request is bound to an explicit Git
worktree before a provider process starts. Prompt instructions are secondary;
the launcher enforces the boundary mechanically.

## Directory roles

The workflow keeps three directory roles separate:

- `TRUSTED_ROOT_CHECKOUT` is the primary checkout returned first by `git
  worktree list`. It is control-plane state only: PR metadata lookup, fetches,
  and creation/removal of workflow-owned worktrees. Its initial HEAD, branch,
  complete porcelain status, and workspace-content fingerprint may already be
  dirty, but must remain byte-for-byte unchanged while agents run.
- `CANDIDATE_WORKTREE` is a unique detached worktree created for one PR and one
  expected HEAD. Reviewers, fixers, and PR validation run there. It must never
  equal `TRUSTED_ROOT_CHECKOUT`.
- `VALIDATION_RUN_DIR` is a unique non-worktree directory for `HOME`, Go caches,
  `TMPDIR`, the golangci-lint cache, and the Git guard wrapper. It must neither
  equal nor contain either checkout, and neither checkout may contain it.

The PR launchers also place each `review-loop` state directory in their
workflow-owned run directory, outside `CANDIDATE_WORKTREE`. This lets exact-state
validation receipts cover ignored/generated candidate inputs while excluding
only the loop's own changing review JSON from candidate state. Fixer and
re-review inputs that must remain project-local are copied through an `os.Root`
capability into a run-unique ignored candidate directory before validation;
those immutable copies are included in the candidate receipt fingerprint.

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
6. the binding file is unchanged and outside the candidate worktree;
7. the review state directory resolves outside the trusted root and its Git
   common directory before any state path is created.

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

For the ordinary technical review/fix/validation path, the reviewed candidate
is also the provider cwd, so both pairs are equal. Trusted-policy passes such as
legitimacy triage and known-finding reconciliation intentionally execute from a
base-pinned worktree while inspecting a separate PR/candidate worktree. In
those passes, `EXPECTED_WORKTREE` / `EXPECTED_HEAD` retain target semantics,
while `AI_WORKTREE_PATH` / `AI_WORKTREE_EXPECTED_HEAD` bind the provider cwd and
trusted-tool HEAD. `EXPECTED_HEAD` must never carry the target-base SHA merely
because the provider executes from the base-pinned worktree.

## Root protection and Git guard

Before the first subprocess, `review-loop` captures `ROOT_HEAD`, `ROOT_BRANCH`,
the complete `ROOT_STATUS`, and a workspace-content fingerprint covering
tracked, staged, untracked, and ignored files reported by Git. Ignored nested
repositories/worktrees are treated as separate roots rather than recursively
hashed. It compares the same values
immediately before and after every reviewer, fixer, and validator. The
fingerprint catches content overwrites even when the porcelain status text is
unchanged. Any
difference emits `ROOT_MUTATION_DETECTED` and stops without reset, clean,
checkout, or other automatic recovery. An already-dirty root is supported
because equality, not cleanliness, is the invariant.

`scripts/internal/rootguard` is the single trusted implementation used by both
`review-loop` and legitimacy triage. Each snapshot has the following exact
semantics:

- `HEAD`, the current branch, and NUL-delimited porcelain v1 status are captured
  independently;
- staged and unstaged state use binary, full-index diffs;
- non-ignored untracked and ignored paths come from separate NUL-delimited Git
  enumerations, are sorted by their raw path bytes, and are opened relative to
  the trusted root;
- every Git-reported path uses `Lstat`; its mode/type is hashed, regular-file
  bytes are streamed through SHA-256 in-process, and a symlink contributes its
  target text without following the link;
- the repository `info/exclude` file and every configured `core.excludesFile`
  are content-aware snapshot inputs, including semantically inert mutations
  that happen not to change the enumerated path set; and
- enumeration, stat, open, read, and post-read identity errors fail closed.

The implementation launches exactly nine Git processes per snapshot for Git
state and path enumeration, independent of the number of regular ignored or
untracked files. It never launches Git once per file. Ignored nested Git
repositories/worktrees remain a single directory entry in their parent's Git
enumeration and are therefore explicit separate protection roots.

This consolidates two former algorithms. The old triage shell implementation
hashed regular files by invoking `git hash-object --no-filters` once per file
and used Git's object hash (SHA-1 in this repository). The former Go
review-loop implementation already used in-process SHA-256 and included
ignored-path mode/type. The consolidated
contract keeps the stronger Go semantics and intentionally strengthens both
callers with raw-byte sorting, NUL-delimited status/path handling, root-scoped
opens, and direct ignored-configuration capture.

Snapshots are repeated at the same existing process boundaries. They are not
continuous filesystem monitoring: a complete mutation and restoration wholly
between two observation boundaries remains outside this contract.

The validation directory contains a defense-in-depth `git` wrapper at the
front of agent `PATH`. Within that wrapper, `TRUSTED_ROOT_CHECKOUT` is
deny-by-default: only an enumerated set of read-only inspection commands is
allowed. Known mutations such as
`switch`, `checkout`, `add`, `commit`, `merge`, `rebase`, `cherry-pick`,
`reset`, `clean`, `restore`, `stash`, `apply`, `revert`, `am`, branch
creation/deletion/move/copy, output-writing/external-command options, unsafe
global configuration overrides, and any unknown subcommand are rejected.
Workflow worktree mutations bypass that agent `PATH`; agent-issued `git worktree
add/remove/move/prune/repair/lock/unlock` against the protected repository are
rejected as unregistered child worktrees. Independent temporary Git repositories
used by validation fixtures may manage their own worktrees. The subprocess does
not receive dedicated variables exposing the real Git path or original `PATH`.

The wrapper is not an OS filesystem sandbox: a subprocess with host access can
deliberately locate an absolute Git executable or write a root file without
using Git. The authoritative enforcement for that threat is the resident before/after
root snapshot, which emits `ROOT_MUTATION_DETECTED` and stops the workflow. It
does not claim to prevent or roll back a hostile same-user process. Tests cover
both wrapper refusal and detection of a deliberate direct-Git bypass.

Detached worktrees for baseline comparison, read-only experiments, and
`BEFORE_FIX` reproduction remain valid when the workflow creates them. The
triage adapter is the model: it creates and owns separate trusted-policy and
untrusted-head worktrees, explicitly binds the read-only Codex cwd to the
trusted one, applies the same content-aware root snapshot, and removes both on
exit. Triage builds its resident runner from the base-pinned trusted tooling
worktree with the pinned Nix Go toolchain before starting the provider. The
same in-memory process owns both snapshots, so replacing an on-disk helper while
the provider runs cannot select the post-snapshot implementation. An agent
cannot silently replace its primary candidate worktree with another worktree.

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
| `ai-audit`, `ai-audit-challenge`, `ai-audit-jira` | exact clean audit HEAD or publication directory under their separate, non-PR contracts | N/A: not PR candidate workflows | NO under the PR binding contract; audit adapters remain read-only |

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

The supported PR launchers additionally provide `--validation-tool-root` and
`--validation-gates-cmd`, which enable process-local exact-state validation
receipts. A low-level caller that omits both remains safe but always executes
the readiness validator; supplying only one is rejected.
