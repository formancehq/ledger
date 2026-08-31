# AI bugfix workflow

Runtime bugfixes follow a fail-closed sequence:

`DISCOVER → REPRODUCE → FIX → VALIDATE → PRE-COMMIT → REVIEW → GUARDED PUSH`

Discovery must record one of `DIRECT_FIX`, `PARTIAL_FIX`,
`RELATED_BUT_DIFFERENT`, `STALE_OR_INVALID`, `ALREADY_FIXED`, or
`NO_EXISTING_WORK`. A bugfix evidence record must also contain:

```text
BEFORE_FIX: BUG_REPRODUCED | REPRODUCTION_BLOCKED | ALREADY_FIXED
AFTER_FIX: PASS
```

When a validation failure occurs, classify it as
`BASELINE_FAILURE`, `CANDIDATE_CAUSED`, `ENVIRONMENTAL`, or `UNKNOWN` before
attributing it to the candidate. An unclassified important failure is not
ready for publication. Non-bugfix changes may omit bug reproduction evidence.

Before Go validation, `scripts/ai-bugfix-gate environment` compares the
effective `go version` with the repository `go.mod` directive, reports the
selected binary and `GOROOT`, and rejects a host toolchain selected outside
the repository environment. Being inside `nix develop` alone is not evidence
of the effective toolchain.

Immediately before a guarded push, the loop runs `just pre-commit` and checks
`git status --porcelain`. If pre-commit changes files, the candidate must be
updated and re-reviewed; the workflow never resets those changes to hide them.
After final pre-commit and exact review, the launcher re-fetches the target and
requires it to remain at the run's immutable base before guarded publication.
Any target advance requires explicit synchronization and a fresh trust pipeline,
regardless of whether the changed paths overlap the candidate.
