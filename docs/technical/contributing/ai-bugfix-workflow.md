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

Before exact review, the loop runs applicable base-pinned normalization to a
one-replay fixpoint. Exact-state targeted validation after review must leave the
candidate clean; the workflow never resets generated changes to hide them and
does not replay an identical pre-commit again before push. The launcher then
re-fetches the target and
requires it to remain at the run's immutable base before guarded publication.
Any target advance requires explicit synchronization and a fresh trust pipeline,
regardless of whether the changed paths overlap the candidate.
