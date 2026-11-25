# Bugfix workflow

Runtime bugfixes follow this sequence:

`DISCOVER → REPRODUCE → FIX → VALIDATE → REVIEW`

Record the original failure and verify that the regression test fails before
the fix and passes after it. If reproduction is blocked, explain why and state
what evidence supports the fix.

When a validation failure occurs, classify it as
`BASELINE_FAILURE`, `CANDIDATE_CAUSED`, `ENVIRONMENTAL`, or `UNKNOWN` before
attributing it to the candidate. An unclassified important failure is not
ready for publication. Non-bugfix changes may omit bug reproduction evidence.

Before Go validation, `scripts/ai-bugfix-gate environment` verifies that the
effective Nix-provided `go version` is at least the minimum language version
declared by the repository `go.mod`, then reports the selected binary and
`GOROOT`. It rejects an older compiler or a host toolchain selected outside the
repository environment. Being inside `nix develop` alone is not evidence of
the effective toolchain.

Before review, run `bash scripts/agent-check` and the focused tests appropriate
to the fix. For a PR candidate, run `AI_REVIEW_BASE_SHA=<base-sha> bash
scripts/agent-check-pr` against the exact target base. Inspect any generated
changes before publishing.
