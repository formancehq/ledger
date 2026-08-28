# Repository invariant gates

Some repository rules are precise enough to enforce mechanically. The
`scripts/check-repo-invariants` entry point turns that narrow set into a shared
local and CI gate. It runs from `scripts/agent-check` and from the dedicated
`Repository Invariants` workflow.

The checker deliberately covers tracked files and non-ignored untracked files.
This keeps a local agent handoff equivalent to the committed tree that CI will
inspect and prevents a newly created file from escaping validation merely
because it has not been staged yet.

## Enforced rules

The current rules are:

1. Go test files must not call `time.Sleep`. Tests should synchronize on the
   state they need or use `require.Eventually` / `require.Never` with a bounded
   deadline.
2. Production Go files under the deterministic FSM boundary must not call
   `os.Getenv`, `os.LookupEnv`, or `os.Environ`. Node-local configuration may
   gate admission but cannot affect how a committed entry is applied.
3. Protocol definitions under `misc/proto` must not declare `reserved` fields
   while Ledger v3 remains unreleased and field numbers are kept sequential.
4. The nested monitoring-dashboard Go tests must remain reachable from the
   Default CI workflow. The dashboard test recipe regenerates the committed
   dashboards before testing them, and `pre-commit` must continue to invoke
   that recipe.
5. The `Default` GitHub Actions workflow must retain an unconditional
   `Tests-Operator` job that runs the full default-tag `misc/operator` unit
   suite through the pinned Nix toolchain. The command is kept explicit so
   the nested Go module cannot silently fall outside root-module `./...` tests.

The FSM boundary is recursive and consists of:

- `internal/infra/state/`
- `internal/infra/plan/`
- `internal/infra/preload/`
- `internal/domain/processing/`

## Why the checker parses source

The Go checks use `go/parser` rather than matching raw text. This ignores
comments and string literals, recognizes explicit and dot-import aliases, and
distinguishes an imported package name from a shadowing local variable. Files
are parsed independently, so build tags do not remove a file from the check.

The protobuf check masks comments and quoted strings before detecting the
`reserved` keyword. Declarations remain detectable when the keyword and its
field numbers are split across lines.

The dashboard reachability check inspects the tracked nested module, Just
recipe metadata, and the Default workflow. It fails if the module loses its
tracked tests, generation no longer precedes the tests, the nested
`go test ./...` command disappears, or CI stops invoking `pre-commit`.

The operator-test reachability check parses the workflow as YAML and requires
the exact argument-free unit-test command. It rejects job or step conditions,
allowed failures, non-Nix execution, and added build tags; integration/envtest
and Chainsaw coverage remain separate suites.

## Extending the gate

Only add rules with an unambiguous syntactic boundary and a low false-positive
rate. Add focused tests under `scripts/check-repo-invariants_test.go` for both
accepted and rejected forms. Semantic rules that require runtime or architectural
judgment remain review concerns rather than repository gates.

Run the focused gate with:

```bash
nix develop --command bash scripts/check-repo-invariants
```

The full definition of done remains `scripts/agent-check` (or
`scripts/agent-check-full` when the full unit suite is required).
