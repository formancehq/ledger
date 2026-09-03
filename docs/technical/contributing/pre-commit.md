# Input-sensitive pre-commit normalization

`just pre-commit` is a normalization fixpoint, not merely a list of checks. Its
trusted runner is implemented in `scripts/precommit/`; the `justfile` retains
the individual recipes that the runner executes.

Local invocations without an exact target SHA deliberately run every component.
PR automation may skip a component only when it invokes the selector from a
clean worktree pinned to the exact target commit and supplies that commit as
`PRECOMMIT_BASE_SHA`. `scripts/agent-just` pins the recipe and selector to that
trusted worktree. The candidate's `justfile`, selector source, dependency maps,
and dotenv files are never used to decide what may be skipped.

## Trusted component map

Every current mapping is `COMPLETE`. A partial or unknown mapping must not be
used for skipping.

| Component | Inputs | Tool identity and configuration | Committed outputs |
|---|---|---|---|
| Fuzz inventory | every `*_test.go`, every `go.mod`, `scripts/fuzz-targets.txt` | target SHA; invariant/fuzz scanner source; pinned Go toolchain | none |
| Mock/code generation | trusted Go packages containing `//go:generate`, any new/changed directive source, generated protobuf Go, root `go.mod`/`go.sum`; the fixpoint fingerprint covers all root Go | target SHA; `go generate` recipe; pinned `mockgen` | `internal/**/*_generated.go`, `internal/**/*_generated_test.go` |
| Protobuf generation | `misc/proto/*.proto`; every custom `tools/protoc-gen-*` module | target SHA; complete protoc recipe/options; `flake.nix`/`flake.lock` | generated `internal/proto/**/*.pb.go` |
| Operator generation | operator Go excluding generated outputs; operator module manifests; RBAC sync script | target SHA; controller-gen version/recipe; pinned Go toolchain | deepcopy Go, CRDs, RBAC, copied chart CRDs, chart ClusterRole |
| Dashboards | dashboard Jsonnet, lockfile and vendor identity; dashboard Go tests/module manifests | target SHA; Jsonnet/JB recipe and pinned tools | `config/dashboards/*.json` |
| Root tidy | every root-module Go file; root module manifests | target SHA; pinned Go toolchain | root `go.mod`/`go.sum` |
| Operator tidy | every operator Go file; operator module manifests | target SHA; pinned Go toolchain | operator `go.mod`/`go.sum` |
| Model-workload tidy | every workload Go file; workload module manifests | target SHA; pinned Go toolchain | workload `go.mod`/`go.sum` |
| Root lint | every root-module Go file; root module manifests | target SHA; `.golangci.yaml`; pinned linter and build-tag recipe | root-module Go files (`--fix`) |
| Operator lint | every operator Go file; operator module manifests | target SHA; `.golangci.yaml`; pinned linter recipe | operator Go files (`--fix`) |

The map intentionally errs toward broad inputs. Ordinary root Go changes still
run root tidy and lint; changes in a package that owns a generation directive
also run root generation, and test-source changes run the fuzz inventory. This
change does not implement package-level proportional validation.

## Selection and fail-closed rules

The selector compares the exact candidate workspace—including staged,
unstaged, and untracked files—with the supplied target commit. A changed input
or component configuration selects that component. A changed declared output
also selects its producer, even when no source input changed, so a manually
edited generated file cannot bypass normalization.

Missing or mismatched target/tool identity, a dirty trusted-tool worktree, a
non-ancestor base, an unreadable diff, a changed selector/recipe/toolchain, or
an unclassified path selects the full component set. Documentation and known
workflow-only paths are explicitly irrelevant to these normalization outputs.
An existing ignored Jsonnet vendor directory selects dashboard generation
because its relationship cannot be compared with the Git base.

## Fixpoint proof

One runner process keeps exact SHA-256 fingerprints of the candidate workspace
and each component's inputs, configuration, and outputs. After a component
runs, later normalization may invalidate its recorded state. Only invalidated
components run on the next bounded pass. A clean pass is immediately a
fixpoint; it is not replayed. If the bounded fixpoint is not reached, pre-commit
fails with `PRECOMMIT_NON_DETERMINISTIC`.

The fingerprints are in-memory evidence for one invocation only. No candidate-
writable receipt authorizes later skipping. Candidate publication still runs an
independent base-pinned pre-commit gate after exact review and refuses any
mutation of the reviewed SHA.
