# Checker and admission quality review

This is the first bounded application of the
[code quality audit workflow](../contributing/code-quality-audit.md). The review
examines `internal/application/check/**` and
`internal/application/admission/**` at a later clean commit. The current large
`checker.go` and `admission.go` files are entry points for inspection, not
findings by size.

## Change scenarios

- Add or revise one checker verification rule. Trace how the rule obtains
  evidence, reports failures, and is tested without changing unrelated passes.
- Add or revise one admission request or preload requirement. Trace validation,
  normalization, planning, rejection and tests without widening a shared
  mutable state contract.
- Change a shared error or result representation. Identify duplicated
  conversion and whether a narrower reusable helper would reduce divergence.

For each scenario, inspect the public entry point, internal call graph and
existing tests. Document the concrete edit surface and coupling before
proposing extraction. Compare an incremental alternative with leaving the
current design intact. Check ownership of any proposed helper and whether it
would import a new subsystem dependency.

The repository requires all receiver methods for a struct in the same file as
the struct. A proposal to split a large file must therefore use a cohesive
composed subtype or standalone helpers where appropriate; moving the same
struct's methods to several files violates that convention. Preserve FSM
determinism, audit evidence semantics and request behavior. Static complexity
or duplication counts are screening tools, not acceptance criteria.

The output is a short ranked review with evidence, alternative and validation
for each proposal. It must distinguish completed checks from unexecuted ideas
and avoid overlapping active PRs. It authorizes no code refactor by itself.
