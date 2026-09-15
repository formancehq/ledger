# Test reachability and enforcement evidence

The [native manifest](test-reachability-enforcement.json) audits whether the
repository's tests, models, scenarios, verifiers, and executable invariants are
actually collected, reach their intended assertions, propagate failures, and
run at an intentional validation layer. It does not grade assertion quality or
require every test family to block every pull request.

## Evidence contract

For each candidate, record the exact audited SHA and trace the complete path
from source to collection, execution, assertion, result propagation, validation
layer, and integration enforcement. Establish collection with the effective
module boundary, build constraints, package selectors, framework discovery,
runner configuration, and runtime prerequisites together. A source file,
compiled package, green wrapper, or uploaded coverage artifact is not by itself
evidence that the intended assertion ran.

Distinguish local, pull-request, merge, release, scheduled, and external/manual
layers. A test can be intentionally external without being unreachable, but its
documentation and enforcement claims must match that layer. Claims about branch
protection or merge queues require current repository policy evidence in
addition to a successful historical workflow.

Candidates remain hypotheses until independently challenged under the native
[audit workflow](../contributing/ai-audit.md). Keep one finding per broken
source-to-enforcement path and preserve the concrete missed protection and
integration event in severity decisions.

## Antithesis source and build boundary

The Antithesis workload is a nested Go module under
`tests/antithesis/workload`. Its `go.mod` replaces the Ledger root module with a
relative path. During instrumentation, the generated module lives under
`/instrumented/customer`, so the workload image reconstructs the root module at
`/` before the instrumentor runs `go mod tidy`.

That reconstructed module must contain every root package imported transitively
by the selected workload code and tests. Adding a root-module import therefore
changes the image-build closure even when driver discovery remains automatic.
Prove reachability by building the instrumented compiler target with the same
platform and driver filter used by the launch workflow. Successful source
discovery alone is insufficient: `go mod tidy`, instrumentation, and compilation
must all succeed before Antithesis can execute an assertion.

For a model-only launch, retain evidence for the selected
`model/singleton_driver_model` binary and for any server package used by its
real-server integration tests. For the unfiltered workload, verify that the
automatic `bin/cmds/<template>/<driver>` discovery and final Antithesis layout
still agree with the documented templates and scheduling prefixes.

## Ownership and deduplication

| Owner | Boundary |
| --- | --- |
| `test-reachability-enforcement` | Missing collection, build closure, assertion reachability, failure propagation, validation-layer coverage, or integration enforcement for an existing test or executable invariant. |
| Owning production subsystem | Incorrect runtime behavior exposed by a reachable test, model, scenario, or verifier. The test is evidence for that defect, not a second reachability finding. |
| Test implementation or PR review | Weak assertions, fixture correctness, flaky timing, maintainability, and ordinary review suggestions when collection and enforcement remain intact. |
| CI or Antithesis infrastructure | Registry, credentials, runner capacity, or service outages that temporarily prevent execution without a repository-owned collection or propagation defect. |

When one mechanism affects several runners, keep a single root cause and list
the affected validation layers. Before reporting a new gap, check current
workflow definitions, required checks, recent executions, and existing work so
an intentional external campaign or already owned fix is not duplicated.

## Falsifiable diagnostics and proof limits

Use native dry discovery where available, then execute the narrowest command
that crosses the suspected boundary. Compare the expected test, driver, or
assertion inventory with observed collection and record skips, filters, tags,
module roots, prerequisites, and exit-status propagation. Pair a failing probe
with a control that demonstrates the runner can reach a known assertion in the
same environment.

For Antithesis image construction, use Docker with the workflow's target
platform, instrumentation stage, and driver filter. A local uninstrumented Go
test does not disprove a missing Docker build input. Conversely, an image build
does not prove that an external campaign selected the template, ran the driver,
or reported its assertions; retain launch and report evidence for those later
links.

The manifest suggests diagnostics for a separately authorized, read-only
audit. Updating this contract does not launch an audit, change CI policy, or
authorize external publication.
