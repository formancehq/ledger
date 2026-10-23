# Hegel integration

Status: implementation and local validation complete. The final image build is
assigned to CI. No integration image has been published or run in Antithesis cloud.

## Operational need

The existing Antithesis harness combines specialized property drivers with a
concurrent model-based workload. Introduce structured input generation that can
also be exercised locally, while retaining the existing independent oracles,
assertion identities, scenario families, and chaos coverage. The initial pilot
now lives in `workload/internal/hegelintegration` and exercises the actual
shared generators, full uint64 amounts, dependent transaction/replay/revert
sequences and an independent accounting model.

The [complete inventory](HEGEL-INVENTORY.md) classifies all 59 main drivers,
the concurrent model and all 26 business blocks. Operational and check-only
drivers retain their existing responsibility.

## Execution policy

`workload/internal/generation` exposes typed bounded choices on an explicit
callback-scoped `Source`. It does not seed a separate PRNG. Each concurrent
worker owns an independent source; sources never enter the checker or the
deterministic oracle. Scenario blocks use the dependency-free `scenario.Random`
interface, so the root module does not depend on Hegel.

Cloud episodes use `RunOnce`: workload profile, explicit `BackendURandom`,
generation phase, no example database, and one requested case. Those engine
options alone do **not** guarantee one callback invocation. An experimental
failure with the workload profile invoked the callback 33 times. `RunOnce`
therefore guards the callback and records aborted completion independently of
the engine verdict. Failure replay cannot repeat the user's effects. Native
errors and panics return errors; callers emit Antithesis failures. Operational
drivers and the shared sentinel use `internal.RunOperationalGeneration`, which
reports the failure before a caller aborts its round or witness. Only the
`TooSlow` health check is suppressed because episodes can perform bounded RPCs
against a faulted cluster. Other health checks remain enabled.

Cloud business findings remain SDK assertions, not Hegel failure/replay requests.
Antithesis retains system-level replay, faults, scheduling and lifecycle.
Local property tests use normal Hegel shrinking, with a fresh logical ledger,
model and idempotency keys on every invocation. Cluster-wide caches and counters
are shared; this is not a reset of the complete distributed system. The Go
v0.9.27 public API prints and automatically replays counterexample blobs but
does not expose an option for injecting a blob into a later run.

Technical unique identities and retry keys retain their existing source. Small
business pools, zero amounts, self-transfers and invalid query branches remain
reachable. Hegel can bias draws toward boundaries, so retaining domains and
weighted pools does not guarantee the previous sampling frequencies. Colors
retain their geometric distribution through explicit equal-probability boolean
decisions, rather than bit-counting a biased integer. The unchanged distribution
regression passed ten repeated runs after this correction. The bulk
generator retains the geometric transform and 1..5000 bounds; a 5000-request
generation regression test protects that capacity.

## Integration boundaries

| Surface | Required role |
|---|---|
| `workload/bin/cmds/main` (59 entry points at initial inventory) | Classify each driver. Use Hegel for applicable business inputs and scenario choices; retain driver discovery and assertion semantics. |
| `workload/internal/random.go` and `internal/block` | Share structured generators where contracts match. Preserve valid and deliberately invalid inputs and boundary amounts. |
| `workload/bin/cmds/model/singleton_driver_model` | Integrate generation at explicit ownership boundaries. Preserve concurrent workers, the ordered response buffer, candidate-state validation, and lifecycle coverage. |
| `tests/oracle` | Preserve independent expected-state computation. Generated server responses must never become the expected accounting model. |
| In-process SUT assertions | Retain the SDK and instrumentation. Input generation is a workload responsibility; Hegel must not enter the production FSM or alter committed behavior. |
| Kubernetes chaos and recovery | Preserve scheduling, topology changes, deadlines, and recovery checks. Classify parameter generation separately from fault execution. |
| Images, CI, local runners | Pin the Hegel dependency, validate the native Linux engine and armed SDK builds, and execute nested-module tests explicitly. |

## Acceptance requirements

1. Account for every existing driver, including check-only and chaos-only
   drivers. Record why Hegel applies or why the existing mechanism remains.
2. Use Hegel structured draws in applicable generators, rather than drawing one
   seed and leaving all generation to an opaque PRNG.
3. Keep generator decisions separate from oracle expectations. Legal rejection
   remains an observable outcome for the oracle to explain.
4. Define ownership and lifetime for every Hegel case. Do not share a case
   across arbitrary goroutines or let it escape its callback.
5. Separate local shrinking on resettable state from cloud generation against
   a shared, changing cluster. Never replay side effects merely to obtain a
   Hegel failure report. Verify the chosen runner/profile behavior experimentally.
6. Preserve Antithesis-controlled entropy in cloud runs and document the local
   reproduction contract. A seed alone does not reproduce external scheduling.
7. Preserve assertion registration and emission, including failure detection.
   Test deliberate faults and verify that they produce the intended property.
8. Validate the native library in the actual Linux workload runtime. A Go
   cross-compilation alone is insufficient evidence that FFI loading works.
9. Keep existing scenario and generator regression tests; add tests for new
   integration failure modes. Run the canonical repository checks and focused
   workload/model suites.
10. Report local evidence, image evidence, and remote Antithesis evidence
    separately. Cloud campaign execution requires separate authorization.

## Dependency baseline

The workload uses `hegel.dev/go/hegel v0.9.27` with embedded libhegel 0.48.0.
Confirm APIs against that pinned source, not older changelog entries. Hegel is
currently beta; its [compatibility policy](https://hegel.dev/compatibility)
lists Linux amd64/arm64 and macOS arm64 support. Platform support does not prove
compatibility with our instrumented image or deployment configuration.

Hegel's module graph upgrades `purego` and several `golang.org/x` dependencies
inside the workload module. The root server module and its dependency pins are
unchanged. A server started by workload-module tests nevertheless uses the
workload's selected dependency graph; it is not a released-server binary test.

## Local commands

```bash
# Shared generators, real server, independent accounting model and boundaries.
nix develop --command go -C tests/antithesis/workload test \
  ./internal/hegelintegration -count=1 -v -timeout=5m

# Deliberate harness fault: this command must fail with a reduced counterexample.
nix develop --command env HEGEL_INTEGRATION_OMIT_REVERT=1 \
  go -C tests/antithesis/workload test ./internal/hegelintegration \
  -run TestMissingRevertSensitivity -count=1 -v -timeout=5m

# Armed workload suites, including native concurrency and assertion emission.
nix develop --command go -C tests/antithesis/workload test \
  -race -tags enable_antithesis_sdk ./...

# Native Linux loading and a deliberately missing shared-library dependency.
nix develop --command bash tests/antithesis/run_hegel_smoke.sh

# Existing model runner, now using structured generation.
nix develop --command bash tests/antithesis/run_model_test.sh 45
```

The smoke binary is outside Composer's template tree. The workload CI job runs
the smoke script and the armed race suite, followed by a build of the complete
instrumented workload image on Linux amd64. That build does not publish an image
or launch a campaign. Root `agent-check` alone does not
execute nested-module tests. The Docker runner explicitly installs `libgcc-s1`;
native extraction also needs a writable cache or temporary directory that
allows executable memory mappings.

## Observations during integration (2026-10-22)

- Shared-generator properties: 100 cases, 282 creates, 362 idempotency replays
  and 282 reverts; all passed. Sixteen boundary combinations also passed,
  including zero, self-transfers and MaxUint64.
- The missing-revert sensitivity run failed as intended and reduced to a
  one-unit transfer, replayed against a fresh logical ledger.
- The cloud callback guard passed panic/native-error tests and independent
  concurrent runs under the race detector. Removing the guard caused seven
  user callback invocations in the mutation check.
- Five real-server driver tests passed: bulk metadata, idempotence, idempotency
  conflict, balanced/unbalanced transient accounts and insufficient funds.
  A separate deliberate generation panic emitted the expected SDK failure
  while executing the driver callback once.
- A 45-second single-node model run reported no findings. Four index/retype
  coverage probes were not satisfied; this is not exhaustive coverage evidence.
- A 120-second local three-node run with four concurrent workers and node
  restarts reported no findings. Some coverage probes remained unsatisfied;
  this does not replace a guided Antithesis campaign.
- A Go AST comparison retained all 542 existing literal assertion kind/name
  pairs in tracked workload sources. New generation failures add assertions.
- Native Linux amd64 smoke passed under Docker emulation on the arm64 host.
- The complete instrumented arm64 image ran the concurrent model against a
  local server for 20 seconds: four workers, 106 hit assertion records, no
  failing property, and verified model outcomes. The first attempts exposed
  local disk-threshold and advertised-address configuration errors; the final
  run used a fresh server state and corrected test configuration.
- Linux arm64 smoke passed with glibc 2.41 and libgcc 14.2.0-19. Removing
  `libgcc_s.so.1` made the smoke fail as expected.
- On this macOS arm64 host, a 100-iteration microbenchmark measured about
  0.98 ms and 10.8 KB per standalone Hegel posting episode, versus 2.4 us and
  541 bytes for the previous helper. This measures generation overhead only,
  not end-to-end throughput, coverage improvement or cloud bug yield.

- Canonical validation passed with base
  `9545c9600ca8d7bb516e9ed03db5a6ab24dfb0a1`: `agent-check-pr` selected full
  normalization, `agent-check`, root race tests and the scenario suite. The
  later nested-workload correction is validated separately.
- The first independent review found log-only operational generation failures.
  The correction emits an SDK failure in all six affected paths. Focused race
  tests passed, including a deliberate panic with one callback and a failing
  assertion hit. The second structured SOL Medium review completed with no
  accepted/actionable findings. The final helper exited successfully; no extra
  review was run after that clean result.

## Final validation and remaining limits

- The complete armed workload race suite passed after both corrections.
- Final `agent-check`, workload lint and diff whitespace checks passed.
- The final local image compiled all 60 instrumented race-enabled binaries.
  Docker failed while unpacking the exported image with an input/output error;
  no final-image runtime success is claimed. At the user's request, the final
  image build now belongs to the Linux amd64 CI job. No further local build is
  required for this change.
- The earlier full arm64 image and native amd64/arm64 smoke evidence remain
  useful runtime evidence, but do not replace the final CI build.
- The CI workflow changes have not run remotely. The manual `Run Antithesis`
  workflow also publishes images and launches a campaign; it is not a build-only
  workflow and was not dispatched.

No remote Antithesis campaign has been run for this integration. Guided coverage
and cloud throughput remain unmeasured.
