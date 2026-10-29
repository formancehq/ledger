# Testing

## Overview

Ledger v3 uses a multi-level testing strategy to ensure system quality and reliability.

Local validation provides fast engineering feedback and focused evidence. CI
provides the authoritative broad, clean validation before merge. A change must
still preserve a focused regression test and mutation/sensitivity evidence
when appropriate; moving broad suites to CI does not weaken that requirement.
See [Local validation](local-validation.md) for the selection policy.

## Test Types

### Unit Tests

**Location**: Files `*_test.go` in each package

**Objective**: Test individual functions and methods

#### Regression preservation and branch proof

A regression test is a guard for a specific production path, not merely an
example that reaches nearby code.

- When a shared mechanism gains a second production trigger, preserve an
  independent case for the original trigger. Extract expensive setup into a
  helper and drive sibling cases rather than replacing the old trigger with the
  new one.
- Failure-path assertions must distinguish the intended branch. Do not assert a
  substring shared by an earlier guard and a later fallback when the earlier
  guard is the behavior being protected.
- Retry tests must inject a failure exactly once, then succeed. Verify the
  attempt count, final bytes, reset/truncation of partial state, and temporary
  file cleanup; a fixture that fails forever does not test retry recovery.
- Filesystem-boundary tests cover both directions of transfer and include valid
  root/nested paths, lexical traversal, absolute paths, and symlink escapes.
  The final filesystem operation must remain confined even if lexical
  validation is removed or raced.
- When practical, mutation-check the regression by temporarily removing or
  bypassing the guarded production call and confirming that the focused test
  fails. Never keep the mutation in the worktree.

#### Nested AI-tooling fixtures

Tests that launch AI workflow scripts or operate on synthetic Git repositories
must start from `scripts/internal/testenv.Environment` (or
`testenv.Command`). The helper removes an enclosing AI run's expected
worktree/review/validation identity. A fixture then adds only its own synthetic
identity explicitly.

Concurrent subprocess fixtures must use bounded, blocking synchronization and
supervise process exit. A peer that fails before reaching a barrier must abort
its siblings and report every peer's combined stdout/stderr; shell spin loops
and unbounded marker waits are not permitted.

**Example**:
```go
func TestValidateBucketConfig(t *testing.T) {
    tests := []struct {
        name    string
        driver  string
        config  map[string]interface{}
        wantErr bool
    }{
        {
            name:   "valid pebble",
            driver: "pebble",
            config: map[string]interface{}{},
            wantErr: false,
        },
        // ...
    }
    
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            err := service.ValidateBucketConfig(tt.driver, tt.config)
            if (err != nil) != tt.wantErr {
                t.Errorf("ValidateBucketConfig() error = %v, wantErr %v", err, tt.wantErr)
            }
        })
    }
}
```

### Fuzz Tests

Go fuzz declarations are authoritative. The active runner reads
`scripts/fuzz-targets.txt`, and `just fuzz-inventory-check` compares every
package/target pair in that file with the repository's `func Fuzz...(*testing.F)`
declarations. The check reports both missing declarations and stale runner
entries, rejects fuzz declarations hidden by build constraints or platform
filename suffixes, and runs as part of pre-commit and the repository invariant
gate. Keep active fuzz declarations in untagged, platform-independent test
files in the root Go module unless the runner is explicitly extended to support
another mode. A fuzz declaration in a nested Go module fails the inventory check
with the owning module path because the root-module runner cannot invoke it.

Use `just fuzz <duration>` for active fuzzing of every checked target. Use
`just fuzz-check` for seed corpus replay; seed replay is a separate validation
layer and does not perform active fuzzing.

### Integration Tests

**Location**: Files `*_integration_test.go`

**Objective**: Test integration between components

Store integration tests cover both runtime state and log persistence now that log storage is part of the store.

PostgreSQL mirror integration tests use isolated disposable databases and two
independent source transactions to distinguish late commits from permanent
sequence gaps. CI runs the `Postgres` selection in `internal/adapter/v2` and
`internal/application/mirror` with `-tags integration` and the race detector
through the same recipe available locally:

```bash
nix develop .#ci --command just test-postgres-mirror-coverage
```

The recipe writes `build/coverage/postgres-mirror.out`, which CI uploads and
requires when merging coverage profiles.
By default these tests start
PostgreSQL through Testcontainers. To use an existing disposable PostgreSQL
service, set `LEDGER_TEST_POSTGRES_DSN` to its administrator URI; each case
creates and drops its own database. The source-privilege test also creates and
drops a temporary role. Never point this override at a production service.

**Example**:
```go
//go:build integration

func TestStoreIntegration(t *testing.T) {
    store := setupStore(t)
    defer cleanupStore(t, store)
    
    log := &ledger.Log{...}
    err := store.AppendLogs(ctx, 0, log)
    require.NoError(t, err)
    
    cursor, err := store.GetAllLogs(ctx, 0, 0)
    require.NoError(t, err)
    // ...
}
```

### Schemathesis API Conformance Tests

**Location**: `tests/schemathesis/`

Run `just test-schemathesis` in the Nix development environment. The runner
starts a disposable single-node server with authentication disabled and seeds
nonempty responses. Ledger deletion uses a separate sacrificial ledger so the
fixture ledger survives the run.

The Python wrapper invokes the pinned Schemathesis public CLI with the
`examples`, `fuzzing`, and `stateful` phases and positive generation. It selects
`not_a_server_error`, `status_code_conformance`, and
`response_schema_conformance` explicitly and disables security-parameter
generation for the auth-disabled server. Generation is deterministic, the
Hypothesis database is disabled, and shrinking is disabled unless
`SCHEMATHESIS_SHRINK=1`. Keep `SCHEMATHESIS_WORKERS=1` for the reproducible gate;
increase it only for exploratory runs. `MAX_EXAMPLES` controls fuzzing and
stateful generation.

The wrapper reads the CLI's NDJSON report. Passing requires a zero CLI exit
status, an `EngineFinished` event, and an actual successful check in a
`ScenarioFinished` recorder. Every error or check failure remains fatal even
after a later success. Missing or malformed reports and runs without
successful checks fail. Reporting regressions run before the API suite, and
the shell pipeline preserves failures while saving diagnostics under
`/tmp/schemathesis-*.txt`.

### End-to-End Tests (E2E)

**Location**: `tests/e2e/`

**Objective**: Test the complete system with a real cluster

**Framework**: Ginkgo/Gomega

**Example**:
```go
//go:build e2e

var _ = Describe("Simple cluster", func() {
    It("should start successfully", func() {
        Eventually(func(g Gomega) bool {
            state, err := servers[0].client.Cluster.GetClusterState(ctx)
            g.Expect(err).To(Succeed())
            return state.ClusterStateResponse.Data.Leader != nil
        }).Within(5 * time.Second).Should(BeTrue())
    })
})
```

## E2E Test Structure

### Cluster Setup

E2E tests create a cluster through `tests/e2e/testutil`, which allocates the
ports, boots the nodes and waits for a leader:

```go
BeforeEach(func() {
    ctx, servers, _, leaderID = testutil.SetupMultiNodeCluster(3)
})
```

`SetupSingleNode` does the same for a one-node cluster. Both accept extra
instruments for the node under test.

`WithGateway()` places an allocated loopback proxy in front of every Raft and
snapshot service endpoint. Each node advertises its proxy address, so a gateway
interceptor observes the real post-admission peer traffic rather than an unused
side channel. Initial bootstrap discovery starts direct; the proxy also forwards
the bootstrap unary RPCs required after discovery because the returned peer
addresses already point at the proxies. The proxy preserves incoming gRPC
metadata on every backend hop so authenticated peer traffic remains valid.

### Ports are leased, never chosen

A test node's ports come from `testserver.AllocateNodeLease()`. Do not write a
port number in a test.

```go
lease := testserver.AllocateNodeLease()
ports := lease.Ports()

instruments := testserver.DefaultTestInstruments(testserver.TestNodeConfig{
    NodeID:    1,
    ClusterID: "test-cluster",
    Ports:     ports,
    WalDir:    walDir,
    DataDir:   dataDir,
})

server := lease.NewService(cmdserver.NewRunCommandWithBindings,
    testservice.WithInstruments(instruments...),
)
```

The lease binds three loopback sockets up front and **keeps them open**. A port
number alone is not a reservation: any scheme that probes a port and releases it
leaves a window in which another process — typically a sibling test binary
starting at the same moment — binds the same number first. That is what EN-1784
was. Because `lease.NewService` hands those very sockets to the run command
(`network.Bindings`, adopted by `httpserver.WithListener` and the gRPC adapter's
`WithListener`), there is no window at all: the socket the spec holds while it
builds join addresses is the socket the node serves.

`NodePorts` has unexported fields, so a lease is its only source and
`TestNodeConfig` cannot be given a hand-picked port.

Three rules follow:

- **Keep the lease for the node's whole life.** A restart must go through the
  same lease — `RestartNode` and `ScenarioCluster.Restart` do — because peers
  still hold the node's old Raft address. The lease rebinds the same ports for
  each start; it panics if the previous generation is still bound, which means
  the node was not stopped.
- **Never compute one port from another.** The old convention derived the Raft
  port as `grpcPort - 1000`. That is what made two suites collide on 15200: one
  spec's gRPC port mapped onto another node's live HTTP port (EN-1784).
- **Never bind a leased port yourself.** The lifecycle owns every listener it is
  given and releases all of them on stop, including one it never served — restore
  mode is handed a Raft listener it does not use, and a phase that follows it
  must find that port free.

A spec that builds its own instrument list instead of using
`DefaultTestInstruments` still passes leased values through the low-level
instruments — `testserver.WithHTTPPort(ports.HTTP())`,
`WithRaftPort(ports.Raft())`, `WithGRPCPort(ports.GRPC())`. Those take a plain
`int`, so a literal would still compile there. So would one passed to
`testserver.WithJoin(raftAddr)`, or one written into an RPC field that carries an
address — for example `AddLearnerRequest.RaftAddress`, which is a plain string.
The `NodePorts` guard reaches none of these. Feed every one of them from a lease.
`testserver.NewGateway(logger, nodes)` takes no ports at all: it binds its own.

Peers that are deliberately never started need
`testserver.AllocateDeadAddress()`. A hand-picked address for a phantom node can
name a port a live node later gets, which points the leader's Raft transport at a
real listener in its own cluster instead of at a dead address. A dead address is
bound long enough to claim the number for this process and then released, so
connections are refused and no later allocation can reuse it. `phantomPeer()` in
`tests/e2e/cluster` is built from it.

One window remains, by design: between a node's stop and its restart the ports
are unbound, so only an unrelated process could interpose. Closing that too
would mean putting a proxy in front of every node, which these suites do not
justify.

### Test Helpers

The package `pkg/testserver` provides helpers:

- `AllocateNodeLease()`: Lease the node's HTTP, gRPC and Raft ports, binding
  the sockets the node will serve
- `AllocateDeadAddress()`: Claim a loopback address that nothing listens on
- `DefaultTestInstruments()`: Build the standard instrument set for a node
- `WithNodeID()`: Configure the Node ID
- `WithRaftElectionTick()`: Configure Raft parameters
- `WithRaftTickInterval()`: Configure the tick interval

## Unit Testing Infrastructure

There is no general-purpose fixture for a cluster of real `*node.Node`
instances, and no in-memory implementation of `node.Transport`. Node unit tests
build a single real `*node.Node` over real storage in temporary directories:

- **Applier fixture:** `newTestApplierSetup` (`internal/infra/node/applier_test.go`)
  builds an `Applier` over a real Pebble store, WAL, spool and FSM. Its
  `newTestApplierSetupWith*` variants inject the local response sink, the
  notifier or the ConfChange handler.
- **Raft transport mock:** `MockTransport` (`internal/infra/node/transport_generated_test.go`)
  is generated by mockgen from `transport.go`. Tests in the `node` package use
  it to stub the receive channels and assert outbound `Send` calls. Regenerate it with
  `go generate ./...` after changing the `Transport` interface; do not edit it
  by hand.
- **Single-voter service fixture:** `newMembershipServiceNode`
  (`internal/application/membership/membership_node_test.go`) runs one voter
  through its real WAL, FSM, orchestrate loop and `DefaultTransport`. It never
  calls `Membership.Start`, so registering a learner commits the ConfChange
  without dialing the learner's address, and the tests run in a synctest
  bubble.
- **Small WAL segments:** etcd preallocates each Raft WAL segment, plus one
  spare, at 64 MB. On APFS a test binary that opens many WALs turns that into
  GBs of physical writes and temporary space. In the default untagged
  root-module suite (`go test ./...`, which local validation runs), every test
  binary that opens a WAL, directly or in a process it re-executes, calls
  `waltest.UseSmallSegments()` (`internal/storage/wal/waltest`) from its
  `TestMain` before `m.Run`. The tagged `e2e` and `scenario` suites and the
  separate Antithesis workload module do not call it yet, so they still
  allocate production-size segments. A binary that imports `cmd/server` does
  not inherit its `TestMain`. It sets etcd's process-wide
  `wal.SegmentSizeBytes` to 1 MiB, except under `-test.bench`. Set it only
  there: etcd reads the value on every append, so changing it while a WAL is
  open races with parallel tests. The binaries that set it today are the root
  package, `cmd/server`, `internal/bootstrap`, `internal/infra/node`,
  `internal/storage/wal` and `internal/application/membership`. Add a new
  binary to this list when its tests start opening WALs. Size segment-boundary
  fixtures from the live `wal.SegmentSizeBytes`, never from a hard-coded size.
  The `waltest` package's own binary keeps the production size and covers it.

Multi-node behavior is covered in three places:

- **Raft-level harness:** `readIndexRestartHarness`
  (`internal/infra/node/read_index_restart_test.go`) runs three `raft.RawNode`
  instances over `raft.MemoryStorage`, delivers messages directly through
  `Step`, and supports restarts. It tests Raft protocol behavior without the
  `Node`, WAL or FSM.
- **Routed checkpoint fence tests:**
  `internal/bootstrap/controller_routed_checkpoint_fence_test.go` runs real
  nodes over loopback gRPC: a follower behind a leader RPC handler, or a real
  leader and follower. They stay on real time.
- **E2E suites and Antithesis:** replication, partitions and restarts across
  full nodes.

## Antithesis (Autonomous Testing)

**Location**: `tests/antithesis/`

**Objective**: Drive the running system with randomized workloads and assert
high-level correctness properties under fault injection (node kills, network
faults, restarts). Structured business inputs use Hegel with the URandom backend;
the [Antithesis SDK](https://antithesis.com) retains operational randomness,
technical identities and `assert.*` calls that become catalogued properties.
Hegel cloud episodes have an explicit at-most-once callback guard: engine failure
replay must not repeat external effects. Concurrent workers own separate
callback-scoped sources. Local shrinking tests reset their logical ledger and
independent model on every invocation.

See [Hegel integration](../../../tests/antithesis/HEGEL.md) for the generator
contract, native Linux checks and local commands, and
[the applicability inventory](../../../tests/antithesis/HEGEL-INVENTORY.md) for
every driver's generation boundary. The Hegel dependency stays in the workload
module; scenario blocks expose a dependency-free typed randomness interface.
Root-module tests do not execute these nested-module property tests.

### Launch tooling

The manual `Run Antithesis` workflow and the launch recipes accept an optional
comma-separated `test_templates` selection. An empty selection runs all
templates. Compose `just run` takes minutes and sends `antithesis.duration`;
convert older hour-based invocations to minutes. Deploy compatible Antithesis
launchers before using the updated parameters. See the
[launch selection guide](../../../tests/antithesis/README.md#launch-selection)
for recipe examples and workflow compatibility with older source refs.

### Drivers and templates

A **driver** is a standalone workload binary under
`tests/antithesis/workload/bin/cmds/`. Drivers are grouped into two **test
templates** — Antithesis runs exactly one template per execution history, so a
driver's template decides who it shares the timeline with:

| Template | Location | Shape |
|----------|----------|-------|
| `main` | `bin/cmds/main/` | Dozens of drivers run **concurrently** against one faulted cluster. Each targets a feature area (transactions, reverts, idempotency, account types, metadata, backups, …) and asserts feature-local invariants. |
| `model` | `bin/cmds/model/` | A single driver (`singleton_driver_model`) **owns the whole timeline** — no other driver runs alongside it — because it drives the system itself and checks global consensus conformance. |

The split is wired in `tests/antithesis/workload/Dockerfile`.

#### Scenario randomness and reusable driver episodes

Scenario blocks receive `scenario.Random`, which exposes `Intn`, `Int63n`
and `Uint64`. Existing `scenario.RandFunc` callers retain the same one-draw
modulo mapping for positive bounds; a raw `func() uint64` must be explicitly
wrapped with `scenario.RandFunc`. Custom `Block.Run` callbacks must accept
`scenario.Random`. The interface adds no generator dependency to the root
module and leaves the choice of random source to the caller.

The Numscript lifecycle and projection-alignment drivers expose reusable
execution functions with explicit business inputs. Their generated inputs retain
the original fixed version pair (`1.0.0`, `2.0.0`) and asset (`USD/2`) as explicit
regression branches. Technical identities still use SDK entropy; the RPC sequence
and assertion names are preserved.

#### Dedicated-ledger query oracles

The reference-race, definitive-errors and bulk-atomicity drivers declare the
reference index on their private ledgers; bulk atomicity also declares the
transaction-address index. `CreateQueryOracleLedger` puts ledger creation and
index declarations in one idempotent proposal. This preserves the bulk audit
oracle's limit of one successful setup proposal. A ledger-name collision stops
the invocation without reusing the existing ledger or reporting a finding.

Every filtered check uses `ReadOracleTransactions`. The actual linearizable
query gates readiness on the replica serving it. An `INDEX_BUILDING` response
restarts the complete query within a ten-second context, discarding partial
rows. A status response from a different replica cannot certify readiness.
Only clean EOF makes a page conclusive; ten rows suffice for these absence and
at-most-one checks. Permanent setup/read errors, including missing indexes,
emit an Unreachable assertion with the ledger, operation, error and gRPC code.
Read errors also include the filter and partial transaction IDs.
Transient failures and caller cancellation do not certify a business result.
The shared RPC classification is unchanged.

Each driver's `TestDriverQueryOracles` invokes its real entry point against a
local server and captures SDK JSON in a child process. It requires the original
Always properties to emit `hit:true` and `condition:true`, including both bulk
effect checks and the separate audit check. False Always or Unreachable hits
fail these healthy-driver tests. Sensitivity tests use committed
matching activity for absence checks and an explicitly injected second response
ID for uniqueness. The latter validates the oracle, not an engine duplicate.
Run the three driver packages and `./internal` from the nested
`tests/antithesis/workload` module; root-module tests do not include them.

### Model-based conformance test (`singleton_driver_model`)

This is an in-memory **model checker**: it runs a deterministic reference model
of the ledger alongside the real server and asserts that **every observed
response is explainable by some valid serialization of the bulks currently in
flight**. It is a single-process driver requiring no Antithesis platform — it
runs against a local single node (fast inner loop) or an N-node Raft cluster
(leadership-change / snapshot-install coverage), and is wired into the `model`
template for platform runs.

#### What it verifies

The core invariant — *every observed response is consistent with some
serialization of the in-flight bulks* — turns the consensus guarantees into
concrete assertions. A violation surfaces as a failed Antithesis assertion, a
server/driver panic, or (on a cluster) failure to return to N voters after a
restart. It catches:

- **FSM determinism / cache–Pebble consistency** — a committed bulk whose
  outcome (assigned transaction id, post-commit volumes, stored metadata,
  chart/schema mutation) the model rejects or predicts differently.
- **Lost or double-applied commits** — a success returning no committed log, or
  volumes/ids that don't line up with the model's forward prediction.
- **Illegal business outcomes** — a failure the model can't reproduce under any
  serialization, or a linearizable read returning state outside the candidate
  set.
- **Incomplete direct audit reads** — `GetAuditEntry` must return one
  `AuditItem` per order for a modeled, committed success. `ListAuditEntries`
  omits item bodies, so the model checks its entries without this count rule.

It exercises the chart of accounts, STRICT/AUDIT enforcement switches through
both setter forms, transactions and reverts (with post-commit volumes), color
segregation (postings draw a color geometrically, so the uncolored bucket
dominates while a long tail of rarely-touched ones keeps appearing),
account/transaction/ledger metadata, the typed-metadata schema and
its index lifecycle (create, retype with serving-window closure, remove), the
prepared-query registry (create, update, delete), and the transient/ephemeral
persistence classes — and reads them back: account, whole-ledger,
transaction-by-id, and declared-schema reads, the filtered, paginated list
surface (ListAccounts, ListTransactions, ListLogs, indexed metadata-range
queries), and the prepared-query surface (ListPreparedQueries, and
ExecutePreparedQuery in both LIST and AGGREGATE_VOLUMES mode, with generated
parameter bindings) are all validated against the model.

The Apply workload also opts into each supported skippable reason: reference
conflicts, already-reverted transactions, missing metadata, and account types
that already exist or are absent. The oracle predicts per-order rollback,
skipped-log reason/code/facts and IDs, continuation, and whole-batch rollback after
a later non-skippable failure. Invalid opt-ins must fail admission. Enforcement
mode, chart, and ledger metadata readback must match the same model snapshot.
Signing-key lifecycle and signed submissions remain outside this model driver.

#### Ledger lifecycle coverage

The model driver generates ledger creation, deletion, mirror promotion, and
maintenance toggles in the same concurrent bulk stream as business writes. The
oracle owns ledger existence and filters deleted names from reads and generation.
Mirror ledgers remain eligible for lifecycle operations and reads, but enter the
business-write pool only after their promotion has committed.
Because deletion permanently reserves a name, committed creations grow the
`model-<runID>-<n>` pool; creation is biased when deletion shrinks the live pool.

Maintenance mode is a global admission gate modeled in `GlobalState.Apply`.
The workload retry predicate surfaces a first-attempt `MAINTENANCE_MODE`
rejection even though the transport code is `Unavailable`. When maintenance is
observed after an ambiguous transport attempt, an enabling request stops because
it may itself have activated the gate; the driver submits that rejection for
model validation while preserving the request as a possibly committed
predecessor. Other idempotency-keyed requests keep retrying through the
already-scheduled recovery window so their ambiguous outcome becomes definitive.
A successful or ambiguously committed enable schedules a randomly delayed
disable through the normal in-flight/processor path, preventing all workers from
becoming stuck behind the gate. Enable generation is deliberately rarer than
disable generation because every active window pauses useful business coverage.
Concurrent ambiguous enables are coalesced into one state-equivalent optional
predecessor so they cannot exceed the candidate-search capacity.
Startup and shutdown also make a best-effort disable so an interrupted run
cannot block the next invocation.

Each of deletion, promotion, and maintenance has a required coverage marker.
Creation back-pressure keeps the live pool near its configured size while still
allowing retained tombstone names to accumulate as required by the service contract.

EN-1627's original successful same-name recreation expectation does not match the
current service contract: deletion retains a tombstone and recreation returns
`LEDGER_DELETED`. The model tests that rejection. It does not claim to prove
projection cleanup by querying a successfully recreated ledger. Concurrent
ledger-scoped reads validate NotFound against candidate lifecycle states when a
selected ledger is deleted before the read executes. Reads otherwise hide retired
ledgers even when ledger-scoped rows remain physically present. Repeated
deletion can still operate on retained tombstones. Other ledger-scoped writes,
including ledger metadata changes, return `LEDGER_DELETED`. The service-backed
lifecycle scenario compares the supported outcomes with the real API.

#### How it works

N workers fan out across a fleet of ledgers, dispatching bulks concurrently;
the model mirrors the single Raft log (one global re-order buffer, one committed
state across all ledgers). The pieces:

The forward model is a standalone library under `tests/oracle`; the driver
under `tests/antithesis/workload/bin/cmds/model/singleton_driver_model/` owns
the harness around it:

| File | Role |
|------|------|
| `tests/oracle/model.go` | The pure forward model: `GlobalState`/`LedgerState` + `Apply`, which predicts the server's legal outcome for a bulk, atomically across whatever ledgers it touches. `GlobalState` also holds the global log stream keyed by sequence (`Log`/`Logs`), filled as sequences are learned from commit responses; like the server's log zone it outlives the ledgers it indexes. |
| `tests/oracle/bulk.go` / `accessors.go` | The `Bulk` submission shape the model consumes; read-only accessors over model state for validators and tools. |
| `tests/oracle/oracletest/` | Shared helpers for tests that drive the oracle. |
| `tests/oracle/cmd/replay` | Offline replayer for `MODEL_DUMP_BATCHES` captures (see below). |
| `checker.go` (driver) | Harness bookkeeping: the in-flight set, the re-order buffer, and the committed model state. |
| `processor.go` (driver) | One goroutine that re-orders observed responses by log sequence and drains them into the committed state in commit order. |
| `search.go` (driver) | `candidateBases` — enumerates the states the server could legitimately be in. |
| `validate.go` (driver) | The conformance checks for committed bulks, failures, and reads. |
| `actions.go` / `reads.go` (driver) | Random bulk generation; account, whole-ledger, transaction-by-id, and metadata-schema read execution. |
| `queries.go` / `queries_logs.go` (driver) | Filtered, paginated list reads (ListAccounts, ListTransactions, ListLogs) generated against the model's committed state and validated window-by-window. |
| `indexes.go` (driver) | Metadata-index lifecycle: create/retype/remove generation, readiness polling, retype-window bookkeeping, and indexed range-query validation. |
| `metadata_filters.go` (driver) | Typed metadata filter generation shared by the query validators. |
| `prepared_queries.go` (driver) | Prepared-query registry lifecycle generation, and the parameterize/substitute pair: a concrete filter's leaves become parameter references at creation, and binding them back at execution reproduces it — so the query evaluators above are reused unchanged. |
| `prepared_queries_read.go` (driver) | `ListPreparedQueries` and `ExecutePreparedQuery` (LIST and AGGREGATE_VOLUMES) validation. Each candidate base supplies its OWN stored definition, so a concurrent update or delete is a legal alternative rather than a finding. |

The key primitive is **`candidateBases`**: a committed bulk drains in
log-sequence order, so the committed model state is its exact predecessor and
its outcome is checked deterministically. A *failure* or a *read* has no fixed
position, so it is validated against the set of states the server could be in —
the committed state folded with the in-flight bulks in every commit-consistent
order. Only operations dispatched no later than the observation's **high-water
mark** are folded: one dispatched after the observation's response cannot have
preceded it, so folding it would invent a state the server was never in. This
lets failures and reads validate against the model without quiescing the
workload.

#### Running it

```bash
# Single node, fast inner loop (default 60s). No fault injection.
just test-model

# 3-node cluster with rolling restarts (default 180s) — the CI gate.
# Kills one node at a time keeping quorum; a killed node stays down long
# enough that it rejoins via snapshot install, exercising snapshot transfer
# and follower restore.
just test-model-cluster

# Custom duration / topology via the runner directly:
tests/antithesis/run_model_test.sh --nodes 5 300
```

Both targets call `tests/antithesis/run_model_test.sh`, which builds the server
and driver and runs the checker. It exits non-zero on the first finding by
default. A successful run also requires at least one
`singleton_driver_model: model outcome verified` assertion hit, emitted only
after a definitive server outcome reaches model validation. Driver liveness,
assertion registration, and ledger-setup assertions do not satisfy that gate.

It reports coverage probes that were not satisfied in the sampled trajectory
without turning those stochastic gaps into correctness failures (see below).

The local runner's shell-fixture tests use a logical clock. Time remains before
its deadline until the fake driver has written the scenario's assertions and
installed its shutdown handler; the runner then gets one live monitor iteration
before expiration. The early-exit case holds time before the deadline so the
runner must detect actual process termination. Fixture events and clock replies
use blocking pipes, with process-group cancellation and collected server/driver
logs on infrastructure failure. The outer timeout is a watchdog, never evidence
that the intended model state was reached.

Common tunables (full list in the script header):

| Variable | Meaning |
|----------|---------|
| `MODEL_LEDGERS` / `MODEL_WORKERS` | Fleet size and concurrency. Workers are capped at 6 because candidate-state search is exponential in outstanding writes; two additional slots are reserved for maintenance recovery and one retained ambiguous enable. |
| `MODEL_DEBUG` | Enable driver debug logging. |
| `MODEL_FAIL_FAST` | Stop on first finding (default); `0` runs the full duration. |
| `MODEL_DUMP_BATCHES` | Log every submitted bulk (`[batch-dump]` lines) for deterministic offline replay through `tests/oracle/cmd/replay`. |
| `RESTART_INTERVAL` / `DEAD_TIME` | Cluster restart cadence and how long a killed node stays down. |
| `COMPACTION_MARGIN` | Raft entries between snapshots; low values force snapshot recovery. |
| `RESTORE_INTERVAL` | Seconds between backup/restore cycles with `--restore`. |

#### Coverage probes

A green run proves nothing about a query path it never took. `coverage.go`
registers one `Sometimes` per index the oracle models — the nine the generator
churns, plus one per entity target for the metadata-field indexes and one for
the retype window — and the runner reports any that were never satisfied.
A probe is satisfied only by a page that the index was needed for AND that the
oracle verified; a refusal the model predicted proves the lifecycle gate, not
that the index can answer.

They are `Sometimes` rather than `Reachable` because a `Reachable` hard-wires
its condition to true and so never produces a failing evaluation for
Antithesis to steer on. On the platform, the run branches and biases toward
unsatisfied probes, making Antithesis the authoritative exhaustive-coverage
environment. Locally there is one short linear trajectory and no guidance, so
missing probes remain visible as diagnostics rather than making CI depend on
random generator choices. Apply probes additionally require each skipped reason
followed by a successful order, both mode setters in both modes, and a rejected
invalid skip opt-in. A selected request does not satisfy a probe: the observed
outcome must pass oracle validation.

Probe names are data-driven, so the instrumentor cannot catalogue them; they
are registered through `assert.AssertRaw`, as `internal/block/block.go` does.

Because these probes are false by design on most queries, the runner treats
assertion classes differently: a false `Always` / `AlwaysOrUnreachable` /
`Unreachable` is a finding, a false `Sometimes` is not. The exception is
`STRICT_SOMETIMES`, the shared helpers whose `assert.Sometimes(IsTolerated(err),
...)` probes are invariants wearing the wrong primitive; locally there is no
fault injection and transients are retried to a definitive outcome, so those
must hold on every call. `check-repo-invariants` pins that list against its
call sites.

#### Restore cycles (`--restore`, single node)

`run_model_test.sh --restore` periodically quiesces the driver, takes an
incremental backup, kills the node, bootstraps a fresh store from the backup
(running the `RebuildDelta` replay), and relaunches on it — the driver then
resumes and its ordinary checks validate the rebuilt state. The run fails if
no cycle completed or any cycle failed. The driver-side knobs are
`MODEL_RESTORE_INTERVAL` and `MODEL_RESTORE_TIMEOUT` (seconds; the timeout is
the per-cycle lease after which the driver gives up waiting and resumes).

On Antithesis (k8s template) the same driver rendezvous is serviced by the
`restore-orchestrator` sidecar in the workload pod
(`tests/antithesis/workload/restore-orchestrator.sh`), which drives the
operator through a full disaster-recovery pass: quiesce-point backup, Cluster
teardown (CR + PVCs), restore-mode round-trip (`ledgerctl restore download` /
`finalize`), and the flip back to a full cluster. The
`singleton_driver_model: restore cycle completed` Sometimes assertion in the
report is the proof the path actually ran.

The sidecar does not infer finalization from a CLI error string. A lost
`restore finalize` response is accepted only when the restore-mode pod still
exposes both `checkpoints/0` and a valid `RESTORED` marker. The marker is the
server's durable commit point and is written after checkpoint publication; when
the marker is absent, including after a restart that forgot in-memory staging
state, the sidecar tears the attempt down and retries the complete restore.

#### Maintaining the model

The model in `tests/oracle/model.go` is a hand-written mirror of the server's
*intended* behavior; nothing keeps the two in sync automatically. Treat them as one unit —
when a change alters behavior the model relies on, update the model in the same
change, or the next run will either cry wolf (model stricter than the server) or
go blind (model more lenient than the server).

Where to make the matching change:

| Server change | Model change |
|---|---|
| New request type or ledger action | Add a `case` to `applyOne` in `tests/oracle/model.go` **and** a generator in the driver's `actions.go`. |
| Changed business/validation rule (new rejection condition, enforcement change, volume math) | Update the matching `apply*` predictor in `tests/oracle/model.go`. |
| New or changed response field the test should check | Update the validator in `validate.go` and the predicted effect it compares against. |
| New rejection reason | Return the matching `domain.ErrReason*` from the right model branch so `validateFailure` can explain it. |
| New persisted projection or read surface | Point reads: add the read in `reads.go` and a validator in `validate.go`, mirroring the account/ledger reads. Filtered/paginated list surfaces: follow the `queries.go` / `queries_logs.go` pattern (generate against committed model state, validate the returned window). Index-backed reads: `indexes.go`. Prepared queries: model the registry op in `tests/oracle/model.go` (`LedgerOf`, `logKindFor`, `applyOne` in lockstep) and validate in `prepared_queries_read.go`. |

To diagnose a finding deterministically, capture the run with
`MODEL_DUMP_BATCHES=1` and feed the dump back through the model offline:
`go run ./tests/oracle/cmd/replay <dump-file> [target-ledger]` folds the
committed bulks in log-sequence order (the server's true serialization) and
prints each touched ledger's final model state; `--submits` additionally
compares every bulk's model outcome to the server's, valid for single-worker
runs where dispatch order is the serialization.

The model is built to fail loud on anything it hasn't been taught: `Apply`
panics on an unmodeled request type or action rather than skipping it — the same
"never silently skip a should-not-happen branch" rule the server follows. So an
action you forget to model shows up as a finding, not a green run. Keep that
panic; don't downgrade it to a soft skip.

The runtime mechanisms the model cross-checks against (audit hash chain,
double-entry balance, the offline `store check`) are documented in
[Log Integrity and Correctness](../../ops/correctness.md).

The local model runner supervises the driver's exit independently of rolling
restart recovery. An exit before the requested duration or a spontaneous nonzero
exit fails the run. A clean watchdog exit after the requested duration is valid
when an already-started restart is still recovering; recovery and model assertion
checks must still pass.

## Best Practices

### Avoid `time.Sleep`

❌ **Bad**:
```go
time.Sleep(2 * time.Second)
// Check state
```

✅ **Good**:
```go
Eventually(func(g Gomega) bool {
    state, err := client.GetState()
    g.Expect(err).To(Succeed())
    return state.IsReady()
}).Within(10 * time.Second).WithPolling(500 * time.Millisecond).Should(BeTrue())
```

### Use `testing/synctest` for time-dependent unit tests

In-process unit tests should not depend on the wall clock. A test that waits
for a production retry delay, a ticker, or a short `time.After` deadline is
slow, and it can fail when CI is loaded. Run such a test inside a
`synctest.Test` bubble:

- Time inside the bubble is fake. It moves only when every goroutine in the
  bubble is durably blocked, so a one-second retry delay elapses at once.
- `synctest.Wait()` returns when every other goroutine in the bubble is
  durably blocked. After it returns, assert the state directly instead of
  polling with `require.Eventually`.
- A `time.After` watchdog in the bubble measures fake time, so scheduler
  load cannot make it fire. It still fires before a slower fake-time
  operation completes, so it must exceed every timer the operation waits on.
- When the observation is monotonic, such as a counter, a generation, or
  events that are only recorded and never removed, a `require.Never` window
  becomes `time.Sleep(d); synctest.Wait(); require.Zero(t, count())`.
  Because fake time is free, `d` can cover several production periods. A
  single sample misses a condition that turns true and then false again, so
  for such a condition record a sticky violation in the code under test or
  keep polling.

```go
func TestRetryHonorsDelay(t *testing.T) {
    t.Parallel()
    synctest.Test(t, func(t *testing.T) {
        m := newManager(t) // build every fixture inside the bubble
        m.Start()
        t.Cleanup(m.Stop) // runs inside the bubble, before it ends

        time.Sleep(retryDelay)
        synctest.Wait()
        require.Equal(t, int64(2), m.attempts.Load())
    })
}
```

Follow these rules:

- Call `t.Parallel()` on the outer `t`. Do not call `Parallel`, `Run`, or
  `Deadline` on the `t` of the bubble. For subtests, start one bubble inside
  each `t.Run` closure.
- Build fixtures inside the bubble. A goroutine or channel created outside
  the bubble is not managed by it, and a bubbled channel or timer panics when
  it is used from outside.
- Stop every goroutine started in the bubble before the bubble returns.
  Fake time stops when the bubble function returns. A goroutine that exits
  only on an idle timer then never exits, and `synctest.Test` panics with a
  deadlock. Production workers that outlive their owner on an idle timer
  should instead exit when their work drains, as the Pebble event logger
  worker does.
- A mutex wait is not durable blocking, so a goroutine parked on a leaked
  `sync.Mutex` stops fake time: no watchdog fires, and the test, including its
  cleanup, hangs until the `go test` timeout instead of failing. When a test
  guards such a regression, probe the lock with `TryLock`, release whichever
  hold exists, then assert, as
  `internal/infra/node/force_remove_durability_test.go` does. Otherwise keep
  the test out of a bubble.
- Pebble and file I/O work inside a bubble. Fake time does not move while a
  goroutine is in a system call or waits on a mutex.
- Do not use a bubble for real network I/O: `net.Listen`, `net.Dial`,
  `httptest.Server`, or gRPC over TCP. Do not use it for Docker,
  testcontainers, subprocesses, or envtest. Fake time does not move while a
  goroutine waits on a socket. Use `net.Pipe` or an in-memory fake instead.

For examples, see `internal/infra/node/node_shutdown_test.go`,
`internal/bootstrap/node_hook_test.go`, and
`internal/application/events/manager_startup_retry_test.go`.

### Defensive Checks

Always check that IDs are valid before use:

```go
Expect(followerID).NotTo(BeZero(), "followerID should not be zero")
Expect(followerID).To(BeNumerically(">", 0))
Expect(followerID).To(BeNumerically("<=", countInstances))
```

### Cleanup

Use `DeferCleanup` for automatic cleanup:

```go
BeforeEach(func() {
    tmpDir := GinkgoT().TempDir()
    DeferCleanup(func() {
        Expect(os.RemoveAll(tmpDir)).To(Succeed())
    })
})
```

## Test Execution

### Unit Tests

```bash
go test ./...
```

### Integration Tests

```bash
go test -tags integration ./...
```

Tests built with event-sink feature tags such as `kafka` or `clickhouse` start Testcontainers from package `TestMain`, so they require Docker access even when using `-run '^$'` for compile-only checks.

### Optional-feature internal tests

CI includes the offline bootstrap command tests in the optional-feature coverage
recipe below. Run just these command tests with the `s3` tag:

```bash
nix develop .#ci --command go test -race -tags s3 ./cmd/ledgerctl/store -count=1 -timeout 5m
```

These command tests use an in-process object-store HTTP endpoint, without Docker
or cloud credentials. The retry regression creates a real full checkpoint and
nonempty incremental delta, interrupts after completed or partially committed
rebuild, then checks exact volumes/balances and Checker health after retry.

CI's `Tests` job runs `just test-internal-coverage` after light unit coverage.
It selects `./internal/...` and `./cmd/ledgerctl/store` with every optional feature
tag from `all_tags` and the race detector. This executes Kafka, NATS, ClickHouse and Databricks event
tests, S3 and Azure backup tests, and the S3 bootstrap lifecycle regression.
Full-tag E2E runs select different packages and do not execute these assertions.
The separate `integration` tag is not enabled by this recipe.

Kafka, ClickHouse and S3 tests require Docker for their Testcontainers; NATS
runs in-process, and the current internal Azure and Databricks tests require
no cloud credentials. Reproduce the CI step with:

```bash
nix develop .#ci --command just test-internal-coverage
```

The job uploads `build/coverage/internal.out` alongside `unit.out`. CI coverage
merging requires the internal profile, and `just coverage-all` also includes
this suite. A failure blocks the existing `Tests` release and image gates.

### E2E Tests

#### Pinned RC binary upgrade (Chainsaw)

After the harness is integrated into `release/v3.0`, run
`just test-rc1-rolling-upgrade` from a clean checkout of the integrated
revision. `tests/upgrade/run.sh` fetches the target branch, records both exact
commit SHAs, builds Linux images from those revisions, and runs the Chainsaw
scenario in a dedicated kind cluster. Its three Pods use separate persistent
WAL and data claims; Chainsaw replaces followers and then the old leader while
maintenance remains active. The test checks source and target transactions,
reads on every voter, mixed-window write rejection, durable Raft cursors,
leadership transfer, and each voter’s audit/derived-store checker.

The `.#upgrade` Nix shell provides Chainsaw, kind, kubectl, gh, and Docker client;
a local Docker daemon is still required. `UPGRADE_PHASE_SECONDS` sets the
minimum mixed-window soak (default 15 seconds). Use `KEEP_WORKDIR=1` to retain
revision and image hashes and failed Pod logs. The runner is separate from the
Antithesis model suite because that driver intentionally toggles maintenance.

#### Published release to branch HEAD (operator)

Dispatch `.github/workflows/release-operator-upgrade.yml` with a
`release/vX.Y` branch, or run
`RELEASE_BRANCH=release/v3.0 just test-release-operator-upgrade` locally. The
launcher selects the latest published stable or RC release tag reachable from
the branch HEAD, pulls its published image, builds the operator and target
Ledger from the exact HEAD, and runs Chainsaw in an isolated kind cluster.
The target Pod image tag is its full local image ID. The test checks a real
operator-driven StatefulSet rollout, mixed-version maintenance rejection,
retained PVC identities, data before and after, and every voter's store check.
The workflow saves the evidence under `build/upgrade-operator/` and uploads it
even on failure. A Docker daemon and access to the published GHCR image are
required. The image-ID tag does not exercise adjacent-RC tag admission.

```bash
# With Ginkgo
ginkgo run -tags e2e ./tests/e2e

# Or with go test
go test -tags e2e ./tests/e2e/...
```

#### Feature-gated suites

Some e2e files carry an extra build tag on top of `e2e` (for example
`//go:build e2e && s3`). They are compiled only when that tag is passed, so a
plain `-tags e2e` run neither builds nor runs them:

| Tag | Coverage | External dependency |
|-----|----------|---------------------|
| `s3` | backup, restore, bootstrap | MinIO (Testcontainers) |
| `azure` | Azure Blob backup | Azurite (Testcontainers) |
| `clickhouse` | ClickHouse event sink | ClickHouse (Testcontainers) |
| `nats` | NATS event sink | none (embedded in-process server) |
| `databricks` | Databricks event sink | real workspace credentials; skips itself when unset |

The bootstrap regression `TestRestoreDownloadStopsWithFxApplication` also needs
the `s3` tag to compile the production S3 backend, but uses an in-process HTTP
server and requires no MinIO or Docker when run alone. CI's `Tests` job includes
it in the optional-feature internal suite. Run just this regression with:

```bash
nix develop --command go test -race -tags s3 ./internal/bootstrap \
  -run '^TestRestoreDownloadStopsWithFxApplication$' -count=1 -timeout 2m
```

`just test-e2e` runs the light set for a fast local loop. **CI runs the full tag
set** through separate isolated Business and Cluster invocations of
`just test-e2e-coverage`, so a green `just test-e2e` does not guarantee a green
pipeline. The recipe's no-argument form retains the combined package selection
for local coverage. Reproduce the CI set locally with:

```bash
just test-e2e-full
```

`just lint` does not pass the `e2e` tag, so it excludes these packages even
when feature tags are configured. The full CI run is therefore responsible
for both compiling these files and exercising their assertions.

The MinIO and Azurite images used by this required gate are version-pinned in
`tests/e2e/testutil/containers.go`. Update those pins deliberately and validate
the complete full-tag suite when upgrading either emulator.

#### Docker socket on macOS

Testcontainers looks for `/var/run/docker.sock`. When Docker Desktop is
installed but stopped, that path exists as a symlink to its inactive socket,
and container startup fails with `Cannot connect to the Docker daemon` even
though the `docker` CLI works against another runtime. With OrbStack, point
Testcontainers at the right socket:

```bash
export DOCKER_HOST=unix://$HOME/.orbstack/run/docker.sock
```

### Operator Integration Tests

The default Nix shell provides `setup-envtest`, and its shell hook exports `KUBEBUILDER_ASSETS`. The first shell start downloads the etcd and kube-apiserver binaries. The lean `ci` shell does not include envtest. The `Tests-Operator` CI job uses the dedicated `operator-tests` shell for the focused Ledger target admission regressions, selecting Kubernetes 1.32.x assets:

```bash
nix develop .#operator-tests --command just operator-test-admission
```

```bash
# Run from misc/operator/ inside the default shell
cd misc/operator
go test -tags=integration ./internal/controller/ -v -count=1 -timeout=120s
```

### Specific Tests

```bash
# Single test
ginkgo run -tags e2e ./tests/e2e -focus "should continue to work"

# Tests with debug
DEBUG=true ginkgo run -tags e2e ./tests/e2e
```

## Coverage

### Generate Coverage

```bash
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out
```

The `just` coverage recipes (`test-coverage`, `test-internal-coverage`,
`test-postgres-mirror-coverage`,
`test-antithesis-assertions`, `test-e2e-coverage`, `test-scenarios-coverage`,
`fuzz-check-coverage`) run through `scripts/go-test-coverage` rather than
`-coverprofile`. With `-coverpkg=github.com/formancehq/ledger/v3/internal/...`,
`-coverprofile` concatenates one full profile per test binary, so the unit
profile grew past 1 GB. The script makes each binary write binary counters to
`build/coverage/<name>.covdata/` and merges them with
`go tool covdata textfmt` into a single `build/coverage/<name>.out` entry per
block. A `go test -exec` wrapper also copies the meta-data that cmd/go provides
for tested packages no test binary imports. Those packages still count as
uncovered, exactly as they did with `-coverprofile`.

### Coverage Goals

- **Minimum**: 70% for critical code
- **Ideal**: 80%+ for main components
- **Focus**: FSM, Business services, HTTP handlers

## Operator Integration Tests

**Location**: `misc/operator/internal/controller/*_test.go`

**Build tag**: `//go:build integration`

**Framework**: controller-runtime [envtest](https://book.kubebuilder.io/reference/envtest) — starts a real API server and etcd, installs CRDs, and runs the reconciler against them.

### Architecture

`suite_test.go` boots a shared envtest environment via `TestMain`:

1. Installs CRDs from `config/crd/bases/`
2. Creates a controller-runtime manager with metrics disabled
3. Registers `ClusterReconciler`
4. Starts the manager in a background goroutine

Each test gets an isolated namespace via `createTestNamespace(t)`. Helpers `newCluster` and `newLedgerDefaults` create minimal valid CRs.

### Test Files

| File | Coverage |
|------|----------|
| `reconcile_basic_test.go` | ServiceAccount, Services, StatefulSet creation, image defaults, ports, volumes, env vars, ownerRefs |
| `reconcile_defaults_test.go` | LedgerDefaults merge, override, not-found degraded, update propagation |
| `reconcile_validation_test.go` | Even replicas, ingress without hosts, validation recovery |
| `reconcile_status_test.go` | Phase=Pending, observedGeneration |
| `reconcile_ingress_test.go` | HTTP/gRPC Ingress (nginx, traefik), cleanup on disable, default paths |
| `reconcile_pdb_test.go` | PDB creation and cleanup |
| `reconcile_update_test.go` | Spec hash rolling updates |

### Writing New Tests

- Use `requireEventually(t, condition, msg)` (10s timeout, 250ms poll) to wait for reconciliation
- envtest has no kubelet — pods never become Ready, so `ReadyReplicas` stays 0
- Cluster-scoped resources (LedgerDefaults) need `t.Cleanup` for deletion
- Assertion helpers (`requireOwnerRef`, `requirePort`, `requireEnvVar`, etc.) are in `reconcile_basic_test.go`

## Performance Tests

Performance tests use k6 for load testing. See `tests/perf/README.md` for complete documentation.

### Running k6 Tests Locally

`k6` lives in the dedicated `perf` Nix shell, not the default one. Enter it with `nix develop .#perf` first.

```bash
# Run a specific scenario
k6 run tests/perf/scripts/world_to_bank.js

# With environment variables
LEDGER_URL=http://localhost:9000 \
LEDGER_NAME=my-ledger \
DURATION=60s \
VUS=20 \
k6 run tests/perf/scripts/world_to_bank.js

# Run all scenarios
for test in tests/perf/scripts/*.js; do
  echo "Running $test..."
  k6 run "$test"
done
```

### Running k6 Tests with the Kubernetes Operator

The development environment includes the **k6-operator** which allows running k6 tests as Kubernetes-native workloads. This is particularly useful for:

- Running distributed load tests across multiple pods
- Integrating load tests into CI/CD pipelines
- Automated benchmarking in the development cluster

#### Using the k6 Operator

1. **Deploy the development environment** (includes k6-operator):
   ```bash
   cd misc/devenv
   pulumi up
   ```

2. **Create a TestRun resource**:
   ```yaml
   apiVersion: k6.io/v1alpha1
   kind: TestRun
   metadata:
     name: ledger-benchmark
     namespace: bench
   spec:
     parallelism: 4
     script:
       configMap:
         name: k6-test-script
         file: world_to_bank.js
     arguments: -e LEDGER_URL=http://ledger-exp.ledger:9000 -e LEDGER_NAME=benchmark
   ```

3. **Monitor the test**:
   ```bash
   kubectl get testrun -n bench
   kubectl logs -f -l k6_cr=ledger-benchmark -n bench
   ```

#### Benchmark Operator Integration

The development environment also includes a **benchmark-operator** that watches for `TestRun` completions and automatically:
- Generates Grafana dashboard snapshots
- Creates Markdown reports with test results
- Stores artifacts for later analysis

See [Deployment - Development Environment](../../ops/deployment.md#development-environment-pulumi) for more details on the available tools.

### Go Benchmarks (for unit tests)

For Go benchmarks integrated in unit tests:

```bash
go test -bench=. -benchmem ./...
```

## Load Tests

E2E tests can be used for load testing:

```go
It("should handle high load", func() {
    const numTransactions = 1000
    var wg sync.WaitGroup
    
    for i := 0; i < numTransactions; i++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            _, err := client.Transactions.CreateTransaction(...)
            Expect(err).To(Succeed())
        }()
    }
    
    wg.Wait()
})
```

## Debugging Tests

### Detailed Logs

Enable debug logs:

```bash
DEBUG=true ginkgo run -tags e2e ./tests/e2e -v
```

### Pause on Error

Use `GinkgoT().Fail()` to pause for inspection:

```go
if err != nil {
    GinkgoT().Fail() // Pause here for inspection
}
```

### Manual Inspection

To inspect the cluster state during a test:

1. Don't cleanup immediately
2. Use `time.Sleep` temporarily for inspection
3. Access HTTP endpoints directly

## Schemathesis API conformance

Run the CI gate against a disposable single-node server without Docker:

```bash
nix develop .#ci --command bash -c 'MAX_EXAMPLES=10 SCHEMATHESIS_WORKERS=1 just test-schemathesis'
```

The harness runs its Python regression tests before the endpoint checks. Header
generation must satisfy HTTP field-value syntax as well as the OpenAPI schema:
permit HTAB, visible ASCII and obs-text, but exclude other control bytes and DEL.
Schema-derived strategies can generate header values that Go rejects before
routing.
Closing the connection with an unread request body can reset it before the
client receives the rejection, making the gate depend on TCP timing. The
`before_generate_headers` hook filters both inline and constrained shared header
parameters. Keep transport errors fatal: a later successful request does not validate a failed one.

## CI/CD

### GitHub Actions (Example)

```yaml
name: Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-go@v4
        with:
          go-version: '1.27.3'
      - run: go test ./...
      - run: go test -tags integration ./...
      - run: ginkgo run -tags e2e ./tests/e2e
```

## Next Steps

To learn more:

1. [Development](./development.md) - How to write testable code
2. [Architecture](../architecture/overview.md) - Understand the system to test
3. [Data Flows](../architecture/data-flows.md) - Understand the flows to test
