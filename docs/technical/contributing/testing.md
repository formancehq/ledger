# Testing

## Overview

Ledger v3 POC uses a multi-level testing strategy to ensure system quality and reliability.

## Test Types

### Unit Tests

**Location**: Files `*_test.go` in each package

**Objective**: Test individual functions and methods

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

### Integration Tests

**Location**: Files `*_integration_test.go`

**Objective**: Test integration between components

Store integration tests cover both runtime state and log persistence now that log storage is part of the store.

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

E2E tests create a 3-node cluster:

```go
BeforeEach(func() {
    servers = make([]serviceWithClient, 0, 3)
    for i := range 3 {
        server := testservice.New(
            cmdserver.NewRootCommand,
            testservice.WithInstruments(
                testserver.WithNodeID(i+1),
                testserver.WithHTTPPort(9000+i),
                // ...
            ),
        )
        servers = append(servers, serviceWithClient{
            service: server,
            client: client.New(...),
        })
    }
})
```

### Test Helpers

The package `pkg/testserver` provides helpers:

- `WithNodeID()`: Configure the Node ID
- `WithHTTPPort()`: Configure the HTTP port
- `WithRaftElectionTick()`: Configure Raft parameters
- `WithRaftTickInterval()`: Configure the tick interval

## Unit Testing Infrastructure

### Cluster Helper

The `Cluster` struct in `internal/infra/node/node_test.go` provides a complete testing infrastructure for Raft node clusters:

```go
type Cluster struct {
    nodes map[uint64]*ClusterNode
}

type ClusterNode struct {
    ID        uint64
    Node      *Node
    Transport *ChannelTransport
    
    // Underlying implementations
    Store storepkg.Store
    WAL   WAL
    Spool Spool
    
    // Interceptors for testing
    StoreInterceptor *storepkg.StoreInterceptor
    WALInterceptor   *WALInterceptor
    SpoolInterceptor *SpoolInterceptor
}
```

**Key methods**:
- `NewCluster()`: Create a new test cluster
- `AddNode()`: Add a node to the cluster
- `Start()`: Start all nodes
- `Stop()`: Stop all nodes gracefully
- `DisconnectNode()`: Simulate network partition by disconnecting a node
- `ReconnectNode()`: Restore network connectivity
- `RestartNode()`: Simulate node crash and restart
- `WaitForLeader()`: Wait until a leader is elected

### ChannelTransport

`ChannelTransport` (`internal/infra/node/transport_channel.go`) is an in-memory implementation of the `Transport` interface using Go channels:

```go
transport := NewChannelTransport(nodeID, DefaultChannelTransportConfig())

// Connect two transports
transport1.Connect(transport2)

// Disconnect a peer
transport1.Disconnect(peerID)

// Check connection status
if transport.IsConnected(peerID) {
    // Peer is connected
}
```

**Benefits**:
- **No network overhead**: Messages are sent directly through channels
- **Deterministic testing**: No network timing issues
- **Easy fault injection**: Disconnect/reconnect nodes at will
- **Fast tests**: No gRPC connection setup

### Interceptors

Interceptors allow injecting custom logic into Spool, WAL, and Store operations during tests. They are useful for simulating failures, blocking operations, or inspecting behavior.

#### SpoolInterceptor

```go
interceptor := NewSpoolInterceptor(realSpool)

// Intercept AppendCommittedEntries
interceptor.SetAppendCommittedEntriesInterceptor(
    func(ctx context.Context, delegate Spool, entries []raftpb.Entry) error {
        // Custom logic before/after the real call
        return delegate.AppendCommittedEntries(ctx, entries...)
    },
)

// Clear all interceptors
interceptor.ClearInterceptors()
```

#### WALInterceptor

```go
interceptor := NewWALInterceptor(realWAL)

// Intercept CreateSnapshot to simulate failures
interceptor.SetCreateSnapshotInterceptor(
    func(delegate WAL, i uint64, r *raftpb.ConfState, data []byte) error {
        return errors.New("simulated snapshot failure")
    },
)
```

#### StoreInterceptor

```go
interceptor := store.NewStoreInterceptor(realStore)

// Intercept GetAllLogs to block synchronization
var blockCh = make(chan struct{})
interceptor.SetGetAllLogsInterceptor(
    func(ctx context.Context, delegate store.Store, ledger uint32, startLogID uint64) (*store.LogCursor, error) {
        <-blockCh // Block until unblocked
        return delegate.GetAllLogs(ctx, ledger, startLogID)
    },
)
// Later: close(blockCh) to unblock
```

### LogReaderProvider

`LogReaderProvider` abstracts how a Raft node obtains a `LogReader` for a peer during synchronization:

```go
type LogReaderProvider interface {
    GetForPeer(id uint64) (store.LogReader, error)
}
```

**In tests**, use `ClusterLogReaderProvider` to access peer stores directly:

```go
type ClusterLogReaderProvider struct {
    cluster *Cluster
}

func (p *ClusterLogReaderProvider) GetForPeer(id uint64) (store.LogReader, error) {
    node := p.cluster.GetNodeByID(id)
    if node == nil {
        return nil, fmt.Errorf("peer %d not found", id)
    }
    return node.StoreInterceptor, nil // Use interceptor for test control
}
```

**In production**, use `GRPCLogReaderProvider` to stream logs via gRPC.

## Antithesis (Autonomous Testing)

**Location**: `tests/antithesis/`

**Objective**: Drive the running system with randomized workloads and assert
high-level correctness properties under fault injection (node kills, network
faults, restarts). Workloads are written against the [Antithesis SDK](https://antithesis.com),
whose `random.*` choices the platform steers to explore rare interleavings, and
whose `assert.*` calls become catalogued properties.

### Drivers and templates

A **driver** is a standalone workload binary under
`tests/antithesis/workload/bin/cmds/`. Drivers are grouped into two **test
templates** — Antithesis runs exactly one template per execution history, so a
driver's template decides who it shares the timeline with:

| Template | Location | Shape |
|----------|----------|-------|
| `main` | `bin/cmds/main/` | Dozens of drivers run **concurrently** against one faulted cluster. Each targets a feature area (transactions, reverts, idempotency, account types, metadata, backups, chapters, …) and asserts feature-local invariants. |
| `model` | `bin/cmds/model/` | A single driver (`singleton_driver_model`) **owns the whole timeline** — no other driver runs alongside it — because it drives the system itself and checks global consensus conformance. |

The split is wired in `tests/antithesis/workload/Dockerfile`.

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

It exercises the chart of accounts, transactions and reverts (with post-commit
volumes), account/transaction/ledger metadata, the typed-metadata schema, and
the transient/ephemeral persistence classes.

#### How it works

N workers fan out across a fleet of ledgers, dispatching bulks concurrently;
the model mirrors the single Raft log (one global re-order buffer, one committed
state across all ledgers). The pieces:

| File | Role |
|------|------|
| `model.go` | The pure forward model: `GlobalState`/`LedgerState` + `Apply`, which predicts the server's legal outcome for a bulk, atomically across whatever ledgers it touches. |
| `checker.go` | Harness bookkeeping: the in-flight set, the re-order buffer, and the committed model state. |
| `processor.go` | One goroutine that re-orders observed responses by log sequence and drains them into the committed state in commit order. |
| `search.go` | `candidateBases` — enumerates the states the server could legitimately be in. |
| `validate.go` | The conformance checks for committed bulks, failures, and reads. |
| `actions.go` / `reads.go` | Random bulk generation; `GetAccount` / `GetLedger` read execution. |

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
default. Common tunables (full list in the script header):

| Variable | Meaning |
|----------|---------|
| `MODEL_LEDGERS` / `MODEL_WORKERS` | Fleet size and concurrency. |
| `MODEL_DEBUG` | Enable driver debug logging. |
| `MODEL_FAIL_FAST` | Stop on first finding (default); `0` runs the full duration. |
| `RESTART_INTERVAL` / `DEAD_TIME` | Cluster restart cadence and how long a killed node stays down. |
| `COMPACTION_MARGIN` | Raft entries between snapshots; low values force snapshot recovery. |

#### Maintaining the model

The model in `model.go` is a hand-written mirror of the server's *intended*
behavior; nothing keeps the two in sync automatically. Treat them as one unit —
when a change alters behavior the model relies on, update the model in the same
change, or the next run will either cry wolf (model stricter than the server) or
go blind (model more lenient than the server).

Where to make the matching change:

| Server change | Model change |
|---|---|
| New request type or ledger action | Add a `case` to `applyOne` in `model.go` **and** a generator in `actions.go`. |
| Changed business/validation rule (new rejection condition, enforcement change, volume math) | Update the matching `apply*` predictor in `model.go`. |
| New or changed response field the test should check | Update the validator in `validate.go` and the predicted effect it compares against. |
| New rejection reason | Return the matching `domain.ErrReason*` from the right model branch so `validateFailure` can explain it. |
| New persisted projection or read surface | Add the read in `reads.go` and a validator in `validate.go`, mirroring the account/ledger reads. |

The model is built to fail loud on anything it hasn't been taught: `Apply`
panics on an unmodeled request type or action rather than skipping it — the same
"never silently skip a should-not-happen branch" rule the server follows. So an
action you forget to model shows up as a finding, not a green run. Keep that
panic; don't downgrade it to a soft skip.

The runtime mechanisms the model cross-checks against (audit hash chain,
double-entry balance, the offline `store check`) are documented in
[Log Integrity and Correctness](../../ops/correctness.md).

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

### E2E Tests

```bash
# With Ginkgo
ginkgo run -tags e2e ./tests/e2e

# Or with go test
go test -tags e2e ./tests/e2e/...
```

### Operator Integration Tests

```bash
# Install envtest binaries (one-time)
go run sigs.k8s.io/controller-runtime/tools/setup-envtest@latest use --bin-dir /tmp/envtest-bins

# Run from misc/operator/
cd misc/operator
KUBEBUILDER_ASSETS=$(/tmp/envtest-bins/setup-envtest use -p path) \
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

Performance tests use k6 for load testing. See `k6/README.md` for complete documentation.

### Running k6 Tests Locally

```bash
# Run a specific scenario
k6 run k6/scripts/world_to_bank.js

# With environment variables
LEDGER_URL=http://localhost:9000 \
LEDGER_NAME=my-ledger \
DURATION=60s \
VUS=20 \
k6 run k6/scripts/world_to_bank.js

# Run all scenarios
for test in k6/scripts/*.js; do
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
          go-version: '1.25'
      - run: go test ./...
      - run: go test -tags integration ./...
      - run: ginkgo run -tags e2e ./tests/e2e
```

## Next Steps

To learn more:

1. [Development](./development.md) - How to write testable code
2. [Architecture](../architecture/overview.md) - Understand the system to test
3. [Data Flows](../architecture/data-flows.md) - Understand the flows to test
