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

The `Cluster` struct in `internal/raft/node_test.go` provides a complete testing infrastructure for Raft node clusters:

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

`ChannelTransport` (`internal/raft/transport_channel.go`) is an in-memory implementation of the `Transport` interface using Go channels:

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

### E2E Tests

```bash
# With Ginkgo
ginkgo run -tags e2e ./tests/e2e

# Or with go test
go test -tags e2e ./tests/e2e/...
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

See [Deployment - Development Environment](./deployment.md#development-environment-pulumi) for more details on the available tools.

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
2. [Architecture](./architecture.md) - Understand the system to test
3. [Data Flows](./data-flows.md) - Understand the flows to test
