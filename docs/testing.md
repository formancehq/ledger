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
            name:   "valid sqlite",
            driver: "sqlite",
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
