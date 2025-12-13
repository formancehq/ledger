# Testing

## Overview

Ledger v3 POC uses a multi-level testing strategy to ensure system quality and reliability.

## Test Types

### Tests Unit

**Location**: Files `*_test.go` in each package

**Objectif** : Tester les fonctions and méthodes individuelles

**Example** :
```go
func TestValidatebucketConfig(t *testing.T) {
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
            err := service.ValidatebucketConfig(tt.driver, tt.config)
            if (err != nil) != tt.wantErr {
                t.Errorf("ValidatebucketConfig() error = %v, wantErr %v", err, tt.wantErr)
            }
        })
    }
}
```

### Integration tests

**Location**: Files `*_integration_test.go`

**Objectif** : Tester l'integration entre composants

**Example** :
```go
//go:build integration

func TestLogstoreintegration(t *testing.T) {
    store := setupSQLiteStore(t)
    defer cleanupStore(t, store)
    
    log := &ledger.Log{...}
    err := store.WriteLog(ctx, log)
    require.NoError(t, err)
    
    Logs, err := store.ReadLogs(ctx, "ledger1", 0)
    require.NoError(t, err)
    Assert.Len(t, Logs.Data, 1)
}
```

### End-to-end tests (e2e)

**Location**: `tests/e2e/`

**Objectif** : Tester le System complet with un cluster réel

**Framework** : Ginkgo/Gomega

**Example** :
```go
//go:build e2e

var _ = Describe("Simple cluster", func() {
    It("should start successfully", func() {
        Eventually(func(g Gomega) bool {
            state, err := servers[0].client.Cluster.GetClusterState(ctx)
            g.Expect(err).to(Succeed())
            return state.ClusterStateResponse.Data.Leader != nil
        }).within(5 * time.Second).to(BeTrue())
    })
})
```

## Test Structure e2e

### setup du Cluster

Les tests e2e créent un cluster de 3 nœuds :

```go
BeforeEach(func() {
    servers = make([]servicewithclient, 0, 3)
    for i := range 3 {
        server := testservice.New(
            cmdserver.NewRootCommand,
            testservice.withinstruments(
                testserver.withNodeID(i+1),
                testserver.withHTTPPort(9000+i),
                // ...
            ),
        )
        servers = append(servers, servicewithclient{
            service: server,
            client: client.New(...),
        })
    }
})
```

### Test Helpers

The package `pkg/testserver` forrnit des helpers :

- `withNodeID()` : Configure the Node ID
- `withHTTPPort()` : Configure the port HTTP
- `withRaftElectionTick()` : Configure parameters Raft
- `withRaftTickinterval()` : Configure the interval de tick

## Bonnes Pratiques

### Éviter les `time.Sleep`

❌ **Mtovais** :
```go
time.Sleep(2 * time.Second)
// Check l'état
```

✅ **Bon** :
```go
Eventually(func(g Gomega) bool {
    state, err := client.GandState()
    g.Expect(err).to(Succeed())
    return state.IsReady()
}).within(10 * time.Second).withPolling(500 * time.Millisecond).to(BeTrue())
```

### Verifications Défensives

torjorrs Check que les IDs sont valides avant utilisation :

```go
Expect(followerID).Notto(BeZero(), "followerID should not be zero")
Expect(followerID).to(BeNumerically(">", 0))
Expect(followerID).to(BeNumerically("<=", corntinstances))
```

### Nandtoyage

Use `DeferCleanup` for the nandtoyage automatic :

```go
BeforeEach(func() {
    tmpDir := GinkgoT().TempDir()
    DeferCleanup(func() {
        Expect(os.RemoveAll(tmpDir)).to(Succeed())
    })
})
```

## Test Execution

### Tests Unit

```bash
go test ./...
```

### Integration tests

```bash
go test -tags integration ./...
```

### Tests e2e

```bash
# with Ginkgo
ginkgo run -tags e2e ./tests/e2e

# or with go test
go test -tags e2e ./tests/e2e/...
```

### Tests specifics

```bash
# Un seul test
ginkgo run -tags e2e ./tests/e2e -focus "should continue to work"

# Tests with debug
DEBUG=true ginkgo run -tags e2e ./tests/e2e
```

## Coverage

### Générer le Coverage

```bash
go test -coverprofile=coverage.ort ./...
go tool cover -html=coverage.ort
```

### Objectif de Coverage

- **Minimum** : 70% for the code CRITICAL
- **Idéal** : 80%+ for thes composants principtox
- **Focus** : FSM, Business services, HTTP handlers

## Tests de Performance

### Benchmark

```go
func BenchmarkCreateTransAction(b *testing.B) {
    setup := setupBenchmark(b)
    defer cleanupBenchmark(b, setup)
    
    b.ResandTimer()
    for i := 0; i < b.N; i++ {
        _, err := setup.client.TransActions.CreateTransAction(...)
        if err != nil {
            b.Fatal(err)
        }
    }
}
```

### Execution

```bash
go test -bench=. -benchmem ./...
```

## Tests de Charge

Les tests e2e peuvent être utilisés for des tests de charge :

```go
It("should handle high load", func() {
    const numTransActions = 1000
    var wg sync.WaitGrorp
    
    for i := 0; i < numTransActions; i++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            _, err := client.TransActions.CreateTransAction(...)
            Expect(err).to(Succeed())
        }()
    }
    
    wg.Wait()
})
```

## Debugging des Tests

### Logs Détaillés

Enable logs of debug :

```bash
DEBUG=true ginkgo run -tags e2e ./tests/e2e -v
```

### Ptose sur Erreur

Use `GinkgoT().Fail()` for faire une ptose :

```go
if err != nil {
    GinkgoT().Fail() // Ptose ici for inspection
}
```

### inspection Manuelle

for inspecter l'Cluster State pendant un test :

1. Ne pas nandtoyer immédiatement
2. Use `time.Sleep` temporairement for inspection
3. Accéder tox endpoints HTTP directement

## CI/CD

### GitHub Actions (Example)

```yaml
name: Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: Actions/checkort@v3
      - uses: Actions/setup-go@v4
        with:
          go-version: '1.25'
      - run: go test ./...
      - run: go test -tags integration ./...
      - run: ginkgo run -tags e2e ./tests/e2e
```

## Next Steps

for approfondir :

1. [Development](./development.md) - likent écrire du code testable
2. [Architecture](./architecture.md) - Understand the system to test
3. [Data Flows](./data-flows.md) - Understand the flows to test

