# E2E Tests

End-to-end tests for ledger using Ginkgo.

## Prerequisites

Install Ginkgo:
```bash
go install github.com/onsi/ginkgo/v2/ginkgo
```

## Running Tests

Run all e2e tests:
```bash
ginkgo run -tags e2e ./tests/e2e
```

Run a specific test:
```bash
ginkgo run -tags e2e ./tests/e2e -focus "Single Replica"
```

Run with verbose output:
```bash
ginkgo run -tags e2e -v ./tests/e2e
```

## Test Structure

- `e2e_suite_test.go`: Test suite setup
- `single_replica_test.go`: Tests for single replica deployment
