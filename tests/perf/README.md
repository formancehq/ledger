# k6 Performance Tests

This directory contains k6 performance tests for the ledger service, deployed to Kubernetes via Pulumi.

## Prerequisites

- [k6](https://k6.io/docs/getting-started/installation/) (used to create test archives)
- [Pulumi](https://www.pulumi.com/docs/install/) (deployment)
- Go 1.26+

## Project Structure

```
tests/perf/
├── main.go                              # Pulumi program
├── Pulumi.yaml                          # Pulumi project
├── Pulumi.acme.yaml                     # ACME stack config (benchmark inlined)
├── Pulumi.devenv.yaml                   # Dev env stack config (benchmark inlined)
├── scripts/
│   ├── shared/                          # Shared utilities and configuration
│   │   ├── config.js                    # Configuration file
│   │   ├── options.js                   # k6 options builder
│   │   ├── http_utils.js               # HTTP request utilities
│   │   └── log_utils.js                # Logging utilities
│   ├── world_to_bank.js                 # @world -> @bank
│   ├── world_to_any.js                  # @world -> variable destinations
│   ├── any_to_bank.js                   # Variable sources -> @bank
│   ├── any_bounded_to_any.js            # Bounded overdraft transactions
│   ├── any_unbounded_to_any.js          # Unbounded overdraft transactions
│   ├── any_force_to_any.js              # Force mode transactions
│   └── single_hot_account.js            # Extreme single account contention
└── archives/                            # Generated k6 archives (gitignored)
```

## Kubernetes Deployment

The Pulumi program creates:
1. A **ConfigMap** with the k6 test archive (built automatically via `k6 archive`)
2. A **Benchmark CR** (`benchmark.formance.com/v1alpha1`) with LedgerService/Ledger resources and the TestRun spec

### Deploy

```bash
cd tests/perf

# Select a stack
pulumi stack select acme

# Preview changes
pulumi preview

# Deploy
pulumi up

# Tear down
pulumi destroy
```

### Configuration

The benchmark configuration is inlined directly in each `Pulumi.<stack>.yaml` file under the `perf:benchmark` key:

```yaml
# Pulumi.acme.yaml
config:
  perf:k8s-context: eks-acme-dev-euw1-01
  perf:benchmark:
    script: world_to_bank
    metadata:
      name: acme-world-to-bank
      namespace: ledger-v3
    spec:
      parallelism: 1
      separate: true
      arguments: "-o opentelemetry"
    resources:
      - manifest:
          apiVersion: ledger.formance.com/v1alpha1
          kind: LedgerService
          spec: { ... }
        readyCondition:
          fieldPath: status.phase
          value: Running
    runner:
      env:
        LEDGER_NAME: "ledger0"
        K6_STAGES: "1m:100,5m:100,1m:0"
```

## Local Usage

Run tests locally without Kubernetes:

```bash
k6 run scripts/world_to_bank.js
```

With environment variables:

```bash
LEDGER_URL=http://localhost:9000 \
LEDGER_NAME=my-ledger \
K6_STAGES="1m:10" \
k6 run scripts/world_to_bank.js
```

## Configuration

Tests are configured via environment variables (see `scripts/shared/config.js`):

- `LEDGER_URL`: Base URL of the ledger service (default: `http://localhost:9000`)
- `LEDGER_NAME`: Name of the ledger (default: `ledger0`)
- `DURATION`: Test duration (default: `30s`)
- `VUS`: Number of virtual users (default: `10`)
- `MAX_VUS`: Maximum VUs (default: `100`)
- `BULK_SIZE`: Transactions per bulk request (default: `1`)
- `BULK_ATOMIC`: Atomic mode for bulk operations (default: `false`)

## Available Tests

### Basic Scenarios

1. **world_to_bank.js**: `@world` -> `@bank`
2. **world_to_any.js**: `@world` -> variable destinations
3. **any_to_bank.js**: Variable sources -> `@bank` with unbounded overdraft
4. **any_bounded_to_any.js**: Bounded overdraft transactions
5. **any_unbounded_to_any.js**: Unbounded overdraft (supports `BULK_SIZE`)
6. **any_force_to_any.js**: Force mode transactions (supports `BULK_SIZE`)

### High Contention Scenarios

7. **single_hot_account.js**: Extreme contention on a single account

**Modes** (`CONTENTION_MODE` env var): `deposit`, `withdraw`, `transfer`, `mixed` (default)

## Thresholds

- Error rate < 10%
- 95th percentile HTTP request duration < 500ms
- 95th percentile bulk latency < 500ms

## Benchmark Operator

Benchmark reporting is handled by the operator in `misc/benchmark-operator`, which watches `TestRun` objects, creates Grafana snapshots, and emits a JSON report:

```bash
kubectl get configmap k6-report-<testrun-name> -n <namespace> -o jsonpath='{.data.report\.json}'
```
