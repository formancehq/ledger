# Performance Benchmark Tool

A conventional Go binary for running performance benchmarks on ledger-v3-poc.

## Installation

The required dependencies (`golang.org/x/oauth2` and `github.com/jamiealquiza/tachymeter`) are already included in the project's `go.mod`.

Build the binary:

```bash
go build -o performance ./cmd/performance
```

## Usage

### Running against a local ledger

```bash
./performance --ledger.url=http://localhost:9000 --ledger.name=my-ledger --duration=30s --parallelism=4
```

### Running against a remote ledger with authentication

```bash
./performance \
  --ledger.url=https://ledger.example.com \
  --auth.url=https://auth.example.com \
  --client.id=your-client-id \
  --client.secret=your-client-secret \
  --ledger.name=my-ledger \
  --duration=60s \
  --parallelism=8
```

### Running against a Formance stack

```bash
./performance \
  --stack.url=https://stack.example.com \
  --client.id=your-client-id \
  --client.secret=your-client-secret \
  --ledger.name=my-ledger \
  --duration=60s \
  --parallelism=8
```

## Options

- `--ledger.url`: Ledger URL (required if `--stack.url` is not provided)
- `--stack.url`: Formance stack URL (required if `--ledger.url` is not provided)
- `--auth.url`: Authentication server URL (required if `--ledger.url` is provided)
- `--client.id`: OAuth2 client ID
- `--client.secret`: OAuth2 client secret
- `--duration`: Benchmark execution duration (e.g., `30s`, `5m`)
- `--iterations`: Number of iterations (0 = use duration)
- `--parallelism`: Number of parallel goroutines (default: 1)
- `--ledger.name`: Ledger name (required)
- `--report.file`: Path to JSON report file (optional)
- `--debug`: Enable debug logging

## Examples

### Short benchmark with report

```bash
./performance \
  --ledger.url=http://localhost:9000 \
  --ledger.name=my-ledger \
  --duration=10s \
  --parallelism=4 \
  --report.file=./report.json
```

### Benchmark with fixed number of iterations

```bash
./performance \
  --ledger.url=http://localhost:9000 \
  --ledger.name=my-ledger \
  --iterations=1000 \
  --parallelism=8
```

## Scenarios

The binary currently runs two simple scenarios:

1. **world_to_bank**: Transfer 100 USD from "world" to "bank"
2. **bank_to_user**: Transfer 50 USD from "bank" to "user"

## Report

If `--report.file` is provided, a JSON report will be generated with the following metrics:

- TPS (Transactions per second)
- Average latency
- Min/max latency
- Percentiles (p50, p95, p99)
- Total number of transactions

## Development

To add new scenarios, modify the `main.go` file and add new `ActionProvider` implementations in the `Run()` function.
