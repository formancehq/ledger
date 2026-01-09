# k6 Performance Tests

This directory contains k6 performance tests for the ledger-v3-poc service. These tests provide load testing capabilities for the ledger service.

## Prerequisites

Install k6:

```bash
# macOS
brew install k6

# Linux
sudo gpg -k
sudo gpg --no-default-keyring --keyring /usr/share/keyrings/k6-archive-keyring.gpg --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys C5AD17C747E3415A3642D57D77C6C491D6AC1D69
echo "deb [signed-by=/usr/share/keyrings/k6-archive-keyring.gpg] https://dl.k6.io/deb stable main" | sudo tee /etc/apt/sources.list.d/k6.list
sudo apt-get update
sudo apt-get install k6

# Windows
choco install k6

# Or download from https://k6.io/docs/getting-started/installation/
```

## Configuration

Tests are configured via environment variables. The default configuration is in `config.js`:

- `LEDGER_URL`: Base URL of the ledger service (default: `http://localhost:9000`)
- `LEDGER_NAME`: Name of the ledger to use for tests (default: `test-ledger`)
- `DURATION`: Test duration (default: `30s`)
- `VUS`: Number of virtual users (default: `10`)
- `MAX_VUS`: Maximum number of virtual users (default: `100`)

## Available Tests

The following test scenarios are available:

1. **world_to_bank.js**: Transactions from `@world` to `@bank`
2. **world_to_any.js**: Transactions from `@world` to variable destinations
3. **any_to_bank.js**: Transactions from variable sources to `@bank` with unbounded overdraft
4. **any_bounded_to_any.js**: Transactions from variable sources to variable destinations with bounded overdraft
5. **any_unbounded_to_any.js**: Transactions from variable sources to variable destinations with unbounded overdraft

## Usage

### Basic Usage

Run a single test scenario:

```bash
k6 run k6/scripts/world_to_bank.js
```

### With Environment Variables

```bash
LEDGER_URL=http://localhost:9000 \
LEDGER_NAME=my-ledger \
DURATION=60s \
VUS=20 \
k6 run k6/scripts/world_to_bank.js
```

### Custom Test Options

Override test options directly:

```bash
k6 run --duration 60s --vus 50 k6/scripts/world_to_bank.js
```

### Generate Summary Report

Tests automatically generate summary reports:

```bash
k6 run --out json=results.json k6/scripts/world_to_bank.js
```

### Run All Tests

Run all test scenarios:

```bash
for test in k6/scripts/*.js; do
  echo "Running $test..."
  k6 run "$test"
done
```

## Test Output

Each test outputs:

- **Transaction latency**: Custom metric tracking transaction processing time
- **Error rate**: Percentage of failed requests
- **HTTP request duration**: Standard k6 metric for HTTP request latency
- **Requests per second**: Throughput metric

### Thresholds

Tests include default thresholds:

- Error rate < 10%
- 95th percentile HTTP request duration < 500ms
- 95th percentile transaction latency < 500ms

You can override thresholds in the test files or via k6 options.

## Features

- **Standard load testing tool**: k6 is a widely-used load testing tool with extensive documentation
- **Better reporting**: k6 provides detailed metrics and can export to various formats (JSON, InfluxDB, CloudWatch, etc.)
- **Cloud execution**: k6 can run tests in the cloud via k6 Cloud
- **JavaScript ecosystem**: Easier to extend with JavaScript libraries
- **CI/CD integration**: Better integration with CI/CD pipelines
- **Custom metrics**: Track transaction latency and error rates

## Integration with CI/CD

Example GitHub Actions workflow:

```yaml
name: Performance Tests

on:
  schedule:
    - cron: '0 0 * * *'  # Daily
  workflow_dispatch:

jobs:
  performance:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Install k6
        run: |
          sudo gpg -k
          sudo gpg --no-default-keyring --keyring /usr/share/keyrings/k6-archive-keyring.gpg --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys C5AD17C747E3415A3642D57D77C6C491D6AC1D69
          echo "deb [signed-by=/usr/share/keyrings/k6-archive-keyring.gpg] https://dl.k6.io/deb stable main" | sudo tee /etc/apt/sources.list.d/k6.list
          sudo apt-get update
          sudo apt-get install k6
      - name: Run performance tests
        env:
          LEDGER_URL: http://localhost:9000
          LEDGER_NAME: test-ledger
        run: |
          k6 run --out json=results.json k6/scripts/world_to_bank.js
      - name: Upload results
        uses: actions/upload-artifact@v3
        with:
          name: performance-results
          path: results.json
```

## Troubleshooting

### Connection Errors

If you see connection errors, verify:

1. The ledger service is running and accessible
2. `LEDGER_URL` is correct
3. Network connectivity is working

### High Error Rates

If error rates are high:

1. Check ledger service logs
2. Verify the ledger exists and is accessible
3. Check if the service is overloaded
4. Review threshold settings in the test file

## Further Reading

- [k6 Documentation](https://k6.io/docs/)
- [k6 JavaScript API](https://k6.io/docs/javascript-api/)
- [k6 Metrics](https://k6.io/docs/using-k6/metrics/)
- [k6 Thresholds](https://k6.io/docs/using-k6/thresholds/)

