## Read performance tests

This package benchmarks read latency/throughput for common API queries against a running ledger.

### What it does
- Executes a set of read operations (accounts, transactions, volumes) defined in a YAML file
- Measures latency and throughput per query and reports results

### Prerequisites
- A reachable ledger (local or remote)
- An already populated database with representative data. This suite does not seed or generate data

### Supported operations (YAML `operation` field)
- `list-accounts`: List accounts with optional `filter`, `expand`, and point-in-time (`pit`)
- `list-transactions`: List transactions with optional `filter`, `expand`, and point-in-time (`pit`)
- `list-volumes`: Get volumes with balances with optional time window (`oot` → start, `pit` → end), `insertionDate`, `groupBy`, and `filter`

### Example configuration
See `examples/config.yml` for a complete example.

```yaml
queries:
  - name: list accounts
    operation: list-accounts
    expand: [volumes, effectiveVolumes]
  - name: list volumes for users
    operation: list-volumes
    filter:
      $match:
        account: "::users:"
```

### How to run
From this folder:

```bash
just run my-ledger https://ledger.example.com examples/config.yml
```

Arguments:
- `ledger` (required): ledger name used by the queries
- `ledgerURL` (required): base URL of the target ledger
- `config` (required): path to the YAML configuration
- `bench`, `benchtime`, `count`: forwarded to `go test`

### Environment
- `DEBUG=true` enables HTTP debug logging

### Notes
- These benchmarks must be executed against a pre-populated ledger; the tool will not populate data for you
- Ensure the target ledger has data matching your filters to get meaningful results
- Each YAML `query` becomes a sub-benchmark named by `name`


