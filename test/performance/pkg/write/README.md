## Write performance tests

This package benchmarks write throughput and latency by executing JavaScript scenarios that create transactions under different feature configurations.

### What it does
- Loads JS scenarios from `scripts/` (or a custom script via flag)
- For each scenario, runs with multiple feature sets (MINIMAL, each feature toggled, FULL)
- Measures TPS and latency distribution (Tachymeter) and can capture internal metrics

### Structure
- `scripts/`: built-in scenarios (JS) exporting `next(iteration)` returning actions
- `example_scripts/`: example of a custom scenario

### Running
From this folder:

Run locally (current branch):
```bash
just run
```

Run against a remote ledger:
```bash
just run-remote https://ledger.example.com
```

Compare with main branch (benchstat):
```bash
just compare
```

What it does:
- Runs the write benchmarks on the current branch and saves the output to `./report/writes/benchmark-output-local.txt`
- Clones `main`, runs the same benchmarks there, and saves to `./report/writes/benchmark-output-main.txt`
- Uses `benchstat` to compute a statistical comparison and writes the result to `./report/writes/benchmark-comparison.txt`

**Note:** The comparison command only works with local benchmarks. It cannot be used with remote ledgers since comparing benchmarks running on different machines would not provide meaningful results.


Generate charts (from `report/writes/report.json`):
```bash
just graphs
```

Parameters (for `run`/`run-remote`):
- `bench` benchmark regex (default `.`)
- `p` parallelism (multiplied by `GOMAXPROCS`)
- `benchtime` duration per benchmark
- `count` number of iterations
- `output` path for console output (`./report/writes/benchmark-output.txt` by default)

Examples:
```bash
# Run only write benchmarks
just run BenchmarkWrite

# Run benchmarks matching a specific pattern
just run 'BenchmarkWrite/.*world_to_bank.*'

# Run with custom parallelism and time
just run BenchmarkWrite 4 10s 1
```

### Environment
- `DEBUG=true` enables HTTP debug transport

### Prerequisites
- A reachable ledger (local or remote)
- For meaningful results, ensure the ledger contains representative data for your scenarios

### Custom scenarios
Scenarios export a `next(iteration)` function returning an array of actions. Minimal example:

```javascript
function next() {
  return [
    {
      action: 'CREATE_TRANSACTION',
      data: {
        script: {
          plain: `send [USD/2 100] (\n  source = @world\n  destination = @bank\n)`,
          vars: {}
        }
      }
    }
  ]
}
```

Advanced example (from `example_scripts/example1.js`):
```javascript
const plain = `vars {
    account $order
    account $seller
}
send [USD/2 100] (
    source = @world
    destination = $order
)
send [USD/2 1] (
    source = $order
    destination = @fees
)
send [USD/2 99] (
    source = $order
    destination = $seller
)`

function next(iteration) {
    return [
        {
            action: 'CREATE_TRANSACTION',
            data: {
                script: {
                    plain,
                    vars: {
                        order: `orders:${uuid()}`,
                        seller: `sellers:${iteration % 5}`
                    }
                }
            }
        }
    ]
}
```

### Adding new scenarios
To add a new scenario:
1. Place your JavaScript file in the `scripts/` directory
2. Run it using the pattern `Write/<filename_without_js>` as the first parameter:

```bash
# If you added scripts/my_new_scenario.js
just run 'Write/my_new_scenario'
```

### Running via GitHub workflows
You can also run benchmarks using GitHub Actions workflows:

**Manual trigger** (`workflow_dispatch`):
- Go to Actions → Benchmark → Run workflow
- Configure parameters:
  - `bench`: Benchmark pattern (default: `.`)
  - `parallelism`: Number of parallel benchmarks (default: 5)
  - `duration`: Duration per benchmark (default: 10s)

The workflow will:
1. Run the write benchmarks with your parameters
2. Generate charts automatically
3. Upload results as artifacts under the "graphs" name

**Note**: This runs on GitHub's infrastructure, not against your local environment.

### Outputs
Results are saved under `report/writes/`:
- `report.json` structured results per scenario/config
- `benchmark-output.txt` console output (for benchstat)
- Charts (PNG) if generated via `just graphs`


