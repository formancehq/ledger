# Pebble to RocksDB performance comparison

The `Storage Performance Comparison` workflow runs when its benchmark harness changes in a pull request. It builds test binaries from the Pebble baseline (`c6a99fc92f43f5127c898fd9a727adee3424310d`) and the PR head on one Linux runner, warms both, then alternates five measured runs per engine. The workflow uploads raw Go benchmark output, runner details, and a median summary.

The first comparison covers warm DAL point lookups at 64, 512, and 4,096 bytes, small and large write batches, and one populated account reverse-map index lookup. The benchmarks use fresh stores for each case and the same source fixtures where possible. A positive RocksDB delta means slower `ns/op` for that case. These microbenchmarks do not establish HTTP latency, write throughput under contention, checkpoint pause time, memory use, or production capacity.

To repeat on a Linux machine with the repository's Nix shell and both commits available:

```bash
PEBBLE_REF=c6a99fc92f43f5127c898fd9a727adee3424310d \
ROCKSDB_REF=<rocksdb-commit-sha> \
nix develop --command bash scripts/compare-storage-performance.sh
```

For an end-to-end qualification, run the existing `tests/perf` k6 workload against separately deployed three-node clusters built from the two pinned commits. Keep node type, CPU and memory limits, cache configuration, data size, replica count, request mix, and offered rate equal. Measure sustained throughput, p50/p95/p99 latency, error and dropped-iteration counts, aggregate CPU and RSS, disk growth, and checkpoint/restore pauses. Alternate run order and reject a run with request errors, dropped iterations, restarts, or resource throttling before interpreting an engine delta.
