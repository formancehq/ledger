# Deployment Profiles and Sizing

This guide helps choose a Ledger v3 topology and its main configuration from
customer requirements. It is a starting point for capacity tests, not a
substitute for a load test with the customer's Numscript programs, account
cardinality, metadata, and retention policy.

Keep the defaults unless a requirement or a metric below gives a reason to
change them. The complete flag reference remains in the [CLI reference](./cli.md),
while this page only covers the settings that materially affect deployment
choices.

## Start with the Customer Requirements

Collect these inputs before sizing the cluster:

| Requirement | Question to answer | Main deployment consequence |
|-------------|--------------------|-----------------------------|
| Availability | Is downtime acceptable? How many simultaneous failures must be tolerated? | One node for disposable environments; three nodes for production HA; five only to tolerate two voter failures. |
| Write load | Sustained and peak transactions per second? Typical bulk size? | CPU, WAL latency, network bandwidth, and workload design. |
| Read load | Query rate, filters, result sizes, and freshness requirement? | Main Pebble and read-index caches, indexes, and read routing. |
| Working set | How many accounts and assets are active in a short time window? | FSM cache rotation threshold and preload rate. |
| Data shape | Number of postings and metadata fields per transaction? | CPU, execution-plan size, Raft payload size, and disk growth. |
| Retention | Log and audit history is permanent in the primary store. | Disk sizing and the compression profile. |
| Recovery | Required RPO and RTO? How quickly must a new node join? | Backup cadence, restore drills, snapshot parallelism, and network capacity. |
| Security | Public network, private network, or regulated environment? | TLS, authentication, request/response signing, and BLAKE3. |
| Idempotency | How long may a client retry with the same key? | `--idempotency-ttl`; `0` retains keys forever. |
| Platform | Kubernetes, VMs, storage class, and failure domains? | Persistent volumes, scheduling, DNS, and advertised addresses. |

## Choose the Topology First

| Environment | Voters | Failure tolerance | Guidance |
|-------------|--------|-------------------|----------|
| Local development or disposable demo | 1 | None | Use `--bootstrap`; never present this as an HA setup. |
| Production | 3 | 1 voter | Default production topology. Spread nodes across failure domains and use independent persistent volumes. |
| Exceptional HA requirement | 5 | 2 voters | Use only when the second-failure tolerance justifies extra replication traffic and quorum latency. |

Adding Raft voters improves availability, not write throughput. Ledger v3 uses a
single Raft group, and every committed write is replicated to the quorum. Scale
vertically and optimize the workload before considering more voters.

### Kubernetes operator specifics

The in-repository operator targets development and testing; the supported
production installation is the Formance Stack Operator. When using the
in-repository operator, use its current `Cluster` resource, not the legacy
`Ledger` cluster examples that remain elsewhere in the documentation. It
requires Kubernetes 1.28 or later; PVC/PV deletion protection requires 1.30.

Install the operator and its CRD dependency, then start from the maintained
[Cluster sample](../../misc/operator/config/samples/ledger_v1alpha1_cluster.yaml):

```bash
helm dependency build misc/operator/helm/operator
helm install ledger-operator misc/operator/helm/operator \
  --namespace ledger-system \
  --create-namespace
```

For a three-node deployment, make scheduling and resources explicit:

```yaml
apiVersion: ledger.formance.com/v1alpha1
kind: Cluster
metadata:
  name: customer-ledger
spec:
  replicas: 3
  clusterID: customer-production
  podAntiAffinity:
    enabled: true
    type: hard
    topologyKey: kubernetes.io/hostname
  persistence:
    wal:
      size: 20Gi
    data:
      size: 100Gi
  resources:
    requests:
      cpu: "2000m"
      memory: 4Gi
    limits:
      cpu: "4000m"
      memory: 4Gi
  goMemLimitRatio: 90
```

The operator does not enable anti-affinity when `podAntiAffinity` is omitted.
It also does not currently create a PodDisruptionBudget. Create one separately
for a three-voter cluster:

```yaml
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: customer-ledger
spec:
  minAvailable: 2
  selector:
    matchLabels:
      app.kubernetes.io/name: ledger
      app.kubernetes.io/instance: customer-ledger
```

The operator derives `GOMEMLIMIT` from the container memory limit using
`goMemLimitRatio` (90% by default). Keep all voters in a low-latency network
region: cross-region synchronous replication directly increases write latency.
The operator handles first-node bootstrap and joins the other nodes as learners;
see [Cluster Lifecycle](./cluster-operations.md).

## Initial Resource Profiles

These profiles are conservative starting points per node. Benchmark before
production and size for peak traffic, compaction, backup, and follower catch-up,
not only steady-state traffic.

| Profile | Topology | CPU | Memory | Storage | Initial configuration |
|---------|----------|-----|--------|---------|-----------------------|
| Local / demo | 1 node | 2 cores | 2 GiB | One local SSD volume | `GOMEMLIMIT=1800MiB`, `--pebble-cache-size=256Mi`, `--pebble-memtable-size=64Mi` |
| Production baseline | 3 nodes | 4 cores | 4 GiB | Persistent SSD; separate WAL and data volumes when possible | `GOMEMLIMIT=3600MiB`; application defaults |
| High write throughput | 3 nodes | 8 cores | 4-8 GiB | Provisioned-IOPS SSD/NVMe; separate WAL and data volumes | Defaults first; bulk requests around 50 transactions; tune from queue, preload, and Pebble metrics |
| Read-heavy | 3 nodes | 4-8 cores | 8 GiB | Fast data volume; optionally isolate the read index | `GOMEMLIMIT=7200MiB`, `--pebble-cache-size=4Gi`, start with `--read-index-cache-size=128Mi` |

The default in-memory budget is approximately 3.2 GiB per node even before
workload-dependent gRPC buffers and transient allocations. A 512 MiB container
limit is therefore not compatible with the default storage configuration. The
server logs its estimated memory budget at startup; treat an estimate above
`GOMEMLIMIT` as a deployment error. See [Memory Management](./memory.md) for the
full formula.

The published performance reference reached approximately 106K transactions/s
with three nodes, 8 cores and 4 GiB per node, gp3 storage, and atomic bulks of 50.
It is evidence for that exact benchmark shape, not a general capacity promise.
See [Performance Benchmarks](../sales/benchmarks.md).

## Storage Sizing

Use distinct paths and, for demanding workloads, distinct devices:

| Path | Workload | Recommendation |
|------|----------|----------------|
| `--wal-dir` | Synchronous Raft WAL writes on the write critical path | Lowest-latency durable SSD/NVMe. Never use ephemeral storage in production. |
| `--data-dir` | Primary Pebble store, checkpoints, and projections | High-IOPS persistent SSD with compaction headroom. |
| `--read-index-dir` | Rebuildable query indexes | Fast persistent storage; isolation can protect the primary store from read-index I/O. Directory isolation is currently available only for CLI/VM deployments; the Operator keeps the read index on the data volume. |
| `--pebble-wal-failover-dir` | Secondary Pebble WAL path | A different physical volume; using the same device does not provide useful failover. This extra volume is currently available only for CLI/VM deployments. |

Measure disk growth during a representative test, including compactions and
indexes, then extrapolate it over the hot-retention period. Keep projected peak
usage below the warning level and enough free space for compaction. By default,
Ledger blocks writes at 80% usage and resumes below 75%; alert before the block
threshold. See [Disk Space Limiting](./disk-space.md).

## Recovery Objectives

Translate the customer's recovery promise into an operating procedure:

| Objective | Deployment decision | Validation |
|-----------|---------------------|------------|
| RPO | Run incremental backups at least as often as the maximum acceptable data-loss window. Store them outside the cluster failure domain. | Verify backup completion and retention; a configured job without a successful artifact does not meet the RPO. |
| RTO | Choose online restore or offline bootstrap from the measured recovery path, dataset size, download bandwidth, and rebuild time. | Time a full restore of production-sized data and compare the result with the promised RTO. |
| Disaster readiness | Retain the credentials, encryption keys, manifests, and versioned binary needed to restore into a fresh environment. | Run a restore exercise after material storage, version, credential, or topology changes, and at a regular customer-approved cadence. |

See [Backup and Restore](./backup-restore.md) for the supported backup and restore
flows.

See [Availability and Disaster Recovery](./availability-and-recovery.md) for
multi-region topology trade-offs and the supported responses to quorum loss.

## Tune by Workload Signal

| Customer need or observed signal | Parameter or action | Trade-off / guardrail |
|-----------------------------------|---------------------|-----------------------|
| Memory limit below 4 GiB | Reduce `--pebble-cache-size`, then `--pebble-memtable-size`; set `GOMEMLIMIT` to about 90% of the container limit | More disk reads and flushes; watch write stalls and query latency. |
| Read latency is high and disk reads dominate | Increase `--pebble-cache-size`; for filtered listings, increase `--read-index-cache-size` and create only the required indexes | Directly increases RSS. An index improves its matching query but adds build, storage, and write cost. |
| Admission preloads are slow and active keys are reused | Increase `--cache-rotation-threshold` from the default `1000` | Approximately linear cache-memory growth; roll the value consistently across the cluster. |
| Workload continually creates unique accounts | Keep the cache threshold near the default and focus on fast storage | A larger cache provides little benefit without key reuse. |
| Many Pebble lookups are for absent keys | Enable Bloom filters only for affected attribute types with a reliable distinct-key bound between rebuilds or an explicit resize policy | Filters consume fixed memory on every node and lose effectiveness when `expectedKeys` is exceeded. Do not enable them by habit. |
| More Numscript texts than the `1024`-entry cache | First replace generated scripts with variables; only then increase `--numscript-cache-size` | Increasing the cache hides inefficient script templating and consumes memory. |
| Propose queue fills | Use bulks, check leader CPU and WAL latency, then consider `--raft-propose-queue-capacity` | A larger queue absorbs bursts but does not increase sustainable throughput and increases latency under overload. |
| Pebble write stalls | Check storage latency and compaction metrics; consider more IOPS or `--pebble-max-concurrent-compactions` | More compactions consume CPU, I/O, and temporary memory. |
| Inter-node bandwidth is constrained | Test `--grpc-compression` for Ledger's outgoing internal gRPC pools | Saves inter-node bandwidth but costs CPU and can increase latency. Client-call compression remains a client configuration decision. |
| Followers join slowly | Increase `--snapshot-parallelism` from `4` and, for very large snapshots, `--snapshot-session-ttl` | More parallelism consumes network, disk I/O, file descriptors, and memory on both nodes. |
| Large values cause compaction amplification | Load-test `--pebble-value-separation` | Can reduce compaction I/O but adds read amplification and blob lifecycle tuning. |
| High-cardinality observability is expensive | Keep `--admission-metrics` disabled in steady-state high-throughput production; sample successful traces | Reduces diagnostic detail. Re-enable temporarily while investigating. |

For operator-managed clusters, the flags in this table map to the following CR
fields:

| CLI control | Operator field |
|-------------|----------------|
| `--pebble-cache-size`, `--pebble-memtable-size` | `spec.pebble.cacheSize`, `spec.pebble.memTableSize` |
| `--read-index-cache-size` | `spec.readIndex.pebble.cacheSize` |
| `--cache-rotation-threshold` | `spec.cache.rotationThreshold` |
| `--numscript-cache-size` | `spec.numscriptCacheSize` |
| `--raft-propose-queue-capacity` | `spec.raft.proposeQueueCapacity` |
| `--pebble-max-concurrent-compactions` | `spec.pebble.maxConcurrentCompactions` |
| `--grpc-compression` | `spec.grpcCompression` |
| `--snapshot-parallelism`, `--snapshot-session-ttl` | `spec.snapshot.parallelism`, `spec.snapshot.sessionTTL` |
| `--pebble-value-separation` | `spec.pebble.valueSeparation.enabled` |
| `--admission-metrics` | `spec.admissionMetrics` |

### Choose Pebble compression from the LSM workload

Compression is configured independently for the primary store and the read
index. Each value selects the profile used when Pebble writes a new SST block
at one of the seven LSM levels, from L0 through L6. It does not compress the
WAL. A change takes effect on new flushes and compactions; existing SSTs adopt
it gradually as Pebble rewrites them.

Ledger's default is:

```text
fastest,fastest,fastest,fastest,fast,fast,balanced
```

This keeps the frequently rewritten L0-L3 levels cheap, applies adaptive
compression to L4-L5, and spends more CPU on L6, where most long-lived data
normally resides. Keep this default until measurements identify CPU, storage
capacity, or storage bandwidth as the limiting resource.

| Profile | Behavior with the currently pinned Pebble 2.1.4 | Workload fit | Main cost |
|---------|-------------------------------------------------|--------------|-----------|
| `none` | Stores blocks without block compression | Only after profiling proves that even the compression attempt is material and the data is already incompressible | Maximum disk footprint, read I/O, backup size, and compaction I/O |
| `snappy` | Attempts Snappy for data, values, indexes, filters, and metadata, retaining blocks uncompressed when reduction is insufficient | Predictable low-CPU compatibility profile | Usually a lower compression ratio than adaptive profiles |
| `default` | Currently the same as `snappy` in the pinned Pebble version | Acceptable when following Pebble's version-dependent default is intentional | Its meaning may change after a Pebble upgrade; use `snappy` for a stable explicit choice |
| `fastest` | MinLZ-fastest on most architectures and Snappy on arm64; skips compression when the reduction is too small | Hot levels, CPU-bound writes, and latency-sensitive compactions | Favors CPU over disk reduction |
| `fast` | Fastest for data and metadata blocks, Zstd level 1 for value blocks, with adaptive fallback | Warm levels containing compressible values | More CPU than `fastest`, primarily for values |
| `balanced` | Zstd level 1 for data and value blocks, fastest for other blocks, with adaptive fallback | Cold, read-heavy data when storage bandwidth or capacity matters | Higher compaction and decompression CPU |
| `good` | Zstd level 3 for data and value blocks, fastest for other blocks, with adaptive fallback | Deep cold levels when disk reduction is worth additional background CPU | Highest CPU cost among the adaptive profiles |
| `zstd` | Zstd level 3 uniformly for every compressible block type | A deliberate uniform policy validated by benchmarks | Can spend CPU on hot data and non-data blocks where it has little payoff |

Use exactly seven comma-separated values. The following starting points change
only the levels justified by the workload:

| Observed constraint | Starting profile | Why |
|---------------------|------------------|-----|
| Balanced production workload | `fastest,fastest,fastest,fastest,fast,fast,balanced` | Preserves Ledger's tested default. |
| CPU-bound ingestion with healthy disk headroom | `fastest,fastest,fastest,fastest,fastest,fastest,fastest` | Reduces compaction CPU before considering `none`; `fastest` already abandons blocks that do not compress enough. |
| Storage-bound or large read-heavy history with spare CPU | `fastest,fastest,fastest,fastest,fast,balanced,good` | Keeps hot levels cheap and concentrates stronger compression on long-lived levels. |
| Incompressible payloads and measured compression CPU | `none,none,none,none,none,none,none` | Use only if a representative benchmark shows an end-to-end gain after accounting for extra I/O. |

For Kubernetes, configure the stores separately so an index-heavy read
workload does not force the same choice on the consensus store:

```yaml
spec:
  pebble:
    compression: "fastest,fastest,fastest,fastest,fast,fast,balanced"
  readIndex:
    pebble:
      compression: "fastest,fastest,fastest,fastest,fast,balanced,good"
```

Compare steady-state CPU, compaction duration, write stalls, Pebble VFS
operations, storage-level throughput, disk growth, backup size, and read
latency before and after the change. Run the comparison after enough
compaction has occurred to rewrite a representative share of the old SSTs. A
smaller database immediately after a configuration change is not evidence that
foreground latency improved.

### Enable application Bloom filters only with a cardinality plan

Ledger has two different Bloom-filter layers:

- Pebble SSTable filters are always configured internally at 10 bits per key
  on every LSM level. They help Pebble avoid reading irrelevant SSTables after
  a request reaches the database and have no customer-facing sizing flag.
- Ledger's application Bloom filters sit before Pebble in the shared
  execution-plan preload resolver used by admission, mirror, and other
  proposal producers. They are optional and configured independently for each
  attribute type with `expectedKeys` and `fpRate`. A definite absence avoids
  the Pebble lookup entirely.

The application filters are useful only when all of the following are true:

1. Cache misses frequently ask for keys that do not exist. Existing-key reads
   cannot be skipped by a Bloom filter.
2. The live keys loaded by the last rebuild plus the distinct keys added since
   then have a reliable bound, or the operator has an explicit policy to
   measure, redimension, and rebuild the filter before its capacity is
   exhausted. The filter is monotonic: deleting a key does not clear its bits,
   so high churn can exhaust the capacity even when the live dataset stays
   small.
3. Avoided storage latency is worth the per-node memory and rebuild I/O.

Exceeding `expectedKeys` does not create false negatives or corrupt results.
The bitset stays at its allocated size, false positives rise, and the filter
eventually stops avoiding useful reads. For an unbounded ledger without a
monitored resize policy, leaving the filter disabled is better than assigning
an arbitrary large number.

The blocked-filter implementation uses approximately 11 bits per expected key
at a 1% target false-positive rate and 17 bits per key at 0.1%, rounded to
512-bit blocks. The allocation exists independently on every node:

| `expectedKeys` | Memory per node at `fpRate: "0.01"` | Memory per node at `fpRate: "0.001"` |
|----------------|--------------------------------------|---------------------------------------|
| 1 million | about 1.3 MiB | about 2.0 MiB |
| 10 million | about 13 MiB | about 20 MiB |
| 100 million | about 131 MiB | about 203 MiB |

These figures cover the main bitset; allow additional runtime and persisted
block-tracking overhead. Start at 1%. A false positive is safe and costs only
the Pebble read that the filter failed to avoid, so lowering the rate is useful
only when storage misses remain expensive after the 1% filter is enabled.

Choose types from the actual preload workload:

| Type | Cardinality unit | When it can help | Typical decision |
|------|------------------|------------------|------------------|
| `volumes` | One key per ledger, account, asset, and color combination | First-touch postings produce many absent-key preloads and storage misses are expensive | Best high-throughput candidate when the maximum combination count is contractually bounded; often the largest filter |
| `metadata` | One key per account metadata field | A bounded entity population receives a fixed metadata schema and many fields are written for the first time | Enable selectively; arbitrary field names or create/delete churn make the cumulative bound unreliable |
| `references` | One key per retained transaction reference | A finite import or a product has a hard ceiling or a managed resize/rebuild schedule | Usually disable for indefinitely growing transaction history without that schedule |
| `transactions` | One state key per transaction | Revert or transaction-metadata traffic contains many non-existent IDs | Normal traffic targets existing transactions, so the negative ratio is usually too low to justify a large filter |
| `ledgers`, `boundaries` | Approximately one key of each type per ledger | Requests frequently target unknown ledger names | Cheap when ledger count is bounded, but normal traffic is mostly positive; validate the avoided-read ratio |
| `numscriptVersions`, `numscriptContents` | One latest pointer per script name and one key per saved version | A finite script catalog has repeated first-save or missing-version checks | Reasonable for a bounded catalog; usually small and management-path only |
| `ledgerMetadata` | One key per ledger metadata field | Ledger metadata keys come from a small governed schema | Bounded but normally too low-frequency to matter |
| `preparedQueries` | One key per named prepared query | The catalog is bounded and management calls often probe new or missing names | Usually negligible compared with volume preloads |
| `sinkConfigs` | One key per event sink | Control-plane calls frequently probe missing sink names | Usually too few operations to justify enabling |
| `indexes` | One key per index registry entry | Index-management operations repeatedly probe missing definitions | Low-cardinality control plane; CLI-only in the current operator API and normally not worth enabling |

Derive `expectedKeys` from the live cardinality loaded at rebuild plus the
forecast cumulative distinct additions before the next rebuild, then add
explicit headroom and a resize threshold. This example is valid only for a
customer whose sizing period caps that combined population at 25 million
volume keys and 250,000 metadata keys; it adds roughly 25% headroom:

```yaml
spec:
  bloom:
    volumes:
      expectedKeys: 32000000
      fpRate: "0.01"
    metadata:
      expectedKeys: 320000
      fpRate: "0.01"
```

Leave every other type omitted, which is equivalent to `expectedKeys: 0`.
Changing any one setting purges the persisted Bloom namespace, rebuilds all
enabled application filters from a full attribute scan, and temporarily sets
the global `bloom.ready` gauge to `0`. While it is not ready, preload
resolution safely falls back to Pebble reads. Roll out one sizing change at a
time and wait for every node to report ready before judging performance.

Validate ratios per attribute `type`. Validate `bloom.ready` globally for the
whole filter set:

- `bloom.negatives / bloom.lookups` is the fraction of filter checks that
  avoid Pebble. If it remains low, disable that type.
- `bloom.false_positives / (bloom.negatives + bloom.false_positives)`
  approximates the observed false-positive rate for absent keys. Sustained
  growth above the target indicates that the cardinality assumption is stale.
- `bloom.ready` must return to `1`; also verify that preload resolution latency
  and Pebble VFS read operations actually fall.

See [Performance Tuning](./performance-tuning.md#54-bloom-filters),
[Monitoring](./monitoring.md#bloom-filter-metrics), and the
[CLI reference](./cli.md#server-bloom-filter-flags) for the related controls
and metrics.

Do not tune Raft election ticks, heartbeat ticks, message size, or processing
intervals from a generic profile. Change them only after measuring actual
network latency and queue behavior. Bad timing values can reduce availability.

## Requirement-Specific Additions

### Security or regulated audit

Use the production topology, then add:

- `--tls-mode required` for both service and inter-node gRPC;
- a `--cluster-secret` only together with TLS;
- OIDC or Ed25519 API authentication with least-privilege scopes;
- `--hash-algorithm blake3` (the default) for cryptographic collision
  resistance;
- receipt and/or response signing when clients must independently verify server
  results;
- scheduled backups, restore drills, and periodic `ledgerctl store check` runs.

BLAKE3 does not provide an external trust anchor: an attacker who can rewrite the
store can also recompute the chain from the modified point onward. See the
[audit hash keying threat model](./deployment.md#audit-hash-keying--threat-model)
before making regulatory or non-repudiation claims.

`--sentinel-mode` performs additional consistency checks and Pebble reads. Use it
for tests or targeted diagnostics unless its production overhead has been
validated with the customer's load.

### Long client retry window

Set `--idempotency-ttl` to at least the maximum retry window promised to clients.
Treat changing it as an intentional migration, not ordinary tuning, and keep it
consistent across nodes. Non-zero persisted values are validated at startup;
for backward compatibility, a persisted `0` is treated as an older unset field
and may transition to a finite value without the unsafe override. `0` means keys
never expire and must be paired with a deliberate storage-growth policy.

### High-throughput ingestion

The client-side request shape is as important as server flags:

- use Numscript variables instead of generating unique scripts;
- begin with bulks of 50 and measure throughput, latency, and payload size;
- keep the WAL on the fastest durable volume;
- prefer BLAKE3 for cryptographic collision resistance; select XXH3 only when
  the customer explicitly accepts non-cryptographic corruption detection;
- do not expect extra Raft voters to increase write throughput.

See [Performance Guidelines](./performance-tuning.md) for workload design.

## Production Baseline Checklist

Before go-live, verify:

- three voters are healthy, on separate failure domains, and can re-elect a
  leader after one node is stopped;
- PVC retention, backup retention, encryption, and restore procedures match the
  customer's RPO/RTO;
- `node-id`, `cluster-id`, advertised address, data paths, and idempotency TTL are
  stable across restarts;
- TLS and authentication are enabled, and the Raft port is not publicly exposed;
- memory estimates fit below `GOMEMLIMIT` and the container limit;
- steady-state and peak disk usage stay below alert thresholds;
- dashboards cover Raft leadership, queue saturation, write stalls, preload
  latency, read-index lag, disk usage, and request errors;
- a representative peak load test includes compaction, backup, node restart,
  follower catch-up, and one-node failure;
- configuration changes follow the documented [rolling upgrade](./cluster-operations.md#cluster-configuration-updates)
  or the component-specific migration procedure.

## Related Documentation

- [Ledger Operator](../../misc/operator/README.md) — current `Cluster` CRD and Kubernetes mechanics
- [Memory Management](./memory.md) — per-component memory budget
- [Performance Guidelines](./performance-tuning.md) — client and hot-path tuning
- [Monitoring](./monitoring.md) — metrics, dashboards, and alerts
- [Backup and Restore](./backup-restore.md) — RPO/RTO implementation
- [Availability and Disaster Recovery](./availability-and-recovery.md) — multi-region placement and failure recovery
- [Server Binary Upgrades](./upgrades.md) — pre-GA compatibility boundary and upgrade classes
- [Authentication](./authentication.md) and [TLS Migration](./tls-migration.md)
- [CLI Reference](./cli.md) — exhaustive server flag reference
