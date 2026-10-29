# Performance benchmarks

The pipeline builds and pushes the image, connects to acme-dev through Tailscale,
authenticates to EKS, and runs the existing `tests/perf` Pulumi program with a
local backend. The job ends after submission. Kubernetes runs the benchmark and
creates the Grafana snapshots; GitHub does not collect results or wait for load.

## Triggers

- Add exactly one of `perf-5m` or `perf-10m` to an internal PR. `PR Images` builds
  its exact head commit first. The performance workflow starts only after that
  build succeeds and the current PR still has the matching head and label.
- Every Sunday at 02:00 UTC, the workflow builds the default branch and submits
  the 24-hour profile. It skips execution when the benchmark pool is occupied.
- Manual dispatch selects a 5-minute, 10-minute or 24-hour profile from the
  default branch. Manual runs queue when busy.

The submission job checks out only the trusted default branch. PR code never
executes with cluster credentials. The image tag is resolved to a digest and its
OCI commit/source labels are verified before `pulumi up`.

## Pulumi configuration

CI uses a fresh local backend under `RUNNER_TEMP`, not Pulumi Cloud or S3.
The workflow calls `pulumi login`, `pulumi stack init` and `pulumi up` directly.
There are no submission helpers or result collectors.

The program accepts `perf:profile`, `perf:commit`, `perf:image`, `perf:run-id`,
`perf:run-attempt`, `perf:pr`, `perf:when-busy`, `perf:workload-commit` and
`perf:k8s-context`. Its Benchmark name is
`ledger-<sha12>-<workflow-run-id>-<attempt>`. All resource references and the
Grafana `cluster` variable derive from that name. Re-running a commit creates a
new Benchmark; reapplying a completed Benchmark does not restart it.

| Profile | k6 stages | Active deadline |
| --- | --- | --- |
| `smoke-5m` | `30s:400,4m:800,30s:0` | 2 hours |
| `smoke-10m` | `1m:400,8m:800,1m:0` | 2 hours |
| `soak-24h` | `1m:400,23h58m:800,1m:0` | 26 hours |

Load durations include ramps. Provisioning, capture and cleanup take additional
time. All profiles use the same `world_to_bank` workload, bulk size 50, three
Ledger replicas with 8 CPUs and 16 GiB each, the dedicated `ledger-m8a` pool,
and separate io2 data/WAL volumes of 500 GiB and 15 GiB per node. Only metrics
are exported; logs and traces must not overload the shared collector.

The k6 archive keeps local `shared` imports, excludes system environment values
and is rejected above 900 KiB. Keep the runtime/initializer image version aligned
with Nix k6 when updating the profile. Existing k6 thresholds remain unchanged.

## Setup

Deploy the matching benchmark-operator lifecycle CRD, RBAC and controller before
enabling the workflow. CI checks the required lifecycle schema. The operator
owns execution-group exclusivity, deadlines and cleanup after the runner exits.
Its lock remains held while workloads, PVCs or PVs remain. Reports and snapshots
are retained. Cancelling a GitHub job does not cancel an already submitted run;
request `spec.cancel: true` on the exact Benchmark when interruption is needed.

Configure the GitHub environment `acme-perf`, restricted to the default branch:

- Variables: `PERF_AWS_ROLE_ARN`, `PERF_TS_OAUTH_CLIENT_ID`, optional
  `PERF_AWS_REGION` (default `eu-west-1`) and `PERF_TS_TAGS`
  (default `tag:github-actions-ledger-perf`).
- Secret: `PERF_TS_OAUTH_SECRET`.
- Existing build credentials: `NUMARY_GITHUB_TOKEN` and `GORELEASER_KEY`.

The AWS role must trust the repository's `acme-perf` environment through GitHub
OIDC and have `eks:DescribeCluster` plus EKS/Kubernetes access to Benchmarks and
ConfigMaps in `ledger-v3` and read access to the Benchmark CRD. Tailscale must
route and permit the CI identity to the private EKS API on TCP 443. Networking,
EKS authentication and Kubernetes permissions are separate requirements.

## Local use and validation

Enter `nix develop .#perf`, then use the existing Pulumi commands from
`tests/perf`, starting with `pulumi login --local`. Keep user-specific stack
configuration separate from CI's unique stack names. CI's ephemeral state is not
used for later teardown; the operator owns that lifecycle.

`nix develop .#perf --command just test-perf` runs Go race tests, creates an
archive without running k6, and checks Pulumi resource serialization with a mock
monitor. It does not submit a Benchmark or contact Kubernetes. Root
`scripts/agent-check` remains required. Live CI access, snapshot capture and CSI
volume cleanup require separate verification; local tests do not prove them.
