# Asynchronous performance benchmarks

## Execution model

Ledger keeps the Pulumi program in `tests/perf`. CI runs `pulumi up` against
acme-dev through Tailscale, with an isolated local backend. The submission job
does not wait for the load, collect its results, or destroy the stack. Kubernetes
owns execution and cleanup after the runner disappears. A successful GitHub job
means **submitted**, not a successful benchmark or successful snapshot capture.

The trusted default branch supplies the Pulumi program, workload scripts and
hardware profile. A PR supplies only the Ledger image built from its exact head
commit. The workflow verifies the image artifact's source run, current PR head,
repository, tag, digest and OCI revision/source labels before cluster access.
Fork PRs cannot submit. No `pull_request_target` workflow executes PR code.

Every Benchmark is named `ledger-<sha12>-<workflow-run-id>-<attempt>`. Its full
commit, workload checkout commit and immutable image reference are recorded in annotations. The archive
ConfigMap, resource references, HTTP service address and Grafana `cluster`
variable derive from this same identity. Re-running a workflow creates a new
Benchmark even for an unchanged commit; reapplying a completed Benchmark does not.

## Profiles and budgets

| Profile | k6 stages | Queue policy | Active deadline |
| --- | --- | --- | --- |
| `smoke-5m` | `30s:400,4m:800,30s:0` | Queue | 2 hours |
| `smoke-10m` | `1m:400,8m:800,1m:0` | Queue | 2 hours |
| `soak-24h` | `1m:400,23h58m:800,1m:0` | Skip for schedule; queue for manual runs | 26 hours |

Durations include ramps, but exclude provisioning, snapshot capture and cleanup.
The active deadline starts when the operator acquires the execution group and
covers provisioning, load and capture. Waiting requests expire after two hours.
Cleanup continues after a deadline; a timeout never permits another run while
old workloads or volumes still exist.

All profiles use `world_to_bank`, bulk size 50, atomic bulk requests, and
postings rather than Numscript. k6 retains the existing `errors rate<0.1` and
`bulk_latency p(95)<500` thresholds; GitHub does not evaluate those results.
The archive embeds `shared` imports, excludes system environment variables and
is rejected above 900 KiB to leave ConfigMap size headroom. k6 runner and
initializer images share a version and digest compatible with the Nix archive
tool. Keep them synchronized when upgrading Nix k6.

Hardware is fixed at three Ledger replicas, each with 8 CPUs, 16 GiB RAM,
500 GiB io2 data and 15 GiB io2 WAL. The `ledger-m8a` pool and required pod
anti-affinity place one benchmark Ledger pod per host. Per run this reserves
24 CPUs, 48 GiB RAM and 1,545 GiB of persistent storage, plus a separate k6
runner requesting 4 CPUs and 8 GiB. io2 IOPS and billing depend on the actual
StorageClass; these resource and time bounds are not a monetary cost cap.

Only metrics are exported to the existing monitoring collector. Do not add
Ledger logs or traces: throughput can overwhelm the shared collector. Existing
dashboards are not changed. The operator captures the configured Ledger
dashboard with `grafanaVariables.cluster=<benchmark>-res-0`. Snapshot names use
`ledger-<benchmark>-run-<UTC-date>-<TestRun-creation-Unix-seconds>`.
Snapshots and `k6-report-<testrun>` report ConfigMaps are retained. The report
contains Grafana metadata and URLs, not k6 threshold results.

## CI triggers

- Add exactly one label, `perf-5m` or `perf-10m`, to an internal PR. The label
  requests its image build without requiring `build-images`, and persists for
  each new head SHA. Unrelated labels do not cancel image builds.
- `Performance benchmarks` admits the successful `PR Images` run and rechecks
  the current PR immediately before submission. A newer submission cancels
  older requests for the same PR that are still pending or queued. It does not
  interrupt a running benchmark. A removed label prevents later submissions;
  it does not stop an already admitted run.
- The schedule runs every Sunday at 02:00 UTC. It builds the current default
  branch and submits the 24-hour profile with `whenBusy=Skip`. It never
  interrupts an active benchmark.
- `workflow_dispatch` submits a selected profile from the default branch or
  requests cancellation using the exact Benchmark name and UID. Manual runs
  queue when busy. An explicit cancellation asks the operator to stop workloads
  and clean their volumes; cancelling the GitHub job alone does not stop a run
  already admitted to Kubernetes.

The same execution group, `acme-ledger-m8a`, covers every profile and namespace.
GitHub concurrency serializes short submission jobs only; the operator's
durable Kubernetes Lease protects the actual workload through cleanup.

## Required setup before enabling CI

Deploy the companion `internal-benchmark-operator` lifecycle update first,
including its CRD, RBAC and controller. Submission refuses an older CRD missing
`executionGroup`, `activeDeadlineSeconds`, `queueDeadlineSeconds`, `whenBusy`
or `cancel`. The controller must be running the matching version; an updated
CRD alone is not proof that the lifecycle controller is ready. Verify an empty
benchmark pool and the effective io2 reclaim policy before the first run.

Create the GitHub environment `acme-perf`, restrict deployments to the default
branch, and configure:

| Setting | Kind | Value |
| --- | --- | --- |
| `PERF_AWS_ROLE_ARN` | Variable | Dedicated AWS role for acme-dev EKS access |
| `PERF_AWS_REGION` | Variable | `eu-west-1` by default |
| `PERF_TS_OAUTH_CLIENT_ID` | Variable | Tailscale OAuth client identifier |
| `PERF_TS_TAGS` | Variable | `tag:github-actions-ledger-perf` by default |
| `PERF_TS_OAUTH_SECRET` | Secret | Tailscale OAuth client secret |

The existing image build also needs `NUMARY_GITHUB_TOKEN` and `GORELEASER_KEY`.
`IMAGE_REPOSITORY` may override the trusted GHCR repository. No backend bucket
or Pulumi Cloud token is required.

The AWS OIDC trust must bind the repository and subject
`repo:formancehq/ledger:environment:acme-perf` (adjust the repository if renamed),
with audience `sts.amazonaws.com`. Grant `eks:DescribeCluster` for
`eks-acme-dev-euw1-01` and register the role in EKS access configuration.
The Kubernetes identity needs create/get/list/watch/update/patch/delete on
Benchmarks and ConfigMaps in `ledger-v3`, plus get access to the single
`benchmarks.benchmark.formance.com` CRD for the preflight. It does not need
direct Cluster, Ledger, Pod or volume deletion rights: the operator owns those.

Tailscale provides networking only. Its ephemeral tagged CI identity must have
an approved route and ACL to the private EKS API on TCP 443. EKS authentication
and Kubernetes authorization remain separate. Verify these permissions and
connectivity before enabling labels or schedule; this change does not provision
the OAuth client, AWS role, GitHub environment or routes.

## Local commands and state

Use the same pinned environment and recipes locally:

```bash
# Validation only: unit/race tests and real k6 archive generation, no load.
nix develop .#perf --command just test-perf

# This command applies resources and starts a benchmark after admission.
nix develop .#perf --command just perf-submit \
  smoke-5m <full-commit-sha> \
  ghcr.io/formancehq/ledger:<tag>@sha256:<digest> <run-id> <attempt> 0

# Targeted cancellation; the API server checks the UID atomically.
nix develop .#perf --command just perf-cancel <benchmark-name> <benchmark-uid>
```

The helper copies only program inputs into `build/perf/runs/<benchmark>/project`.
It never copies or edits user `Pulumi.maxence*.yaml` files. Its local backend is
isolated under `RUNNER_TEMP/pulumi-state/<benchmark>` in CI, or the run directory
locally; `PERF_STATE_DIR` overrides that base. Stack passphrases and Pulumi home
are ephemeral. Config contains no service secrets. Do not depend on this state
for subsequent `pulumi destroy`; cancellation and cleanup use Kubernetes.
`PERF_KUBE_CONTEXT` overrides the local target context; CI fixes it to acme-dev.

Legacy hand-managed stacks still work from `tests/perf` with the `perf` shell.
They opt into lifecycle locking only when configured with the new execution
fields. Preserve user-specific stack configs and their workload choices.

## Validation and rollout boundary

`Performance tooling tests` validates the perf Go module with the race detector,
creates an archive without running k6, and tests CI admission and submission
using fake APIs/processes. Root `scripts/agent-check` remains required.

Before production use, separately verify EKS/Tailscale access, the deployed
operator version, cleanup after provisioning/k6/capture failure and cancellation,
and the absence of PVCs/PVs after teardown. CSI reclaim policy `Delete` must
complete cloud-volume deletion; local fake-client tests do not prove AWS EBS
deletion. Operator restarts and runner disappearance must not release the lock
before cleanup. A stuck `Retain` volume requires explicit operator intervention.
