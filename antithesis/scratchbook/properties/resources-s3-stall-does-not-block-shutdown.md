# resources-s3-stall-does-not-block-shutdown — S3 stalls cannot pin graceful shutdown forever

| | |
|---|---|
| **Priority** | P1 |
| **Type** | Liveness |
| **Confidence** | High for the static cancellation and lifecycle wiring; medium until a real stalled S3 operation and old-process drain are observed together in Antithesis |
| **Property** | If a positive-grace pod deletion reaches a replica while one of that replica's balance-history maintenance S3 operations is in flight, runtime cancellation makes the operation and maintenance worker exit before the Fx stop deadline. Only an old-process post-drain signal satisfies the graceful half of the property; a replacement pod becoming Ready after a hard delete or forced exit does not. After the S3 fault heals, the replacement replica rejoins. |
| **Invariant** | SUT: `Sometimes(gracefulStopStarted && maintenanceS3OperationActiveAtCancel && maintenanceWorkerExited, "pit: graceful shutdown drained active S3 maintenance operation")`, emitted after `<-done` and carrying the maintenance phase and S3 verb. Workload: `Sometimes(positiveGraceDeleteIssued && oldPodUIDGone && replacementReady && replicaRejoined, "pit: replica rejoins after graceful S3-stalled restart")`. The hard-delete driver remains a separate crash property and must not satisfy either guard. |
| **Antithesis Angle** | Enable and accelerate PIT tiering/remote GC, wait for a SUT marker identifying an active maintenance `HeadObject`, `GetObject`, upload, list, or delete, overlap a Ledger↔MinIO network stall with a separate positive-grace pod deletion, and search cancellation during dial, response-header wait, body transfer, multipart work, and retry backoff. Keep the MinIO fault through the old-process drain check, then heal it and verify replacement readiness and voter rejoin. |
| **Why It Matters** | `balanceHistoryMaintenanceWorker.Stop` waits synchronously for its sole goroutine. A lifecycle deadline does not prove the worker drained: an uncancellable SDK call can consume it, abandon later close hooks, and make the process exit ungracefully even though Kubernetes subsequently replaces the pod. |

## Code evidence

- `internal/bootstrap/balance_history_maintenance.go:68-112` creates one background context; `Stop` cancels it and then waits on `<-done>` without accepting the Fx stop context or another deadline.
- `internal/bootstrap/balance_history_maintenance.go:114-204` runs compaction, tiering, and remote GC serially on that goroutine and passes the worker context to every phase. A blocked remote call therefore blocks the worker's `done` close.
- `internal/bootstrap/balance_history.go:297-357,360-405` quiesces maintenance before the verifier and builder and closes the archive/store only in later lifecycle work; these hooks deliberately ignore their Fx context while calling `quiesce`/`close`.
- `tests/antithesis/workload/bin/cmds/main/singleton_driver_rolling_restart/main.go:1-8,113-139` explicitly describes and performs a hard pod delete. `tests/antithesis/workload/internal/k8s.go:257-263` fixes `GracePeriodSeconds` to zero and says the helper is not suitable for graceful shutdown. Replacement readiness and voter recovery consequently cannot prove that the old process drained.
- `tests/antithesis/k8s/workload.yaml:19-23` already authorizes pod deletion, so a separate helper/driver can issue a deletion with an explicit positive grace period without expanding RBAC. `misc/operator/internal/controller/reconcile_statefulset.go:466-498` does not set a pod termination grace period; the test must supply its intended grace explicitly instead of relying on an implicit manifest default.
- `internal/infra/coldstorage/s3.go:29-54,88-108,115-133,194-269` supplies no custom AWS HTTP client, retryer, or operation timeout, but passes each caller context to upload, head, get, list, and delete. `internal/storage/balancehistoryarchive/store.go:197-262,423-433` propagates that context through cold hydration and checks it during response-body copying.
- Neither `tests/antithesis/k8s/cluster.yaml:63-78` nor `tests/antithesis/config/docker-compose.yaml:40-47` sets `AWS_RETRY_MODE` or `AWS_MAX_ATTEMPTS`, so the checked-in harness does not override those defaults.

## Instrumentation status

The deterministic maintenance test proves drainage only with a mock function that explicitly waits on `ctx.Done`; it does not execute the AWS transport. The current rolling-restart driver proves hard-delete replacement and rejoin, not graceful termination. No assertion correlates (a) the old process, (b) an actually active maintenance S3 verb, (c) cancellation, and (d) the worker's `done` close.

Required instrumentation is deliberately narrow:

1. Attach low-cardinality maintenance provenance (`compaction-hydration`, `tier`, or `remote-gc`) to the worker context, and track entry/exit of S3 `head`, `get`, `upload`, `list`, and `delete` calls made with that provenance. Do not use phase entry alone as the guard: a tier or GC pass may have no remote work.
2. In `Stop`, snapshot the active maintenance S3 verb before cancelling. After `<-done`, emit one SUT `Sometimes` assertion containing the old process/pod identity, phase, verb, and whether the operation was active at cancellation. An assertion before waiting, a replacement-pod signal, or a log emitted only by the new process is not a drain oracle.
3. Add a separate workload helper and singleton driver that set an explicit positive pod-deletion grace longer than the configured 15-second Fx stop timeout plus scheduling margin. Preserve the existing `grace=0` helper and driver as the hard-crash contrast.
4. Gate the workload result on both the old-process SUT assertion and the new pod's UID/readiness/voter convergence. Attempts where no maintenance S3 call was active at cancellation are inconclusive and must be retried, not counted as passes.

An independent per-operation S3 timeout is not required to state this shutdown
property. Repository code proves that the maintenance context is passed into
each archive operation and checked while response bodies are copied; the real
stalled-MinIO campaign is required to prove that the linked SDK and transport
actually drain promptly. A separate timeout remains an operational policy and
would not replace the cancellation property.

### Investigation Log

#### Can the checked-in Antithesis workload distinguish graceful termination from immediate process kill?

- **Examined:** `tests/antithesis/workload/bin/cmds/main/singleton_driver_rolling_restart/main.go:1-8,101-139`; `tests/antithesis/workload/internal/k8s.go:257-263`; `tests/antithesis/k8s/workload.yaml:19-23`; `misc/operator/internal/controller/reconcile_statefulset.go:466-498`; `internal/bootstrap/balance_history.go:297-405`; and the service/Fx lifecycle dependencies.
- **Found:** the only checked-in rolling-restart path is intentionally hard (`GracePeriodSeconds=0`). Its pod UID, Ready, and voter checks observe the replacement only. The workload already has permission to add a distinct positive-grace delete, while an assertion emitted by the old process after `maintenance.Stop` observes the missing lifecycle fact. Fx limits the stop phase to 15 seconds, so failure is an uncompleted graceful drain even if process replacement eventually succeeds.
- **Not found:** an existing positive-grace restart helper, an old-process completion marker, or a manifest-set termination-grace value on which this test could safely rely.
- **Conclusion:** resolved. Do not infer graceful completion from the current rolling-restart driver. Add an explicit positive-grace driver and require the old-process post-`done` SUT signal; retain `grace=0` as a separate hard-termination case.

#### Which retry mode and transport deadlines are active, and is an independent operation timeout needed for shutdown?

- **Examined:** `internal/infra/coldstorage/s3.go`,
  `internal/storage/balancehistoryarchive/store.go`, both checked-in Antithesis
  Ledger environment manifests and `go.mod` version pins. No external module
  source was used.
- **Found:** the product and harness install no retry or HTTP-client override.
  Every product S3 operation receives the maintenance context, and the archive
  reader checks cancellation while copying the body.
- **Not found:** `AWS_RETRY_MODE`, `AWS_MAX_ATTEMPTS`, a custom HTTP client or a
  product-level S3 operation deadline in the checked-in repository. Repository
  evidence alone cannot prove how every linked retry/transport phase reacts.
- **Conclusion:** resolved as a runtime property. Require the guarded real-S3
  execution and old-process drain signal; do not claim external SDK timing or
  cancellation behavior from repo-only evidence.
