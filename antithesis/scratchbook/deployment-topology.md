---
sut_path: /Users/flemzord/Developer/Formance/ledger
commit: fb3f9f8334c7fbbcc27c2c211c77da36762e6aaf
updated: 2026-07-31
external_references: []
---

# PIT Antithesis deployment topology

## Summary

Reuse the repository's Kubernetes Antithesis harness. It already has the
necessary service boundaries: three independent Ledger replicas, a separate
MinIO object store, the Ledger operator and one workload pod. The smallest
useful first change is configuration plus PIT workload drivers; no new service
container is required for the core correctness campaign.

The first campaign should test temporal correctness, follower-local convergence,
restart replay and ordinary cold-tier behavior. Object corruption, total MinIO
loss, backup/restore and operator failure should be separate campaigns so their
control planes do not dilute the core state space.

## Existing topology

```text
                         +----------------------+
                         | workload pod         |
                         | drivers + oracle     |
                         +----+------------+----+
                              |            |
                     ClusterIP|            |headless/per-node gRPC
                              v            v
          +----------------+  Raft  +----------------+  Raft  +----------------+
          | ledger-0       |<------>| ledger-1       |<------>| ledger-2       |
          | primary Pebble |        | primary Pebble |        | primary Pebble |
          | PIT peer store |        | PIT peer store |        | PIT peer store |
          +-------+--------+        +-------+--------+        +-------+--------+
                  \                       |                         /
                   \ S3 chapters + node-owned PIT runs            /
                    +-------------------+-------------------------+
                                        v
                               +------------------+
                               | MinIO            |
                               | bucket: archives |
                               +------------------+

          +------------------+             +------------------+
          | Ledger operator  |             | NATS             |
          | harness control  |             | outside PIT path |
          +------------------+             +------------------+
```

## Component plan

| Component | Image source | Role | Count | Connections | Fault policy |
|---|---|---|---:|---|---|
| Ledger | Existing `Dockerfile.antithesis`, built with `s3,nats`, race detector and Antithesis instrumentation | SUT service: Raft/FSM, primary Pebble and replica-local PIT workers/store | 3 | Raft to peers, gRPC from workload, S3 to MinIO | Network, hang, throttle and termination. Termination must be enabled for crash-window properties. |
| MinIO | Existing pinned image in `tests/antithesis/k8s/minio.yaml` | Dependency for archived primary chapters and PIT runs | 1 | S3 from every Ledger replica | Network/hang/throttle in core cold campaign. Termination only after adding durable storage, or in an explicit total-loss campaign. |
| Workload | Existing `tests/antithesis/workload/Dockerfile` | Stateful client, independent monetary oracle and SDK assertions | 1 | Round-robin and direct per-node gRPC; optionally Kubernetes control for restore | Exclude from network/node faults. A failed oracle must not be confused with a SUT defect. |
| Ledger operator | Existing `misc/operator/Dockerfile` | Harness control plane that creates the three-node StatefulSet | 1 | Kubernetes API and Ledger management ports | Exclude in data-path campaign. Make faultable only in a separate scaling/control-plane campaign. |
| NATS | Existing harness image | Existing event-sink dependency, not part of PIT | 1 | Ledger event sink | Omit from a PIT-only config if practical, otherwise exclude from faults. |
| Kubernetes control plane and init jobs | Environment/config images | Harness infrastructure | as provided | Operator, MinIO init and image preload | Exclude. They are not part of the PIT SUT. |

The legacy Compose topology is useful for local iteration but is not the
production Antithesis deployment. Keep its PIT environment variables in parity
so driver development can reproduce the same feature activation.

## Required configuration

PIT is disabled by default. Add the following through the existing `extraEnv`
list on every Ledger replica, and mirror it in Compose:

```yaml
- name: BALANCE_HISTORY_ENABLED
  value: "true"
- name: BALANCE_HISTORY_COLD_TIER
  value: "true"
- name: BALANCE_HISTORY_BUILDER_BATCH_SIZE
  value: "32"
- name: BALANCE_HISTORY_BACKFILL_YIELD
  value: "1ms"
- name: BALANCE_HISTORY_COMPACTION_THRESHOLD
  value: "2"
- name: BALANCE_HISTORY_MAINTENANCE_INTERVAL
  value: "200ms"
- name: BALANCE_HISTORY_MAX_COMPACTIONS_PER_PASS
  value: "1"
- name: BALANCE_HISTORY_WAL_SYNC_INTERVAL
  value: "5s"
- name: BALANCE_HISTORY_VERIFIER_INTERVAL
  value: "2s"
- name: BALANCE_HISTORY_VERIFIER_REPLAY_EVERY
  value: "4"
- name: BALANCE_HISTORY_RETAIN_LOCAL_RUNS
  value: "1"
- name: BALANCE_HISTORY_ARCHIVE_CACHE_MAX_BYTES
  value: "8Mi"
- name: BALANCE_HISTORY_MAX_SEGMENT_BYTES
  value: "64Ki"
- name: BALANCE_HISTORY_MAX_RUNS_PER_TIER_PASS
  value: "4"
- name: BALANCE_HISTORY_TIER_INTERVAL
  value: "1s"
- name: BALANCE_HISTORY_REMOTE_GC_INTERVAL
  value: "1s"
- name: BALANCE_HISTORY_REMOTE_GC_GRACE_PERIOD
  value: "5s"
- name: BALANCE_HISTORY_REMOTE_GC_SCAN_LIMIT
  value: "100"
- name: BALANCE_HISTORY_REMOTE_GC_DELETE_LIMIT
  value: "10"
```

The threshold-two, one-compaction-per-200-ms combination satisfies the static
retirement-capacity check. The five-second history WAL interval deliberately
widens the crash-replay window without weakening primary Ledger durability.
Use a shorter interval in a later steady-state capacity campaign.

This is a test profile, not production guidance. The short verifier, tier and
GC intervals exist to make rare lifecycle paths reachable with small synthetic
data.

### Campaign profiles

The configuration above is the `pit-core-cold` profile. It enables the PIT cold
tier and exercises MinIO fetch/upload/GC. MinIO termination is disabled unless a
PVC is added; asymmetric network, hang and throttle faults remain enabled.

The `pit-restore` profile is intentionally different:

- run only the isolated `model` template;
- set `BALANCE_HISTORY_ENABLED=true`;
- keep `BALANCE_HISTORY_COLD_TIER=false` so replica-owned PIT runs are not a DR
  input;
- persist MinIO because authoritative primary chapter archives are an external
  DR prerequisite;
- create and verify a genuinely archived primary chapter before the full backup;
- restore into fresh PVCs, validate PIT rebuild, then resume model workers.

These profiles must not be collapsed into one global set of assumptions.

## Storage changes

### MinIO durability

The Kubernetes MinIO deployment now mounts the `minio-data` 2 GiB PVC at
`/data`. Pod replacement therefore preserves authoritative chapter archives
and PIT runs within the lifetime of the test namespace. MinIO termination can
be enabled for the first data-path campaign; namespace/PVC deletion remains a
destructive source-loss scenario and must stay outside that campaign.

### PIT local storage

The PIT store and verifier scratch space currently share each replica's 2 GiB
primary data PVC. Keep that topology initially because it reflects the current
operator, but record disk usage and avoid interpreting ENOSPC as isolated PIT
degradation. A dedicated history PVC is required before testing the stronger
claim that PIT disk pressure cannot affect the primary store.

## Workload design

### Core `main` campaign

Add these commands under `bin/cmds/main/`:

| Driver | Scheduling | Responsibility |
|---|---|---|
| `first_point_in_time` | `first_` | Create a reserved ledger and deterministic seed state; wait for the first exact PIT result, not merely pod health. |
| `parallel_driver_point_in_time` | `parallel_driver_` | Generate out-of-order effective timestamps, ordinary/future/backdated transactions, both reversal modes and reads on both axes. Maintain an oracle from reconciled committed logs. |
| `parallel_driver_point_in_time_cold` | `parallel_driver_` | Query old cutoffs repeatedly while existing chapter/maintenance/restart drivers create hot/cold transitions. Use short RPC deadlines and exact error reasons. |
| `singleton_driver_point_in_time_archive` | `singleton_driver_` | Request archive, wait for the real archiver-produced `ARCHIVED` state, prove cold readability, and never send manual confirmation. Include the out-of-order-chapter scenario. |
| `eventually_point_in_time_convergence` | `eventually_` | After faults and writers stop, establish quiescence, query each replica directly at a common watermark and compare to the independent oracle. |

Do not reuse assertion names from `existing-assertions.md`. Use stable literal
messages prefixed `pit:`.

### Oracle rules

- Own dedicated ledger names through new `OwnedLedgerPrefix` constants.
- Reconcile ambiguous writes through idempotency/log reads before adding them to
  expected history.
- Store each resolved posting effect with effective timestamp, returned
  insertion timestamp and log sequence.
- For a successful response, fold only expected effects through the response's
  `logWatermark` and requested temporal cutoff.
- Validate ledger incarnation, axis, normalized timestamp, non-empty token and
  `logWatermark >= minLogSequence`.
- Never compare view tokens or physical manifest versions across replicas.
- Use the no-retry per-node client to observe `HISTORY_BUILDING` and
  `HISTORY_BEHIND`; use the standard retry client for final convergence.
- Match `HISTORY_SOURCE_MISSING` and `HISTORY_CORRUPT` by exact error reason in
  deliberate fault scenarios. Do not globally tolerate gRPC `Internal`.

### Restore campaign

Antithesis chooses exactly one template per execution history. The current full
backup/PVC teardown/restore cycle exists only in `model`, so `main` drivers
cannot validate it.

Extend `singleton_driver_model` with the monetary timeline and PIT checks. The
template already owns the backup/PVC teardown/restore handshake, and no unrelated
writer can introduce legitimate post-backup state. After restore it should
accept only fail-closed PIT states until the peer store rebuilds, then require an
exact result. It must not run concurrently with unrelated writers because writes
after the backup are legitimately absent after restore.

## Fault-to-property mapping

| Fault | Targeted transition | Expected observation |
|---|---|---|
| Ledger partition/bad node | Replica-local builder falls behind | Any success remains exact; retryable lag is observable; quiescent replicas eventually converge. |
| Ledger hard kill | NoSync suffix, compaction/tier/GC crash windows | Restart gate remains closed until replay; no partial manifest or double effect. |
| CPU throttle/hang | Builder/verifier/maintenance delay | Live Ledger remains usable where Raft permits; PIT is exact or fail-closed. |
| Asymmetric Ledger-to-MinIO partition | Cold fetch, source replay, upload and inventory | No truncated success; after quiet period recoverable operations progress. |
| MinIO restart with PVC | In-flight S3 request and cache behavior | Local copies/cache may serve exact data; otherwise typed failure, then recovery. |
| Clock fault, if enabled | Remote-GC grace | Root revalidation still prevents live-object deletion; backward time may delay reclamation. |
| Thread pause/CPU modulation | Lease, cache singleflight and lock interleavings | No deadlock, lease underflow or rooted delete. Thread pausing needs SUT instrumentation. |
| Custom object removal/corruption | Missing/corrupt multipart cold run | Exact fail-closed reason, never partial success; test the documented repair action. |

## Readiness and quiet-period checks

Antithesis setup readiness should continue to use the existing workload init and
cluster health. PIT readiness is a workload condition because the global health
checker deliberately ignores the peer projection.

For final liveness:

1. stop writers and faults using the harness's existing eventual phase;
2. establish two-barrier quiescence and a common persisted index;
3. poll each replica with bounded RPCs until it serves the required watermark;
4. assert the exact monetary result and provenance;
5. report separate reachability for effective, insertion, reversal, cold fetch,
   restart replay, compaction, tiering and remote-GC paths.

Liveness under unbounded continuous faults is not required.

## Local preflight

Before publishing images:

```bash
nix develop --command bash -c '
  cd tests/antithesis/workload
  GOROOT= go test ./...
  GOROOT= go build ./bin/cmds/main/...
'
```

For local cluster iteration:

```bash
cd tests/antithesis
just deploy-local
```

The local-only `run_model_test.sh` exercises only the model template and is not
a substitute for the multi-node PIT campaign.

## Remote launch

The existing K8s recipe builds the Ledger/operator/workload/config images,
pushes them and calls the Antithesis launch API. Required environment variables
remain the ones already consumed by the repository:

- `ANTITHESIS_REPOSITORY`
- `ANTITHESIS_PASSWORD`
- `ANTITHESIS_REPORT_RECIPIENT`

Targeted development run for the implemented redundant-scope property:

```bash
cd tests/antithesis
just k8s-run 2 \
  'ledger v3 PIT focused smoke' \
  'main/first_default_ledger,main/parallel_driver_pit_scope_equivalence,main/eventually_pit_scope_equivalence'
```

Six-hour cold/recovery campaign:

```bash
cd tests/antithesis
just k8s-run 6 \
  'ledger v3 PIT cold tier and recovery' \
  'main/first_default_ledger,main/parallel_driver_pit_scope_equivalence,main/singleton_driver_rolling_restart,main/eventually_pit_scope_equivalence'
```

An empty include argument ships the entire existing `main` and `model` driver
set, but the composer selects one template per history. Focused includes are
preferable while establishing PIT signals.

## Assumptions

- Kubernetes mode remains the authoritative remote Antithesis environment.
- The first campaign does not mutate object bytes or inject ENOSPC.
- Workload and operator/control-plane faults are excluded unless a property
  explicitly targets them.
- The test profile is applied uniformly to all three Ledger replicas.

## Open Questions

- Are Ledger/MinIO node termination and clock faults enabled by the tenant?
- Does the tenant preserve dynamically provisioned PVCs across every MinIO pod
  termination fault it injects?
- Can the operator gain a dedicated balance-history PVC before resource-fault
  testing?
- Is a test-only MinIO helper acceptable for removing/restoring exact objects,
  or should corruption remain in deterministic tests?
- What fault-exclusion parameters does the K8s launch webhook currently apply?
  The repository's K8s launch recipe does not encode an exclusion list.
