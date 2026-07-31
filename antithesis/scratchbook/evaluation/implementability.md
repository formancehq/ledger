---
sut_path: /Users/flemzord/Developer/Formance/ledger
commit: fb3f9f8334c7fbbcc27c2c211c77da36762e6aaf
updated: 2026-07-31
external_references: []
---

# Property evaluation — Lens 3: Implementability

## Verdict

The catalog is implementable as a portfolio only after splitting it into several campaigns and adding the missing PIT workload layer. At this revision, PIT is disabled in the Antithesis manifests, there is no PIT monetary oracle, no no-retry per-node client, and no PIT-specific SDK assertion. Ten properties have a complete workload/topology design, fourteen are implementable after the SUT or campaign changes already identified in their evidence files, three remain underspecified, and five cannot meet their full premise with the current harness.

Statuses used below:

- **Implementable:** the current topology can support the property after ordinary workload code and PIT configuration are added.
- **Conditional:** the oracle is specified, but named SUT instrumentation or a separate campaign/topology change is a prerequisite.
- **Underspecified:** the stated invariant still depends on an observation or deterministic precondition for which no sound choice has been made.
- **Blocked:** the current repository harness cannot create or prove a required fault. A concrete unblock is listed.

## Catalog-wide findings

### The shared PIT workload layer is mandatory but straightforward

Every public property depends on work not yet present: enable the feature, add a raw trailer-aware PIT client, add a single-target no-retry variant of `DialPerNode`, classify `ErrorInfo.reason` exactly, reserve driver-owned ledgers, and maintain an independent arbitrary-precision monetary oracle keyed by ledger incarnation, axis, timestamp, audit/log result and returned watermark. `deployment-topology.md` specifies these pieces, while `existing-assertions.md` confirms that none of the 369 checked-in assertions is PIT-specific.

This is a catalog-wide implementation dependency, not a separate feasibility defect. It should be implemented once and reused by all workload properties.

### One timeline cannot exercise the whole catalog meaningfully

The properties require at least five focused campaigns:

1. public semantics, metadata, forwarding and quiescent convergence;
2. compaction, tiering, cold reads and remote GC with accelerated valid settings;
3. repair/crash paths with Ledger termination enabled;
4. scaling and follower synchronization with the operator fault policy chosen explicitly;
5. the isolated `model` backup/restore timeline with durable MinIO.

Trying to run all 33 properties in one six-hour history would dilute the rare transition signals and make `Sometimes` failures uninterpretable. The topology already recommends this split.

### Internal maintenance safety needs SDK assertions, not black-box inference

The public API exposes monetary results and view provenance, but not compaction CAS decisions, run leases, archive mutation epochs, remote-GC roots/candidates, repair subphases or completed maintenance passes. The following properties correctly require surgical SUT assertions or diagnostics: `integrity-authoritative-prefix-only`, `concurrency-pinned-view-maintenance-stability`, `concurrency-compaction-cas-preserves-suffix`, `concurrency-tier-lease-precedes-local-delete`, `concurrency-builder-source-snapshot-archive-purge`, `concurrency-remote-gc-live-roots-protected`, `concurrency-remote-gc-inventory-upload-linearization`, `recovery-repair-gate-survives-crashes`, `lifecycle-follower-snapshot-install-fails-closed`, `replay-remote-delete-ack-is-idempotent`, `resources-logical-run-debt-reconverges`, and `resources-s3-stall-does-not-block-shutdown`.

These additions are feasible because the relevant checks sit outside the FSM hot path and use already-available local values. They are prerequisites: workload success alone cannot prove that the intended internal transition occurred.

### Object mutation is absent from the planned topology

The Kubernetes workload has no MinIO client/configuration for listing, seeding, removing, replacing or restoring one exact object. MinIO has no PVC. Therefore exact missing/corrupt-object and namespace-fixture properties cannot run as specified. A separate object-fault campaign needs:

- a persistent MinIO `/data` volume;
- workload-scoped MinIO credentials and a bounded helper for exact keys;
- explicit preservation of authoritative chapter and backup objects;
- no broad tolerance of gRPC `Internal`.

This blocks the complete forms of `protocol-pit-error-contract`, `integrity-cold-content-verified`, `recovery-source-missing-heals-same-process`, and `security-pit-remote-gc-owner-containment`. It also blocks the missing/corrupt-source variant of `recovery-repair-gate-survives-crashes`, although that property can first use the constructible out-of-order-chapter quarantine path.

### Fault authority is not encoded in the Kubernetes launch recipe

Repository manifests do not say whether the tenant enables process termination, clock faults, durable-storage loss, or link-selective Ledger-to-MinIO partitions, nor which components the launch webhook excludes. Consequently `recovery-unsynced-suffix-replays`, `wildcard-live-path-survives-pit-dependency-fault`, and `resources-s3-stall-does-not-block-shutdown` are conditional on external campaign capabilities even though their SUT/workload logic is otherwise specified.

## Per-property assessment

| Property | Observation and required instrumentation | Topology/fault support | Preconditions and practicality | Status |
|---|---|---|---|---|
| `concurrency-api-primary-history-boundary` | Trailer/result are public, but the exact primary snapshot head used inside the request is not. The proposed SUT signal is only `Reachable`, not a safety check binding ledger ID/head to the returned view. | Ordinary write, forwarding and delete/recreate races are supported. | A caller barrier proves only a lower bound, not `observedPrimaryHead` from the request. | **Underspecified** |
| `integrity-dual-axis-reversal-exactness` | Fully observable with a trailer-aware independent effect oracle; optional SUT reachability for reversal variants improves search. | Current three-node topology is sufficient. | Use a bounded driver-owned ledger and direct/modelled postings; broad Numscript/mirror coverage can be incremental. | **Implementable** |
| `protocol-current-metadata-historical-money` | Public result plus same-node `GetIndexStatus` exposes the necessary current-index gate; no SUT instrumentation is required. | Per-node gRPC is present. | Requires a new stable-schema metadata oracle and a quiescent gate between index status and PIT read. | **Implementable** |
| `integrity-ledger-incarnation-isolation` | Current numeric ledger ID and PIT trailer/result are public. | Delete/recreate, restart and per-node reads are supported. | A driver-owned ledger prevents unrelated mutations; workload oracle must key effects by incarnation. | **Implementable** |
| `protocol-pit-view-provenance` | gRPC trailer and HTTP header are directly observable; suggested SUT `Unreachable` branches are diagnostic only. | gRPC exists; HTTP requires a small direct client/address helper but no new container. | Cheap on every successful PIT/live control. | **Implementable** |
| `protocol-pit-error-contract` | Mapping is public and a no-retry client is sufficient for building, lag and unsupported filters. Exact missing/corrupt reachability requires object mutation. | Current MinIO is ephemeral and the workload has no exact-object helper. | Split deterministic/public classes from the missing/corrupt object campaign. | **Blocked** for the complete property |
| `protocol-leader-forwarding-provenance` | Node-specific cluster state plus matching tokens gives a sound leader/term/manifest bracket; no SUT oracle needed. | Per-node addresses and explicit leader routing are available. | Add leader-consistency and no-retry helpers; retry discarded pairs rather than sleeping. | **Implementable** |
| `wildcard-agent-unfiltered-fast-path-equals-account-fold` | Entirely public; equal tokens make the two calls comparable. | Existing topology suffices. | Small isolated ledger bounds exhaustive row scans; quiescent command guarantees all four option modes. | **Implementable** |
| `integrity-authoritative-prefix-only` | Monetary exactness is public, while sequence/item completeness needs the proposed SUT pre-publication assertion. | Partitions, chapters and kills are supported if termination is enabled. | Reuse one independent oracle; failed/no-log proposal completeness remains an internal assertion. | **Conditional** on SUT instrumentation |
| `integrity-atomic-publication-restart` | Public results detect torn/incorrect state; publication/barrier/replay SDK markers are needed to prove the crash window. | Hard pod deletion preserves the PVC, but actual NoSync-byte loss is not guaranteed. | Repeated kills are practical; full old-or-new coverage depends on storage-fault semantics. | **Conditional** |
| `integrity-layout-independent-semantics` | Public timestamp matrices check money, but the stated canonical semantic-digest invariant is internal. The evidence leaves test-only digest diagnostic versus bounded public checks unresolved. | Compaction/tiering topology is supported after PIT enablement. | Computing replay/semantic digests at every transition may be expensive; sampling policy is unspecified. | **Underspecified** |
| `concurrency-pinned-view-maintenance-stability` | Public exactness plus a SUT marker comparing pinned/latest manifest versions proves overlap. | Separate MinIO and concurrent maintenance are sufficient. | Bounded large/cold reads are practical; do not rely on duration alone as overlap proof. | **Conditional** on SUT reachability |
| `concurrency-compaction-cas-preserves-suffix` | Compaction completion/stale CAS is invisible publicly; proposed SUT assertion and reachability signals are precise and cheap. | Accelerated maintenance and hard kills support the scenario. | Small threshold-two runs make the path frequent. | **Conditional** on SUT instrumentation |
| `concurrency-tier-lease-precedes-local-delete` | Exact archive/lease/delete premise is internal; proposed assertion at phase two is feasible. | Cold tier and MinIO support the path. | A pinned cold view plus accelerated tiering is bounded; a pause hook would improve but is not essential once reachability exists. | **Conditional** on SUT instrumentation |
| `integrity-cold-content-verified` | Exact result/reason is public; verified-before-record guidance is feasible in SUT. | No exact object deletion/replacement mechanism and no durable MinIO exist. Network stalls do not create checksum mismatch. | Multipart fixture is manageable after a dedicated object helper is added. | **Blocked** |
| `concurrency-builder-source-snapshot-archive-purge` | Batch completeness and overlap require the proposed SUT assertion/reachability; workload can verify final money/cold readability. | Real archiver and MinIO exist. | Replace the unsafe manual-confirm driver; accelerated chapter lifecycle is practical. | **Conditional** on SUT overlap instrumentation |
| `concurrency-remote-gc-live-roots-protected` | Root/lease/delete predicate is internal and precisely located before `Delete`. Public retained-watermark reads are the end-to-end backstop. | Cold tier/GC supported after accelerated configuration; MinIO must remain durable for termination variants. | Candidate creation and two inventories/grace fit a focused multi-hour campaign. | **Conditional** on SUT instrumentation and durable MinIO for restart variants |
| `concurrency-remote-gc-inventory-upload-linearization` | Epoch, scan restart and orphan publication are internal; proposed assertions cover them. | Tier/upload/reset share the current Ledger/MinIO topology. | One-second GC and five-second grace make bounded fixtures practical. Destination-rotation checking is optional once the SUT invariant is authoritative. | **Conditional** on SUT instrumentation |
| `security-pit-remote-gc-owner-containment` | Delete-target assertion is feasible, but black-box preservation needs seeded foreign, malformed, chapter and same-owner objects. | Current workload cannot seed arbitrary MinIO keys; MinIO is ephemeral. Scaling can exercise ordinal reuse only after fixtures exist. | The full fixture plus two scans, grace, scale down/up and rebuild belongs in a dedicated long campaign. | **Blocked** |
| `recovery-unsynced-suffix-replays` | Public pre-kill watermark and post-restart lag prove actual loss; SUT durability markers guide placement. | Grace-zero restart exists, but repo evidence cannot establish loss of NoSync PVC bytes. | Repeated attempts are otherwise bounded and sound. | **Blocked** pending storage-fault semantics or a disk seam |
| `recovery-repair-gate-survives-crashes` | The SDK-only repair phase trace and pre-clear invariant are feasible; public no-retry checks cover the gate. | Process kills are needed. Out-of-order chapter topology can induce a repair path without object mutation. | Eight subphases require a focused campaign; do not require every kill point in one timeline. | **Conditional** on SUT phase instrumentation and termination faults |
| `recovery-source-missing-heals-same-process` | Process identity, exact error and eventual result are observable; origin/repair SUT signals are specified. | Requires removal and restoration of the exact source object while keeping Ledger alive; no such helper exists. | Quiet same-process recovery is cheap once the fixture exists and is expected to find the documented current defect. | **Blocked** |
| `lifecycle-follower-snapshot-install-fails-closed` | Exact activation-to-builder epoch needs SUT instrumentation; target sync status, local head, live result and PIT result are public. | The topology supports follower partitions/restarts, but the workload cannot by itself guarantee the leader compacted far enough to force snapshot transfer. | Use `Sometimes` on observed sync status and a focused low-margin campaign; ordinary forward snapshots may never produce a wrong public success. | **Conditional** |
| `wildcard-out-of-order-chapters-fail-closed` | Public exact/error oracle plus one SUT topology reachability signal is sufficient. | Existing chapter API and real archiver can construct N+1-before-N. | Three small chapters and accelerated archiver fit easily. | **Implementable** |
| `wildcard-out-of-order-chapters-recover` | Direct per-node exact PIT after a quiet prefix completion is observable. | Same topology as its safety companion. | Reuse one fixture; bounded rebuild/certification fits the 10-minute eventual command under capped data. | **Implementable** |
| `coordination-quiescent-pit-convergence` | Stable leader membership, exact per-node persisted index, local status and PIT views are public. One builder-ready SUT marker is optional guidance. | Current three-node topology and eventual phase support it. | The specified 60-second index sub-gate inside a 10-minute bounded-data command is practical. | **Implementable** |
| `coordination-scale-up-no-premature-local-pit` | Ordinal FQDN, structured errors and immutable view are public; membership-derived `NodeID` is not required. | Operator scaling and addresses for ordinals 0–6 exist. | Add no-retry client and one modelled barrier; OrderedReady serializes joins and keeps the fixture manageable. | **Implementable** |
| `lifecycle-backup-restore-rebuilds-history` | Restore completion and exact PIT are observable in `model`; optional boot/certification SDK signals improve diagnosis. | Requires extending `model`, enabling PIT, preserving MinIO on a PVC and producing a real archived chapter. None is configured now. | A 15-minute restore-cycle budget is plausible, but this must be isolated from `main` writers and other properties. | **Conditional** on a separate durable-MinIO model campaign |
| `idempotency-keyed-apply-changes-pit-once` | Audit/log reconciliation and PIT delta are observable, but the workload has no deterministic point that times out after commit and before the response. | Elections and response loss are supported generically. | Random deadlines may exercise only pre-acceptance timeouts; the rare ambiguous branch needs an explicit SUT checkpoint/proxy signal. | **Underspecified** |
| `replay-remote-delete-ack-is-idempotent` | Candidate state is internal; proposed delete/ack/retry signals and assertion are sufficient. | Cold GC plus hard restart support it after durable MinIO and termination are enabled. | Exact kill placement is rare but Antithesis can use the SUT checkpoint; fixture is small. | **Conditional** |
| `wildcard-live-path-survives-pit-dependency-fault` | Live write/read are public, but `minioIsolation` is not observable from the workload. | A link-selective Ledger→MinIO fault that preserves Raft/workload traffic is not encoded in the K8s recipe. | Cheap once a targeted fault label/proxy or SUT active-failure signal exists. | **Conditional** on fault targeting and phase observation |
| `resources-logical-run-debt-reconverges` | Current gauges are not workload-readable. The proposed coherent debug snapshot, builder head and completed-pass counter fully specify the oracle. | Existing topology supports stalls/restarts after metrics/diagnostic wiring. | Pass-count budget avoids a false latency SLO; bounded run counts make it practical. | **Conditional** on the new diagnostic surface |
| `resources-s3-stall-does-not-block-shutdown` | Old-process post-drain signal, active S3 verb tracking and positive-grace workload helper are all required and specified. | Requires a Ledger→MinIO stall plus positive-grace deletion; current K8s fault policy is unknown. | Focus one S3 verb/phase per campaign; otherwise combinations are too broad. | **Conditional** on SUT instrumentation and targeted faults |

## Exact infeasible or underspecified properties

### Underspecified

- `concurrency-api-primary-history-boundary`: replace the unobservable workload term `observedPrimaryHead` with either a SUT `Always` at the controller boundary or a response/test diagnostic that binds the primary snapshot ledger ID/log head to the returned view. A caller-supplied minimum alone is weaker than the property.
- `integrity-layout-independent-semantics`: choose one authoritative implementation: sampled SUT semantic-digest comparison with an explicit cost bound, or a finite public timestamp/account matrix. The current invariant requires both without deciding how the internal digest is observed.
- `idempotency-keyed-apply-changes-pit-once`: define a checkpoint that proves the first response was lost after commit, or weaken the targeted branch to a reconciled ambiguous result without claiming post-commit placement. Random short deadlines are not a reliable precondition.

### Blocked by the current harness

- `protocol-pit-error-contract` (missing/corrupt reachability only), `integrity-cold-content-verified`, and `recovery-source-missing-heals-same-process`: require a persistent exact-object remove/replace/restore helper.
- `security-pit-remote-gc-owner-containment`: additionally requires arbitrary namespace fixtures and a durable MinIO campaign.
- `recovery-unsynced-suffix-replays`: requires confirmation that the environment can lose NoSync PVC bytes, a node/disk fault, or a deterministic storage rollback seam.

## Passes

- The Kubernetes topology separates Ledger replicas, MinIO and the workload, so ordinary Raft, S3 and per-node service failures can be explored independently at the container level.
- Static ordinal FQDNs cover all supported 3/5/7 scaling targets, and `x-consistency: stale` plus a no-retry client can attribute one PIT response to one replica.
- The public PIT trailer contains enough ledger/selector/watermark provenance for exact monetary oracles and cross-replica comparisons without exposing physical manifest state.
- `GetClusterState{NodeId}` and `GetIndexStatus` provide the exact-node Raft, synchronization and read-index observations needed by forwarding, metadata, snapshot and convergence properties.
- Existing quiescence, scaling, rolling-restart, chapter, backup and restore drivers provide reusable lifecycle scaffolding. The evaluation does not require a new service container for the core campaign.
- Accelerated configuration remains statically valid and turns builder, verifier, compaction, tier and GC paths into practical targets with small fixtures. The catalog correctly avoids wall-clock performance assertions.
- The restore property is now scoped to the only implementable fresh-PVC contract: the isolated `model` template with authoritative archives preserved separately.

## Uncertainties requiring campaign decisions

- Whether Ledger/MinIO termination, clock faults and link-selective network faults are enabled by the target Antithesis tenant.
- Whether grace-zero pod deletion can discard un-fsynced Pebble/PVC bytes.
- Whether the first cold campaign will add a MinIO PVC or forbid MinIO termination.
- Whether a bounded workload MinIO mutator is acceptable for exact missing/corrupt and namespace fixtures.
- Whether test/debug diagnostics are acceptable for run-debt snapshots; SDK-only instrumentation is sufficient for the other internal properties.
- What K8s launch exclusions protect the workload, restore sidecar, operator and control plane from unrelated faults.

Until these are decided, the blocked/conditional properties must not install mandatory `Sometimes` assertions in the core campaign: a permanently unreachable precondition would report a harness limitation as a SUT liveness defect.
