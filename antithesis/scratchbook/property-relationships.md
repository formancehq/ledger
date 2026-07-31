---
sut_path: /Users/flemzord/Developer/Formance/ledger
commit: fb3f9f8334c7fbbcc27c2c211c77da36762e6aaf
updated: 2026-07-31
external_references: []
---

# PIT property relationships

## Summary

The catalog has one public monetary oracle, several physical mechanisms that
must preserve it, and liveness properties that require those mechanisms to
recover. Relationships here are suspected testing/dominance links, not formal
logical proofs.

## Cluster: public view and monetary oracle

- `concurrency-api-primary-history-boundary`
- `integrity-dual-axis-reversal-exactness`
- `protocol-current-metadata-historical-money`
- `integrity-ledger-incarnation-isolation`
- `protocol-leader-forwarding-provenance`
- `unfiltered-fast-path-equals-account-fold`

The first property binds source prefix/incarnation; dual-axis exactness supplies
the independent monetary oracle. Its merged provenance and error predicates
ensure the caller can distinguish that exact success from fail-closed outcomes.

The fast-summary/account-fold property is a cheaper metamorphic oracle over two
redundant storage scopes. On a small fully modeled ledger,
`integrity-dual-axis-reversal-exactness` likely dominates it; keep the fold check
because it can run frequently without reconstructing the complete audit oracle.

## Cluster: source prefix, persistence and repair

- `integrity-authoritative-prefix-only`
- `integrity-atomic-publication-restart`
- `integrity-layout-independent-semantics`
- `concurrency-verifier-maintenance-coherence`
- `boot-readiness-reconciles-persisted-history`
- `recovery-unsynced-suffix-replays`
- `recovery-repair-gate-survives-crashes`
- `recovery-source-missing-heals-same-process`
- `transient-minio-failure-recovers-without-sticky-source-loss`
- `lifecycle-follower-snapshot-install-fails-closed`
- `restore-ahead-history-fails-closed`
- `lifecycle-backup-restore-rebuilds-history`

`integrity-authoritative-prefix-only` protects publication input.
`integrity-atomic-publication-restart` protects one local publication.
`integrity-layout-independent-semantics` protects the logical result across
all later physical rewrites.
`concurrency-verifier-maintenance-coherence` proves the independent verifier
can observe those rewrites coherently and still make bounded progress.
`recovery-unsynced-suffix-replays` then tests the permitted atomic suffix loss.
`recovery-repair-gate-survives-crashes` is the broader safety envelope around
all rebuild reasons; the source-missing and restore properties specialize its
liveness and authority-reset boundaries.

## Cluster: physical rewrite, cold tier and reclamation

- `concurrency-pinned-view-maintenance-stability`
- `concurrency-compaction-cas-preserves-suffix`
- `concurrency-tier-lease-precedes-local-delete`
- `integrity-cold-content-verified`
- `concurrency-verifier-maintenance-coherence`
- `concurrency-builder-source-snapshot-archive-purge`
- `concurrency-remote-gc-live-roots-protected`
- `concurrency-remote-gc-inventory-upload-linearization`
- `security-pit-remote-gc-owner-containment`
- `replay-remote-delete-ack-is-idempotent`
- `remote-gc-queue-converges`

Pinned-view stability is the public result invariant across all maintenance.
The compaction, tier and remote-GC properties are stronger internal conditions:
they explain why the result remains exact and catch destructive mistakes before
a particular query happens to observe them.

The remote-GC properties form a protocol chain:

```text
owner/destination containment
  -> epoch-valid inventory
  -> fresh root/lease revalidation
  -> idempotent delete and durable acknowledgement
```

No later property compensates for an earlier unsafe delete.

## Cluster: chapters and hot/cold authority

- `concurrency-builder-source-snapshot-archive-purge`
- `out-of-order-chapters-fail-closed`
- `out-of-order-chapters-recover`
- `integrity-cold-content-verified`

The snapshot/purge property covers one normal hot-to-cold handoff. The two
out-of-order properties target a different, FSM-valid topology currently
rejected by the PIT source. Fail-closed safety does not imply eventual recovery,
so both remain separate.

## Cluster: distributed routing, membership and convergence

- `protocol-leader-forwarding-provenance`
- `lifecycle-follower-snapshot-install-fails-closed`
- `linearizable-pit-partition-fails-closed`
- `coordination-scale-up-no-premature-local-pit`
- `coordination-quiescent-pit-convergence`

Routing/follower/scale-up properties protect individual nodes during movement.
Quiescent convergence is the final liveness check across the stable membership
set. It does not require identical physical manifest versions or tokens.

## Cluster: retries and exactly-once effects

- `idempotency-keyed-apply-changes-pit-once`
- `recovery-unsynced-suffix-replays`
- `replay-remote-delete-ack-is-idempotent`

These properties share retry mechanisms but protect different domains: client
business effects, derived-history replay and destructive object maintenance.
They do not dominate one another.

## Cluster: operational independence and bounded recovery

- `live-path-survives-pit-dependency-fault`
- `cancelled-cold-reads-release-resources`
- `resources-logical-run-debt-reconverges`
- `resources-s3-stall-does-not-block-shutdown`

Live-path independence is the product boundary. Run-debt and shutdown properties
test whether asynchronous recovery eventually stops consuming resources or
blocking lifecycle progress after the optional dependency returns.

## Assumptions

- Relationship edges describe shared mechanisms/evidence, not requirements to
  execute all properties in one template.
- Backup/restore remains isolated from the ordinary concurrent main workload.
- Remote-GC internal assertions are necessary because public aggregate queries
  cannot prove which object was considered for deletion.

## Open Questions

- None beyond the property-level and deployment-level questions already
  recorded in the catalog and evidence files.
