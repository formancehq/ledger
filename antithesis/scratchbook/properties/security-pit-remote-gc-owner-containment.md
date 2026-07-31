# security-pit-remote-gc-owner-containment — Remote GC cannot cross the owned PIT namespace

| | |
|---|---|
| **Priority** | P0 |
| **Type** | Safety |
| **Attention focus** | Security Boundaries |
| **Confidence** | High. The configured owner is deliberately the stable Raft node ID, including across process/PVC incarnations. |

## Property

A replica's PIT remote collector may enumerate and delete only canonical
content-addressed run objects under that replica's configured owner namespace.
It must never delete another node's identical digest, an authoritative primary
chapter, a malformed object, or a nonzero-chapter object nested under the PIT
prefix. A replacement process using the same Raft node ID remains the same
logical owner: it may reuse a predecessor object only after independently
encoding the same content address and verifying the remote object, and it may
reclaim predecessor objects only when they remain unrooted after the normal
two-observation, grace-period and current-root checks.

## Exact assertion and rationale

In the cold-tier campaign, seed the same digest under two owner namespaces,
seed a primary chapter in the shared base bucket, and seed malformed/nonzero
chapter objects under the collecting owner's prefix. Make only the first
owner's canonical object eligible and inject a kill between delete and durable
acknowledgement. After deletion/retry:

```go
assert.Always(
    !ownedOrphanExists &&
        foreignOwnerObjectExists &&
        authoritativeChapterExists &&
        malformedObjectExists &&
        nonzeroChapterObjectExists,
    "pit: remote GC preserves every object outside its owned canonical namespace",
    details,
)
```

Add a surgical assertion in `balancehistoryarchive.Store.Delete`, immediately
before the catalog call:

```go
assert.AlwaysOrUnreachable(
    bucketID == namespace.objectBucket(digest) && chapterID == 0,
    "pit: remote GC delete target stays inside the configured owner namespace",
    details,
)
```

and a unique `Reachable` signal after a successful owned orphan delete:
`"pit: remote GC completed an owned orphan delete"`.
`AlwaysOrUnreachable` is appropriate for the optional destructive path; the
paired reachability assertion prevents a targeted cold campaign from passing
without exercising deletion. The black-box `Always` is the authoritative
cross-object check.

Add an ordinal-reuse variant: tier two objects on the highest ordinal, scale it
down until its PVCs are deleted, then scale it back up with the same node ID and
a fresh local history store. Have the replacement independently rebuild and
tier content matching one predecessor digest while leaving the other digest
unreferenced. After two complete inventories and the grace period, assert that
the rebuilt digest remains remotely present and the unreferenced digest is
deleted. Enumeration of an old object alone must never make it a live manifest
root.

## Antithesis angle

Use separate per-node clients and a shared MinIO, then kill the collecting node
after remote deletion but before the sync acknowledgement. Restart and let the
idempotent retry occur while other replicas tier the same content. Network
faults during paginated listing exercise cursor boundaries and ensure a foreign
key cannot be promoted into a durable candidate after resume.

Also scale down the highest StatefulSet ordinal through the operator's PVC
cleanup and scale it back up. The replacement receives a fresh Raft
`InstanceID` but the same numeric node ID and therefore the same PIT owner
namespace. Interleave its audit-driven rebuild/tiering with accelerated remote
GC: matching content may be verified and rooted, while unrooted predecessor
objects may be reclaimed. This is same-owner continuation, not an exception to
distinct-owner containment.

## Why it matters

The bucket contains both rebuild-authoritative chapters and replica-owned PIT
runs. Crossing either the feature namespace or node-owner boundary can destroy
the source needed to rebuild history or another replica's only cold copy.

## Code evidence

- `internal/storage/balancehistoryarchive/namespace.go:15-55` builds one prefix
  `<base>/balance-history/nodes/<owner>/runs` and rejects empty, traversal and
  separator-bearing base/owner components.
- `internal/storage/balancehistoryarchive/namespace.go:68-84` accepts only exact
  lowercase SHA-256 child names at chapter zero.
- `internal/storage/balancehistoryarchive/store.go:355-405` lists below the owned
  prefix, filters every returned object through the namespace parser, and
  reconstructs delete coordinates from a digest instead of accepting an
  arbitrary key.
- `internal/bootstrap/balance_history.go:113-118,194-203` scopes the PIT prefix to
  the configured cold bucket (or cluster ID) and derives owner ID from the Raft
  node ID.
- `internal/infra/node/config.go:54-60` gives each pod/PVC incarnation a distinct
  persisted `InstanceID`, but that identity is intentionally not part of the PIT
  archive owner configured by bootstrap.
- `misc/operator/internal/controller/reconcile_statefulset.go:170-176,628-632`
  deletes PVCs after scale-down and deterministically restores the same node ID
  as `ordinal+1` when that ordinal returns; `raft_scaledown.go:404-435` performs
  the PVC deletion.
- `internal/storage/balancehistorystore/tier.go:161-237,353-421` lets a fresh
  local store establish the configured destination binding, but starts tiering
  only from locally rebuilt manifest runs.
- `internal/storage/balancehistoryarchive/store.go:98-140` independently encodes
  each run to derive its content address and verifies any already-present object
  before returning its reference. It does not adopt objects discovered by GC.
- `internal/storage/balancehistorystore/remote_gc.go:449-500,503-641,696-776`
  initializes fresh local GC state for the matching binding, inventories the
  stable owner namespace, requires a later observation plus grace, and captures
  the replacement's current manifest roots before deletion.
- `internal/infra/coldstorage/object_catalog.go:29-44` keeps destructive catalog
  authority separate from ordinary archive I/O and defines deletion as one
  exact bucket/chapter object.
- `internal/storage/balancehistoryarchive/reclaimer_test.go:16-129` is the
  deterministic baseline for foreign listing and identical-digest deletion
  containment.
- `internal/storage/balancehistorystore/remote_gc_test.go:102-277` covers the
  second-observation/grace requirement, idempotent crash retry, active-view
  block and latest-manifest root preservation. There is no combined operator
  scale-down/PVC-delete/ordinal-reuse regression test yet.

## Existing assertion coverage

`antithesis/scratchbook/existing-assertions.md` says there are no SUT or workload
SDK assertions in the PIT archive/remote-GC packages. The deterministic
reclaimer tests are evidence for the intended boundary, not Antithesis
instrumentation. Both suggested messages are therefore **missing**.

## Open questions

None.

### Investigation Log

- **Question investigated:** When a StatefulSet ordinal is removed with its PVC
  and later recreated with the same Raft node ID, may the replacement use and
  reclaim the prior process's namespace, or must ownership include a durable
  process incarnation?
- **Examined:** archive namespace construction and parsing, bootstrap owner
  derivation, Raft `InstanceID`, operator node-ID/PVC lifecycle, tier binding and
  publication, remote-GC preparation/eligibility/root capture, deterministic
  archive and GC tests, and the PIT remote-GC architecture/CLI documentation.
- **Found:** `OwnerID` is explicitly a stable node identity and bootstrap fixes
  it to `node-<Raft NodeID>`; the operator reuses `ordinal+1` while a deleted PVC
  produces a new `InstanceID`. A fresh local history store can bind that same
  namespace and inventory its canonical objects. It cannot promote them from
  inventory: live reuse occurs only when locally rebuilt content independently
  produces the same digest and the archive verifies that object. Otherwise the
  object is an unrooted candidate and can be deleted only after the ordinary
  observation, grace and current-root safeguards.
- **Not found:** an end-to-end deterministic test combining operator scale-down,
  PVC deletion, same-ordinal replacement, pre-existing remote objects and both
  the verified-reuse and orphan-reclamation outcomes. This is the regression
  scenario added above, not an unresolved ownership question.
- **Conclusion:** resolved from repository evidence. Ownership is per stable
  numeric Raft node ID, not per `InstanceID`; same-ordinal replacement is
  same-owner continuation. Permanently retired node IDs remain outside every
  live replica's authority and require operator/decommission cleanup, so the
  distinct-owner containment property is unchanged.
