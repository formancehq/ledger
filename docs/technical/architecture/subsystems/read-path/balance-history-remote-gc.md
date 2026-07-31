# Balance-history remote garbage collection

Balance-history remote garbage collection reclaims content-addressed run
objects that are no longer reachable from the local replica's manifests. It is
deliberately separate from local archive-cache eviction:

- local cache eviction removes a verified downloaded copy and can fetch it
  again from cold storage;
- remote GC deletes the durable cold object and is therefore destructive.

The v3.0 implementation provides the storage protocol and a bounded
`RemoteCollector.Collect` operation. When point-in-time history and its cold
tier are enabled, server bootstrap starts a maintenance worker that invokes the
collector on its configured interval and budget. Local-only history still runs
the same worker for bounded compaction, without remote calls.

## Ownership and destination binding

Each replica owns exactly one logical namespace:

```text
<cluster>/balance-history/nodes/<nodeID>/runs/<sha256>
```

Enumeration and deletion never cross the stable node ID. A shared cluster
namespace is unsafe because one replica's manifest cannot prove that another
replica released an object. Retired-node namespaces remain an operator or
future decommission-tool responsibility; a live replica never adopts them.

Logical ownership is not enough. Two filesystem roots or two S3 buckets can
use the same cluster and node strings. Every archive therefore exposes a
versioned, non-secret destination identity:

- filesystem: hash of the absolute cleaned root path;
- S3: hash of endpoint, region, and bucket;
- archive: hash of that physical identity plus the complete logical namespace.

Credentials are never part of the identity. The history store persists the
identity and a monotonically increasing archive mutation epoch at a dedicated
`archiveBindingKey` prefix outside the local reset range. Publishing the first
archived reference validates or writes that binding in the same Pebble batch
as the manifest update. On restart, `ConfigureTiering` refuses a missing or
different binding whenever any manifest run is archived.

The configured archive namespace has a single writer: the Store instance that
owns its binding. Runtime uploads must go through `Store.Tier`, and runtime
deletes must go through `RemoteCollector`; bootstrap owns both. `Tier` advances
the mutation epoch durably before any remote operation that may create an
object. Calling `Archive.Archive` directly bypasses that epoch and is not an
authorized runtime path. Tests may do so only to construct explicit orphan
fixtures.

`Reset`, `ResetForRebuild`, and `ResetForSourceRepair` delete manifests and
local runs but deliberately preserve both the binding and the remote-GC
cursor/candidates. Each reset advances the mutation epoch in the same durable
batch as the local deletion, invalidating every earlier empty-inventory proof.
A destination cannot be disabled or replaced—even when no archived run
remains—until the old destination has a completed inventory for the current
mutation epoch with empty cursor, scan counters, inventory, and candidate
queue. This prevents reset or reconfiguration from silently abandoning remote
bytes. There is no force-abandon path in v3.0; exceptional retired-node cleanup
belongs to a future decommission tool or a manual operator procedure outside
the live Store.

The identity detects configuration drift, not replacement behind identical
coordinates. Recreating a filesystem mount or S3 bucket with the same path,
endpoint, region, and bucket name produces the same identity. Operational
change control must treat such replacement as destructive. The grace period,
two completed observations, manifest root check, and archive gate still
prevent deletion of a currently referenced object, but cannot prove the
continuity of an external service recreated at identical coordinates.

## Durable state machine

Remote-GC metadata uses Pebble prefixes above the range deleted by `Reset`:

```text
0x20  state: format, namespace, destination, scan epoch, completed-inventory
      epoch, cursor, scan cycle, inventory, queue totals, oldest observation
0x21  candidate by SHA-256: destination, size, first observation time,
      first and last observation cycles
0x22  archive destination binding + mutation epoch, preserved across Reset
```

One bounded call performs:

```text
Acquire archiveGate for writing
  -> List one owned page outside mutationMu
  -> under mutationMu, compare the scan epoch with the binding mutation epoch
  -> if changed, Sync a discarded cursor/counters and restart at the prefix
  -> otherwise Sync page candidates + cursor + cycle state
  -> on the final page only, persist completedInventoryEpoch = scanEpoch
  -> release archiveGate
  -> require grace period and observation in a later scan cycle
  -> reacquire archiveGate for writing
  -> capture latest roots and verify there are no active Views
  -> retire rooted candidates with a Sync acknowledgement
  -> Delete unrooted objects outside mutationMu
  -> Sync each successful deletion acknowledgement
```

An object seen only once is pruned after a later complete scan no longer sees
it. A twice-observed candidate remains queued even if a later list omits it:
this is the crash-recovery case where remote deletion succeeded but the local
acknowledgement did not. The next call repeats the idempotent exact-key delete
and then acknowledges it.

Two collector instances may attempt work concurrently, but the archive gate
serializes each bounded list-and-Sync page with uploads and reconfiguration.
Cursor synchronization still rejects a stale prepared page instead of
overwriting newer state; an epoch mismatch discards it without certifying the
page. Every acknowledgement rereads the current state and current candidate
under `mutationMu`, applies only its queue delta, and preserves cursor and cycle
advances made while remote deletion was in flight. Result queue totals and
queue gauges are reread from durable state after every post-page-Sync exit,
including active-View blocks, reconfiguration, root failures, and remote delete
failures. The original operation error remains authoritative if that
best-effort refresh also fails.

`prepareState` does not constitute an inventory proof. A proof exists only
after the final page and its `completedInventoryEpoch` are durably synced. A
list failure after initial state creation therefore cannot permit destination
rotation. Each scan pins the binding's current mutation epoch. If `Tier` or a
reset advances it while listing is outside `mutationMu`, page synchronization
discards the entire cursor and partial counters and the next bounded call
restarts from the namespace prefix. Only a final page observed under an
unchanged epoch can certify an empty destination.

### Crash windows

| Crash point | Durable outcome | Recovery |
|---|---|---|
| After epoch advance, before/during upload | Earlier empty proof is invalid | A new full inventory is required even if upload failed |
| After upload, before manifest CAS | Object may be an unrooted orphan and proof is invalid | A new full inventory discovers it and applies grace |
| After reset batch | Local roots are gone and the epoch is advanced atomically | A new full inventory is required before rotation |
| Before page Sync | Cursor and candidates are unchanged | The page is listed again |
| Mutation during remote list | Page epoch differs from the binding epoch | Partial scan is durably discarded and restarted |
| After page Sync, before delete | Candidate remains queued | Root and grace checks repeat |
| During delete | Exact-key delete may or may not have completed | Idempotent delete repeats |
| After delete, before ack | Mature candidate remains queued | Missing object is deleted idempotently, then acked |
| After ack | Candidate and queue totals are durably retired | No further action |

An upload that succeeds before its manifest compare-and-swap loses the race is
not special-cased. It has no root, so a later inventory discovers it as an
ordinary orphan and applies the same grace protocol.

## Concurrency and roots

The lock order is fixed:

```text
archiveGate -> mutationMu -> leaseMu
```

`Tier` holds the archive gate for reading from the durable pre-upload mutation
epoch advance through the first remote `Exists` or `Archive` operation and the
manifest compare-and-swap. The collector holds it for writing around each
bounded `List` plus page Sync, then reacquires it while it captures roots and
performs remote deletes. A list therefore cannot certify the interval between
the pre-upload epoch advance and object creation, including an upload whose
manifest compare-and-swap is later lost. Remote `List` and `Delete` calls never
hold `mutationMu`; deletion retains only the archive gate.

A bounded remote `List` can temporarily delay `Tier` and `ConfigureTiering`.
It never takes the Store mutation lock during remote I/O, so it does not block
the FSM or the balance-history builder. Resets do not wait for the archive
gate; their atomic epoch advance makes an in-flight page discard itself at
Sync instead.

The latest manifest is always a root. Active View leases are stored only by
manifest version, and `Reset` may reuse versions, creating an ABA ambiguity.
Without widening the CRITICAL Store lease structure, the safe policy is
conservative: if any View lease is active, the collector deletes no object in
that call and returns `ErrRemoteGCRootsUnavailable`. Once all Views close, the
next call rechecks the latest manifest and resumes. This also covers a View
whose manifest key was removed by `Reset` while its Pebble snapshot remained
open.

`ConfigureTiering` never publishes an incomplete state. Initial configuration
holds `mutationMu`, persists the destination binding, and then atomically makes
the complete archive, identity, reclaimer, and gate visible. Reconfiguration
holds the old gate for writing before `mutationMu`. A delayed `Tier` candidate
revalidates the state after acquiring the read gate and performs no remote I/O
when disable or rotation won the race.

## S3 requirements

The runtime identity must describe the actual endpoint, region, and bucket.
The service role needs the narrow permissions used by the archive and
collector, including:

- `s3:ListBucket` on the configured bucket and owned prefix;
- `s3:GetObject`, `s3:PutObject`, and `s3:DeleteObject` on owned objects;
- multipart permissions required by the uploader.

With S3 versioning enabled, `DeleteObject` normally creates a delete marker; it
does not immediately reclaim noncurrent object bytes. Capacity reclamation
therefore also requires lifecycle rules that expire noncurrent versions and
delete markers. Configure a rule to abort incomplete multipart uploads as
well. The v1 collector intentionally lists and deletes current keys only; it
does not enumerate object versions.

## Metrics and alerts

`RemoteCollector.RegisterMetrics` exposes label-free instruments:

| Instrument | Type | Unit | Suggested alert |
|---|---|---|---|
| `balancehistory.remote_gc.inventory.objects` | Gauge | `{object}` | Unexpected discontinuity |
| `balancehistory.remote_gc.inventory.bytes` | Gauge | `By` | Capacity growth outside forecast |
| `balancehistory.remote_gc.queue.objects` | Gauge | `{object}` | Sustained growth after two scan cycles |
| `balancehistory.remote_gc.queue.bytes` | Gauge | `By` | Sustained reclaimable-byte growth |
| `balancehistory.remote_gc.queue.oldest_age` | Gauge | `s` | Greater than grace plus two scan intervals |
| `balancehistory.remote_gc.blocked.active_view.cycles` | Counter | `{cycle}` | Persistent increase with long-lived queries |
| `balancehistory.remote_gc.deleted.objects` | Counter | `{object}` | Logical current-object delete rate |
| `balancehistory.remote_gc.deleted.bytes` | Counter | `By` | Logical current-object bytes, not physical version reclamation |
| `balancehistory.remote_gc.list.failures` | Counter | `{failure}` | Any sustained increase |
| `balancehistory.remote_gc.delete.failures` | Counter | `{failure}` | Any sustained increase |
| `balancehistory.remote_gc.list.duration` | Histogram | `s` | Backend latency regression |
| `balancehistory.remote_gc.delete.duration` | Histogram | `s` | Backend latency regression |
| `balancehistory.remote_gc.last_completed_inventory.timestamp` | Gauge | `s` | Older than the configured scan objective |

An active-View safety block increments only the blocked-cycle counter. It is
not a delete failure. The completed-inventory timestamp advances immediately
after the page and cursor Sync finishes a full scan, even if a later root block
or delete failure prevents reclamation. A list or page-Sync failure cannot
advance it. A scan invalidated by an archive mutation also cannot advance it;
the timestamp records only a final page synced under an unchanged mutation
epoch.

## Operational policy

- Use a grace period longer than the maximum expected inventory interval and
  storage-listing convergence window. The library default is 24 hours.
- Keep one collector instance per Store in normal operation. The durable CAS
  checks make accidental overlap safe, but overlapping scans waste remote I/O.
- Never point a running store at a new destination until the old destination's
  durable inventory proof is completely empty. `Reset` does not waive this
  requirement.
- Decommission cleanup must prove the node owner is permanently retired before
  deleting its namespace.
- Monitor remote inventory independently from the byte-bounded local cache;
  cache eviction success says nothing about durable remote reclamation.
