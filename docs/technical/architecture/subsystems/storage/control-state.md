# Configuration and capability state

## Requirement and MVP boundary

Capability activation must not consume an operator's configuration revision or
wait on member readiness before ordinary configuration can change. A requested
activation must survive leader changes and restart without operator input. No
business proposal may pass the activation barrier after a request is accepted.
Acceptance means the request's Raft entry has committed and applied;
an interrupted, uncommitted proposal is not a durable request.

The MVP has two bounded aggregates, in the existing Pebble database:

| Key in ZoneGlobal | Value | Owner |
| --- | --- | --- |
| `0x13` / `SubGlobClusterPolicy` | ClusterPolicy with no capability entries | SetClusterPolicy, operator revision |
| `0x18` / `SubGlobCapabilityState` | CapabilityState | TransitionCapability, monotonic per-ID transitions |

ClusterPolicy retains its name, revision, original storage key, and role as the
aggregate for flag-derived configuration. TTL, checkpoint capacity and metadata
limits remain together; only capabilities are extracted. ReadClusterPolicy
assembles the compatibility view in memory, not a third stored authority.
Capabilities share one bounded record;
there is no per-field revision, generic settings store, or additional database.
The FSM writes both rows in its existing atomic batch. Readers assemble them
from one snapshot, including final proposal admission and backup capture.

## Write semantics

Configuration uses its own existing revision contract: lower is stale, equal
and identical is a no-op, equal and different conflicts, greater replaces the
configuration. Current SetClusterPolicy proposals reject embedded capability
fields. Their `configuration_only=true` marker distinguishes them from released
proposals without changing the meaning of historical WAL entries. The FSM handler
combines the candidate with the latest applied capability state, not the
producer's earlier read. Ordinary reconciliation never waits for support of a
new, inactive capability. Active capabilities still enforce membership safety.

TransitionCapability addresses one ID. Request adds `requested=true` without
activating; activation requires a committed request. Repeated requests and
activation are idempotent. Requesting an already active capability never clears
it. Neither transition changes the configuration revision. Historical entries
with `active=false, requested=false` remain inactive, not pending requests.

The production activation worker automatically discovers supported IDs ready
on all current voters and learners, then commits a durable request. It rereads
committed policy and rechecks readiness before activation. Empty membership or
an unready member cannot arm a new request. The existing membership coordinator
holds membership stable through apply; final proposal admission checks support
before index allocation. The next leader resumes durable pending intent, and a
lost response is resolved by rereading committed state rather than undoing it.
Before discovery, the leader commits its own changed support through a membership
refresh. Local restart/snapshot routing writes cannot add capability evidence;
single-member clusters use the same committed refresh, without a remote RPC.

Before request commit, final admission pauses business proposals once all
members support any locally implemented inactive capability. After commit,
durable requested state keeps the barrier
until activation, even if readiness regresses. This closes the interval between
drain release and activation without any startup flag. The fence affects only
admission; FSM apply never reads readiness or node-local configuration.

Business admission holds an order batch rejected by this final barrier, then
reprepares it from committed policy after a bounded poll interval. It releases
the old preload/guard before waiting and retains no Raft index. Cancellation,
write health and leader readiness remain enforced. Action metrics count one
caller operation; preparation/proposal phase histograms record each attempt.
A submitted proposal's outcome is never retried by this path.

## Durability and migration

Storage schema 3 is a mandatory stopped upgrade from schema 2. Before recovery,
workers or listeners start, ValidateOrPersistConfig checks schema and identity,
then reads the committed legacy policy. One synchronous Pebble batch writes the
ClusterPolicy at its original key `0x13` without capabilities, writes the extracted
CapabilityState at `0x18`, and updates the schema anchor to 3.
WriteSession.Commit uses NoSync: after that atomic visibility change, startup
calls Store.SyncWAL and waits for its fsync before allowing recovery or serving.
No business history is scanned, rewritten or reconstructed. Values, revision,
inactive entries and active entries retain the same logical state at the same
applied index. An absent legacy policy becomes an explicitly empty pair.

Before batch commit the old rows and anchor remain authoritative; cancellation
leaves them unchanged. After commit only the split rows and schema-3 anchor
are visible, but durability is confirmed only after SyncWAL succeeds. A commit
or fsync failure aborts startup without rolling back visible state; reopen
resolves whether the complete old or complete new batch survived. Do not
attempt binary rollback after an uncertain migration outcome.
Restart is idempotent. A schema-3 store with a missing aggregate, invalid IDs,
or capabilities embedded in both records fails closed rather than deriving defaults from
flags. Identity overrides cannot bypass schema or committed policy checks.

During normal apply, WriteSet stages the control view and may advance private
FSM state before commit. The aggregate rows and applied progress share the
existing durable batch. Admission's committed view and membership gate are
published only after successful commit. A commit failure is fatal to serving;
there is no promise of in-memory rollback or permission to continue serving
the private prepared state.

Released orders and native SetClusterPolicy proposals without the new marker
retain their original replay semantics, including embedded capabilities. They
fold historical policy into the two rows without changing historical
business/audit bytes. New production writers emit SetClusterPolicy with
`configuration_only=true` and TransitionCapability. The marker is a persisted
command discriminator, not an operator flag. No old writer may coexist with
them: an old decoder ignores the marker and cannot preserve detached capabilities.
Snapshot recovery assembles the same view as startup migration.

## Backups and rollback

Both aggregates survive checkpoints. Full and incremental manifests carry one
control-state snapshot at a single source applied index, after legacy delta
folding on restore. Requested state, active state, configuration and its
revision are preserved across clusters. Member support is not exported as
activation evidence: the target checks its own membership. The source applied
index is provenance, not the destination's Raft progress.

The manifest's existing `clusterPolicy.data` field now contains
`{"version":1,"policy":"<base64 protobuf>"}` instead of a base64 string.
The released RC.4 decoder expects bytes at that known field and rejects the
object. New readers accept both forms and reject unknown versions. Every new
full backup also writes the envelope, including for uninitialized policy, so
an old reader cannot silently restore only the checkpoint and drop intent.
Publication still uploads immutable objects before atomically replacing the
manifest; failure retains the previously published backup.

The schema upgrade closes binary rollback before capability activation: RC.4
cannot open schema 3. New Raft payloads independently close WAL rollback.
Restarting cannot roll back migration or cancel capability activation.
Recover using the target binary and preserved volumes, or restore a separately
retained pre-upgrade backup with its matching binary, accepting loss of writes
after that backup. Never point an old binary at migrated volumes or new backups.

## Cost and integrity

Migration is O(number of capability entries) with one atomic batch and no
history scan. Each current read uses two aggregate lookups in one snapshot;
when capability state is absent, the schema anchor distinguishes a released
store from an incomplete migration. Each transition rewrites the bounded aggregate
pair; FSM apply adds no Pebble reads or cache coverage. Activation work is
bounded by capabilities and members and runs on the existing one-second tick.

This preserves the existing deployment trust boundary. Configuration quotas
are operational; capability state is deployment compatibility governance,
not accounting truth. It selects which marker admission writes into the
audited proposal. The marker, semantic validation and resulting business
effects remain audit-bound and checker-verified; the checker does not prove
the historical request or activation instant. Protect store and backup access
and manifest integrity. This explicit exception is not an exemption for ledger
schemas, permissions, enforcement modes or business values.

Proofs: TestControlStateSchemaTwoMigration,
TestControlStateIndependentRevisionsAndRecovery,
TestCapabilityActivationDurableIntentAndIndependentConfiguration,
TestControlStateThroughRunAndRestart,
TestCapabilityActivationPendingIncrementalRestore, and
TestControlStateManifestRejectsLegacyDecoder.
