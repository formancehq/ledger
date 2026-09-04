package indexbuilder

import (
	"fmt"

	"github.com/cockroachdb/pebble/v2"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// parseScopedReverseMapKey decodes k and asserts it belongs to (ledger, ns).
// Every caller scans a Pebble prefix built from exactly (ledger, ns) —
// gcReverseMapVersion, processSchemaRewrite, and both scans in
// purgeReverseMapForKey — so a divergence between the decoded
// key and the scope the caller iterated means the stored key is corrupt in a
// way ParseReverseMapKey's own shape checks don't reject, not a runtime
// condition. Per CLAUDE.md invariant #7 that gets a loud error, never a
// silent skip. This is the single chokepoint for that assertion: callers
// wrap the returned error with their own site-specific context and keep
// their own soft continue/return for a legitimate metadata-key or version
// mismatch (that check stays out of this helper — it isn't a corruption
// signal).
func parseScopedReverseMapKey(k []byte, ledger, ns string) (readstore.ParsedReverseMapKey, error) {
	rk, err := readstore.ParseReverseMapKey(k)
	if err != nil {
		return readstore.ParsedReverseMapKey{}, err
	}

	if rk.Ledger != ledger || rk.Namespace != ns {
		return readstore.ParsedReverseMapKey{}, fmt.Errorf("invariant: rmap key %x decoded to ledger %q ns %q, expected %q/%q", k, rk.Ledger, rk.Namespace, ledger, ns)
	}

	return rk, nil
}

// gcVersionAt purges every readstore key tied to (ledger, ns, metaKey,
// version): the forward index range and the eidx range via DeleteRange,
// and the rmap rows via iter+DeleteKey.
//
// The forward and eidx keyspaces have a clean per-version prefix
// (MetadataIndexPrefixV / EntityExistsKeyPrefixV) so DeleteRange is
// the natural primitive. The rmap key embeds version *after* the
// entity, so per-version rows are interleaved with rows from other
// versions — DeleteRange doesn't apply. The iter cost is bounded by
// the indexed entity count for this metadata field at the given
// version, the same bound the rewrite itself walked.
//
// All mutations land on the caller's batch — the caller decides when
// to commit (typically alongside the atomic switch). The rmap iter
// uses a fresh snapshot, so the GC observes committed state only and
// never collides with the rewrite's in-flight v_pending writes (a
// different keyspace) buffered in the same batch.
func (b *Builder) gcVersionAt(batch *dal.WriteSession, kb *dal.KeyBuilder, ledger, ns, metaKey string, version uint32) error {
	fwdPrefix := readstore.MetadataIndexPrefixV(kb, ledger, ns, metaKey, version)
	fwdUpper := readstore.IncrementBytes(fwdPrefix)

	if err := batch.DeleteRangeNoSync(fwdPrefix, fwdUpper); err != nil {
		return fmt.Errorf("gc forward index at v=%d: %w", version, err)
	}

	eidxPrefix := readstore.EntityExistsKeyPrefixV(kb, ledger, ns, metaKey, version)
	eidxUpper := readstore.IncrementBytes(eidxPrefix)

	if err := batch.DeleteRangeNoSync(eidxPrefix, eidxUpper); err != nil {
		return fmt.Errorf("gc eidx at v=%d: %w", version, err)
	}

	return b.gcReverseMapVersion(batch, kb, ledger, ns, metaKey, version)
}

// gcReverseMapVersion iterates the rmap range for (ledger, ns) and
// queues a DeleteKey on every row matching (metaKey, version). Mirrors
// the filter logic processSchemaRewrite uses to identify v_current
// entries, but writes deletes instead of re-encodings.
//
// The iter runs against the read store directly (no snapshot). The
// caller already holds a snapshot wherever the surrounding atomic
// switch lives — taking a second one here would just pin extra SSTs
// for the duration of the scan without buying anything: the keys we
// queue are buffered in `batch` and won't land on disk until the
// caller commits, and any concurrent live writes can only mutate
// v_current/v_pending which we don't touch here (gcVersionAt only
// runs for v_old or boot-orphan versions, both quiescent by
// construction).
func (b *Builder) gcReverseMapVersion(batch *dal.WriteSession, kb *dal.KeyBuilder, ledger, ns, metaKey string, version uint32) error {
	rmapPrefix := readstore.ReverseMapPrefix(kb, ledger, ns)
	upper := readstore.IncrementBytes(rmapPrefix)

	iter, err := b.readStore.DB().NewIter(&pebble.IterOptions{
		LowerBound: rmapPrefix,
		UpperBound: upper,
	})
	if err != nil {
		return fmt.Errorf("opening rmap iter for gc: %w", err)
	}

	defer func() { _ = iter.Close() }()

	for iter.First(); iter.Valid(); iter.Next() {
		k := iter.Key()

		rk, err := parseScopedReverseMapKey(k, ledger, ns)
		if err != nil {
			return fmt.Errorf("gc rmap: %w", err)
		}

		if rk.MetadataKey != metaKey || rk.Version != version {
			continue
		}

		if err := batch.DeleteKey(cloneBytes(k)); err != nil {
			return fmt.Errorf("gc rmap entry: %w", err)
		}
	}

	// A truncated scan is indistinguishable from exhaustion on !iter.Valid(),
	// and returning nil here would report the version as reclaimed while leaving
	// rows behind — the same silent-truncation shape as purgeReverseMapForKey.
	if err := iter.Error(); err != nil {
		return fmt.Errorf("scanning rmap for gc: %w", err)
	}

	return nil
}

// purgeOrphanVersions reclaims read-store entries that don't belong
// to either (current_version, pending_version) for any indexed
// metadata field. Called once at boot, after IndexVersionState is
// restored from disk.
//
// The atomic switch (processSchemaRewrite) does an inline GC of v_old
// in the same batch as the version promotion, so steady-state operation
// never leaves orphans behind. This sweep handles the crash window:
// a node that died after the switch committed but before its
// follow-up activity (re-retypes, snapshot installs that left stale
// version data, etc.) can reboot with orphan v_n keyspaces. Each
// orphan version is purged via gcVersionAt — DeleteRange is a cheap
// tombstone even when the range is already empty, so the scan is
// safe to run unconditionally.
//
// Cost: iterate versions 1..max(current, pending) skipping the live
// pair, calling gcVersionAt on each. DeleteRange on the forward/eidx
// keyspaces is constant cost regardless of contents, but
// gcReverseMapVersion under the hood scans the whole namespace's rmap
// rows once per non-live version — so the total cost is
// O((maxV - liveVersions) × rmap_rows_in_ns) per indexed field, NOT
// O(maxV). In practice maxV ≤ 2 in normal operation (one current,
// one pending) so the scan reduces to zero versions in the common
// case; only operator-driven rapid retype storms make this measurable.
// A single-pass version of this sweep (scan rmap once, batch deletes
// across all orphan versions) is a worthwhile follow-up if real
// workloads hit it.
func (b *Builder) purgeOrphanVersions() error {
	if b.indexVersions == nil {
		return nil
	}

	kb := dal.NewKeyBuilder()

	for ledgerName, inner := range b.indexVersions {
		cfg := b.ledgerConfig(ledgerName)
		if cfg == nil {
			continue
		}

		for canonical, state := range inner {
			idx, ok := cfg.byCanonical[canonical]
			if !ok || idx.GetId() == nil {
				continue
			}

			meta, ok := idx.GetId().GetKind().(*commonpb.IndexID_Metadata)
			if !ok || meta.Metadata == nil {
				// Only metadata indexes are versioned today. Builtin
				// indexes never carry a forward_encoding_version, so any
				// IndexVersionState pointing at one is itself stale.
				continue
			}

			ns := namespaceForTarget(meta.Metadata.GetTarget())
			if ns == "" {
				continue
			}

			metaKey := meta.Metadata.GetKey()

			maxV := max(state.CurrentVersion, state.PendingVersion)
			if maxV == 0 {
				continue
			}

			batch := b.readStore.NewBatch()

			var purged []uint32

			for v := uint32(1); v <= maxV; v++ {
				if v == state.CurrentVersion || v == state.PendingVersion {
					continue
				}

				if err := b.gcVersionAt(batch, kb, ledgerName, ns, metaKey, v); err != nil {
					_ = batch.Cancel()

					return err
				}

				purged = append(purged, v)
			}

			if len(purged) == 0 {
				_ = batch.Cancel()

				continue
			}

			if err := batch.Commit(); err != nil {
				return fmt.Errorf("committing orphan purge for %s/%s: %w", ledgerName, canonical, err)
			}

			b.logger.WithFields(map[string]any{
				"ledger":  ledgerName,
				"field":   metaKey,
				"current": state.CurrentVersion,
				"pending": state.PendingVersion,
				"purged":  purged,
			}).Infof("Purged orphan index versions")
		}
	}

	return nil
}

// eventGCKeyBudget bounds how many event keys one tick's GC pass visits per
// zone — enough to keep up with sustained metadata churn (the walk runs ~10
// times a second) while keeping each pass well under the tick interval.
const eventGCKeyBudget = 4096

type eventGCZoneFunc func(zone byte, resume []byte, watermark uint64, budget int) (pruned int, next []byte, err error)

type eventGCCycleState struct {
	active    bool
	completed bool
	resume    []byte

	cycleWatermark  uint64
	cycleWriteEpoch uint64

	completedWatermark  uint64
	completedWriteEpoch uint64
}

func (b *Builder) gcEventZone(zone byte, resume []byte, watermark uint64, budget int) (int, []byte, error) {
	if b.eventGCZone != nil {
		return b.eventGCZone(zone, resume, watermark, budget)
	}

	return readstore.GCEventZone(b.readStore.DB(), zone, resume, watermark, budget)
}

func (b *Builder) eventGCCycle(zone byte) *eventGCCycleState {
	if b.eventGCCycles == nil {
		b.eventGCCycles = make(map[byte]*eventGCCycleState, 2)
	}

	state := b.eventGCCycles[zone]
	if state == nil {
		state = &eventGCCycleState{}
		b.eventGCCycles[zone] = state
	}

	return state
}

// flushWriteBatch couples event-GC scheduling to the actual commit boundary:
// zone dirtiness is captured before Flush resets the batch, but write epochs
// advance only after that commit succeeds. Cancelled, failed and event-free
// batches therefore cannot schedule a spurious sweep.
func (b *Builder) flushWriteBatch() error {
	eventZones := b.wb.EventZones()
	if err := b.wb.Flush(); err != nil {
		return err
	}

	if b.eventGCWriteEpoch == nil {
		b.eventGCWriteEpoch = make(map[byte]uint64, 2)
	}

	for _, zone := range []byte{readstore.PrefixMetadataIndex, readstore.PrefixEntityExists} {
		if eventZones.Has(zone) {
			b.eventGCWriteEpoch[zone]++
		}
	}

	return nil
}

// runEventGC advances edge-triggered reclamation of superseded metadata /
// exists index events by one budgeted slice per active zone. A completed zone
// stays idle until either the lease-bounded watermark or that zone's committed
// write epoch advances.
//
// The fold cursor is only a proposal: BeginGC lowers it to the minimum live
// pin and publishes the result as the registry's reclaim floor, under the
// same lock that admits new leases. A reader whose pin is already registered
// keeps its history; one that arrives afterwards with a lower pin is refused
// and re-pins. Nothing here may assume a future pin is at least the fold
// cursor — a pin is read from a handle that can be arbitrarily older.
func (b *Builder) runEventGC(cursor uint64) {
	watermark := b.readStore.Leases().BeginGC(cursor)
	if watermark == 0 {
		return
	}

	for _, zone := range []byte{readstore.PrefixMetadataIndex, readstore.PrefixEntityExists} {
		state := b.eventGCCycle(zone)
		writeEpoch := b.eventGCWriteEpoch[zone]

		if !state.active {
			covered := state.completed &&
				watermark <= state.completedWatermark &&
				writeEpoch <= state.completedWriteEpoch
			if covered {
				continue
			}

			state.active = true
			state.resume = nil
			state.cycleWatermark = watermark
			state.cycleWriteEpoch = writeEpoch
		}

		pruned, next, err := b.gcEventZone(zone, state.resume, state.cycleWatermark, eventGCKeyBudget)
		if err != nil {
			// One zone failing says nothing about the other; sweeping it is
			// what keeps the surviving zone from growing without bound. Keep
			// this zone's exact cycle tuple and resume key for its retry.
			b.logger.Errorf("event GC pass on zone %#x failed: %v", zone, err)

			continue
		}

		state.resume = next
		if next == nil {
			state.active = false
			state.completed = true
			state.completedWatermark = state.cycleWatermark
			state.completedWriteEpoch = state.cycleWriteEpoch
		}

		if pruned > 0 {
			b.logger.Debugf("event GC reclaimed %d events in zone %#x", pruned, zone)
		}
	}
}
