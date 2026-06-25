package indexbuilder

import (
	"fmt"

	"github.com/cockroachdb/pebble/v2"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

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

		_, mk, v, parsed := parseReverseMapKey(k, rmapPrefix, ns)
		if !parsed || mk != metaKey || v != version {
			continue
		}

		if err := batch.DeleteKey(cloneBytes(k)); err != nil {
			return fmt.Errorf("gc rmap entry: %w", err)
		}
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
