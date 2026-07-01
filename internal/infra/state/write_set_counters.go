package state

import (
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// countKeyDeltas counts per-ledger new keys (+1) and deletions (-1) from merge results.
// A new key is identified by Old not being defined (first time in the parent KeyStore).
func countKeyDeltas[K attributes.Key, T any](
	updates []attributes.Update[K, T],
	deletions []attributes.Deletion[K],
	getLedgerName func(K) string,
) map[string]int64 {
	deltas := make(map[string]int64)

	for i := range updates {
		if !updates[i].Old.IsDefined() {
			deltas[getLedgerName(updates[i].Key)]++
		}
	}

	for i := range deletions {
		deltas[getLedgerName(deletions[i].Key)]--
	}

	return deltas
}

// applyDelta safely adds a signed delta to a uint64 counter, clamping at zero on underflow.
func applyDelta(current uint64, delta int64) uint64 {
	if delta >= 0 {
		return current + uint64(delta)
	}

	sub := uint64(-delta)
	if sub > current {
		return 0
	}

	return current - sub
}

// isVolumePreloadZero returns true if the volume pair is the zero placeholder
// injected by the preloader for keys that don't exist in Pebble.
// Unlike isVolumeZeroBalance (input == output), this checks input == 0 AND output == 0.
func isVolumePreloadZero(v *raftcmdpb.VolumePair) bool {
	if v == nil {
		return true
	}

	in := v.GetInput()
	out := v.GetOutput()

	return (in == nil || (in.GetV0() == 0 && in.GetV1() == 0 && in.GetV2() == 0 && in.GetV3() == 0)) &&
		(out == nil || (out.GetV0() == 0 && out.GetV1() == 0 && out.GetV2() == 0 && out.GetV3() == 0))
}

// countVolumeDeltas counts per-ledger new volume keys.
// A volume is "new" if Old is either undefined or a zero preload placeholder
// (the preloader always seeds missing volumes with {0,0} in the cache).
func countVolumeDeltas(updates []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair]) map[string]int64 {
	deltas := make(map[string]int64)

	for i := range updates {
		old := updates[i].Old
		if !old.IsDefined() || isVolumePreloadZero(old.Value()) {
			deltas[updates[i].Key.LedgerName]++
		}
	}

	return deltas
}

// updateBoundaryCounters computes attribute key deltas and updates LedgerBoundaries
// for each affected ledger. Must be called before Derived.Boundaries.Merge().
//
// Only the two attribute-derived counters (volume_count, metadata_count) live
// here now — reference_count, posting_count, revert_count, numscript_execution_count,
// ephemeral_evicted_count and transient_used_count all migrated to the
// usagebuilder (EN-1420) which derives them from the audit chain instead.
// The two remaining counters are covered by EN-1422.
//
// purgedVolumes and transientVolumes are still consumed here — not to feed
// their own counter (that lives in the usagebuilder), but to correctly
// subtract them from volume_count: an ephemeral volume that never survives
// commit and a transient volume that never persists must not count as a live
// volume-store entry.
func (b *WriteSet) updateBoundaryCounters(
	volumeUpdates []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair],
	purgedVolumes []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair],
	transientVolumes []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair],
	metadataUpdates []attributes.Update[domain.MetadataKey, *commonpb.MetadataValue],
	metadataDeletions []attributes.Deletion[domain.MetadataKey],
) {
	volumeDeltas := countVolumeDeltas(volumeUpdates)

	// Ephemeral volumes are purged after commit — subtract from volume count.
	for i := range purgedVolumes {
		volumeDeltas[purgedVolumes[i].Key.LedgerName]--
	}
	// Transient volumes are never persisted — subtract from volume count.
	for i := range transientVolumes {
		volumeDeltas[transientVolumes[i].Key.LedgerName]--
	}

	metadataDeltas := countKeyDeltas(metadataUpdates, metadataDeletions, func(k domain.MetadataKey) string { return k.LedgerName })

	// Collect all affected ledgers.
	affected := make(map[string]struct{})
	for ledger := range volumeDeltas {
		affected[ledger] = struct{}{}
	}
	for ledger := range metadataDeltas {
		affected[ledger] = struct{}{}
	}

	for ledgerName := range affected {
		boundariesReader, err := b.Boundaries().Get(domain.LedgerKey{Name: ledgerName})
		if err != nil {
			continue
		}

		boundaries := boundariesReader.Mutate()
		boundaries.VolumeCount = applyDelta(boundaries.GetVolumeCount(), volumeDeltas[ledgerName])
		boundaries.MetadataCount = applyDelta(boundaries.GetMetadataCount(), metadataDeltas[ledgerName])
		b.Boundaries().Put(domain.LedgerKey{Name: ledgerName}, boundaries)
	}
}
