package state

import (
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

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
// Only volume_count lives here now — reference_count, posting_count,
// revert_count, numscript_execution_count, ephemeral_evicted_count and
// transient_used_count all migrated to the usagebuilder (EN-1420) which
// derives them from the audit chain. metadata_count was dropped: the
// admission preload no longer injects the old value for metadata keys,
// so `Old.IsDefined()` no longer distinguishes "new key" from "overwrite"
// — the counter drifted from cardinality to write-event-count. It will
// come back on a sound foundation later.
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

	for ledgerName := range volumeDeltas {
		boundariesReader, err := b.Boundaries().Get(domain.LedgerKey{Name: ledgerName})
		if err != nil {
			continue
		}

		boundaries := boundariesReader.Mutate()
		boundaries.VolumeCount = applyDelta(boundaries.GetVolumeCount(), volumeDeltas[ledgerName])
		b.Boundaries().Put(domain.LedgerKey{Name: ledgerName}, boundaries)
	}
}
