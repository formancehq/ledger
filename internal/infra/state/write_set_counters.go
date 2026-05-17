package state

import (
	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/infra/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
)

// countKeyDeltas counts per-ledger new keys (+1) and deletions (-1) from merge results.
// A new key is identified by Old not being defined (first time in the parent KeyStore).
func countKeyDeltas[K attributes.Key, T any](
	updates []attributes.Update[K, T],
	deletions []attributes.Deletion[K],
	getLedger func(K) string,
) map[string]int64 {
	deltas := make(map[string]int64)

	for i := range updates {
		if !updates[i].Old.IsDefined() {
			deltas[getLedger(updates[i].Key)]++
		}
	}

	for i := range deletions {
		deltas[getLedger(deletions[i].Key)]--
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
			deltas[updates[i].Key.Ledger]++
		}
	}

	return deltas
}

// updateBoundaryCounters computes attribute key deltas and updates LedgerBoundaries
// for each affected ledger. Must be called before Derived.Boundaries.Merge().
func (b *WriteSet) updateBoundaryCounters(
	volumeUpdates []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair],
	purgedVolumes []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair],
	transientVolumes []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair],
	metadataUpdates []attributes.Update[domain.MetadataKey, *commonpb.MetadataValue],
	metadataDeletions []attributes.Deletion[domain.MetadataKey],
	referenceUpdates []attributes.Update[domain.TransactionReferenceKey, *commonpb.TransactionReferenceValue],
) {
	volumeDeltas := countVolumeDeltas(volumeUpdates)

	// Per-ledger ephemeral/transient counters (monotonic).
	ephemeralEvicted := make(map[string]uint64)
	transientUsed := make(map[string]uint64)

	// Ephemeral volumes are purged after commit — subtract from volume count.
	for i := range purgedVolumes {
		ledger := purgedVolumes[i].Key.Ledger
		volumeDeltas[ledger]--
		ephemeralEvicted[ledger]++
	}
	// Transient volumes are never persisted — subtract from volume count.
	for i := range transientVolumes {
		ledger := transientVolumes[i].Key.Ledger
		volumeDeltas[ledger]--
		transientUsed[ledger]++
	}

	metadataDeltas := countKeyDeltas(metadataUpdates, metadataDeletions, func(k domain.MetadataKey) string { return k.Ledger })
	referenceDeltas := countKeyDeltas(referenceUpdates, nil, func(k domain.TransactionReferenceKey) string { return k.Ledger })

	// Collect all affected ledgers.
	affected := make(map[string]struct{})
	for ledger := range volumeDeltas {
		affected[ledger] = struct{}{}
	}
	for ledger := range metadataDeltas {
		affected[ledger] = struct{}{}
	}
	for ledger := range referenceDeltas {
		affected[ledger] = struct{}{}
	}
	for ledger := range ephemeralEvicted {
		affected[ledger] = struct{}{}
	}
	for ledger := range transientUsed {
		affected[ledger] = struct{}{}
	}

	for ledger := range affected {
		boundaries, ok := b.GetBoundaries(ledger)
		if !ok {
			continue
		}

		boundaries = boundaries.CloneVT()
		boundaries.VolumeCount = applyDelta(boundaries.GetVolumeCount(), volumeDeltas[ledger])
		boundaries.MetadataCount = applyDelta(boundaries.GetMetadataCount(), metadataDeltas[ledger])
		boundaries.ReferenceCount = applyDelta(boundaries.GetReferenceCount(), referenceDeltas[ledger])
		boundaries.EphemeralEvictedCount += ephemeralEvicted[ledger]
		boundaries.TransientUsedCount += transientUsed[ledger]
		b.PutBoundaries(ledger, boundaries)
	}
}
