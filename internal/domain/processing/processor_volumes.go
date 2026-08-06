package processing

import (
	"github.com/holiman/uint256"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// postCommitVolumeAccumulator keeps the latest mutable volume produced while
// postings are applied. It avoids reading the same coverage-gated cells again
// solely to build the immutable transaction snapshot. A later posting touching
// the same tuple replaces the pointer with that posting's newer cloned value.
type postCommitVolumeAccumulator struct {
	inline   [4]accumulatedPostCommitVolume
	overflow []accumulatedPostCommitVolume
	count    int
	indexes  map[domain.VolumeKey]int
}

type accumulatedPostCommitVolume struct {
	key    domain.VolumeKey
	volume *raftcmdpb.VolumePair
}

func (a *postCommitVolumeAccumulator) init(postingCount int) {
	expected := postingCount * 2
	if expected > len(a.inline) {
		a.overflow = make([]accumulatedPostCommitVolume, 0, expected-len(a.inline))
	}
}

func (a *postCommitVolumeAccumulator) capture(key domain.VolumeKey, volume *raftcmdpb.VolumePair) {
	if a == nil {
		return
	}

	if a.indexes != nil {
		if index, ok := a.indexes[key]; ok {
			a.at(index).volume = volume

			return
		}
	} else {
		for i := range a.count {
			entry := a.at(i)
			if entry.key == key {
				entry.volume = volume

				return
			}
		}

		if a.count == 8 {
			a.indexes = make(map[domain.VolumeKey]int, a.count+1)
			for i := range a.count {
				a.indexes[a.at(i).key] = i
			}
		}
	}

	entry := accumulatedPostCommitVolume{key: key, volume: volume}
	if a.count < len(a.inline) {
		a.inline[a.count] = entry
	} else {
		a.overflow = append(a.overflow, entry)
	}
	a.count++

	if a.indexes != nil {
		a.indexes[key] = a.count - 1
	}
}

func (a *postCommitVolumeAccumulator) at(index int) *accumulatedPostCommitVolume {
	if index < len(a.inline) {
		return &a.inline[index]
	}

	return &a.overflow[index-len(a.inline)]
}

func (a *postCommitVolumeAccumulator) build() *commonpb.PostCommitVolumes {
	rows := make([]*commonpb.PostCommitVolume, 0, a.count)
	var scratch uint256.Int

	for i := range a.count {
		accumulated := a.at(i)
		key := accumulated.key
		volume := accumulated.volume
		volume.GetInput().IntoUint256(&scratch)
		input := scratch.Dec()
		volume.GetOutput().IntoUint256(&scratch)
		output := scratch.Dec()

		rows = append(rows, &commonpb.PostCommitVolume{
			Account: key.Account,
			Asset:   key.Asset,
			Color:   key.Color,
			Input:   input,
			Output:  output,
		})
	}

	out := &commonpb.PostCommitVolumes{Volumes: rows}
	out.SortVolumes()

	return out
}
