package ledger

import (
	"context"

	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/storage"
)

type transactionVolumeAggregator struct {
	agg         *volumeAggregator
	postVolumes core.AggregatedVolumes
	preVolumes  core.AggregatedVolumes
	previousTx  *transactionVolumeAggregator
}

func (tva *transactionVolumeAggregator) postCommitVolumes() core.AggregatedVolumes {
	return tva.postVolumes
}

func (tva *transactionVolumeAggregator) preCommitVolumes() core.AggregatedVolumes {
	return tva.preVolumes
}

func (tva *transactionVolumeAggregator) transfer(ctx context.Context, from, to, asset string, amount uint64) error {
	if tva.preVolumes == nil {
		tva.preVolumes = core.AggregatedVolumes{}
	}
	if tva.postVolumes == nil {
		tva.postVolumes = core.AggregatedVolumes{}
	}
	for _, addr := range []string{from, to} {
		if _, ok := tva.preVolumes[addr][asset]; !ok {
			current := tva.previousTx
			found := false
			if _, ok := tva.preVolumes[addr]; !ok {
				tva.preVolumes[addr] = core.Volumes{}
			}
			for current != nil {
				if v, ok := current.postVolumes[addr][asset]; ok {
					tva.preVolumes[addr][asset] = v
					found = true
					break
				}
				current = current.previousTx
			}
			if !found {
				v, err := tva.agg.store.GetAccountVolume(ctx, addr, asset)
				if err != nil {
					return err
				}
				tva.preVolumes[addr][asset] = v
			}
		}
		if _, ok := tva.postVolumes[addr][asset]; !ok {
			if _, ok := tva.postVolumes[addr]; !ok {
				tva.postVolumes[addr] = core.Volumes{}
			}
			tva.postVolumes[addr][asset] = tva.preVolumes[addr][asset]
		}
	}
	v := tva.postVolumes[from][asset]
	v.Output += int64(amount)
	tva.postVolumes[from][asset] = v

	v = tva.postVolumes[to][asset]
	v.Input += int64(amount)
	tva.postVolumes[to][asset] = v

	return nil
}

type volumeAggregator struct {
	store storage.Store
	txs   []*transactionVolumeAggregator
}

func (agg *volumeAggregator) nextTx() *transactionVolumeAggregator {
	var previousTx *transactionVolumeAggregator
	if len(agg.txs) > 0 {
		previousTx = agg.txs[len(agg.txs)-1]
	}
	tva := &transactionVolumeAggregator{
		agg:        agg,
		previousTx: previousTx,
	}
	agg.txs = append(agg.txs, tva)
	return tva
}

func (agg *volumeAggregator) aggregatedPostCommitVolumes() core.AggregatedVolumes {
	ret := core.AggregatedVolumes{}
	for i := len(agg.txs) - 1; i >= 0; i-- {
		tx := agg.txs[i]
		postVolumes := tx.postCommitVolumes()
		for account, volumes := range postVolumes {
			for asset, volume := range volumes {
				if _, ok := ret[account]; !ok {
					ret[account] = core.Volumes{}
				}
				if _, ok := ret[account][asset]; !ok {
					ret[account][asset] = volume
				}
			}
		}
	}
	return ret
}

func (agg *volumeAggregator) aggregatedPreCommitVolumes() core.AggregatedVolumes {
	ret := core.AggregatedVolumes{}
	for i := 0; i < len(agg.txs); i++ {
		tx := agg.txs[i]
		preVolumes := tx.preCommitVolumes()
		for account, volumes := range preVolumes {
			for asset, volume := range volumes {
				if _, ok := ret[account]; !ok {
					ret[account] = core.Volumes{}
				}
				if _, ok := ret[account][asset]; !ok {
					ret[account][asset] = volume
				}
			}
		}
	}
	return ret
}

func newVolumeAggregator(store storage.Store) *volumeAggregator {
	return &volumeAggregator{
		store: store,
	}
}
