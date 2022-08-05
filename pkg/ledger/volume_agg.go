package ledger

import (
	"context"

	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/storage"
)

type transactionVolumeAggregator struct {
	agg         *volumeAggregator
	postVolumes core.AccountsAssetsVolumes
	preVolumes  core.AccountsAssetsVolumes
	previousTx  *transactionVolumeAggregator
}

func (tva *transactionVolumeAggregator) postCommitVolumes() core.AccountsAssetsVolumes {
	return tva.postVolumes
}

func (tva *transactionVolumeAggregator) preCommitVolumes() core.AccountsAssetsVolumes {
	return tva.preVolumes
}

func (tva *transactionVolumeAggregator) transfer(
	ctx context.Context,
	from, to, asset string,
	amount *core.MonetaryInt,
) error {
	if tva.preVolumes == nil {
		tva.preVolumes = core.AccountsAssetsVolumes{}
	}
	if tva.postVolumes == nil {
		tva.postVolumes = core.AccountsAssetsVolumes{}
	}
	for _, addr := range []string{from, to} {
		if _, ok := tva.preVolumes[addr][asset]; !ok {
			current := tva.previousTx
			found := false
			if _, ok := tva.preVolumes[addr]; !ok {
				tva.preVolumes[addr] = core.AssetsVolumes{}
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
				v, err := tva.agg.store.GetVolumes(ctx, addr, asset)
				if err != nil {
					return err
				}
				tva.preVolumes[addr][asset] = v
			}
		}
		if _, ok := tva.postVolumes[addr][asset]; !ok {
			if _, ok := tva.postVolumes[addr]; !ok {
				tva.postVolumes[addr] = core.AssetsVolumes{}
			}
			tva.postVolumes[addr][asset] = tva.preVolumes[addr][asset]
		}
	}
	v := tva.postVolumes[from][asset]
	v.Output = v.Output.Add(amount)
	tva.postVolumes[from][asset] = v

	v = tva.postVolumes[to][asset]
	v.Input = v.Input.Add(amount)
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

func (agg *volumeAggregator) aggregatedPostCommitVolumes() core.AccountsAssetsVolumes {
	ret := core.AccountsAssetsVolumes{}
	for i := len(agg.txs) - 1; i >= 0; i-- {
		tx := agg.txs[i]
		postVolumes := tx.postCommitVolumes()
		for account, volumes := range postVolumes {
			for asset, volume := range volumes {
				if _, ok := ret[account]; !ok {
					ret[account] = core.AssetsVolumes{}
				}
				if _, ok := ret[account][asset]; !ok {
					ret[account][asset] = volume
				}
			}
		}
	}
	return ret
}

func (agg *volumeAggregator) aggregatedPreCommitVolumes() core.AccountsAssetsVolumes {
	ret := core.AccountsAssetsVolumes{}
	for i := 0; i < len(agg.txs); i++ {
		tx := agg.txs[i]
		preVolumes := tx.preCommitVolumes()
		for account, volumes := range preVolumes {
			for asset, volume := range volumes {
				if _, ok := ret[account]; !ok {
					ret[account] = core.AssetsVolumes{}
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
