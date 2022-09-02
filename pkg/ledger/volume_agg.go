package ledger

import (
	"context"

	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/storage"
)

type transactionVolumeAggregator struct {
	agg               *volumeAggregator
	postCommitVolumes core.AccountsAssetsVolumes
	preCommitVolumes  core.AccountsAssetsVolumes
	previousTx        *transactionVolumeAggregator
}

func (tva *transactionVolumeAggregator) findInPreviousTxs(addr, asset string) *core.Volumes {
	current := tva.previousTx
	for current != nil {
		if v, ok := current.postCommitVolumes[addr][asset]; ok {
			return &v
		}
		current = current.previousTx
	}
	return nil
}

func (tva *transactionVolumeAggregator) transfer(
	ctx context.Context,
	from, to, asset string,
	amount *core.MonetaryInt,
) error {
	for _, addr := range []string{from, to} {
		if !tva.preCommitVolumes.HasAccountAndAsset(addr, asset) {
			previousVolumes := tva.findInPreviousTxs(addr, asset)
			if previousVolumes != nil {
				tva.preCommitVolumes.SetVolumes(addr, asset, *previousVolumes)
			} else {
				volumesFromStore, err := tva.agg.store.GetVolumes(ctx, addr, asset)
				if err != nil {
					return err
				}
				tva.preCommitVolumes.SetVolumes(addr, asset, volumesFromStore)
			}
		}
		if !tva.postCommitVolumes.HasAccountAndAsset(addr, asset) {
			tva.postCommitVolumes.SetVolumes(addr, asset, tva.preCommitVolumes.GetVolumes(addr, asset))
		}
	}
	tva.postCommitVolumes.AddOutput(from, asset, amount)
	tva.postCommitVolumes.AddInput(to, asset, amount)

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

func newVolumeAggregator(store storage.Store) *volumeAggregator {
	return &volumeAggregator{
		store: store,
	}
}
