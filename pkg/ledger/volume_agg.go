package ledger

import (
	"context"

	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/storage"
)

type transaction struct {
	PostCommitVolumes core.AccountsVolumes
	PreCommitVolumes  core.AccountsVolumes

	aggregator *volumeAggregator
	previousTx *transaction
}

func (a *transaction) transfer(ctx context.Context, source, destination, asset string, amount uint64) error {
	if _, ok := a.PreCommitVolumes[source]; !ok {
		a.PreCommitVolumes[source] = core.AssetsVolumes{}
	}

	if _, ok := a.PreCommitVolumes[destination]; !ok {
		a.PreCommitVolumes[destination] = core.AssetsVolumes{}
	}

	if _, ok := a.PostCommitVolumes[source]; !ok {
		a.PostCommitVolumes[source] = core.AssetsVolumes{}
	}

	if _, ok := a.PostCommitVolumes[destination]; !ok {
		a.PostCommitVolumes[destination] = core.AssetsVolumes{}
	}

	if _, ok := a.PreCommitVolumes[source][asset]; !ok {
		found := false
		currentTx := a.previousTx
		for currentTx != nil {
			if v, ok := currentTx.PostCommitVolumes[source][asset]; ok {
				a.PreCommitVolumes[source][asset] = v
				found = true
				break
			}
			currentTx = currentTx.previousTx
		}

		if !found {
			v, err := a.aggregator.store.GetAccountAssetVolumes(ctx, source, asset)
			if err != nil {
				return err
			}
			a.PreCommitVolumes[source][asset] = v
		}
	}

	if _, ok := a.PostCommitVolumes[source][asset]; !ok {
		a.PostCommitVolumes[source][asset] = a.PreCommitVolumes[source][asset]
	}

	a.PostCommitVolumes[source][asset] = core.Volumes{
		Input:  a.PostCommitVolumes[source][asset].Input,
		Output: a.PostCommitVolumes[source][asset].Output + int64(amount),
	}

	if _, ok := a.PreCommitVolumes[destination][asset]; !ok {
		found := false
		currentTx := a.previousTx
		for currentTx != nil {
			if v, ok := currentTx.PostCommitVolumes[destination][asset]; ok {
				a.PreCommitVolumes[destination][asset] = v
				found = true
				break
			}
			currentTx = currentTx.previousTx
		}

		if !found {
			v, err := a.aggregator.store.GetAccountAssetVolumes(ctx, destination, asset)
			if err != nil {
				return err
			}
			a.PreCommitVolumes[destination][asset] = v
		}
	}

	if _, ok := a.PostCommitVolumes[destination][asset]; !ok {
		a.PostCommitVolumes[destination][asset] = a.PreCommitVolumes[destination][asset]
	}

	a.PostCommitVolumes[destination][asset] = core.Volumes{
		Input:  a.PostCommitVolumes[destination][asset].Input + int64(amount),
		Output: a.PostCommitVolumes[destination][asset].Output,
	}

	return nil
}

type volumeAggregator struct {
	store storage.Store
	txs   []*transaction
}

func (a *volumeAggregator) nextTx() *transaction {
	var previousTx *transaction
	if len(a.txs) > 0 {
		previousTx = a.txs[len(a.txs)-1]
	}
	tx := &transaction{
		PreCommitVolumes:  core.AccountsVolumes{},
		PostCommitVolumes: core.AccountsVolumes{},
		aggregator:        a,
		previousTx:        previousTx,
	}
	a.txs = append(a.txs, tx)
	return tx
}

func (a *volumeAggregator) aggregatedPostCommitVolumes() core.AccountsVolumes {
	ret := core.AccountsVolumes{}
	for i := len(a.txs) - 1; i >= 0; i-- {
		for account, assetsVolumes := range a.txs[i].PostCommitVolumes {
			for asset, volumes := range assetsVolumes {
				if _, ok := ret[account]; !ok {
					ret[account] = core.AssetsVolumes{}
				}
				if _, ok := ret[account][asset]; !ok {
					ret[account][asset] = volumes
				}
			}
		}
	}
	return ret
}

func (a *volumeAggregator) aggregatedPreCommitVolumes() core.AccountsVolumes {
	ret := core.AccountsVolumes{}
	for _, tx := range a.txs {
		for account, assetsVolumes := range tx.PreCommitVolumes {
			for asset, volumes := range assetsVolumes {
				if _, ok := ret[account]; !ok {
					ret[account] = core.AssetsVolumes{}
				}
				if _, ok := ret[account][asset]; !ok {
					ret[account][asset] = volumes
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
