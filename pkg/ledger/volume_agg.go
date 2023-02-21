package ledger

import (
	"context"

	"github.com/numary/ledger/pkg/core"
	"github.com/pkg/errors"
)

type TxVolumeAggregator struct {
	agg        *VolumeAggregator
	previousTx *TxVolumeAggregator

	PreCommitVolumes  core.AccountsAssetsVolumes
	PostCommitVolumes core.AccountsAssetsVolumes
}

func (tva *TxVolumeAggregator) FindInPreviousTxs(addr, asset string) *core.Volumes {
	current := tva.previousTx
	for current != nil {
		if v, ok := current.PostCommitVolumes[addr][asset]; ok {
			return &v
		}
		current = current.previousTx
	}
	return nil
}

func (tva *TxVolumeAggregator) Transfer(
	ctx context.Context,
	from, to, asset string,
	amount *core.MonetaryInt,
	accs map[string]*core.AccountWithVolumes,
) error {
	for _, addr := range []string{from, to} {
		if !tva.PreCommitVolumes.HasAccountAndAsset(addr, asset) {
			previousVolumes := tva.FindInPreviousTxs(addr, asset)
			if previousVolumes != nil {
				tva.PreCommitVolumes.SetVolumes(addr, asset, *previousVolumes)
			} else {
				var vol core.Volumes
				var ok1, ok2 bool
				_, ok1 = accs[addr]
				if ok1 {
					_, ok2 = accs[addr].Volumes[asset]
				}
				if ok1 && ok2 {
					vol = accs[addr].Volumes[asset]
				} else {
					acc, err := tva.agg.l.GetAccount(ctx, addr)
					if err != nil {
						return errors.Wrap(err, "getting account while transferring")
					}
					if accs[addr] == nil {
						accs[addr] = acc
					}
					accs[addr].Volumes[asset] = acc.Volumes[asset]
					vol = accs[addr].Volumes[asset]
				}
				tva.PreCommitVolumes.SetVolumes(addr, asset, vol)
			}
		}
		if !tva.PostCommitVolumes.HasAccountAndAsset(addr, asset) {
			tva.PostCommitVolumes.SetVolumes(addr, asset, tva.PreCommitVolumes.GetVolumes(addr, asset))
		}
	}
	tva.PostCommitVolumes.AddOutput(from, asset, amount)
	tva.PostCommitVolumes.AddInput(to, asset, amount)

	return nil
}

type VolumeAggregator struct {
	l   *Ledger
	txs []*TxVolumeAggregator
}

func (agg *VolumeAggregator) NextTx() *TxVolumeAggregator {
	var previousTx *TxVolumeAggregator
	if len(agg.txs) > 0 {
		previousTx = agg.txs[len(agg.txs)-1]
	}
	tva := &TxVolumeAggregator{
		agg:        agg,
		previousTx: previousTx,
	}
	agg.txs = append(agg.txs, tva)
	return tva
}

func NewVolumeAggregator(l *Ledger) *VolumeAggregator {
	return &VolumeAggregator{
		l: l,
	}
}
