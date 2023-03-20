package bus

import (
	"github.com/formancehq/ledger/pkg/core"
)

func aggregatePostCommitVolumes(txs ...core.ExpandedTransaction) core.AccountsAssetsVolumes {
	ret := core.AccountsAssetsVolumes{}
	for i := len(txs) - 1; i >= 0; i-- {
		tx := txs[i]
		for _, posting := range tx.Postings {
			if !ret.HasAccountAndAsset(posting.Source, posting.Asset) {
				ret.SetVolumes(posting.Source, posting.Asset,
					tx.PostCommitVolumes.GetVolumes(posting.Source, posting.Asset))
			}
			if !ret.HasAccountAndAsset(posting.Destination, posting.Asset) {
				ret.SetVolumes(posting.Destination, posting.Asset,
					tx.PostCommitVolumes.GetVolumes(posting.Destination, posting.Asset))
			}
		}
	}
	return ret
}
