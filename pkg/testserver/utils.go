package testserver

import (
	"github.com/formancehq/go-libs/v2/collectionutils"
	"github.com/formancehq/go-libs/v2/pointer"
	"github.com/formancehq/go-libs/v2/time"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/pkg/client/models/components"
)

func ConvertSDKTxToCoreTX(tx *components.V2Transaction) ledger.Transaction {
	return ledger.Transaction{
		TransactionData: ledger.TransactionData{
			Postings:   collectionutils.Map(tx.Postings, ConvertSDKPostingToCorePosting),
			Timestamp:  time.New(tx.Timestamp),
			InsertedAt: time.New(*tx.InsertedAt),
			Metadata:   tx.Metadata,
			Reference: func() string {
				if tx.Reference == nil {
					return ""
				}
				return *tx.Reference
			}(),
		},
		ID:                         pointer.For(int(tx.ID.Int64())),
		PostCommitVolumes:          ConvertSDKPostCommitVolumesToCorePostCommitVolumes(tx.PostCommitVolumes),
		PostCommitEffectiveVolumes: ConvertSDKPostCommitVolumesToCorePostCommitVolumes(tx.PostCommitEffectiveVolumes),
	}
}

func ConvertSDKPostCommitVolumesToCorePostCommitVolumes(volumes map[string]map[string]components.V2Volume) ledger.PostCommitVolumes {
	ret := ledger.PostCommitVolumes{}
	for account, volumesByAsset := range volumes {
		for asset, volumes := range volumesByAsset {
			ret.Merge(ledger.PostCommitVolumes{
				account: {
					asset: ledger.Volumes{
						Input:  volumes.Input,
						Output: volumes.Output,
					},
				},
			})
		}
	}
	return ret
}

func ConvertSDKPostingToCorePosting(p components.V2Posting) ledger.Posting {
	return ledger.Posting{
		Source:      p.Source,
		Destination: p.Destination,
		Asset:       p.Asset,
		Amount:      p.Amount,
	}
}
