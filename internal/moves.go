package ledger

import (
	"math/big"
	"slices"

	"github.com/formancehq/go-libs/v3/collectionutils"
	"github.com/formancehq/go-libs/v3/time"
)

type Move struct {
	TransactionID              uint64    `json:"transactionID"`
	IsSource                   bool      `json:"isSource"`
	Account                    string    `json:"account"`
	Amount                     *big.Int  `json:"amount"`
	Asset                      string    `json:"asset"`
	InsertionDate              time.Time `json:"insertionDate"`
	EffectiveDate              time.Time `json:"effectiveDate"`
	PostCommitVolumes          *Volumes  `json:"postCommitVolumes"`
	PostCommitEffectiveVolumes *Volumes  `json:"postCommitEffectiveVolumes"`
}

type Moves []*Move

func (m Moves) ComputePostCommitEffectiveVolumes() PostCommitVolumes {
	type key struct {
		Account string
		Asset   string
	}

	visited := collectionutils.Set[key]{}

	// We need to find the more recent move for each account/asset.
	// We will iterate on moves by starting by the more recent.
	slices.Reverse(m)

	ret := PostCommitVolumes{}
	for _, move := range m {
		if visited.Contains(key{
			Account: move.Account,
			Asset:   move.Asset,
		}) {
			continue
		}
		ret = ret.Merge(PostCommitVolumes{
			move.Account: VolumesByAssets{
				move.Asset: *move.PostCommitEffectiveVolumes,
			},
		})
		visited.Put(key{
			Account: move.Account,
			Asset:   move.Asset,
		})
	}

	return ret
}
