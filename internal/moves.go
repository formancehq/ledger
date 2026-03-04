package ledger

import (
	"slices"

	"github.com/formancehq/go-libs/v3/bun/bunpaginate"
	"github.com/formancehq/go-libs/v3/collectionutils"
	"github.com/formancehq/go-libs/v3/time"
	"github.com/uptrace/bun"
)

type Move struct {
	bun.BaseModel `bun:"table:moves"`

	TransactionID              uint64                 `bun:"transactions_id,type:bigint"`
	IsSource                   bool                `bun:"is_source,type:bool"`
	Account                    string              `bun:"accounts_address,type:varchar"`
	Amount                     *bunpaginate.BigInt `bun:"amount,type:numeric"`
	Asset                      string              `bun:"asset,type:varchar"`
	InsertionDate              time.Time           `bun:"insertion_date,type:timestamp,nullzero"`
	EffectiveDate              time.Time           `bun:"effective_date,type:timestamp,nullzero"`
	PostCommitVolumes          *Volumes            `bun:"post_commit_volumes,type:jsonb"`
	PostCommitEffectiveVolumes *Volumes            `bun:"post_commit_effective_volumes,type:jsonb,scanonly"`
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
