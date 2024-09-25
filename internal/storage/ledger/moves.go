package ledger

import (
	"context"
	ledger "github.com/formancehq/ledger/internal"
	"math/big"

	"github.com/formancehq/go-libs/bun/bunpaginate"
	"github.com/formancehq/go-libs/platform/postgres"
	"github.com/formancehq/go-libs/time"
	"github.com/formancehq/ledger/internal/tracing"
	"github.com/uptrace/bun"
)

func (s *Store) SortMovesBySeq(date *time.Time) *bun.SelectQuery {
	ret := s.db.NewSelect().
		ModelTableExpr(s.GetPrefixedRelationName("moves")).
		Where("ledger = ?", s.ledger.Name).
		Order("seq desc")

	if date != nil && !date.IsZero() {
		ret = ret.Where("insertion_date <= ?", date)
	}

	return ret
}

func (s *Store) SelectDistinctMovesBySeq(date *time.Time) *bun.SelectQuery {
	ret := s.db.NewSelect().
		TableExpr("(?) moves", s.SortMovesBySeq(date)).
		DistinctOn("accounts_seq, account_address, asset").
		Column("accounts_seq", "account_address", "asset").
		ColumnExpr("first_value(account_address_array) over (partition by (accounts_seq, account_address, asset) order by seq desc) as account_address_array").
		ColumnExpr("first_value(post_commit_volumes) over (partition by (accounts_seq, account_address, asset) order by seq desc) as post_commit_volumes").
		Where("ledger = ?", s.ledger.Name)

	if date != nil && !date.IsZero() {
		ret = ret.Where("insertion_date <= ?", date)
	}

	return ret
}

func (s *Store) SelectDistinctMovesByEffectiveDate(date *time.Time) *bun.SelectQuery {
	ret := s.db.NewSelect().
		TableExpr(s.GetPrefixedRelationName("moves")).
		DistinctOn("accounts_seq, asset").
		Column("accounts_seq", "asset").
		ColumnExpr("first_value(account_address) over (partition by (accounts_seq, account_address, asset) order by effective_date desc, seq desc) as account_address").
		ColumnExpr("first_value(account_address_array) over (partition by (accounts_seq, account_address, asset) order by effective_date desc, seq desc) as account_address_array").
		ColumnExpr("first_value(post_commit_effective_volumes) over (partition by (accounts_seq, account_address, asset) order by effective_date desc, seq desc) as post_commit_effective_volumes").
		Where("ledger = ?", s.ledger.Name)

	if date != nil && !date.IsZero() {
		ret = ret.Where("effective_date <= ?", date)
	}

	return ret
}

func (s *Store) insertMoves(ctx context.Context, moves ...*Move) error {
	_, err := tracing.TraceWithLatency(ctx, "InsertMoves", tracing.NoResult(func(ctx context.Context) error {
		_, err := s.db.NewInsert().
			Model(&moves).
			ModelTableExpr(s.GetPrefixedRelationName("moves")).
			Returning("to_json(post_commit_volumes) as post_commit_volumes, to_json(post_commit_effective_volumes) as post_commit_effective_volumes").
			Exec(ctx)

		return postgres.ResolveError(err)
	}))

	return err
}

type Move struct {
	bun.BaseModel `bun:"table:moves"`

	Ledger                     string              `bun:"ledger,type:varchar"`
	IsSource                   bool                `bun:"is_source,type:bool"`
	Account                    string              `bun:"account_address,type:varchar"`
	AccountAddressArray        []string            `bun:"account_address_array,type:jsonb"`
	Amount                     *bunpaginate.BigInt `bun:"amount,type:numeric"`
	Asset                      string              `bun:"asset,type:varchar"`
	TransactionSeq             int                 `bun:"transactions_seq,type:int"`
	AccountSeq                 int                 `bun:"accounts_seq,type:int"`
	InsertionDate              time.Time           `bun:"insertion_date,type:timestamp"`
	EffectiveDate              time.Time           `bun:"effective_date,type:timestamp"`
	PostCommitVolumes          *ledger.Volumes     `bun:"post_commit_volumes,type:jsonb,scanonly"`
	PostCommitEffectiveVolumes *ledger.Volumes     `bun:"post_commit_effective_volumes,type:jsonb,scanonly"`
}

type Moves []*Move

func (m Moves) volumeUpdates() []AccountsVolumes {

	aggregatedVolumes := make(map[string]map[string][]*Move)
	for _, move := range m {
		if _, ok := aggregatedVolumes[move.Account]; !ok {
			aggregatedVolumes[move.Account] = make(map[string][]*Move)
		}
		aggregatedVolumes[move.Account][move.Asset] = append(aggregatedVolumes[move.Account][move.Asset], move)
	}

	ret := make([]AccountsVolumes, 0)
	for account, movesByAsset := range aggregatedVolumes {
		for asset, moves := range movesByAsset {
			volumes := ledger.NewEmptyVolumes()
			for _, move := range moves {
				if move.IsSource {
					volumes.Output.Add(volumes.Output, (*big.Int)(move.Amount))
				} else {
					volumes.Input.Add(volumes.Input, (*big.Int)(move.Amount))
				}
			}
			ret = append(ret, AccountsVolumes{
				Ledger:      moves[0].Ledger,
				Account:     account,
				Asset:       asset,
				Input:       volumes.Input,
				Output:      volumes.Output,
				AccountsSeq: moves[0].AccountSeq,
			})
		}
	}

	return ret
}

func (m Moves) ComputePostCommitVolumes() TransactionsPostCommitVolumes {
	ret := TransactionsPostCommitVolumes{}
	for _, move := range m {
		ret = append(ret, TransactionPostCommitVolume{
			AggregatedAccountVolume: AggregatedAccountVolume{
				Volumes: *move.PostCommitVolumes,
				Asset:   move.Asset,
			},
			Account: move.Account,
		})
	}
	return ret
}

func (m Moves) ComputePostCommitEffectiveVolumes() TransactionsPostCommitVolumes {
	ret := TransactionsPostCommitVolumes{}
	for _, move := range m {
		ret = append(ret, TransactionPostCommitVolume{
			AggregatedAccountVolume: AggregatedAccountVolume{
				Volumes: *move.PostCommitEffectiveVolumes,
				Asset:   move.Asset,
			},
			Account: move.Account,
		})
	}
	return ret
}
