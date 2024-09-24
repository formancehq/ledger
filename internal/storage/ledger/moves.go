package ledger

import (
	"context"
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
	PostCommitVolumes          Volumes             `bun:"post_commit_volumes,type:jsonb,scanonly"`
	PostCommitEffectiveVolumes Volumes             `bun:"post_commit_effective_volumes,type:jsonb,scanonly"`
}

type Moves []*Move

func (m Moves) BalanceUpdates() map[string]map[string]*big.Int {
	ret := make(map[string]map[string]*big.Int)
	for _, move := range m {
		if _, ok := ret[move.Account]; !ok {
			ret[move.Account] = make(map[string]*big.Int)
		}
		if _, ok := ret[move.Account][move.Asset]; !ok {
			ret[move.Account][move.Asset] = big.NewInt(0)
		}
		amount := big.NewInt(0).Set((*big.Int)(move.Amount))
		if move.IsSource {
			amount = big.NewInt(0).Neg(amount)
		}
		ret[move.Account][move.Asset] = ret[move.Account][move.Asset].Add(ret[move.Account][move.Asset], amount)
	}

	return ret
}
