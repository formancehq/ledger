package ledger

import (
	"context"
	"slices"

	. "github.com/formancehq/go-libs/collectionutils"
	ledger "github.com/formancehq/ledger/internal"

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
		DistinctOn("accounts_address, asset").
		Column("accounts_address", "asset").
		ColumnExpr("first_value(post_commit_volumes) over (partition by (accounts_address, asset) order by seq desc) as post_commit_volumes").
		Where("ledger = ?", s.ledger.Name)

	if date != nil && !date.IsZero() {
		ret = ret.Where("insertion_date <= ?", date)
	}

	return ret
}

func (s *Store) SelectDistinctMovesByEffectiveDate(date *time.Time) *bun.SelectQuery {
	ret := s.db.NewSelect().
		TableExpr(s.GetPrefixedRelationName("moves")).
		DistinctOn("accounts_address, asset").
		Column("accounts_address", "asset").
		ColumnExpr("first_value(post_commit_effective_volumes) over (partition by (accounts_address, asset) order by effective_date desc, seq desc) as post_commit_effective_volumes").
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
			// todo: to_json required?
			Returning("to_json(post_commit_volumes) as post_commit_volumes, to_json(post_commit_effective_volumes) as post_commit_effective_volumes").
			Exec(ctx)

		return postgres.ResolveError(err)
	}))

	return err
}

type Move struct {
	bun.BaseModel `bun:"table:moves"`

	Ledger                     string              `bun:"ledger,type:varchar"`
	TransactionID              int                 `bun:"transactions_id,type:bigint"`
	IsSource                   bool                `bun:"is_source,type:bool"`
	Account                    string              `bun:"accounts_address,type:varchar"`
	Amount                     *bunpaginate.BigInt `bun:"amount,type:numeric"`
	Asset                      string              `bun:"asset,type:varchar"`
	InsertionDate              time.Time           `bun:"insertion_date,type:timestamp"`
	EffectiveDate              time.Time           `bun:"effective_date,type:timestamp"`
	PostCommitVolumes          *ledger.Volumes     `bun:"post_commit_volumes,type:jsonb"`
	PostCommitEffectiveVolumes *ledger.Volumes     `bun:"post_commit_effective_volumes,type:jsonb,scanonly"`
}

type Moves []*Move

func (m Moves) ComputePostCommitEffectiveVolumes() ledger.PostCommitVolumes {
	type key struct {
		Account string
		Asset   string
	}

	visited := Set[key]{}

	// we need to find the more recent move for each account/asset
	slices.Reverse(m)

	ret := ledger.PostCommitVolumes{}
	for _, move := range m {
		if visited.Contains(key{
			Account: move.Account,
			Asset:   move.Asset,
		}) {
			continue
		}
		ret = ret.Merge(ledger.PostCommitVolumes{
			move.Account: ledger.VolumesByAssets{
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
