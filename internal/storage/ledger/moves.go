package ledger

import (
	"context"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/formancehq/ledger/pkg/features"

	"github.com/formancehq/go-libs/v2/platform/postgres"
	"github.com/formancehq/go-libs/v2/time"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/tracing"
	"github.com/uptrace/bun"
)

func (s *Store) SortMovesBySeq(date *time.Time) (*bun.SelectQuery, error) {

	ret := s.db.NewSelect()
	if !s.ledger.HasFeature(features.FeatureMovesHistory, "ON") {
		return nil, ledgercontroller.NewErrMissingFeature(features.FeatureMovesHistory)
	}

	ret = ret.
		ModelTableExpr(s.GetPrefixedRelationName("moves")).
		Where("ledger = ?", s.ledger.Name).
		Order("seq desc")

	if date != nil && !date.IsZero() {
		ret = ret.Where("insertion_date <= ?", date)
	}

	return ret, nil
}

func (s *Store) SelectDistinctMovesBySeq(date *time.Time) (*bun.SelectQuery, error) {
	sortMovesBySeq, err := s.SortMovesBySeq(date)
	if err != nil {
		return nil, err
	}
	ret := s.db.NewSelect().
		TableExpr("(?) moves", sortMovesBySeq).
		DistinctOn("accounts_address, asset").
		Column("accounts_address", "asset").
		ColumnExpr("first_value(post_commit_volumes) over (partition by (accounts_address, asset) order by seq desc) as post_commit_volumes").
		Where("ledger = ?", s.ledger.Name)

	if date != nil && !date.IsZero() {
		ret = ret.Where("insertion_date <= ?", date)
	}

	return ret, nil
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

func (s *Store) InsertMoves(ctx context.Context, moves ...*ledger.Move) error {
	_, err := tracing.TraceWithMetric(
		ctx,
		"InsertMoves",
		s.tracer,
		s.insertMovesHistogram,
		tracing.NoResult(func(ctx context.Context) error {
			_, err := s.db.NewInsert().
				Model(&moves).
				Value("ledger", "?", s.ledger.Name).
				ModelTableExpr(s.GetPrefixedRelationName("moves")).
				Returning("post_commit_volumes, post_commit_effective_volumes").
				Exec(ctx)

			return postgres.ResolveError(err)
		}),
	)

	return err
}
