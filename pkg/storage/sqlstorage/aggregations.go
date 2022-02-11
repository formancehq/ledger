package sqlstorage

import (
	"context"
	"github.com/huandu/go-sqlbuilder"
)

func (s *Store) countTransactions(ctx context.Context, exec executor) (int64, error) {
	var count int64

	sb := sqlbuilder.NewSelectBuilder()
	sb.Select("count(*)")
	sb.From(s.table("transactions"))

	sqlq, args := sb.Build()

	err := exec.QueryRowContext(ctx, sqlq, args...).Scan(&count)

	return count, s.error(err)
}

func (s *Store) CountTransactions(ctx context.Context) (int64, error) {
	return s.countTransactions(ctx, s.db)
}

func (s *Store) countAccounts(ctx context.Context, exec executor) (int64, error) {
	var count int64

	sb := sqlbuilder.NewSelectBuilder()
	sb.
		Select("count(*)").
		From(s.table("addresses")).
		BuildWithFlavor(s.flavor)

	sqlq, args := sb.Build()
	err := exec.QueryRowContext(ctx, sqlq, args...).Scan(&count)

	return count, s.error(err)
}

func (s *Store) CountAccounts(ctx context.Context) (int64, error) {
	return s.countAccounts(ctx, s.db)
}

func (s *Store) countMeta(ctx context.Context, exec executor) (int64, error) {
	var count int64

	sb := sqlbuilder.NewSelectBuilder()

	sb.
		Select("count(*)").
		From(s.table("metadata"))

	sqlq, args := sb.BuildWithFlavor(s.flavor)

	q := exec.QueryRowContext(ctx, sqlq, args...)
	err := q.Scan(&count)

	return count, s.error(err)
}

func (s *Store) CountMeta(ctx context.Context) (int64, error) {
	return s.countMeta(ctx, s.db)
}

func (s *Store) aggregateBalances(ctx context.Context, exec executor, address string) (map[string]int64, error) {
	balances := map[string]int64{}

	volumes, err := s.aggregateVolumes(ctx, exec, address)
	if err != nil {
		return balances, s.error(err)
	}

	for asset := range volumes {
		balances[asset] = volumes[asset]["input"] - volumes[asset]["output"]
	}

	return balances, nil
}

func (s *Store) AggregateBalances(ctx context.Context, address string) (map[string]int64, error) {
	return s.aggregateBalances(ctx, s.db, address)
}

func (s *Store) aggregateVolumes(ctx context.Context, exec executor, address string) (map[string]map[string]int64, error) {
	volumes := map[string]map[string]int64{}

	agg1 := sqlbuilder.NewSelectBuilder()
	agg1.
		Select("asset", "'_out'", "sum(amount)").
		From(s.table("postings")).Where(agg1.Equal("source", address)).
		GroupBy("asset")

	agg2 := sqlbuilder.NewSelectBuilder()
	agg2.
		Select("asset", "'_in'", "sum(amount)").
		From(s.table("postings")).Where(agg2.Equal("destination", address)).
		GroupBy("asset")

	union := sqlbuilder.Union(agg1, agg2)

	sb := sqlbuilder.NewSelectBuilder()
	sb.Select("*")
	sb.From(sb.BuilderAs(union, "assets"))

	sqlq, args := sb.BuildWithFlavor(s.flavor)
	rows, err := exec.QueryContext(ctx, sqlq, args...)

	if err != nil {
		return volumes, s.error(err)
	}

	for rows.Next() {
		var row = struct {
			asset  string
			t      string
			amount int64
		}{}

		err := rows.Scan(&row.asset, &row.t, &row.amount)

		if err != nil {
			return volumes, s.error(err)
		}

		if _, ok := volumes[row.asset]; !ok {
			volumes[row.asset] = map[string]int64{}
		}

		if row.t == "_out" {
			volumes[row.asset]["output"] += row.amount
		} else {
			volumes[row.asset]["input"] += row.amount
		}
	}
	if rows.Err() != nil {
		return nil, s.error(rows.Err())
	}

	return volumes, nil
}

func (s *Store) AggregateVolumes(ctx context.Context, address string) (map[string]map[string]int64, error) {
	return s.aggregateVolumes(ctx, s.db, address)
}
