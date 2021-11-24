package sqlite

import (
	"github.com/huandu/go-sqlbuilder"
)

func (s *SQLiteStore) CountTransactions() (int64, error) {
	var count int64

	sb := sqlbuilder.NewSelectBuilder()
	sb.Select("count(*)")
	sb.From("transactions")

	sqlq, args := sb.Build()

	err := s.db.QueryRow(sqlq, args...).Scan(&count)

	return count, err
}

func (s *SQLiteStore) CountAccounts() (int64, error) {
	var count int64

	sb := sqlbuilder.NewSelectBuilder()

	sb.
		Select("count(*)").
		From("addresses").
		BuildWithFlavor(sqlbuilder.SQLite)

	sqlq, args := sb.Build()

	err := s.db.QueryRow(sqlq, args...).Scan(&count)

	return count, err
}

func (s *SQLiteStore) CountMeta() (int64, error) {
	var count int64

	sb := sqlbuilder.NewSelectBuilder()

	sb.
		Select("count(*)").
		From("metadata")

	sqlq, args := sb.BuildWithFlavor(sqlbuilder.SQLite)

	q := s.db.QueryRow(sqlq, args...)
	err := q.Scan(&count)

	return count, err
}

func (s *SQLiteStore) AggregateBalances(address string) (map[string]int64, error) {
	balances := map[string]int64{}

	volumes, err := s.AggregateVolumes(address)

	if err != nil {
		return balances, err
	}

	for asset := range volumes {
		balances[asset] = volumes[asset]["input"] - volumes[asset]["output"]
	}

	return balances, nil
}

func (s *SQLiteStore) AggregateVolumes(address string) (map[string]map[string]int64, error) {
	volumes := map[string]map[string]int64{}

	agg1 := sqlbuilder.NewSelectBuilder()
	agg1.
		Select("asset", "'_out'", "sum(amount)").
		From("postings").Where(agg1.Equal("source", address)).
		GroupBy("asset")

	agg2 := sqlbuilder.NewSelectBuilder()
	agg2.
		Select("asset", "'_in'", "sum(amount)").
		From("postings").Where(agg2.Equal("destination", address)).
		GroupBy("asset")

	union := sqlbuilder.Union(agg1, agg2)

	sb := sqlbuilder.NewSelectBuilder()
	sb.Select("*")
	sb.From(sb.BuilderAs(union, "assets"))

	sqlq, args := sb.BuildWithFlavor(sqlbuilder.SQLite)

	rows, err := s.db.Query(sqlq, args...)

	if err != nil {
		return volumes, err
	}

	for rows.Next() {
		var row = struct {
			asset  string
			t      string
			amount int64
		}{}

		err := rows.Scan(&row.asset, &row.t, &row.amount)

		if err != nil {
			return volumes, err
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

	return volumes, nil
}
