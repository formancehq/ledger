package sqlite

import (
	"github.com/huandu/go-sqlbuilder"
)

func (s *SQLiteStore) CountTransactions() (int64, error) {
	var count int64

	sb := sqlbuilder.NewSelectBuilder()
	sb.Select("count(*)").From("transactions")
	sb.Where(sb.Equal("ledger", s.ledger))

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
		Where(sb.Equal("ledger", s.ledger)).
		BuildWithFlavor(sqlbuilder.SQLite)

	sqlq, args := sb.Build()

	err := s.db.QueryRow(sqlq, args...).Scan(&count)

	return count, err
}

func (s *SQLiteStore) AggregateBalances(address string) (map[string]int64, error) {
	balances := map[string]int64{}

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
		return balances, err
	}

	for rows.Next() {
		var row = struct {
			asset  string
			t      string
			amount int64
		}{}

		err := rows.Scan(&row.asset, &row.t, &row.amount)

		if err != nil {
			return balances, err
		}

		if row.t == "_out" {
			balances[row.asset] -= row.amount
		} else {
			balances[row.asset] += row.amount
		}
	}

	return balances, nil
}
