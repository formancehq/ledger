package postgres

import (
	"context"

	"github.com/huandu/go-sqlbuilder"
)

func (s *PGStore) AggregateBalances(address string) (map[string]int64, error) {
	balances := map[string]int64{}

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

	sqlq, args := sb.BuildWithFlavor(sqlbuilder.PostgreSQL)

	rows, err := s.Conn().Query(
		context.TODO(),
		sqlq,
		args...,
	)

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
