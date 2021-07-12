package sqlite

import (
	"fmt"

	"github.com/huandu/go-sqlbuilder"
	"github.com/numary/ledger/core"
	"github.com/numary/ledger/ledger/query"
)

func (s *SQLiteStore) FindAccounts(q query.Query) (query.Cursor, error) {
	c := query.Cursor{}
	results := []core.Account{}

	queryRem := sqlbuilder.Select("count(*)")
	queryRem.From("addresses")

	if q.After != "" {
		queryRem.Where(queryRem.LessThan("address", q.After))
	}

	sqlRem, args := queryRem.BuildWithFlavor(sqlbuilder.SQLite)

	var remaining int

	err := s.db.QueryRow(
		sqlRem,
		args...,
	).Scan(&remaining)

	if err != nil {
		return c, err
	}

	sb := sqlbuilder.NewSelectBuilder()
	sb.
		Select("address").
		From("addresses").
		Where(sb.Equal("ledger", s.ledger)).
		GroupBy("address").
		OrderBy("address desc").
		Limit(q.Limit)

	if q.After != "" {
		sb.Where(sb.LessThan("address", q.After))
	}

	sqlq, args := sb.BuildWithFlavor(sqlbuilder.SQLite)

	fmt.Println(sqlq, args)

	rows, err := s.db.Query(
		sqlq,
		args...,
	)

	if err != nil {
		return c, err
	}

	for rows.Next() {
		var address string

		err := rows.Scan(&address)

		if err != nil {
			return c, err
		}

		results = append(results, core.Account{
			Address:  address,
			Contract: "default",
		})
	}

	total, _ := s.CountAccounts()

	c.PageSize = q.Limit
	c.HasMore = len(results) < remaining
	c.Remaining = remaining - len(results)
	c.Total = int(total)
	c.Data = results

	return c, nil
}
