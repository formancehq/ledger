package sqlite

import (
	"fmt"
	"math"

	"github.com/huandu/go-sqlbuilder"
	"github.com/numary/ledger/core"
	"github.com/numary/ledger/ledger/query"
	"github.com/spf13/viper"
)

func (s *SQLiteStore) FindAccounts(q query.Query) (query.Cursor, error) {
	q.Limit = int(math.Max(-1, math.Min(float64(q.Limit), 100))) + 1

	c := query.Cursor{}
	results := []core.Account{}

	sb := sqlbuilder.NewSelectBuilder()
	sb.
		Select("address").
		From("addresses").
		GroupBy("address").
		OrderBy("address desc").
		Limit(q.Limit)

	if q.After != "" {
		sb.Where(sb.LessThan("address", q.After))
	}

	sqlq, args := sb.BuildWithFlavor(sqlbuilder.SQLite)
	if viper.GetBool("debug") {
		fmt.Println(sqlq, args)
	}

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

		account := core.Account{
			Address:  address,
			Contract: "default",
		}

		meta, err := s.GetMeta("account", account.Address)
		if err != nil {
			return c, err
		}
		account.Metadata = meta

		results = append(results, account)
	}

	c.PageSize = q.Limit - 1

	c.HasMore = len(results) == q.Limit
	if c.HasMore {
		results = results[:len(results)-1]
	}
	c.Data = results

	total, _ := s.CountAccounts()
	c.Total = int(total)

	return c, nil
}
