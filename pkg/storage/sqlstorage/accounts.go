package sqlstorage

import (
	"context"
	"github.com/sirupsen/logrus"
	"math"

	"github.com/huandu/go-sqlbuilder"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger/query"
)

func (s *Store) FindAccounts(ctx context.Context, q query.Query) (query.Cursor, error) {
	// We fetch an additional account to know if we have more documents
	q.Limit = int(math.Max(-1, math.Min(float64(q.Limit), 100))) + 1

	c := query.Cursor{}
	results := make([]core.Account, 0)

	sb := sqlbuilder.NewSelectBuilder()
	sb.
		Select("address").
		From(s.table("addresses")).
		GroupBy("address").
		OrderBy("address desc").
		Limit(q.Limit)

	if q.After != "" {
		sb.Where(sb.LessThan("address", q.After))
	}

	sqlq, args := sb.BuildWithFlavor(s.flavor)
	logrus.Debugln(sqlq, args)

	rows, err := s.db.QueryContext(
		ctx,
		sqlq,
		args...,
	)

	if err != nil {
		return c, s.error(err)
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

		meta, err := s.GetMeta(ctx, "account", account.Address)
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

	total, _ := s.CountAccounts(ctx)
	c.Total = total

	return c, nil
}
