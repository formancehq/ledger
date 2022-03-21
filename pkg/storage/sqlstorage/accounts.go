package sqlstorage

import (
	"context"
	"github.com/numary/go-libs/sharedapi"
	"math"

	"github.com/huandu/go-sqlbuilder"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger/query"
)

func (s *Store) findAccounts(ctx context.Context, exec executor, q query.Query) (sharedapi.Cursor, error) {
	// We fetch an additional account to know if we have more documents
	q.Limit = int(math.Max(-1, math.Min(float64(q.Limit), 100))) + 1

	c := sharedapi.Cursor{}
	results := make([]core.Account, 0)

	sb := sqlbuilder.NewSelectBuilder()
	sb.
		Select("address").
		From(s.schema.Table("addresses")).
		GroupBy("address").
		OrderBy("address desc").
		Limit(q.Limit)

	if q.After != "" {
		sb.Where(sb.LessThan("address", q.After))
	}

	sqlq, args := sb.BuildWithFlavor(s.schema.Flavor())

	rows, err := exec.QueryContext(ctx, sqlq, args...)
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
			Address: address,
		}

		meta, err := s.getMeta(ctx, exec, "account", account.Address)
		if err != nil {
			return c, err
		}
		account.Metadata = meta

		results = append(results, account)
	}
	if rows.Err() != nil {
		return sharedapi.Cursor{}, s.error(rows.Err())
	}

	c.PageSize = q.Limit - 1

	c.HasMore = len(results) == q.Limit
	if c.HasMore {
		results = results[:len(results)-1]
	}
	c.Data = results

	total, _ := s.countAccounts(ctx, exec)
	c.Total = total

	return c, nil
}

func (s *Store) FindAccounts(ctx context.Context, q query.Query) (sharedapi.Cursor, error) {
	return s.findAccounts(ctx, s.schema, q)
}
