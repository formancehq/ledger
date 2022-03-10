package sqlstorage

import (
	"context"
	"database/sql"
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
		Select("address", "metadata").
		From(s.schema.Table("accounts")).
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
		account := core.Account{}
		var (
			addr sql.NullString
			m    sql.NullString
		)
		err := rows.Scan(&addr, &m)
		if err != nil {
			return c, err
		}
		err = rows.Scan(&account.Address, &account.Metadata)
		if err != nil {
			return c, err
		}
		results = append(results, account)
	}
	if rows.Err() != nil {
		return c, rows.Err()
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

func (s *Store) FindAccounts(ctx context.Context, q query.Query) (sharedapi.Cursor, error) {
	return s.findAccounts(ctx, s.schema, q)
}

func (s *Store) getAccount(ctx context.Context, exec executor, addr string) (core.Account, error) {

	sb := sqlbuilder.NewSelectBuilder()
	sb.
		Select("address", "metadata").
		From(s.schema.Table("accounts")).
		Where(sb.Equal("address", addr))

	sqlq, args := sb.BuildWithFlavor(s.schema.Flavor())
	row := exec.QueryRowContext(ctx, sqlq, args...)

	account := core.Account{}
	err := row.Scan(&account.Address, &account.Metadata)
	if err != nil {
		if err == sql.ErrNoRows {
			return core.Account{}, nil
		}
		return core.Account{}, err
	}

	return account, nil
}

func (s *Store) GetAccount(ctx context.Context, addr string) (core.Account, error) {
	return s.getAccount(ctx, s.schema, addr)
}
