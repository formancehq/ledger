package sqlstorage

import (
	"context"
	"database/sql"
	"github.com/huandu/go-sqlbuilder"
	"github.com/numary/go-libs/sharedapi"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger/query"
	"math"
	"strings"
)

func (s *Store) accountsQuery(p map[string]interface{}) *sqlbuilder.SelectBuilder {

	sb := sqlbuilder.NewSelectBuilder()
	sb.
		From(s.schema.Table("accounts"))

	if metadata, ok := p["metadata"]; ok {
		for k, metaValue := range metadata.(map[string]string) {
			arg := sb.Args.Add(metaValue)
			// TODO: Need to find another way to specify the prefix since Table() methods does not make sense for functions and procedures
			sb.Where(s.schema.Table("meta_compare(metadata, " + arg + ", '" + strings.Join(strings.Split(k, "."), "', '") + "')"))
		}
	}
	if address, ok := p["address"]; ok && address.(string) != "" {
		arg := sb.Args.Add("^" + address.(string) + "$")
		switch s.Schema().Flavor() {
		case sqlbuilder.PostgreSQL:
			sb.Where("address ~* " + arg)
		case sqlbuilder.SQLite:
			sb.Where("address REGEXP " + arg)
		}
	}

	return sb
}

func (s *Store) findAccounts(ctx context.Context, exec executor, q query.Query) (sharedapi.Cursor, error) {
	// We fetch an additional account to know if we have more documents
	q.Limit = int(math.Max(-1, math.Min(float64(q.Limit), 100))) + 1

	c := sharedapi.Cursor{}
	results := make([]core.Account, 0)

	sb := s.accountsQuery(q.Params).
		Select("address", "metadata").
		Limit(q.Limit).
		OrderBy("address desc")

	if q.After != "" {
		sb.Where(sb.LessThan("address", q.After))
	}

	sqlq, args := sb.BuildWithFlavor(s.schema.Flavor())

	rows, err := exec.QueryContext(ctx, sqlq, args...)
	if err != nil {
		return c, s.error(err)
	}
	defer rows.Close()

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

	total, _ := s.countAccounts(ctx, exec, q.Params)
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
