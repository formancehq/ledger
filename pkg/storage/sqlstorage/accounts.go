package sqlstorage

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/davecgh/go-spew/spew"
	"github.com/huandu/go-sqlbuilder"
	"github.com/numary/go-libs/sharedapi"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger/query"
)

func (s *Store) accountsQuery(p map[string]interface{}) *sqlbuilder.SelectBuilder {
	sb := sqlbuilder.NewSelectBuilder()
	sb.From(s.schema.Table("accounts"))

	if metadata, ok := p["metadata"]; ok {
		for key, value := range metadata.(map[string]string) {
			arg := sb.Args.Add(value)
			// TODO: Need to find another way to specify the prefix since Table() methods does not make sense for functions and procedures
			sb.Where(s.schema.Table(
				fmt.Sprintf("%s(metadata, %s, '%s')",
					SQLCustomFuncMetaCompare, arg, strings.ReplaceAll(key, ".", "', '")),
			))
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

func (s *Store) getAccounts(ctx context.Context, exec executor, q query.Query) (sharedapi.Cursor, error) {
	accounts := make([]core.Account, 0)

	if q.Limit < 0 {
		return sharedapi.Cursor{Data: accounts}, nil
	}

	// We fetch an additional account to know if there is more
	q.Limit += 1

	spew.Dump(q)

	sb := s.accountsQuery(q.Params).
		Select("address", "metadata").
		OrderBy("address desc")
	if q.After != "" {
		sb.Where(sb.L("address", q.After))
	}
	sb.Limit(q.Limit)

	sqlq, args := sb.BuildWithFlavor(s.schema.Flavor())
	rows, err := exec.QueryContext(ctx, sqlq, args...)
	if err != nil {
		return sharedapi.Cursor{}, s.error(err)
	}
	defer func(rows *sql.Rows) {
		if err := rows.Close(); err != nil {
			panic(err)
		}
	}(rows)

	for rows.Next() {
		account := core.Account{}
		if err := rows.Scan(&account.Address, &account.Metadata); err != nil {
			return sharedapi.Cursor{}, err
		}

		accounts = append(accounts, account)
	}
	if rows.Err() != nil {
		return sharedapi.Cursor{}, rows.Err()
	}

	previous := ""
	if q.After != "" && len(accounts) > 0 {
		previous, err = tokenMarshal(PaginationToken{})
		if err != nil {
			return sharedapi.Cursor{}, s.error(err)
		}
	}

	next := ""
	if len(accounts) == q.Limit {
		accounts = accounts[:len(accounts)-1]
		next, err = tokenMarshal(PaginationToken{})
		if err != nil {
			return sharedapi.Cursor{}, s.error(err)
		}
	}

	return sharedapi.Cursor{
		PageSize: len(accounts),
		Previous: previous,
		Next:     next,
		Data:     accounts,
	}, nil
}

func (s *Store) GetAccounts(ctx context.Context, q query.Query) (sharedapi.Cursor, error) {
	return s.getAccounts(ctx, s.schema, q)
}

func (s *Store) getAccount(ctx context.Context, exec executor, addr string) (core.Account, error) {
	sb := sqlbuilder.NewSelectBuilder()
	sb.Select("address", "metadata").
		From(s.schema.Table("accounts")).
		Where(sb.Equal("address", addr))

	sqlq, args := sb.BuildWithFlavor(s.schema.Flavor())
	row := exec.QueryRowContext(ctx, sqlq, args...)
	if err := row.Err(); err != nil {
		return core.Account{}, err
	}

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
