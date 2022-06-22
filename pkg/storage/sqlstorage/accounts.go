package sqlstorage

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/huandu/go-sqlbuilder"
	"github.com/numary/go-libs/sharedapi"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger/query"
)

func (s *Store) buildAccountsQuery(p map[string]interface{}) (*sqlbuilder.SelectBuilder, AccPaginationToken) {
	sb := sqlbuilder.NewSelectBuilder()
	t := AccPaginationToken{}
	sb.From(s.schema.Table("accounts"))

	if address, ok := p["address"]; ok && address.(string) != "" {
		arg := sb.Args.Add("^" + address.(string) + "$")
		switch s.Schema().Flavor() {
		case sqlbuilder.PostgreSQL:
			sb.Where("address ~* " + arg)
		case sqlbuilder.SQLite:
			sb.Where("address REGEXP " + arg)
		}
		t.AddressRegexpFilter = address.(string)
	}

	if metadata, ok := p["metadata"]; ok {
		for key, value := range metadata.(map[string]string) {
			arg := sb.Args.Add(value)
			// TODO: Need to find another way to specify the prefix since Table() methods does not make sense for functions and procedures
			sb.Where(s.schema.Table(
				fmt.Sprintf("%s(metadata, %s, '%s')",
					SQLCustomFuncMetaCompare, arg, strings.ReplaceAll(key, ".", "', '")),
			))
		}
		t.MetadataFilter = metadata.(map[string]string)
	}

	return sb, t
}

func (s *Store) getAccounts(ctx context.Context, exec executor, q query.Accounts) (sharedapi.Cursor[core.Account], error) {
	accounts := make([]core.Account, 0)

	if q.Limit == 0 {
		return sharedapi.Cursor[core.Account]{Data: accounts}, nil
	}

	sb, t := s.buildAccountsQuery(q.Params)
	sb.Select("address", "metadata")
	sb.OrderBy("address desc")

	if q.AfterAddress != "" {
		sb.Where(sb.L("address", q.AfterAddress))
		t.AfterAddress = q.AfterAddress
	}

	// We fetch an additional account to know if there is more
	sb.Limit(int(q.Limit + 1))
	t.Limit = q.Limit
	sb.Offset(int(q.Offset))

	sqlq, args := sb.BuildWithFlavor(s.schema.Flavor())
	rows, err := exec.QueryContext(ctx, sqlq, args...)
	if err != nil {
		return sharedapi.Cursor[core.Account]{}, s.error(err)
	}
	defer func(rows *sql.Rows) {
		if err := rows.Close(); err != nil {
			panic(err)
		}
	}(rows)

	for rows.Next() {
		account := core.Account{}
		if err := rows.Scan(&account.Address, &account.Metadata); err != nil {
			return sharedapi.Cursor[core.Account]{}, err
		}

		accounts = append(accounts, account)
	}
	if rows.Err() != nil {
		return sharedapi.Cursor[core.Account]{}, rows.Err()
	}

	var previous, next string
	if q.Offset-q.Limit > 0 {
		t.Offset = q.Offset - q.Limit
		raw, err := json.Marshal(t)
		if err != nil {
			return sharedapi.Cursor[core.Account]{}, s.error(err)
		}
		previous = base64.RawURLEncoding.EncodeToString(raw)
	}

	if len(accounts) == int(q.Limit+1) {
		accounts = accounts[:len(accounts)-1]
		t.Offset = q.Offset + q.Limit
		raw, err := json.Marshal(t)
		if err != nil {
			return sharedapi.Cursor[core.Account]{}, s.error(err)
		}
		next = base64.RawURLEncoding.EncodeToString(raw)
	}

	return sharedapi.Cursor[core.Account]{
		PageSize: len(accounts),
		HasMore:  next != "",
		Previous: previous,
		Next:     next,
		Data:     accounts,
	}, nil
}

func (s *Store) GetAccounts(ctx context.Context, q query.Accounts) (sharedapi.Cursor[core.Account], error) {
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
	if err := row.Scan(&account.Address, &account.Metadata); err != nil {
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
