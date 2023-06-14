package sqlstorage

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/formancehq/go-libs/api"
	"github.com/huandu/go-sqlbuilder"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger"
	"github.com/pkg/errors"
)

// This regexp is used to validate the account name
// If the account name is not valid, it means that the user putted a regex in
// the address filter, and we have to change the postgres operator used.
var accountNameRegex = regexp.MustCompile(`^[a-zA-Z_0-9]+$`)

func (s *Store) buildAccountsQuery(p ledger.AccountsQuery) (*sqlbuilder.SelectBuilder, AccPaginationToken) {
	sb := sqlbuilder.NewSelectBuilder()
	t := AccPaginationToken{}
	sb.From(s.schema.Table("accounts"))

	var (
		address         = p.Filters.Address
		metadata        = p.Filters.Metadata
		balance         = p.Filters.Balance
		balanceOperator = p.Filters.BalanceOperator
	)

	if address != "" {
		switch s.Schema().Flavor() {
		case sqlbuilder.PostgreSQL:
			src := strings.Split(address, ":")
			sb.Where(fmt.Sprintf("jsonb_array_length(address_json) = %d", len(src)))

			for i, segment := range src {
				if segment == ".*" || segment == "*" || segment == "" {
					continue
				}

				operator := "=="
				if !accountNameRegex.MatchString(segment) {
					operator = "like_regex"
				}

				arg := sb.Args.Add(segment)
				sb.Where(fmt.Sprintf("address_json @@ ('$[%d] %s \"' || %s::text || '\"')::jsonpath", i, operator, arg))
			}
		case sqlbuilder.SQLite:
			arg := sb.Args.Add("^" + address + "$")
			sb.Where("address REGEXP " + arg)
		}
		t.AddressRegexpFilter = address
	}

	for key, value := range metadata {
		arg := sb.Args.Add(value)
		// TODO: Need to find another way to specify the prefix since Table() methods does not make sense for functions and procedures
		sb.Where(s.schema.Table(
			fmt.Sprintf("%s(metadata, %s, '%s')",
				SQLCustomFuncMetaCompare, arg, strings.ReplaceAll(key, ".", "', '")),
		))
	}
	t.MetadataFilter = metadata

	if balance != "" {
		sb.Join(s.schema.Table("volumes"), "accounts.address = volumes.account")
		balanceOperation := "volumes.input - volumes.output"

		balanceValue, err := strconv.ParseInt(balance, 10, 0)
		if err != nil {
			// parameter is validated in the controller for now
			panic(errors.Wrap(err, "invalid balance parameter"))
		}

		if balanceOperator != "" {
			switch balanceOperator {
			case ledger.BalanceOperatorLte:
				sb.Where(sb.LessEqualThan(balanceOperation, balanceValue))
			case ledger.BalanceOperatorLt:
				sb.Where(sb.LessThan(balanceOperation, balanceValue))
			case ledger.BalanceOperatorGte:
				sb.Where(sb.GreaterEqualThan(balanceOperation, balanceValue))
			case ledger.BalanceOperatorGt:
				sb.Where(sb.GreaterThan(balanceOperation, balanceValue))
			case ledger.BalanceOperatorE:
				sb.Where(sb.Equal(balanceOperation, balanceValue))
			case ledger.BalanceOperatorNe:
				sb.Where(sb.NotEqual(balanceOperation, balanceValue))
			default:
				// parameter is validated in the controller for now
				panic("invalid balance operator parameter")
			}
		} else { // if no operator is given, default to gte
			sb.Where(sb.GreaterEqualThan(balanceOperation, balanceValue))
		}

		t.BalanceFilter = balance
		t.BalanceOperatorFilter = balanceOperator
	}

	return sb, t
}

func (s *Store) GetAccounts(ctx context.Context, q ledger.AccountsQuery) (api.Cursor[core.Account], error) {
	accounts := make([]core.Account, 0)

	if q.PageSize == 0 {
		return api.Cursor[core.Account]{Data: accounts}, nil
	}

	sb, t := s.buildAccountsQuery(q)
	sb.Select("address", "metadata")
	sb.OrderBy("address desc")

	if q.AfterAddress != "" {
		sb.Where(sb.L("address", q.AfterAddress))
		t.AfterAddress = q.AfterAddress
	}

	// We fetch an additional account to know if there is more
	sb.Limit(int(q.PageSize + 1))
	t.PageSize = q.PageSize
	sb.Offset(int(q.Offset))

	executor, err := s.executorProvider(ctx)
	if err != nil {
		return api.Cursor[core.Account]{}, err
	}

	sqlq, args := sb.BuildWithFlavor(s.schema.Flavor())
	rows, err := executor.QueryContext(ctx, sqlq, args...)
	if err != nil {
		return api.Cursor[core.Account]{}, s.error(err)
	}
	defer rows.Close()

	for rows.Next() {
		account := core.Account{
			Metadata: core.Metadata{},
		}
		if err := rows.Scan(&account.Address, &account.Metadata); err != nil {
			return api.Cursor[core.Account]{}, err
		}

		accounts = append(accounts, account)
	}
	if rows.Err() != nil {
		return api.Cursor[core.Account]{}, rows.Err()
	}

	var previous, next string
	if q.Offset > 0 {
		offset := int(q.Offset) - int(q.PageSize)
		if offset < 0 {
			t.Offset = 0
		} else {
			t.Offset = uint(offset)
		}
		raw, err := json.Marshal(t)
		if err != nil {
			return api.Cursor[core.Account]{}, s.error(err)
		}
		previous = base64.RawURLEncoding.EncodeToString(raw)
	}

	if len(accounts) == int(q.PageSize+1) {
		accounts = accounts[:len(accounts)-1]
		t.Offset = q.Offset + q.PageSize
		raw, err := json.Marshal(t)
		if err != nil {
			return api.Cursor[core.Account]{}, s.error(err)
		}
		next = base64.RawURLEncoding.EncodeToString(raw)
	}

	hasMore := next != ""
	return api.Cursor[core.Account]{
		PageSize:           int(q.PageSize),
		HasMore:            hasMore,
		Previous:           previous,
		Next:               next,
		Data:               accounts,
		PageSizeDeprecated: int(q.PageSize),
		HasMoreDeprecated:  &hasMore,
	}, nil
}

func (s *Store) GetAccount(ctx context.Context, addr string) (*core.Account, error) {
	sb := sqlbuilder.NewSelectBuilder()
	sb.Select("address", "metadata").
		From(s.schema.Table("accounts")).
		Where(sb.Equal("address", addr))

	account := core.Account{
		Address:  addr,
		Metadata: core.Metadata{},
	}

	executor, err := s.executorProvider(ctx)
	if err != nil {
		return nil, err
	}

	sqlq, args := sb.BuildWithFlavor(s.schema.Flavor())
	row := executor.QueryRowContext(ctx, sqlq, args...)
	if err := row.Err(); err != nil {
		return nil, err
	}

	if err := row.Scan(&account.Address, &account.Metadata); err != nil {
		if err == sql.ErrNoRows {
			return &account, nil
		}
		return nil, err
	}

	return &account, nil
}

func (s *Store) ensureAccountExists(ctx context.Context, account string) error {
	var accountBy string
	switch s.schema.Flavor() {
	case sqlbuilder.PostgreSQL:
		a, err := json.Marshal(strings.Split(account, ":"))
		if err != nil {
			return err
		}
		accountBy = string(a)
	case sqlbuilder.SQLite:
		accountBy = account
	}

	sb := sqlbuilder.NewInsertBuilder()
	sb = sb.InsertInto(s.schema.Table("accounts"))

	switch s.schema.Flavor() {
	case sqlbuilder.PostgreSQL:
		sb = sb.Cols("address", "metadata", "address_json").
			Values(account, "{}", accountBy).
			SQL("ON CONFLICT DO NOTHING")
	case sqlbuilder.SQLite:
		sb = sb.Cols("address", "metadata").
			Values(account, "{}").
			SQL("ON CONFLICT DO NOTHING")
	}

	sqlq, args := sb.BuildWithFlavor(s.schema.Flavor())

	executor, err := s.executorProvider(ctx)
	if err != nil {
		return err
	}

	_, err = executor.ExecContext(ctx, sqlq, args...)
	return s.error(err)
}

func (s *Store) UpdateAccountMetadata(ctx context.Context, address string, metadata core.Metadata, at time.Time) error {
	ib := sqlbuilder.NewInsertBuilder()

	metadataData, err := json.Marshal(metadata)
	if err != nil {
		return err
	}

	placeholder := ib.Var(metadataData)
	ib = ib.InsertInto(s.schema.Table("accounts"))

	switch s.schema.Flavor() {
	case sqlbuilder.PostgreSQL:
		addressBy, err := json.Marshal(strings.Split(address, ":"))
		if err != nil {
			return err
		}
		ib = ib.Cols("address", "metadata", "address_json").
			Values(address, metadataData, addressBy).
			SQL(fmt.Sprintf("ON CONFLICT (address) DO UPDATE SET metadata = accounts.metadata || %s", placeholder))
	case sqlbuilder.SQLite:
		ib = ib.Cols("address", "metadata").
			Values(address, metadataData).
			SQL(fmt.Sprintf("ON CONFLICT (address) DO UPDATE SET metadata = json_patch(metadata,  %s)", placeholder))
	}

	executor, err := s.executorProvider(ctx)
	if err != nil {
		return err
	}

	sqlq, args := ib.BuildWithFlavor(s.schema.Flavor())
	_, err = executor.ExecContext(ctx, sqlq, args...)
	if err != nil {
		return err
	}

	lastLog, err := s.GetLastLog(ctx)
	if err != nil {
		return errors.Wrap(err, "reading last log")
	}

	return s.appendLog(ctx, core.NewSetMetadataLog(lastLog, at, core.SetMetadata{
		TargetType: core.MetaTargetTypeAccount,
		TargetID:   address,
		Metadata:   metadata,
	}))
}
