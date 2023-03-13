package ledger

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/ledger"
	"github.com/formancehq/stack/libs/go-libs/api"
	"github.com/pkg/errors"
	"github.com/uptrace/bun"
)

const accountsTableName = "accounts"

type Accounts struct {
	bun.BaseModel `bun:"accounts,alias:accounts"`

	Address  string         `bun:"address,type:varchar,unique,notnull"`
	Metadata map[string]any `bun:"metadata,type:jsonb,default:'{}'"`
}

type AccountsPaginationToken struct {
	PageSize              uint                   `json:"pageSize"`
	Offset                uint                   `json:"offset"`
	AfterAddress          string                 `json:"after,omitempty"`
	AddressRegexpFilter   string                 `json:"address,omitempty"`
	MetadataFilter        map[string]string      `json:"metadata,omitempty"`
	BalanceFilter         string                 `json:"balance,omitempty"`
	BalanceOperatorFilter ledger.BalanceOperator `json:"balanceOperator,omitempty"`
}

func (s *Store) buildAccountsQuery(p ledger.AccountsQuery) (*bun.SelectQuery, AccountsPaginationToken) {
	sb := s.schema.NewSelect(accountsTableName).
		Model((*Accounts)(nil))
	t := AccountsPaginationToken{}

	var (
		address         = p.Filters.Address
		metadata        = p.Filters.Metadata
		balance         = p.Filters.Balance
		balanceOperator = p.Filters.BalanceOperator
	)

	if address != "" {
		sb.Where("address ~* ?", "^"+address+"$")
		t.AddressRegexpFilter = address
	}

	for key, value := range metadata {
		// TODO: Need to find another way to specify the prefix since Table() methods does not make sense for functions and procedures
		sb.Where(s.schema.Table(
			fmt.Sprintf("%s(metadata, ?, '%s')",
				SQLCustomFuncMetaCompare, strings.ReplaceAll(key, ".", "', '")),
		), value)
	}
	t.MetadataFilter = metadata

	if balance != "" {
		sb.Join("JOIN " + s.schema.Table("volumes")).
			JoinOn("accounts.address = volumes.account")
		balanceOperation := "volumes.input - volumes.output"

		balanceValue, err := strconv.ParseInt(balance, 10, 0)
		if err != nil {
			// parameter is validated in the controller for now
			panic(errors.Wrap(err, "invalid balance parameter"))
		}

		if balanceOperator != "" {
			switch balanceOperator {
			case ledger.BalanceOperatorLte:
				sb.Where(fmt.Sprintf("%s <= ?", balanceOperation), balanceValue)
			case ledger.BalanceOperatorLt:
				sb.Where(fmt.Sprintf("%s < ?", balanceOperation), balanceValue)
			case ledger.BalanceOperatorGte:
				sb.Where(fmt.Sprintf("%s >= ?", balanceOperation), balanceValue)
			case ledger.BalanceOperatorGt:
				sb.Where(fmt.Sprintf("%s > ?", balanceOperation), balanceValue)
			case ledger.BalanceOperatorE:
				sb.Where(fmt.Sprintf("%s = ?", balanceOperation), balanceValue)
			case ledger.BalanceOperatorNe:
				sb.Where(fmt.Sprintf("%s != ?", balanceOperation), balanceValue)
			default:
				// parameter is validated in the controller for now
				panic("invalid balance operator parameter")
			}
		} else { // if no operator is given, default to gte
			sb.Where(fmt.Sprintf("%s >= ?", balanceOperation), balanceValue)
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
	sb.OrderExpr("address desc")

	if q.AfterAddress != "" {
		sb.Where("address < ?", q.AfterAddress)
		t.AfterAddress = q.AfterAddress
	}

	// We fetch an additional account to know if there is more
	sb.Limit(int(q.PageSize + 1))
	t.PageSize = q.PageSize
	sb.Offset(int(q.Offset))

	rows, err := s.schema.QueryContext(ctx, sb.String())
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
		PageSize: int(q.PageSize),
		HasMore:  hasMore,
		Previous: previous,
		Next:     next,
		Data:     accounts,
	}, nil
}

func (s *Store) GetAccount(ctx context.Context, addr string) (*core.Account, error) {
	query := s.schema.NewSelect(accountsTableName).
		Model((*Accounts)(nil)).
		Where("address = ?", addr).
		String()

	account := core.Account{
		Address:  core.AccountAddress(addr),
		Metadata: core.Metadata{},
	}

	row := s.schema.QueryRowContext(ctx, query)
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

func (s *Store) GetAccountWithVolumes(ctx context.Context, account string) (*core.AccountWithVolumes, error) {
	query := s.schema.NewSelect(accountsTableName).
		Model((*Accounts)(nil)).
		ColumnExpr("accounts.metadata, volumes.asset, volumes.input, volumes.output").
		Join("LEFT OUTER JOIN "+s.schema.Table("volumes")).
		JoinOn("accounts.address = volumes.account").
		Where("accounts.address = ?", account).String()

	rows, err := s.schema.QueryContext(ctx, query)
	if err != nil {
		return nil, s.error(err)
	}
	defer rows.Close()

	acc := core.Account{
		Address:  core.AccountAddress(account),
		Metadata: core.Metadata{},
	}
	assetsVolumes := core.AssetsVolumes{}

	for rows.Next() {
		var asset, inputStr, outputStr sql.NullString
		if err := rows.Scan(&acc.Metadata, &asset, &inputStr, &outputStr); err != nil {
			return nil, s.error(err)
		}

		if asset.Valid {
			assetsVolumes[asset.String] = core.Volumes{
				Input:  core.NewMonetaryInt(0),
				Output: core.NewMonetaryInt(0),
			}

			if inputStr.Valid {
				input, err := core.ParseMonetaryInt(inputStr.String)
				if err != nil {
					return nil, s.error(err)
				}
				assetsVolumes[asset.String] = core.Volumes{
					Input:  input,
					Output: assetsVolumes[asset.String].Output,
				}
			}

			if outputStr.Valid {
				output, err := core.ParseMonetaryInt(outputStr.String)
				if err != nil {
					return nil, s.error(err)
				}
				assetsVolumes[asset.String] = core.Volumes{
					Input:  assetsVolumes[asset.String].Input,
					Output: output,
				}
			}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, s.error(err)
	}

	res := &core.AccountWithVolumes{
		Account: acc,
		Volumes: assetsVolumes,
	}
	res.Balances = res.Volumes.Balances()

	return res, nil
}

func (s *Store) CountAccounts(ctx context.Context, q ledger.AccountsQuery) (uint64, error) {
	sb, _ := s.buildAccountsQuery(q)
	count, err := sb.Count(ctx)
	return uint64(count), s.error(err)
}

func (s *Store) ensureAccountExists(ctx context.Context, account string) error {
	a := &Accounts{
		Address:  account,
		Metadata: make(map[string]interface{}),
	}

	_, err := s.schema.NewInsert(accountsTableName).
		Model(a).
		Ignore().
		Exec(ctx)

	return s.error(err)
}

func (s *Store) UpdateAccountMetadata(ctx context.Context, address string, metadata core.Metadata) error {
	a := &Accounts{
		Address:  address,
		Metadata: metadata,
	}

	_, err := s.schema.NewInsert(accountsTableName).
		Model(a).
		On("CONFLICT (address) DO UPDATE").
		Set("metadata = accounts.metadata || EXCLUDED.metadata").
		Exec(ctx)
	return err

	// return s.appendLog(ctx, core.NewSetMetadataLog(at, core.SetMetadata{
	// 	TargetType: core.MetaTargetTypeAccount,
	// 	TargetID:   address,
	// 	Metadata:   metadata,
	// }))
}
