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
	"github.com/formancehq/ledger/pkg/storage"
	"github.com/formancehq/stack/libs/go-libs/api"
	"github.com/pkg/errors"
	"github.com/uptrace/bun"
)

const (
	accountsTableName = "accounts"
)

type Accounts struct {
	bun.BaseModel `bun:"accounts,alias:accounts"`

	Address  string         `bun:"address,type:varchar,unique,notnull"`
	Metadata map[string]any `bun:"metadata,type:jsonb,default:'{}'"`
}

type AccountsPaginationToken struct {
	PageSize              uint                    `json:"pageSize"`
	Offset                uint                    `json:"offset"`
	AfterAddress          string                  `json:"after,omitempty"`
	AddressRegexpFilter   string                  `json:"address,omitempty"`
	MetadataFilter        map[string]string       `json:"metadata,omitempty"`
	BalanceFilter         string                  `json:"balance,omitempty"`
	BalanceOperatorFilter storage.BalanceOperator `json:"balanceOperator,omitempty"`
}

func (s *Store) buildAccountsQuery(ctx context.Context, p storage.AccountsQuery) (*bun.SelectQuery, AccountsPaginationToken) {
	sb := s.schema.NewSelect(accountsTableName).Model((*Accounts)(nil))
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
		sb.Join("JOIN " + s.schema.Table(volumesTableName)).
			JoinOn("accounts.address = volumes.account")
		balanceOperation := "volumes.input - volumes.output"

		balanceValue, err := strconv.ParseInt(balance, 10, 0)
		if err != nil {
			// parameter is validated in the controller for now
			panic(errors.Wrap(err, "invalid balance parameter"))
		}

		if balanceOperator != "" {
			switch balanceOperator {
			case storage.BalanceOperatorLte:
				sb.Where(fmt.Sprintf("%s <= ?", balanceOperation), balanceValue)
			case storage.BalanceOperatorLt:
				sb.Where(fmt.Sprintf("%s < ?", balanceOperation), balanceValue)
			case storage.BalanceOperatorGte:
				sb.Where(fmt.Sprintf("%s >= ?", balanceOperation), balanceValue)
			case storage.BalanceOperatorGt:
				sb.Where(fmt.Sprintf("%s > ?", balanceOperation), balanceValue)
			case storage.BalanceOperatorE:
				sb.Where(fmt.Sprintf("%s = ?", balanceOperation), balanceValue)
			case storage.BalanceOperatorNe:
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

func (s *Store) GetAccounts(ctx context.Context, q storage.AccountsQuery) (api.Cursor[core.Account], error) {
	accounts := make([]core.Account, 0)

	if q.PageSize == 0 {
		return api.Cursor[core.Account]{Data: accounts}, nil
	}

	sb, t := s.buildAccountsQuery(ctx, q)
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
		Address:  addr,
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

func (s *Store) getAccountWithVolumes(ctx context.Context, exec interface {
	QueryContext(
		ctx context.Context, query string, args ...interface{},
	) (*sql.Rows, error)
}, account string) (*core.AccountWithVolumes, error) {

	query := s.schema.NewSelect(accountsTableName).
		Model((*Accounts)(nil)).
		ColumnExpr("accounts.metadata, volumes.asset, volumes.input, volumes.output").
		Join("LEFT OUTER JOIN "+s.schema.Table(volumesTableName)+" volumes").
		JoinOn("accounts.address = volumes.account").
		Where("accounts.address = ?", account).String()

	rows, err := exec.QueryContext(ctx, query)
	if err != nil {
		return nil, s.error(err)
	}
	defer rows.Close()

	acc := core.Account{
		Address:  account,
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

	return &core.AccountWithVolumes{
		Account: acc,
		Volumes: assetsVolumes,
	}, nil
}

func (s *Store) GetAccountWithVolumes(ctx context.Context, account string) (*core.AccountWithVolumes, error) {
	return s.getAccountWithVolumes(ctx, s.schema, account)
}

func (s *Store) CountAccounts(ctx context.Context, q storage.AccountsQuery) (uint64, error) {
	sb, _ := s.buildAccountsQuery(ctx, q)
	count, err := sb.Count(ctx)
	return uint64(count), s.error(err)
}

func (s *Store) EnsureAccountExists(ctx context.Context, account string) error {
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

}

func (s *Store) ComputeAccount(ctx context.Context, address string) (*core.AccountWithVolumes, error) {
	tx, err := s.schema.BeginTx(ctx, &sql.TxOptions{
		ReadOnly: true,
	})
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	account, err := s.getAccountWithVolumes(ctx, tx, address)
	if err != nil {
		return nil, err
	}

	nextLogID, err := s.getNextLogID(ctx, tx)
	if err != nil {
		return nil, err
	}

	notReadLogs, err := s.readLogsStartingFromID(ctx, tx, nextLogID)
	if err != nil {
		return nil, err
	}

	for _, log := range notReadLogs {
		switch log.Type {
		case core.NewTransactionLogType:
			for _, posting := range log.Data.(core.NewTransactionLogPayload).Transaction.Postings {
				volumes, ok := account.Volumes[posting.Asset]
				if !ok {
					volumes.Input = core.NewMonetaryInt(0)
					volumes.Output = core.NewMonetaryInt(0)
				}
				switch {
				case posting.Source == address:
					volumes.Output = volumes.Output.Add(posting.Amount)
				case posting.Destination == address:
					volumes.Input = volumes.Input.Add(posting.Amount)
				}
				account.Volumes[posting.Asset] = volumes
			}
		case core.SetMetadataLogType:
			if log.Data.(core.SetMetadataLogPayload).TargetID == address {
				account.Metadata = account.Metadata.Merge(log.Data.(core.SetMetadataLogPayload).Metadata)
			}
		}
	}

	return account, nil
}
