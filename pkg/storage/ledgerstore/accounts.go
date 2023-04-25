package ledgerstore

import (
	"context"
	"database/sql"
	"fmt"
	"math/big"
	"strconv"
	"strings"

	"github.com/formancehq/ledger/pkg/core"
	storageerrors "github.com/formancehq/ledger/pkg/storage/errors"
	"github.com/formancehq/stack/libs/go-libs/api"
	"github.com/formancehq/stack/libs/go-libs/metadata"
	"github.com/pkg/errors"
	"github.com/uptrace/bun"
)

const (
	accountsTableName = "accounts"
)

type Accounts struct {
	bun.BaseModel `bun:"accounts,alias:accounts"`

	Address  string            `bun:"address,type:varchar,unique,notnull"`
	Metadata map[string]string `bun:"metadata,type:jsonb,default:'{}'"`
}

func (s *Store) buildAccountsQuery(p AccountsQuery) *bun.SelectQuery {
	sb := s.schema.NewSelect(accountsTableName).Model((*Accounts)(nil))

	if p.Filters.Address != "" {
		sb.Where("address ~* ?", "^"+p.Filters.Address+"$")
	}

	for key, value := range p.Filters.Metadata {
		// TODO: Need to find another way to specify the prefix since Table() methods does not make sense for functions and procedures
		sb.Where(s.schema.Table(
			fmt.Sprintf("%s(metadata, ?, '%s')",
				SQLCustomFuncMetaCompare, strings.ReplaceAll(key, ".", "', '")),
		), value)
	}

	if p.Filters.Balance != "" {
		sb.Join("LEFT JOIN " + s.schema.Table(volumesTableName)).
			JoinOn("accounts.address = volumes.account")
		balanceOperation := "coalesce(volumes.input - volumes.output, 0)"

		balanceValue, err := strconv.ParseInt(p.Filters.Balance, 10, 0)
		if err != nil {
			// parameter is validated in the controller for now
			panic(errors.Wrap(err, "invalid balance parameter"))
		}

		if p.Filters.BalanceOperator != "" {
			switch p.Filters.BalanceOperator {
			case BalanceOperatorLte:
				sb.Where(fmt.Sprintf("%s <= ?", balanceOperation), balanceValue)
			case BalanceOperatorLt:
				sb.Where(fmt.Sprintf("%s < ?", balanceOperation), balanceValue)
			case BalanceOperatorGte:
				sb.Where(fmt.Sprintf("%s >= ?", balanceOperation), balanceValue)
			case BalanceOperatorGt:
				sb.Where(fmt.Sprintf("%s > ?", balanceOperation), balanceValue)
			case BalanceOperatorE:
				sb.Where(fmt.Sprintf("%s = ?", balanceOperation), balanceValue)
			case BalanceOperatorNe:
				sb.Where(fmt.Sprintf("%s != ?", balanceOperation), balanceValue)
			default:
				// parameter is validated in the controller for now
				panic("invalid balance operator parameter")
			}
		} else { // if no operator is given, default to gte
			sb.Where(fmt.Sprintf("%s >= ?", balanceOperation), balanceValue)
		}
	}

	return sb
}

func (s *Store) GetAccounts(ctx context.Context, q AccountsQuery) (*api.Cursor[core.Account], error) {
	if !s.isInitialized {
		return nil, storageerrors.StorageError(storageerrors.ErrStoreNotInitialized)
	}
	recordMetrics := s.instrumentalized(ctx, "get_accounts")
	defer recordMetrics()

	return UsingOffset(ctx, s.buildAccountsQuery(q), OffsetPaginatedQuery[AccountsQueryFilters](q),
		func(account *core.Account, scanner interface{ Scan(args ...any) error }) error {
			return scanner.Scan(&account.Address, &account.Metadata)
		})
}

func (s *Store) GetAccount(ctx context.Context, addr string) (*core.Account, error) {
	if !s.isInitialized {
		return nil, storageerrors.StorageError(storageerrors.ErrStoreNotInitialized)
	}
	recordMetrics := s.instrumentalized(ctx, "get_account")
	defer recordMetrics()

	query := s.schema.NewSelect(accountsTableName).
		Model((*Accounts)(nil)).
		Where("address = ?", addr).
		String()

	row := s.schema.QueryRowContext(ctx, query)
	if err := row.Err(); err != nil {
		return nil, storageerrors.PostgresError(err)
	}

	var account core.Account
	err := row.Scan(&account.Address, &account.Metadata)
	if err != nil {
		return nil, storageerrors.PostgresError(err)
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
		return nil, storageerrors.PostgresError(err)
	}
	defer rows.Close()

	acc := core.Account{
		Address:  account,
		Metadata: metadata.Metadata{},
	}
	assetsVolumes := core.AssetsVolumes{}

	for rows.Next() {
		var asset, inputStr, outputStr sql.NullString
		if err := rows.Scan(&acc.Metadata, &asset, &inputStr, &outputStr); err != nil {
			return nil, storageerrors.PostgresError(err)
		}

		if asset.Valid {
			assetsVolumes[asset.String] = core.Volumes{
				Input:  big.NewInt(0),
				Output: big.NewInt(0),
			}

			if inputStr.Valid {
				input, ok := new(big.Int).SetString(inputStr.String, 10)
				if !ok {
					panic("unable to create big int")
				}
				assetsVolumes[asset.String] = core.Volumes{
					Input:  input,
					Output: assetsVolumes[asset.String].Output,
				}
			}

			if outputStr.Valid {
				output, ok := new(big.Int).SetString(outputStr.String, 10)
				if !ok {
					panic("unable to create big int")
				}
				assetsVolumes[asset.String] = core.Volumes{
					Input:  assetsVolumes[asset.String].Input,
					Output: output,
				}
			}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, storageerrors.PostgresError(err)
	}

	return &core.AccountWithVolumes{
		Account: acc,
		Volumes: assetsVolumes,
	}, nil
}

func (s *Store) GetAccountWithVolumes(ctx context.Context, account string) (*core.AccountWithVolumes, error) {
	if !s.isInitialized {
		return nil, storageerrors.StorageError(storageerrors.ErrStoreNotInitialized)
	}
	recordMetrics := s.instrumentalized(ctx, "get_account_with_volumes")
	defer recordMetrics()

	return s.getAccountWithVolumes(ctx, s.schema, account)
}

func (s *Store) CountAccounts(ctx context.Context, q AccountsQuery) (uint64, error) {
	if !s.isInitialized {
		return 0, storageerrors.StorageError(storageerrors.ErrStoreNotInitialized)
	}
	recordMetrics := s.instrumentalized(ctx, "count_accounts")
	defer recordMetrics()

	sb := s.buildAccountsQuery(q)
	count, err := sb.Count(ctx)
	return uint64(count), storageerrors.PostgresError(err)
}

func (s *Store) EnsureAccountExists(ctx context.Context, account string) error {
	if !s.isInitialized {
		return storageerrors.StorageError(storageerrors.ErrStoreNotInitialized)
	}
	recordMetrics := s.instrumentalized(ctx, "ensure_account_exists")
	defer recordMetrics()

	a := &Accounts{
		Address:  account,
		Metadata: metadata.Metadata{},
	}

	_, err := s.schema.NewInsert(accountsTableName).
		Model(a).
		Ignore().
		Exec(ctx)

	return storageerrors.PostgresError(err)
}

func (s *Store) EnsureAccountsExist(ctx context.Context, accounts []string) error {
	if !s.isInitialized {
		return storageerrors.StorageError(storageerrors.ErrStoreNotInitialized)
	}
	recordMetrics := s.instrumentalized(ctx, "ensure_accounts_exist")
	defer recordMetrics()

	accs := make([]*Accounts, len(accounts))
	for i, a := range accounts {
		accs[i] = &Accounts{
			Address:  a,
			Metadata: metadata.Metadata{},
		}
	}

	_, err := s.schema.NewInsert(accountsTableName).
		Model(&accs).
		Ignore().
		Exec(ctx)

	return storageerrors.PostgresError(err)
}

func (s *Store) UpdateAccountMetadata(ctx context.Context, address string, metadata metadata.Metadata) error {
	if !s.isInitialized {
		return storageerrors.StorageError(storageerrors.ErrStoreNotInitialized)
	}
	recordMetrics := s.instrumentalized(ctx, "update_account_metadata")
	defer recordMetrics()

	a := &Accounts{
		Address:  address,
		Metadata: metadata,
	}

	_, err := s.schema.NewInsert(accountsTableName).
		Model(a).
		On("CONFLICT (address) DO UPDATE").
		Set("metadata = accounts.metadata || EXCLUDED.metadata").
		Exec(ctx)

	return storageerrors.PostgresError(err)
}

func (s *Store) UpdateAccountsMetadata(ctx context.Context, accounts []core.Account) error {
	if !s.isInitialized {
		return storageerrors.StorageError(storageerrors.ErrStoreNotInitialized)
	}
	recordMetrics := s.instrumentalized(ctx, "update_accounts_metadata")
	defer recordMetrics()

	accs := make([]*Accounts, len(accounts))
	for i, a := range accounts {
		accs[i] = &Accounts{
			Address:  a.Address,
			Metadata: a.Metadata,
		}
	}

	_, err := s.schema.NewInsert(accountsTableName).
		Model(&accs).
		On("CONFLICT (address) DO UPDATE").
		Set("metadata = accounts.metadata || EXCLUDED.metadata").
		Exec(ctx)

	return storageerrors.PostgresError(err)
}

type AccountsQuery OffsetPaginatedQuery[AccountsQueryFilters]

type AccountsQueryFilters struct {
	AfterAddress    string            `json:"after"`
	Address         string            `json:"address"`
	Balance         string            `json:"balance"`
	BalanceOperator BalanceOperator   `json:"balanceOperator"`
	Metadata        metadata.Metadata `json:"metadata"`
}

func NewAccountsQuery() AccountsQuery {
	return AccountsQuery{
		PageSize: QueryDefaultPageSize,
		Order:    OrderAsc,
		Filters: AccountsQueryFilters{
			Metadata: metadata.Metadata{},
		},
	}
}

func (a AccountsQuery) WithPageSize(pageSize uint64) AccountsQuery {
	if pageSize != 0 {
		a.PageSize = pageSize
	}

	return a
}

func (a AccountsQuery) WithAfterAddress(after string) AccountsQuery {
	a.Filters.AfterAddress = after

	return a
}

func (a AccountsQuery) WithAddressFilter(address string) AccountsQuery {
	a.Filters.Address = address

	return a
}

func (a AccountsQuery) WithBalanceFilter(balance string) AccountsQuery {
	a.Filters.Balance = balance

	return a
}

func (a AccountsQuery) WithBalanceOperatorFilter(balanceOperator BalanceOperator) AccountsQuery {
	a.Filters.BalanceOperator = balanceOperator

	return a
}

func (a AccountsQuery) WithMetadataFilter(metadata metadata.Metadata) AccountsQuery {
	a.Filters.Metadata = metadata

	return a
}
