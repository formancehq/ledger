package ledger

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"
	"strings"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/storage"
	storageerrors "github.com/formancehq/ledger/pkg/storage/sqlstorage/errors"
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

type AccountsPaginationToken struct {
	PageSize              uint                    `json:"pageSize"`
	Offset                uint                    `json:"offset"`
	AfterAddress          string                  `json:"after,omitempty"`
	AddressRegexpFilter   string                  `json:"address,omitempty"`
	MetadataFilter        map[string]string       `json:"metadata,omitempty"`
	BalanceFilter         string                  `json:"balance,omitempty"`
	BalanceOperatorFilter storage.BalanceOperator `json:"balanceOperator,omitempty"`
}

func (t AccountsPaginationToken) Encode() string {
	return encodePaginationToken(t)
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
	if !s.isInitialized {
		return api.Cursor[core.Account]{},
			storageerrors.StorageError(storage.ErrStoreNotInitialized)
	}
	recordMetrics := s.instrumentalized(ctx, "get_accounts")
	defer recordMetrics()

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
		return api.Cursor[core.Account]{}, storageerrors.PostgresError(err)
	}
	defer rows.Close()

	for rows.Next() {
		account := core.Account{
			Metadata: metadata.Metadata{},
		}
		if err := rows.Scan(&account.Address, &account.Metadata); err != nil {
			return api.Cursor[core.Account]{}, storageerrors.PostgresError(err)
		}

		accounts = append(accounts, account)
	}
	if rows.Err() != nil {
		return api.Cursor[core.Account]{}, storageerrors.PostgresError(rows.Err())
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
			return api.Cursor[core.Account]{}, errors.Wrap(err, "failed to marshal pagination token")
		}
		previous = base64.RawURLEncoding.EncodeToString(raw)
	}

	if len(accounts) == int(q.PageSize+1) {
		accounts = accounts[:len(accounts)-1]
		t.Offset = q.Offset + q.PageSize
		raw, err := json.Marshal(t)
		if err != nil {
			return api.Cursor[core.Account]{}, errors.Wrap(err, "failed to marshal pagination token")
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
	if !s.isInitialized {
		return nil, storageerrors.StorageError(storage.ErrStoreNotInitialized)
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
		return nil, storageerrors.StorageError(storage.ErrStoreNotInitialized)
	}
	recordMetrics := s.instrumentalized(ctx, "get_account_with_volumes")
	defer recordMetrics()

	return s.getAccountWithVolumes(ctx, s.schema, account)
}

func (s *Store) CountAccounts(ctx context.Context, q storage.AccountsQuery) (uint64, error) {
	if !s.isInitialized {
		return 0, storageerrors.StorageError(storage.ErrStoreNotInitialized)
	}
	recordMetrics := s.instrumentalized(ctx, "count_accounts")
	defer recordMetrics()

	sb, _ := s.buildAccountsQuery(ctx, q)
	count, err := sb.Count(ctx)
	return uint64(count), storageerrors.PostgresError(err)
}

func (s *Store) EnsureAccountExists(ctx context.Context, account string) error {
	if !s.isInitialized {
		return storageerrors.StorageError(storage.ErrStoreNotInitialized)
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
		return storageerrors.StorageError(storage.ErrStoreNotInitialized)
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
		return storageerrors.StorageError(storage.ErrStoreNotInitialized)
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
		return storageerrors.StorageError(storage.ErrStoreNotInitialized)
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
