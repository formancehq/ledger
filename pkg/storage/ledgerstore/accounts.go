package ledgerstore

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"

	"github.com/formancehq/ledger/pkg/core"
	storageerrors "github.com/formancehq/ledger/pkg/storage"
	"github.com/formancehq/stack/libs/go-libs/api"
	"github.com/formancehq/stack/libs/go-libs/metadata"
	"github.com/uptrace/bun"
)

const (
	accountsTableName = "accounts"
)

type AccountsQuery OffsetPaginatedQuery[AccountsQueryFilters]

type AccountsQueryFilters struct {
	AfterAddress string            `json:"after"`
	Address      string            `json:"address"`
	Metadata     metadata.Metadata `json:"metadata"`
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

func (a AccountsQuery) WithMetadataFilter(metadata metadata.Metadata) AccountsQuery {
	a.Filters.Metadata = metadata

	return a
}

// This regexp is used to validate the account name
// If the account name is not valid, it means that the user putted a regex in
// the address filter, and we have to change the postgres operator used.
var accountNameRegex = regexp.MustCompile(`^[a-zA-Z_0-9]+$`)

type Account struct {
	bun.BaseModel `bun:"accounts,alias:accounts"`

	Address     string            `bun:"address,type:varchar,unique,notnull"`
	Metadata    map[string]string `bun:"metadata,type:jsonb,default:'{}'"`
	AddressJson []string          `bun:"address_json,type:jsonb"`
}

func (s *Store) buildAccountsQuery(p AccountsQuery) *bun.SelectQuery {
	query := s.schema.NewSelect(accountsTableName).
		Model((*Account)(nil)).
		ColumnExpr("coalesce(accounts.address, moves.account) as address").
		ColumnExpr("coalesce(accounts.metadata, '{}'::jsonb) as metadata").
		Join(fmt.Sprintf(`full join "%s".moves moves on moves.account = accounts.address`, s.schema.Name()))

	if p.Filters.Address != "" {
		src := strings.Split(p.Filters.Address, ":")
		query.Where(fmt.Sprintf("jsonb_array_length(address_json) = %d", len(src)))

		for i, segment := range src {
			if len(segment) == 0 {
				continue
			}
			query.Where(fmt.Sprintf("address_json @@ ('$[%d] == \"' || ?::text || '\"')::jsonpath", i), segment)
		}
	}

	for key, value := range p.Filters.Metadata {
		query.Where(
			fmt.Sprintf(`"%s".%s(metadata, ?, '%s')`, s.schema.Name(),
				SQLCustomFuncMetaCompare, strings.ReplaceAll(key, ".", "', '"),
			), value)
	}

	return s.schema.IDB.NewSelect().
		With("cte1", query).
		DistinctOn("cte1.address").
		ColumnExpr("cte1.address").
		ColumnExpr("cte1.metadata").
		Table("cte1")
}

func (s *Store) GetAccounts(ctx context.Context, q AccountsQuery) (*api.Cursor[core.Account], error) {
	return UsingOffset[AccountsQueryFilters, core.Account](ctx,
		s.buildAccountsQuery(q), OffsetPaginatedQuery[AccountsQueryFilters](q))
}

func (s *Store) GetAccount(ctx context.Context, addr string) (*core.Account, error) {
	account := &core.Account{}
	if err := s.schema.NewSelect(accountsTableName).
		ColumnExpr("address").
		ColumnExpr("metadata").
		Where("address = ?", addr).
		Model(account).
		Scan(ctx, account); err != nil {
		if err == sql.ErrNoRows {
			return &core.Account{
				Address:  addr,
				Metadata: metadata.Metadata{},
			}, nil
		}
		return nil, err
	}

	return account, nil
}

func (s *Store) GetAccountWithVolumes(ctx context.Context, account string) (*core.AccountWithVolumes, error) {
	cte2 := s.schema.NewSelect(accountsTableName).
		Join(fmt.Sprintf(`full join "%s".moves moves on moves.account = accounts.address`, s.schema.Name())).
		Where("account = ?", account).
		Group("moves.asset").
		Column("moves.asset").
		ColumnExpr(fmt.Sprintf(`"%s".first(moves.post_commit_input_value order by moves.timestamp desc) as post_commit_input_value`, s.schema.Name())).
		ColumnExpr(fmt.Sprintf(`"%s".first(moves.post_commit_output_value order by moves.timestamp desc) as post_commit_output_value`, s.schema.Name()))

	cte3 := s.schema.IDB.NewSelect().
		ColumnExpr(`('{"' || data.asset || '": {"input": ' || data.post_commit_input_value || ', "output": ' || data.post_commit_output_value || '}}')::jsonb as asset`).
		TableExpr("cte2 data")

	cte4 := s.schema.IDB.NewSelect().
		ColumnExpr(fmt.Sprintf(`'%s' as account`, account)).
		ColumnExpr(fmt.Sprintf(`"%s".aggregate_objects(data.asset) as volumes`, s.schema.Name())).
		TableExpr("cte3 data")

	accountWithVolumes := &core.AccountWithVolumes{}
	err := s.schema.NewSelect(accountsTableName).
		With("cte2", cte2).
		With("cte3", cte3).
		With("cte4", cte4).
		ColumnExpr(fmt.Sprintf("'%s' as address", account)).
		ColumnExpr("coalesce(accounts.metadata, '{}'::jsonb) as metadata").
		ColumnExpr("cte4.volumes").
		Join(`right join cte4 on cte4.account = accounts.address`).
		Scan(ctx, accountWithVolumes)
	if err != nil {
		return nil, storageerrors.PostgresError(err)
	}

	return accountWithVolumes, nil
}

func (s *Store) CountAccounts(ctx context.Context, q AccountsQuery) (uint64, error) {
	sb := s.buildAccountsQuery(q)
	count, err := sb.Count(ctx)
	return uint64(count), storageerrors.PostgresError(err)
}

func (s *Store) UpdateAccountsMetadata(ctx context.Context, accounts ...core.Account) error {
	accs := make([]*Account, len(accounts))
	for i, a := range accounts {
		accs[i] = &Account{
			Address:     a.Address,
			Metadata:    a.Metadata,
			AddressJson: strings.Split(a.Address, ":"),
		}
	}

	_, err := s.schema.NewInsert(accountsTableName).
		Model(&accs).
		On("CONFLICT (address) DO UPDATE").
		Set("metadata = accounts.metadata || EXCLUDED.metadata").
		Exec(ctx)

	return storageerrors.PostgresError(err)
}
