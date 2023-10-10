package ledgerstore

import (
	"context"
	"errors"
	"fmt"
	"regexp"

	ledger "github.com/formancehq/ledger/internal"
	storageerrors "github.com/formancehq/ledger/internal/storage"
	"github.com/formancehq/ledger/internal/storage/paginate"
	"github.com/formancehq/stack/libs/go-libs/api"
	"github.com/formancehq/stack/libs/go-libs/pointer"
	"github.com/formancehq/stack/libs/go-libs/query"
	"github.com/uptrace/bun"
)

func (store *Store) buildAccountQuery(q PITFilterWithVolumes, query *bun.SelectQuery) *bun.SelectQuery {
	query = query.
		DistinctOn("accounts.address").
		Column("accounts.address").
		ColumnExpr("coalesce(metadata, '{}'::jsonb) as metadata").
		Table("accounts").
		Apply(filterPIT(q.PIT, "insertion_date")).
		Order("accounts.address", "revision desc")

	if q.PIT == nil {
		query = query.Join("left join accounts_metadata on accounts_metadata.address = accounts.address")
	} else {
		query = query.Join("left join accounts_metadata on accounts_metadata.address = accounts.address and accounts_metadata.date < ?", q.PIT)
	}

	if q.ExpandVolumes {
		query = query.
			ColumnExpr("volumes.*").
			Join("join get_account_aggregated_volumes(accounts.address, ?) volumes on true", q.PIT)
	}

	if q.ExpandEffectiveVolumes {
		query = query.
			ColumnExpr("effective_volumes.*").
			Join("join get_account_aggregated_effective_volumes(accounts.address, ?) effective_volumes on true", q.PIT)
	}

	return query
}

func (store *Store) accountQueryContext(qb query.Builder) (string, []any, error) {
	metadataRegex := regexp.MustCompile("metadata\\[(.+)\\]")
	balanceRegex := regexp.MustCompile("balance\\[(.*)\\]")

	return qb.Build(query.ContextFn(func(key, operator string, value any) (string, []any, error) {
		switch {
		case key == "address":
			// TODO: Should allow comparison operator only if segments not used
			if operator != "$match" {
				return "", nil, errors.New("'address' column can only be used with $match")
			}
			switch address := value.(type) {
			case string:
				return filterAccountAddress(address, "accounts.address"), nil, nil
			default:
				return "", nil, fmt.Errorf("unexpected type %T for column 'address'", address)
			}
		case metadataRegex.Match([]byte(key)):
			if operator != "$match" {
				return "", nil, errors.New("'account' column can only be used with $match")
			}
			match := metadataRegex.FindAllStringSubmatch(key, 3)

			return "metadata @> ?", []any{map[string]any{
				match[0][1]: value,
			}}, nil
		case balanceRegex.Match([]byte(key)):
			match := balanceRegex.FindAllStringSubmatch(key, 2)

			return fmt.Sprintf(`(
				select balance_from_volumes(post_commit_volumes)
				from moves
				where asset = ? and account_address = accounts.address
				order by seq desc
				limit 1
			) < ?`), []any{match[0][1], value}, nil
		case key == "balance":
			return fmt.Sprintf(`(
				select balance_from_volumes(post_commit_volumes)
				from moves
				where account_address = accounts.address
				order by seq desc
				limit 1
			) < ?`), nil, nil
		default:
			return "", nil, fmt.Errorf("unknown key '%s' when building query", key)
		}
	}))
}

func (store *Store) buildAccountListQuery(selectQuery *bun.SelectQuery, q *GetAccountsQuery) *bun.SelectQuery {
	selectQuery = store.buildAccountQuery(q.Options.Options, selectQuery)

	if q.Options.QueryBuilder != nil {
		where, args, err := store.accountQueryContext(q.Options.QueryBuilder)
		if err != nil {
			// TODO: handle error
			panic(err)
		}
		return selectQuery.Where(where, args...)
	}

	return selectQuery
}

func (store *Store) GetAccountsWithVolumes(ctx context.Context, q *GetAccountsQuery) (*api.Cursor[ledger.ExpandedAccount], error) {
	return paginateWithOffset[PaginatedQueryOptions[PITFilterWithVolumes], ledger.ExpandedAccount](store, ctx,
		(*paginate.OffsetPaginatedQuery[PaginatedQueryOptions[PITFilterWithVolumes]])(q),
		func(query *bun.SelectQuery) *bun.SelectQuery {
			return store.buildAccountListQuery(query, q)
		},
	)
}

func (store *Store) GetAccount(ctx context.Context, address string) (*ledger.Account, error) {
	account, err := fetch[*ledger.Account](store, ctx, func(query *bun.SelectQuery) *bun.SelectQuery {
		return query.
			ColumnExpr("accounts.address").
			ColumnExpr("coalesce(metadata, '{}'::jsonb) as metadata").
			Table("accounts").
			Join("left join accounts_metadata on accounts_metadata.address = accounts.address").
			Where("accounts.address = ?", address).
			Order("revision desc").
			Limit(1)
	})
	if err != nil {
		if storageerrors.IsNotFoundError(err) {
			return pointer.For(ledger.NewAccount(address)), nil
		}
		return nil, err
	}
	return account, nil
}

func (store *Store) GetAccountWithVolumes(ctx context.Context, q GetAccountQuery) (*ledger.ExpandedAccount, error) {
	account, err := fetch[*ledger.ExpandedAccount](store, ctx, func(query *bun.SelectQuery) *bun.SelectQuery {
		query = store.buildAccountQuery(q.PITFilterWithVolumes, query).
			Where("accounts.address = ?", q.Addr).
			Limit(1)

		return query
	})
	if err != nil {
		if storageerrors.IsNotFoundError(err) {
			return pointer.For(ledger.NewExpandedAccount(q.Addr)), nil
		}
		return nil, err
	}
	return account, nil
}

func (store *Store) CountAccounts(ctx context.Context, q *GetAccountsQuery) (uint64, error) {
	return count(store, ctx, func(query *bun.SelectQuery) *bun.SelectQuery {
		return store.buildAccountListQuery(query, q)
	})
}

type GetAccountQuery struct {
	PITFilterWithVolumes
	Addr string
}

func (q GetAccountQuery) WithPIT(pit ledger.Time) GetAccountQuery {
	q.PIT = &pit

	return q
}

func (q GetAccountQuery) WithExpandVolumes() GetAccountQuery {
	q.ExpandVolumes = true

	return q
}

func (q GetAccountQuery) WithExpandEffectiveVolumes() GetAccountQuery {
	q.ExpandEffectiveVolumes = true

	return q
}

func NewGetAccountQuery(addr string) GetAccountQuery {
	return GetAccountQuery{
		Addr: addr,
	}
}

type GetAccountsQuery paginate.OffsetPaginatedQuery[PaginatedQueryOptions[PITFilterWithVolumes]]

func NewGetAccountsQuery(opts PaginatedQueryOptions[PITFilterWithVolumes]) *GetAccountsQuery {
	return &GetAccountsQuery{
		PageSize: opts.PageSize,
		Order:    paginate.OrderAsc,
		Options:  opts,
	}
}
