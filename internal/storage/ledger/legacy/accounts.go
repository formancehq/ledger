package legacy

import (
	"context"
	"errors"
	"fmt"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"regexp"

	"github.com/formancehq/go-libs/v2/bun/bunpaginate"

	"github.com/formancehq/go-libs/v2/query"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/uptrace/bun"
)

func (store *Store) buildAccountQuery(q PITFilterWithVolumes, query *bun.SelectQuery) *bun.SelectQuery {

	query = query.
		Column("accounts.address", "accounts.first_usage").
		Where("accounts.ledger = ?", store.name).
		Apply(filterPIT(q.PIT, "first_usage")).
		Order("accounts.address").
		ModelTableExpr(store.GetPrefixedRelationName("accounts"))

	if q.PIT != nil && !q.PIT.IsZero() {
		query = query.
			Column("accounts.address").
			ColumnExpr(`coalesce(accounts_metadata.metadata, '{}'::jsonb) as metadata`).
			Join(`
				left join lateral (
					select metadata, accounts_seq
					from `+store.GetPrefixedRelationName("accounts_metadata")+`
					where accounts_metadata.accounts_seq = accounts.seq and accounts_metadata.date < ?
					order by revision desc 
					limit 1
				) accounts_metadata on true
			`, q.PIT)
	} else {
		query = query.ColumnExpr("accounts.metadata")
	}

	if q.ExpandVolumes {
		query = query.
			ColumnExpr("volumes.*").
			Join(`join `+store.GetPrefixedRelationName("get_account_aggregated_volumes")+`(?, accounts.address, ?) volumes on true`, store.name, q.PIT)
	}

	if q.ExpandEffectiveVolumes {
		query = query.
			ColumnExpr("effective_volumes.*").
			Join(`join `+store.GetPrefixedRelationName("get_account_aggregated_effective_volumes")+`(?, accounts.address, ?) effective_volumes on true`, store.name, q.PIT)
	}

	return query
}

func (store *Store) accountQueryContext(qb query.Builder, q ListAccountsQuery) (string, []any, error) {
	metadataRegex := regexp.MustCompile(`metadata\[(.+)]`)
	balanceRegex := regexp.MustCompile(`balance\[(.*)]`)

	return qb.Build(query.ContextFn(func(key, operator string, value any) (string, []any, error) {
		convertOperatorToSQL := func() string {
			switch operator {
			case "$match":
				return "="
			case "$lt":
				return "<"
			case "$gt":
				return ">"
			case "$lte":
				return "<="
			case "$gte":
				return ">="
			}
			panic("unreachable")
		}
		switch {
		case key == "address":
			if operator != "$match" {
				return "", nil, errors.New("'address' column can only be used with $match")
			}
			switch address := value.(type) {
			case string:
				return filterAccountAddress(address, "accounts.address"), nil, nil
			default:
				return "", nil, newErrInvalidQuery("unexpected type %T for column 'address'", address)
			}
		case metadataRegex.Match([]byte(key)):
			if operator != "$match" {
				return "", nil, newErrInvalidQuery("'account' column can only be used with $match")
			}
			match := metadataRegex.FindAllStringSubmatch(key, 3)

			key := "metadata"
			if q.Options.Options.PIT != nil && !q.Options.Options.PIT.IsZero() {
				key = "accounts_metadata.metadata"
			}

			return key + " @> ?", []any{map[string]any{
				match[0][1]: value,
			}}, nil
		case balanceRegex.Match([]byte(key)):
			match := balanceRegex.FindAllStringSubmatch(key, 2)

			return fmt.Sprintf(`(
				select `+store.GetPrefixedRelationName("balance_from_volumes")+`(post_commit_volumes)
				from `+store.GetPrefixedRelationName("moves")+`
				where asset = ? and accounts_address = accounts.address and ledger = ?
				order by seq desc
				limit 1
			) %s ?`, convertOperatorToSQL()), []any{match[0][1], store.name, value}, nil
		case key == "balance":
			return fmt.Sprintf(`(
				select `+store.GetPrefixedRelationName("balance_from_volumes")+`(post_commit_volumes)
				from `+store.GetPrefixedRelationName("moves")+`
				where accounts_address = accounts.address and ledger = ?
				order by seq desc
				limit 1
			) %s ?`, convertOperatorToSQL()), []any{store.name, value}, nil

		case key == "metadata":
			if operator != "$exists" {
				return "", nil, newErrInvalidQuery("'metadata' key filter can only be used with $exists")
			}
			if q.Options.Options.PIT != nil && !q.Options.Options.PIT.IsZero() {
				key = "accounts_metadata.metadata"
			}

			return fmt.Sprintf("%s -> ? IS NOT NULL", key), []any{value}, nil
		default:
			return "", nil, newErrInvalidQuery("unknown key '%s' when building query", key)
		}
	}))
}

func (store *Store) buildAccountListQuery(selectQuery *bun.SelectQuery, q ListAccountsQuery, where string, args []any) *bun.SelectQuery {
	selectQuery = store.buildAccountQuery(q.Options.Options, selectQuery)

	if where != "" {
		return selectQuery.Where(where, args...)
	}

	return selectQuery
}

func (store *Store) GetAccountsWithVolumes(ctx context.Context, q ListAccountsQuery) (*bunpaginate.Cursor[ledger.Account], error) {
	var (
		where string
		args  []any
		err   error
	)
	if q.Options.QueryBuilder != nil {
		where, args, err = store.accountQueryContext(q.Options.QueryBuilder, q)
		if err != nil {
			return nil, err
		}
	}

	return paginateWithOffset[ledgercontroller.PaginatedQueryOptions[PITFilterWithVolumes], ledger.Account](store, ctx,
		(*bunpaginate.OffsetPaginatedQuery[ledgercontroller.PaginatedQueryOptions[PITFilterWithVolumes]])(&q),
		func(query *bun.SelectQuery) *bun.SelectQuery {
			return store.buildAccountListQuery(query, q, where, args)
		},
	)
}

func (store *Store) GetAccountWithVolumes(ctx context.Context, q GetAccountQuery) (*ledger.Account, error) {
	account, err := fetch[*ledger.Account](store, true, ctx, func(query *bun.SelectQuery) *bun.SelectQuery {
		query = store.buildAccountQuery(q.PITFilterWithVolumes, query).
			Where("accounts.address = ?", q.Addr).
			Limit(1)

		return query
	})
	if err != nil {
		return nil, err
	}
	return account, nil
}

func (store *Store) CountAccounts(ctx context.Context, q ListAccountsQuery) (int, error) {
	var (
		where string
		args  []any
		err   error
	)
	if q.Options.QueryBuilder != nil {
		where, args, err = store.accountQueryContext(q.Options.QueryBuilder, q)
		if err != nil {
			return 0, err
		}
	}

	return count[ledger.Account](store, true, ctx, func(query *bun.SelectQuery) *bun.SelectQuery {
		return store.buildAccountListQuery(query, q, where, args)
	})
}
