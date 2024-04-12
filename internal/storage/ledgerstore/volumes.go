package ledgerstore

import (
	"context"
	"fmt"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/stack/libs/go-libs/bun/bunpaginate"
	lquery "github.com/formancehq/stack/libs/go-libs/query"
	"github.com/uptrace/bun"
)

func (store *Store) volumesQueryContext(qb lquery.Builder, q GetVolumesWithBalancesQuery) (string, []any, error) {

	var (
		subQuery string
		args     []any
		err      error
	)

	if q.Options.QueryBuilder != nil {
		subQuery, args, err = q.Options.QueryBuilder.Build(lquery.ContextFn(func(key, operator string, value any) (string, []any, error) {
			switch {
			case key == "account":
				// TODO: Should allow comparison operator only if segments not used
				if operator != "$match" {
					return "", nil, newErrInvalidQuery("'address' column can only be used with $match")
				}

				switch address := value.(type) {
				case string:
					return filterAccountAddress(address, "account_address"), nil, nil
				default:
					return "", nil, newErrInvalidQuery("unexpected type %T for column 'address'", address)
				}
			case metadataRegex.Match([]byte(key)):
				if operator != "$match" {
					return "", nil, newErrInvalidQuery("'metadata' column can only be used with $match")
				}
				match := metadataRegex.FindAllStringSubmatch(key, 3)
				key := "accounts.metadata"

				return key + " @> ?", []any{map[string]any{
					match[0][1]: value,
				}}, nil
			default:
				return "", nil, newErrInvalidQuery("unknown key '%s' when building query", key)
			}
		}))
		if err != nil {
			return "", nil, err
		}
	}

	return subQuery, args, nil

}

func (store *Store) buildVolumesWithBalancesQuery(query *bun.SelectQuery, q GetVolumesWithBalancesQuery, where string, args []any) *bun.SelectQuery {

	filtersForVolumes := q.Options.Options
	dateFilterColumn := "effective_date"

	if filtersForVolumes.UseInsertionDate {
		dateFilterColumn = "insertion_date"
	}

	query = query.
		Column("asset").
		ColumnExpr("sum(case when not is_source then amount else 0 end) as input").
		ColumnExpr("sum(case when is_source then amount else 0 end) as output").
		ColumnExpr("sum(case when not is_source then amount else -amount end) as balance").
		Table("moves")

	if filtersForVolumes.GroupLvl > 0 {
		query = query.ColumnExpr(fmt.Sprintf(`(array_to_string((string_to_array(account_address, ':'))[1:LEAST(array_length(string_to_array(account_address, ':'),1),%d)],':')) as account`, filtersForVolumes.GroupLvl))
	} else {
		query = query.ColumnExpr("account_address as account")
	}

	if where != "" {
		query = query.
			Join(`join lateral (	
			select metadata
			from accounts a 
			where a.seq = moves.accounts_seq
		) accounts on true`).
			Where(where, args...)
	}

	query = query.
		Where("ledger = ?", store.name).
		Apply(filterPIT(filtersForVolumes.PIT, dateFilterColumn)).
		Apply(filterOOT(filtersForVolumes.OOT, dateFilterColumn)).
		GroupExpr("account, asset")

	return query
}

func (store *Store) GetVolumesWithBalances(ctx context.Context, q GetVolumesWithBalancesQuery) (*bunpaginate.Cursor[ledger.VolumesWithBalanceByAssetByAccount], error) {
	var (
		where string
		args  []any
		err   error
	)
	if q.Options.QueryBuilder != nil {
		where, args, err = store.volumesQueryContext(q.Options.QueryBuilder, q)
		if err != nil {
			return nil, err
		}
	}

	return paginateWithOffsetWithoutModel[PaginatedQueryOptions[FiltersForVolumes], ledger.VolumesWithBalanceByAssetByAccount](
		store, ctx, (*bunpaginate.OffsetPaginatedQuery[PaginatedQueryOptions[FiltersForVolumes]])(&q),
		func(query *bun.SelectQuery) *bun.SelectQuery {
			return store.buildVolumesWithBalancesQuery(query, q, where, args)
		},
	)
}

type GetVolumesWithBalancesQuery bunpaginate.OffsetPaginatedQuery[PaginatedQueryOptions[FiltersForVolumes]]

func NewGetVolumesWithBalancesQuery(opts PaginatedQueryOptions[FiltersForVolumes]) GetVolumesWithBalancesQuery {
	return GetVolumesWithBalancesQuery{
		PageSize: opts.PageSize,
		Order:    bunpaginate.OrderAsc,
		Options:  opts,
	}
}
