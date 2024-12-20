package legacy

import (
	"github.com/formancehq/go-libs/v2/bun/bunpaginate"
	"github.com/formancehq/go-libs/v2/pointer"
	"github.com/formancehq/go-libs/v2/query"
	"github.com/formancehq/go-libs/v2/time"
	"github.com/formancehq/ledger/internal/controller/ledger"
)

type PITFilter struct {
	PIT *time.Time `json:"pit"`
	OOT *time.Time `json:"oot"`
}

type PITFilterWithVolumes struct {
	PITFilter
	ExpandVolumes          bool `json:"volumes"`
	ExpandEffectiveVolumes bool `json:"effectiveVolumes"`
}

type FiltersForVolumes struct {
	PITFilter
	UseInsertionDate bool
	GroupLvl         int
}

func NewGetVolumesWithBalancesQuery(opts ledger.PaginatedQueryOptions[FiltersForVolumes]) GetVolumesWithBalancesQuery {
	return GetVolumesWithBalancesQuery{
		PageSize: opts.PageSize,
		Order:    bunpaginate.OrderAsc,
		Options:  opts,
	}
}

type ListTransactionsQuery bunpaginate.ColumnPaginatedQuery[ledger.PaginatedQueryOptions[PITFilterWithVolumes]]

func (q ListTransactionsQuery) WithColumn(column string) ListTransactionsQuery {
	ret := pointer.For((bunpaginate.ColumnPaginatedQuery[ledger.PaginatedQueryOptions[PITFilterWithVolumes]])(q))
	ret = ret.WithColumn(column)

	return ListTransactionsQuery(*ret)
}

func NewListTransactionsQuery(options ledger.PaginatedQueryOptions[PITFilterWithVolumes]) ListTransactionsQuery {
	return ListTransactionsQuery{
		PageSize: options.PageSize,
		Column:   "id",
		Order:    bunpaginate.OrderDesc,
		Options:  options,
	}
}

type GetTransactionQuery struct {
	PITFilterWithVolumes
	ID int
}

func (q GetTransactionQuery) WithExpandVolumes() GetTransactionQuery {
	q.ExpandVolumes = true

	return q
}

func (q GetTransactionQuery) WithExpandEffectiveVolumes() GetTransactionQuery {
	q.ExpandEffectiveVolumes = true

	return q
}

func NewGetTransactionQuery(id int) GetTransactionQuery {
	return GetTransactionQuery{
		PITFilterWithVolumes: PITFilterWithVolumes{},
		ID:                   id,
	}
}

type ListAccountsQuery bunpaginate.OffsetPaginatedQuery[ledger.PaginatedQueryOptions[PITFilterWithVolumes]]

func (q ListAccountsQuery) WithExpandVolumes() ListAccountsQuery {
	q.Options.Options.ExpandVolumes = true

	return q
}

func (q ListAccountsQuery) WithExpandEffectiveVolumes() ListAccountsQuery {
	q.Options.Options.ExpandEffectiveVolumes = true

	return q
}

func NewListAccountsQuery(opts ledger.PaginatedQueryOptions[PITFilterWithVolumes]) ListAccountsQuery {
	return ListAccountsQuery{
		PageSize: opts.PageSize,
		Order:    bunpaginate.OrderAsc,
		Options:  opts,
	}
}

type GetAccountQuery struct {
	PITFilterWithVolumes
	Addr string
}

func (q GetAccountQuery) WithPIT(pit time.Time) GetAccountQuery {
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

type GetAggregatedBalanceQuery struct {
	PITFilter
	QueryBuilder     query.Builder
	UseInsertionDate bool
}

func NewGetAggregatedBalancesQuery(filter PITFilter, qb query.Builder, useInsertionDate bool) GetAggregatedBalanceQuery {
	return GetAggregatedBalanceQuery{
		PITFilter:        filter,
		QueryBuilder:     qb,
		UseInsertionDate: useInsertionDate,
	}
}

type GetVolumesWithBalancesQuery bunpaginate.OffsetPaginatedQuery[ledger.PaginatedQueryOptions[FiltersForVolumes]]

type GetLogsQuery bunpaginate.ColumnPaginatedQuery[ledger.PaginatedQueryOptions[any]]

func (q GetLogsQuery) WithOrder(order bunpaginate.Order) GetLogsQuery {
	q.Order = order
	return q
}

func NewListLogsQuery(options ledger.PaginatedQueryOptions[any]) GetLogsQuery {
	return GetLogsQuery{
		PageSize: options.PageSize,
		Column:   "id",
		Order:    bunpaginate.OrderDesc,
		Options:  options,
	}
}
