package storage

type BalanceOperator string

const (
	BalanceOperatorE   BalanceOperator = "e"
	BalanceOperatorGt  BalanceOperator = "gt"
	BalanceOperatorGte BalanceOperator = "gte"
	BalanceOperatorLt  BalanceOperator = "lt"
	BalanceOperatorLte BalanceOperator = "lte"
	BalanceOperatorNe  BalanceOperator = "ne"

	DefaultBalanceOperator = BalanceOperatorGte
)

func (b BalanceOperator) IsValid() bool {
	switch b {
	case BalanceOperatorE,
		BalanceOperatorGt,
		BalanceOperatorGte,
		BalanceOperatorLt,
		BalanceOperatorNe,
		BalanceOperatorLte:
		return true
	}

	return false
}

func NewBalanceOperator(s string) (BalanceOperator, bool) {
	if !BalanceOperator(s).IsValid() {
		return "", false
	}

	return BalanceOperator(s), true
}

type BalancesQueryFilters struct {
	AfterAddress  string `json:"afterAddress"`
	AddressRegexp string `json:"addressRegexp"`
}

type BalancesQuery OffsetPaginatedQuery[BalancesQueryFilters]

func NewBalancesQuery() BalancesQuery {
	return BalancesQuery{
		PageSize: QueryDefaultPageSize,
		Order:    OrderAsc,
		Filters:  BalancesQueryFilters{},
	}
}

func (q BalancesQuery) GetPageSize() uint64 {
	return q.PageSize
}

func (b BalancesQuery) WithAfterAddress(after string) BalancesQuery {
	b.Filters.AfterAddress = after

	return b
}

func (b BalancesQuery) WithAddressFilter(address string) BalancesQuery {
	b.Filters.AddressRegexp = address

	return b
}

func (b BalancesQuery) WithPageSize(pageSize uint64) BalancesQuery {
	b.PageSize = pageSize
	return b
}
