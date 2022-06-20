package storage

type BalancesQuery struct {
	Limit        uint
	Offset       uint
	AfterAddress string
	Params       BalancesQueryFilters
}
type BalancesModifier *BalancesQuery

type BalancesQueryFilters struct {
	Address string
}

func NewBalancesQuery(offset uint, limit uint, afterAddress string, filters *BalancesQueryFilters) BalancesQuery {
	q := BalancesQuery{
		Limit: QueryDefaultLimit,
	}

	if limit != 0 {
		q.Limit = limit
	}

	q.AfterAddress = afterAddress
	q.Offset = offset

	q.Params.Address = filters.Address

	return q
}
