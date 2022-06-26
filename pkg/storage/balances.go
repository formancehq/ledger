package storage

type BalancesQuery struct {
	Limit        uint
	Offset       uint
	AfterAddress string
	Params       BalancesQueryFilters
}

type BalancesQueryFilters struct {
	Address string
}

func NewBalancesQuery() *BalancesQuery {

	return &BalancesQuery{
		Limit: QueryDefaultLimit,
	}
}

func (b *BalancesQuery) WithLimit(limit uint) *BalancesQuery {
	if limit != 0 {
		b.Limit = limit
	}

	return b
}

func (b *BalancesQuery) WithAfterAddress(after string) *BalancesQuery {
	b.AfterAddress = after

	return b
}

func (b *BalancesQuery) WithOffset(offset uint) *BalancesQuery {
	b.Offset = offset

	return b
}

func (b *BalancesQuery) WithAddressFilter(address string) *BalancesQuery {
	b.Params.Address = address

	return b
}
