package storage

type BalancesQuery struct {
	PageSize     uint
	Offset       uint
	AfterAddress string
	Filters      BalancesQueryFilters
}

type BalancesQueryFilters struct {
	AddressRegexp string
}

func NewBalancesQuery() *BalancesQuery {
	return &BalancesQuery{
		PageSize: QueryDefaultPageSize,
	}
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
	b.Filters.AddressRegexp = address

	return b
}

func (b *BalancesQuery) WithPageSize(pageSize uint) *BalancesQuery {
	b.PageSize = pageSize
	return b
}
