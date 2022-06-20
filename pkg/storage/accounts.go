package storage

type AccountsQuery struct {
	Limit        uint
	Offset       uint
	AfterAddress string
	Params       AccountsQueryFilters
}

type AccountsQueryFilters struct {
	Address         string
	Balance         string
	BalanceOperator BalanceOperator
	Metadata        map[string]string
}

type BalanceOperator string

const (
	BalanceOperatorE   BalanceOperator = "e"
	BalanceOperatorGt  BalanceOperator = "gt"
	BalanceOperatorGte BalanceOperator = "gte"
	BalanceOperatorLt  BalanceOperator = "lt"
	BalanceOperatorLte BalanceOperator = "lte"

	DefaultBalanceOperator = BalanceOperatorGte
)

func (b BalanceOperator) IsValid() bool {
	switch b {
	case BalanceOperatorE,
		BalanceOperatorGt,
		BalanceOperatorGte,
		BalanceOperatorLt,
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

func NewAccountsQuery(offset uint, limit uint, afterAddress string, filters *AccountsQueryFilters) AccountsQuery {
	q := AccountsQuery{
		Limit: QueryDefaultLimit,
	}

	// i'd rather use pointers and check if nil, but c.Query returns objects, so for now
	// i'm testing zero values of object, please fix if you find something better

	if limit != 0 {
		q.Limit = limit
	}

	q.AfterAddress = afterAddress
	q.Offset = offset

	q.Params.Address = filters.Address
	q.Params.Balance = filters.Balance
	q.Params.BalanceOperator = filters.BalanceOperator
	q.Params.Metadata = filters.Metadata

	return q
}
