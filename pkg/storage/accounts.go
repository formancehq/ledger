package storage

type AccountsQuery struct {
	Limit        uint
	Offset       uint
	AfterAddress string
	Filters      AccountsQueryFilters
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

func NewAccountsQuery() *AccountsQuery {

	return &AccountsQuery{
		Limit: QueryDefaultLimit,
	}
}

func (a *AccountsQuery) WithLimit(limit uint) *AccountsQuery {
	if limit != 0 {
		a.Limit = limit
	}

	return a
}

func (a *AccountsQuery) WithOffset(offset uint) *AccountsQuery {
	a.Offset = offset

	return a
}

func (a *AccountsQuery) WithAfterAddress(after string) *AccountsQuery {
	a.AfterAddress = after

	return a
}

func (a *AccountsQuery) WithAddressFilter(address string) *AccountsQuery {
	a.Filters.Address = address

	return a
}

func (a *AccountsQuery) WithBalanceFilter(balance string) *AccountsQuery {
	a.Filters.Balance = balance

	return a
}

func (a *AccountsQuery) WithBalanceOperatorFilter(balanceOperator BalanceOperator) *AccountsQuery {
	a.Filters.BalanceOperator = balanceOperator

	return a
}

func (a *AccountsQuery) WithMetadataFilter(metadata map[string]string) *AccountsQuery {
	a.Filters.Metadata = metadata

	return a
}
