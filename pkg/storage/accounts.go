package storage

import (
	"github.com/formancehq/stack/libs/go-libs/metadata"
)

type AccountsQuery OffsetPaginatedQuery[AccountsQueryFilters]

type AccountsQueryFilters struct {
	AfterAddress    string            `json:"after"`
	Address         string            `json:"address"`
	Balance         string            `json:"balance"`
	BalanceOperator BalanceOperator   `json:"balanceOperator"`
	Metadata        metadata.Metadata `json:"metadata"`
}

func NewAccountsQuery() AccountsQuery {
	return AccountsQuery{
		PageSize: QueryDefaultPageSize,
		Order:    OrderAsc,
		Filters: AccountsQueryFilters{
			Metadata: metadata.Metadata{},
		},
	}
}

func (a AccountsQuery) WithPageSize(pageSize uint64) AccountsQuery {
	if pageSize != 0 {
		a.PageSize = pageSize
	}

	return a
}

func (a AccountsQuery) WithAfterAddress(after string) AccountsQuery {
	a.Filters.AfterAddress = after

	return a
}

func (a AccountsQuery) WithAddressFilter(address string) AccountsQuery {
	a.Filters.Address = address

	return a
}

func (a AccountsQuery) WithBalanceFilter(balance string) AccountsQuery {
	a.Filters.Balance = balance

	return a
}

func (a AccountsQuery) WithBalanceOperatorFilter(balanceOperator BalanceOperator) AccountsQuery {
	a.Filters.BalanceOperator = balanceOperator

	return a
}

func (a AccountsQuery) WithMetadataFilter(metadata metadata.Metadata) AccountsQuery {
	a.Filters.Metadata = metadata

	return a
}
