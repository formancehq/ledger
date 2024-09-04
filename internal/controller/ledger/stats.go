package ledger

import (
	"context"

	"github.com/pkg/errors"
)

type Stats struct {
	Transactions int `json:"transactions"`
	Accounts     int `json:"accounts"`
}

func (ctrl *DefaultController) Stats(ctx context.Context) (Stats, error) {
	var stats Stats

	transactions, err := ctrl.store.CountTransactions(ctx, NewListTransactionsQuery(NewPaginatedQueryOptions(PITFilterWithVolumes{})))
	if err != nil {
		return stats, errors.Wrap(err, "counting transactions")
	}

	accounts, err := ctrl.store.CountAccounts(ctx, NewListAccountsQuery(NewPaginatedQueryOptions(PITFilterWithVolumes{})))
	if err != nil {
		return stats, errors.Wrap(err, "counting accounts")
	}

	return Stats{
		Transactions: transactions,
		Accounts:     accounts,
	}, nil
}
