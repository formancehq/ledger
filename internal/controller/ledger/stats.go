package ledger

import (
	"context"
	"fmt"
)

type Stats struct {
	Transactions int `json:"transactions"`
	Accounts     int `json:"accounts"`
}

func (ctrl *DefaultController) GetStats(ctx context.Context) (Stats, error) {
	var stats Stats

	transactions, err := ctrl.store.CountTransactions(ctx, NewListTransactionsQuery(NewPaginatedQueryOptions(PITFilterWithVolumes{})))
	if err != nil {
		return stats, fmt.Errorf("counting transactions: %w", err)
	}

	accounts, err := ctrl.store.CountAccounts(ctx, NewListAccountsQuery(NewPaginatedQueryOptions(PITFilterWithVolumes{})))
	if err != nil {
		return stats, fmt.Errorf("counting accounts: %w", err)
	}

	return Stats{
		Transactions: transactions,
		Accounts:     accounts,
	}, nil
}
