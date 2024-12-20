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

	transactions, err := ctrl.store.Transactions().Count(ctx, ResourceQuery[any]{})
	if err != nil {
		return stats, fmt.Errorf("counting transactions: %w", err)
	}

	accounts, err := ctrl.store.Accounts().Count(ctx, ResourceQuery[any]{})
	if err != nil {
		return stats, fmt.Errorf("counting accounts: %w", err)
	}

	return Stats{
		Transactions: transactions,
		Accounts:     accounts,
	}, nil
}
