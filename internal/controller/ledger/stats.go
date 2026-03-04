package ledger

import (
	"context"
	"fmt"
	"github.com/formancehq/ledger/internal/storage/common"
)

type Stats struct {
	Transactions int `json:"transactions"`
	Accounts     int `json:"accounts"`
}

func (ctrl *DefaultController) GetStats(ctx context.Context) (Stats, error) {
	var stats Stats

	transactions, err := ctrl.store.Transactions().Count(ctx, common.ResourceQuery[any]{})
	if err != nil {
		return stats, fmt.Errorf("counting transactions: %w", err)
	}

	accounts, err := ctrl.store.Accounts().Count(ctx, common.ResourceQuery[any]{})
	if err != nil {
		return stats, fmt.Errorf("counting accounts: %w", err)
	}

	return Stats{
		Transactions: transactions,
		Accounts:     accounts,
	}, nil
}
