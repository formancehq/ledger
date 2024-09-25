package engine

import (
	"context"

	"github.com/formancehq/ledger/v2/internal/storage/ledgerstore"
	"github.com/pkg/errors"
)

type Stats struct {
	Transactions int `json:"transactions"`
	Accounts     int `json:"accounts"`
}

func (l *Ledger) Stats(ctx context.Context) (Stats, error) {
	var stats Stats

	transactions, err := l.store.CountTransactions(ctx, ledgerstore.NewGetTransactionsQuery(ledgerstore.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{})))
	if err != nil {
		return stats, errors.Wrap(err, "counting transactions")
	}

	accounts, err := l.store.CountAccounts(ctx, ledgerstore.NewGetAccountsQuery(ledgerstore.NewPaginatedQueryOptions(ledgerstore.PITFilterWithVolumes{})))
	if err != nil {
		return stats, errors.Wrap(err, "counting accounts")
	}

	return Stats{
		Transactions: transactions,
		Accounts:     accounts,
	}, nil
}
