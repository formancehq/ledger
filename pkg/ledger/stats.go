package ledger

import (
	"context"

	"github.com/numary/ledger/pkg/ledger/query"
)

type Stats struct {
	Transactions uint64 `json:"transactions"`
	Accounts     uint64 `json:"accounts"`
}

func (l *Ledger) Stats(ctx context.Context) (Stats, error) {
	var stats Stats

	tt, err := l.store.CountTransactions(ctx, query.Query{})

	if err != nil {
		return stats, err
	}

	ta, err := l.store.CountAccounts(ctx, query.Query{})

	if err != nil {
		return stats, err
	}

	return Stats{
		Transactions: tt,
		Accounts:     ta,
	}, nil
}
