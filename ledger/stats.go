package ledger

import "context"

type Stats struct {
	Transactions int64 `json:"transactions"`
	Accounts     int64 `json:"accounts"`
}

func (l *Ledger) Stats(ctx context.Context) (Stats, error) {
	var stats Stats

	tt, err := l.store.CountTransactions(ctx)

	if err != nil {
		return stats, err
	}

	ta, err := l.store.CountAccounts(ctx)

	if err != nil {
		return stats, err
	}

	return Stats{
		Transactions: tt,
		Accounts:     ta,
	}, nil
}
