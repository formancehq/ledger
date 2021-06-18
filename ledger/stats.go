package ledger

type Stats struct {
	Transactions int64 `json:"transactions"`
	Accounts     int64 `json:"accounts"`
}

func (l *Ledger) Stats() (Stats, error) {
	var stats Stats

	tt, err := l.store.CountTransactions()

	if err != nil {
		return stats, err
	}

	ta, err := l.store.CountAccounts()

	if err != nil {
		return stats, err
	}

	return Stats{
		Transactions: int64(tt),
		Accounts:     int64(ta),
	}, nil
}
