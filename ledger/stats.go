package ledger

type Stats struct {
	Transactions int64 `json:"transactions"`
	Accounts     int64 `json:"accounts"`
}

func (l *Ledger) Stats() Stats {
	tt, _ := l.store.CountTransactions()
	ta, _ := l.store.CountAccounts()

	return Stats{
		Transactions: int64(tt),
		Accounts:     int64(ta),
	}
}
