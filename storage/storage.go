package storage

import (
	"numary.io/ledger/core"
	"numary.io/ledger/ledger/query"
)

type Store interface {
	AppendTransaction(core.Transaction) error
	CountTransactions() (int64, error)
	FindTransactions(query.Query) ([]core.Transaction, error)
	FindPostings(query.Query) ([]core.Posting, error)
	AggregateBalances(string) (map[string]int64, error)
	CountAccounts() (int64, error)
	FindAccounts(query.Query) ([]core.Account, error)
	Initialize() error
	Close()
}
