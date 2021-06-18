package storage

import (
	"numary.io/ledger/core"
	"numary.io/ledger/ledger/query"
)

type Store interface {
	AppendTransaction(core.Transaction) error
	CountTransactions() (int64, error)
	FindTransactions(query.Query) (query.Cursor, error)
	AggregateBalances(string) (map[string]int64, error)
	CountAccounts() (int64, error)
	FindAccounts(query.Query) (query.Cursor, error)
	Initialize() error
	Close()
}
