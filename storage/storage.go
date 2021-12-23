package storage

import (
	"context"
	"github.com/numary/ledger/core"
	"github.com/numary/ledger/ledger/query"
)

type Store interface {
	LastTransaction(context.Context) (*core.Transaction, error)
	LastMetaID(context.Context) (int64, error)
	SaveTransactions(context.Context, []core.Transaction) error
	CountTransactions(context.Context) (int64, error)
	FindTransactions(context.Context, query.Query) (query.Cursor, error)
	GetTransaction(context.Context, string) (core.Transaction, error)
	AggregateBalances(context.Context, string) (map[string]int64, error)
	AggregateVolumes(context.Context, string) (map[string]map[string]int64, error)
	CountAccounts(context.Context) (int64, error)
	FindAccounts(context.Context, query.Query) (query.Cursor, error)
	SaveMeta(context.Context, int64, string, string, string, string, string) error
	GetMeta(context.Context, string, string) (core.Metadata, error)
	CountMeta(context.Context) (int64, error)
	Initialize(context.Context) error
	Name() string
	Close(context.Context) error
}
