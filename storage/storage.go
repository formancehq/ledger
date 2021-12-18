package storage

import (
	"context"
	"github.com/numary/ledger/core"
	"github.com/numary/ledger/ledger/query"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
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

func GetStore(name string) (Store, error) {
	driverStr := viper.GetString("storage.driver")
	driver, ok := drivers[driverStr]
	if !ok {
		panic(errors.Errorf(
			"unsupported store: %s",
			driver,
		))
	}
	err := driver.Initialize(context.Background())
	if err != nil {
		return nil, errors.Wrap(err, "initializing driver")
	}

	return driver.NewStore(name)
}
