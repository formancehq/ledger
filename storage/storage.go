package storage

import (
	"github.com/numary/ledger/core"
	"github.com/numary/ledger/ledger/query"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
)

type Store interface {
	LoadState() (*core.State, error)
	SaveTransactions([]core.Transaction) error
	CountTransactions() (int64, error)
	FindTransactions(query.Query) (query.Cursor, error)
	GetTransaction(string) (core.Transaction, error)
	AggregateBalances(string) (map[string]int64, error)
	AggregateVolumes(string) (map[string]map[string]int64, error)
	CountAccounts() (int64, error)
	FindAccounts(query.Query) (query.Cursor, error)
	SaveMeta(int64, string, string, string, string, string) error
	GetMeta(string, string) (core.Metadata, error)
	CountMeta() (int64, error)
	Initialize() error
	Name() string
	Close()
}

func GetStore(name string) (Store, error) {
	driverStr := viper.GetString("storage.driver")
	driver, ok := builtInDrivers[driverStr]
	if !ok {
		panic(errors.Errorf(
			"unsupported store: %s",
			driver,
		))
	}
	err := driver.Initialize()
	if err != nil {
		return nil, errors.Wrap(err, "initializing driver")
	}

	return driver.NewStore(name)
}
