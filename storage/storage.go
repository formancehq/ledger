package storage

import (
	"github.com/numary/ledger/core"
	"github.com/numary/ledger/ledger/query"
	"github.com/numary/ledger/storage/postgres"
	"github.com/numary/ledger/storage/sqlite"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
)

type Store interface {
	SaveTransactions([]core.Transaction) error
	CountTransactions() (int64, error)
	FindTransactions(query.Query) (query.Cursor, error)
	GetTransaction(string) (core.Transaction, error)
	AggregateBalances(string) (map[string]int64, error)
	AggregateVolumes(string) (map[string]map[string]int64, error)
	CountAccounts() (int64, error)
	FindAccounts(query.Query) (query.Cursor, error)
	SaveMeta(string, string, string, string, string, string) error
	GetMeta(string, string) (core.Metadata, error)
	FindMeta(query.Query) (query.Cursor, error)
	CountMeta() (int64, error)
	Initialize() error
	Close()
}

func GetStore(name string) (Store, error) {
	driver := viper.GetString("storage.driver")

	switch driver {
	case "sqlite":
		return sqlite.NewStore(name)
	case "postgres":
		return postgres.NewStore(name)
	default:
		break
	}

	panic(errors.Errorf(
		"unsupported store: %s",
		driver,
	))
}
