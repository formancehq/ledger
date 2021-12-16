package storage

import (
	"context"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/numary/ledger/core"
	"github.com/numary/ledger/ledger/query"
	"github.com/numary/ledger/storage/postgres"
	"github.com/numary/ledger/storage/sqlite"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"log"
	"sync"
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
	SaveMeta(string, string, string, string, string, string) error
	GetMeta(string, string) (core.Metadata, error)
	CountMeta() (int64, error)
	Initialize() error
	Name() string
	Close()
}

type Driver interface {
	Initialize() error
	NewStore(name string) (Store, error)
}

var builtInDrivers = make(map[string]Driver)

func RegisterDriver(name string, driver Driver) {
	builtInDrivers[name] = driver
}

type SQLiteSDriver struct {}

func (d *SQLiteSDriver) Initialize() error {
	return nil
}

func (d *SQLiteSDriver) NewStore(name string) (Store, error) {
	return sqlite.NewStore(name)
}

type PGSqlDriver struct {
	once sync.Once
	pool *pgxpool.Pool
}

func (d *PGSqlDriver) Initialize() error {
	errCh := make(chan error, 1)
	d.once.Do(func() {
		log.Println("initiating postgres pool")

		pool, err := pgxpool.Connect(
			context.Background(),
			viper.GetString("storage.postgres.conn_string"),
		)
		if err != nil {
			errCh <- err
		}
		d.pool = pool
		errCh <- nil
	})
	select {
	case err := <- errCh:
		return err
	default:
		return nil
	}
}

func (d *PGSqlDriver) NewStore(name string) (Store, error) {
	return postgres.NewStore(name, d.pool)
}

func init() {
	RegisterDriver("sqlite", &SQLiteSDriver{})
	RegisterDriver("postgres", &PGSqlDriver{})
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

type Factory interface {
	GetStore(name string) (Store, error)
}
type FactoryFn func(string) (Store, error)

func (f FactoryFn) GetStore(name string) (Store, error) {
	return f(name)
}

var DefaultFactory Factory = FactoryFn(GetStore)

func NewDefaultFactory() (Factory, error) {
	return DefaultFactory, nil
}
