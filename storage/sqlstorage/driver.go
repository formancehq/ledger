package sqlstorage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/huandu/go-sqlbuilder"
	"github.com/numary/ledger/storage"
)

type Flavor = sqlbuilder.Flavor

var (
	SQLite     = sqlbuilder.SQLite
	PostgreSQL = sqlbuilder.PostgreSQL
	MySQL      = sqlbuilder.MySQL
)

var sqlDrivers = map[Flavor]struct {
	driverName string
}{
	SQLite: {
		driverName: "sqlite3",
	},
	PostgreSQL: {
		driverName: "pgx",
	},
}

type ConnStringResolver func(name string) string

// cachedDBDriver is a driver which connect on the database each time the NewStore() method is called.
// Therefore, the provided store is configured to close the *sql.DB instance when the Close() method of the store is called.
// It is suitable for databases engines like SQLite
type openCloseDBDriver struct {
	connString ConnStringResolver
	flavor     Flavor
}

func (d *openCloseDBDriver) Initialize(ctx context.Context) error {
	return nil
}

func (d *openCloseDBDriver) NewStore(name string) (storage.Store, error) {
	cfg, ok := sqlDrivers[d.flavor]
	if !ok {
		return nil, fmt.Errorf("unsupported flavor %s", d.flavor)
	}
	db, err := sql.Open(cfg.driverName, d.connString(name))
	if err != nil {
		return nil, err
	}
	return NewStore(name, d.flavor, db, func(ctx context.Context) error {
		return db.Close()
	})
}

func (d *openCloseDBDriver) Close(ctx context.Context) error {
	return nil
}

func NewOpenCloseDBDriver(flavor Flavor, connString ConnStringResolver) *openCloseDBDriver {
	return &openCloseDBDriver{
		flavor:     flavor,
		connString: connString,
	}
}

// cachedDBDriver is a driver which connect on a database and keep the connection open until closed
// it suitable for databases engines like PostgreSQL or MySQL
// Therefore, the NewStore() method return stores backed with the same underlying *sql.DB instance.
type cachedDBDriver struct {
	where  string
	db     *sql.DB
	flavor Flavor
}

func (s *cachedDBDriver) Initialize(ctx context.Context) error {

	cfg, ok := sqlDrivers[s.flavor]
	if !ok {
		return errors.New("unknown flavor")
	}

	db, err := sql.Open(cfg.driverName, s.where)
	if err != nil {
		return err
	}
	s.db = db
	return nil
}

func (s *cachedDBDriver) NewStore(name string) (storage.Store, error) {
	return NewStore(name, s.flavor, s.db, func(ctx context.Context) error {
		return nil
	})
}

func (d *cachedDBDriver) Close(ctx context.Context) error {
	if d.db == nil {
		return nil
	}
	return d.db.Close()
}

const SQLiteMemoryConnString = "file::memory:?cache=shared"

func NewCachedDBDriver(flavor Flavor, where string) *cachedDBDriver {
	return &cachedDBDriver{
		where:  where,
		flavor: flavor,
	}
}

func NewInMemorySQLiteDriver() *cachedDBDriver {
	return &cachedDBDriver{
		where:  SQLiteMemoryConnString,
		flavor: SQLite,
	}
}

func RegisterInMemorySQLite() {
	storage.RegisterDriver("sqlite-memory", NewInMemorySQLiteDriver())
}

func init() {
	RegisterInMemorySQLite()
}
