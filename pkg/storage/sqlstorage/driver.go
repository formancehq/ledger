package sqlstorage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/huandu/go-sqlbuilder"
	"github.com/numary/ledger/pkg/logging"
	"github.com/numary/ledger/pkg/storage"
)

var sqlDrivers = map[Flavor]struct {
	driverName string
}{}

func UpdateSQLDriverMapping(flavor Flavor, name string) {
	cfg := sqlDrivers[flavor]
	cfg.driverName = name
	sqlDrivers[flavor] = cfg
}

func SQLDriverName(f Flavor) string {
	return sqlDrivers[f].driverName
}

func init() {
	// Default mapping for app driver/sql driver
	UpdateSQLDriverMapping(SQLite, "sqlite3")
	UpdateSQLDriverMapping(PostgreSQL, "pgx")
}

type ConnStringResolver func(name string) string

// openCloseDBDriver is a driver which connect on the database each time the NewStore() method is called.
// Therefore, the provided store is configured to close the *sql.DB instance when the Close() method of the store is called.
// It is suitable for databases engines like SQLite
type openCloseDBDriver struct {
	name       string
	connString ConnStringResolver
	flavor     Flavor
	logger     logging.Logger
}

func (d *openCloseDBDriver) Name() string {
	return d.name
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
	return NewStore(name, sqlbuilder.Flavor(d.flavor), db, d.logger, func(ctx context.Context) error {
		return db.Close()
	})
}

func (d *openCloseDBDriver) Close(ctx context.Context) error {
	return nil
}

func NewOpenCloseDBDriver(logger logging.Logger, name string, flavor Flavor, connString ConnStringResolver) *openCloseDBDriver {
	return &openCloseDBDriver{
		flavor:     flavor,
		connString: connString,
		name:       name,
		logger:     logger,
	}
}

// cachedDBDriver is a driver which connect on a database and keep the connection open until closed
// it suitable for databases engines like PostgreSQL or MySQL
// Therefore, the NewStore() method return stores backed with the same underlying *sql.DB instance.
type cachedDBDriver struct {
	name   string
	where  string
	db     *sql.DB
	flavor Flavor
	logger logging.Logger
}

func (s *cachedDBDriver) Name() string {
	return s.name
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
	return NewStore(name, sqlbuilder.Flavor(s.flavor), s.db, s.logger, func(ctx context.Context) error {
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

func SQLiteFileConnString(path string) string {
	return fmt.Sprintf(
		"file:%s?_journal=WAL",
		path,
	)
}

func NewCachedDBDriver(logger logging.Logger, name string, flavor Flavor, where string) *cachedDBDriver {
	return &cachedDBDriver{
		where:  where,
		name:   name,
		flavor: flavor,
		logger: logger,
	}
}

func NewInMemorySQLiteDriver(logger logging.Logger) *cachedDBDriver {
	return NewCachedDBDriver(logger, "sqlite", SQLite, SQLiteMemoryConnString)
}
