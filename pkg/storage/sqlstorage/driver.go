package sqlstorage

import (
	"context"
	"fmt"
	"github.com/huandu/go-sqlbuilder"
	"github.com/numary/ledger/pkg/storage"
	"github.com/pkg/errors"
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

//type ConnStringResolver func(name string) string

// openCloseDBDriver is a driver which connect on the database each time the NewStore() method is called.
// Therefore, the provided store is configured to close the *sql.DB instance when the Close() method of the store is called.
// It is suitable for databases engines like SQLite
type openCloseDBDriver struct {
	name      string
	flavor    Flavor
	dbFactory func() (DB, error)
}

func (d *openCloseDBDriver) Name() string {
	return d.name
}

func (d *openCloseDBDriver) Initialize(ctx context.Context) error {
	return nil
}

func (d *openCloseDBDriver) NewStore(ctx context.Context, name string) (storage.Store, error) {
	db, err := d.dbFactory()
	if err != nil {
		return nil, err
	}

	schema, err := db.Schema(ctx, name)
	if err != nil {
		return nil, err
	}

	return NewStore(name, sqlbuilder.Flavor(d.flavor), schema, func(ctx context.Context) error {
		return schema.Close(context.Background())
	})
}

func (d *openCloseDBDriver) Close(ctx context.Context) error {
	return nil
}

func (d *openCloseDBDriver) Check(ctx context.Context) error {
	return nil
}

func NewOpenCloseDBDriver(name string, flavor Flavor, dbFactory func() (DB, error)) *openCloseDBDriver {
	return &openCloseDBDriver{
		flavor:    flavor,
		name:      name,
		dbFactory: dbFactory,
	}
}

// cachedDBDriver is a driver which connect on a database and keep the connection open until closed
// it suitable for databases engines like PostgreSQL or MySQL
// Therefore, the NewStore() method return stores backed with the same underlying *sql.DB instance.
type cachedDBDriver struct {
	name      string
	dbFactory func() (DB, error)
	db        DB
	flavor    Flavor
}

func (s *cachedDBDriver) Name() string {
	return s.name
}

func (s *cachedDBDriver) Initialize(ctx context.Context) error {
	if s.db != nil {
		return errors.New("database already initialized")
	}
	db, err := s.dbFactory()
	if err != nil {
		return err
	}
	s.db = db
	return nil
}

func (s *cachedDBDriver) NewStore(ctx context.Context, name string) (storage.Store, error) {
	schema, err := s.db.Schema(ctx, name)
	if err != nil {
		return nil, err
	}
	return NewStore(name, sqlbuilder.Flavor(s.flavor), schema, func(ctx context.Context) error {
		return nil
	})
}

func (d *cachedDBDriver) Close(ctx context.Context) error {
	if d.db == nil {
		return nil
	}
	err := d.db.Close(ctx)
	d.db = nil
	return err
}

func SQLiteFileConnString(path string) string {
	return fmt.Sprintf(
		"file:%s?_journal=WAL",
		path,
	)
}

func NewCachedDBDriver(name string, flavor Flavor, dbFactory func() (DB, error)) *cachedDBDriver {
	return &cachedDBDriver{
		name:      name,
		flavor:    flavor,
		dbFactory: dbFactory,
	}
}
