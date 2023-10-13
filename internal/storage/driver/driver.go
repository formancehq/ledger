package driver

import (
	"context"
	"fmt"
	"net/url"
	"sync"

	"github.com/formancehq/ledger/internal/storage"
	"github.com/formancehq/ledger/internal/storage/ledgerstore"
	"github.com/formancehq/ledger/internal/storage/systemstore"
	"github.com/formancehq/stack/libs/go-libs/logging"
	"github.com/pkg/errors"
	"github.com/uptrace/bun"
)

const SystemSchema = "_system"

type Driver struct {
	systemStore       *systemstore.Store
	lock              sync.Mutex
	connectionOptions storage.ConnectionOptions
	db                *bun.DB
	databasesBySchema map[string]*bun.DB
}

func (d *Driver) GetSystemStore() *systemstore.Store {
	return d.systemStore
}

func (d *Driver) newLedgerStore(name string) (*ledgerstore.Store, error) {
	db, err := d.openDBWithSchema(name)
	if err != nil {
		return nil, err
	}

	return ledgerstore.New(db, name, func(ctx context.Context) error {
		return d.GetSystemStore().DeleteLedger(ctx, name)
	})
}

func (d *Driver) createLedgerStore(ctx context.Context, name string) (*ledgerstore.Store, error) {
	if name == SystemSchema {
		return nil, errors.New("reserved name")
	}

	exists, err := d.systemStore.Exists(ctx, name)
	if err != nil {
		return nil, err
	}
	if exists {
		return nil, storage.ErrStoreAlreadyExists
	}

	_, err = d.systemStore.Register(ctx, name)
	if err != nil {
		return nil, err
	}

	store, err := d.newLedgerStore(name)
	if err != nil {
		return nil, err
	}

	_, err = store.Migrate(ctx)

	return store, err
}

func (d *Driver) CreateLedgerStore(ctx context.Context, name string) (*ledgerstore.Store, error) {
	d.lock.Lock()
	defer d.lock.Unlock()

	return d.createLedgerStore(ctx, name)
}

func (d *Driver) GetLedgerStore(ctx context.Context, name string) (*ledgerstore.Store, error) {
	d.lock.Lock()
	defer d.lock.Unlock()

	exists, err := d.systemStore.Exists(ctx, name)
	if err != nil {
		return nil, err
	}

	var store *ledgerstore.Store
	if !exists {
		store, err = d.createLedgerStore(ctx, name)
	} else {
		store, err = d.newLedgerStore(name)
	}
	if err != nil {
		return nil, err
	}

	return store, nil
}

func (d *Driver) openDBWithSchema(schema string) (*bun.DB, error) {
	parsedConnectionParams, err := url.Parse(d.connectionOptions.DatabaseSourceName)
	if err != nil {
		return nil, storage.PostgresError(err)
	}

	query := parsedConnectionParams.Query()
	query.Set("search_path", schema)
	parsedConnectionParams.RawQuery = query.Encode()

	connectionOptions := d.connectionOptions
	connectionOptions.DatabaseSourceName = parsedConnectionParams.String()

	db, err := storage.OpenSQLDB(connectionOptions)
	if err != nil {
		return nil, err
	}

	d.databasesBySchema[schema] = db

	return db, nil
}

func (d *Driver) Initialize(ctx context.Context) error {
	logging.FromContext(ctx).Debugf("Initialize driver")

	var err error
	d.db, err = storage.OpenSQLDB(d.connectionOptions)
	if err != nil {
		return storage.PostgresError(err)
	}

	_, err = d.db.ExecContext(ctx, fmt.Sprintf(`create schema if not exists "%s"`, SystemSchema))
	if err != nil {
		return storage.PostgresError(err)
	}

	dbWithSystemSchema, err := d.openDBWithSchema(SystemSchema)
	if err != nil {
		return storage.PostgresError(err)
	}

	d.systemStore = systemstore.NewStore(dbWithSystemSchema)

	if err := d.systemStore.Initialize(ctx); err != nil {
		return err
	}

	return nil
}

func (d *Driver) UpgradeAllLedgersSchemas(ctx context.Context) error {
	systemStore := d.GetSystemStore()
	ledgers, err := systemStore.ListLedgers(ctx)
	if err != nil {
		return err
	}

	for _, ledger := range ledgers {
		store, err := d.GetLedgerStore(ctx, ledger)
		if err != nil {
			return err
		}

		logging.FromContext(ctx).Infof("Upgrading storage '%s'", ledger)
		if _, err := store.Migrate(ctx); err != nil {
			return err
		}
	}

	return nil
}

func (d *Driver) Close() error {
	if d.db != nil {
		if err := d.db.Close(); err != nil {
			return err
		}
	}
	for _, db := range d.databasesBySchema {
		if err := db.Close(); err != nil {
			return err
		}
	}
	return nil
}

func New(connectionOptions storage.ConnectionOptions) *Driver {
	return &Driver{
		connectionOptions: connectionOptions,
		databasesBySchema: make(map[string]*bun.DB),
	}
}
