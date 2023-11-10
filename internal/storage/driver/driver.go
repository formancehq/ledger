package driver

import (
	"context"
	"fmt"
	"sync"

	"github.com/formancehq/ledger/internal/storage/sqlutils"

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
	connectionOptions sqlutils.ConnectionOptions
	db                *bun.DB
	databasesBySchema map[string]*bun.DB
}

func (d *Driver) GetSystemStore() *systemstore.Store {
	return d.systemStore
}

func (d *Driver) newLedgerStore(name string) (*ledgerstore.Store, error) {
	db, err := sqlutils.OpenDBWithSchema(d.connectionOptions, name)
	if err != nil {
		return nil, err
	}
	d.databasesBySchema[name] = db

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
		return nil, sqlutils.ErrStoreAlreadyExists
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

func (d *Driver) Initialize(ctx context.Context) error {
	logging.FromContext(ctx).Debugf("Initialize driver")

	var err error
	d.db, err = sqlutils.OpenSQLDB(d.connectionOptions)
	if err != nil {
		return sqlutils.PostgresError(err)
	}

	_, err = d.db.ExecContext(ctx, fmt.Sprintf(`create schema if not exists "%s"`, SystemSchema))
	if err != nil {
		return sqlutils.PostgresError(err)
	}

	dbWithSystemSchema, err := sqlutils.OpenDBWithSchema(d.connectionOptions, SystemSchema)
	if err != nil {
		return sqlutils.PostgresError(err)
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
	if d.systemStore != nil {
		if err := d.systemStore.Close(); err != nil {
			return err
		}
	}
	for _, db := range d.databasesBySchema {
		if err := db.Close(); err != nil {
			return err
		}
	}
	if d.db != nil {
		if err := d.db.Close(); err != nil {
			return err
		}
	}
	return nil
}

func New(connectionOptions sqlutils.ConnectionOptions) *Driver {
	return &Driver{
		connectionOptions: connectionOptions,
		databasesBySchema: make(map[string]*bun.DB),
	}
}
