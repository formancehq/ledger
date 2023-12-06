package driver

import (
	"context"
	"database/sql"
	"sync"

	"github.com/formancehq/stack/libs/go-libs/bun/bunpaginate"

	"github.com/formancehq/stack/libs/go-libs/bun/bunconnect"

	"github.com/formancehq/stack/libs/go-libs/api"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/stack/libs/go-libs/collectionutils"
	"github.com/pkg/errors"
	"github.com/uptrace/bun"

	"github.com/formancehq/ledger/internal/storage/ledgerstore"

	"github.com/formancehq/ledger/internal/storage/sqlutils"

	"github.com/formancehq/ledger/internal/storage/systemstore"
	"github.com/formancehq/stack/libs/go-libs/logging"
)

const defaultBucket = "_default"

var (
	ErrNeedUpgradeBucket   = errors.New("need to upgrade bucket before add a new ledger on it")
	ErrLedgerAlreadyExists = errors.New("ledger already exists")
)

type LedgerConfiguration struct {
	Bucket string `json:"bucket"`
}

type Driver struct {
	systemStore       *systemstore.Store
	lock              sync.Mutex
	connectionOptions bunconnect.ConnectionOptions
	buckets           map[string]*ledgerstore.Bucket
	db                *bun.DB
}

func (d *Driver) GetSystemStore() *systemstore.Store {
	return d.systemStore
}

func (d *Driver) OpenBucket(name string) (*ledgerstore.Bucket, error) {

	bucket, ok := d.buckets[name]
	if ok {
		return bucket, nil
	}

	b, err := ledgerstore.ConnectToBucket(d.connectionOptions, name)
	if err != nil {
		return nil, err
	}
	d.buckets[name] = b

	return b, nil
}

func (d *Driver) GetLedgerStore(ctx context.Context, name string) (*ledgerstore.Store, error) {
	d.lock.Lock()
	defer d.lock.Unlock()

	ledgerConfiguration, err := d.systemStore.GetLedger(ctx, name)
	if err != nil {
		return nil, err
	}

	bucket, err := d.OpenBucket(ledgerConfiguration.Bucket)
	if err != nil {
		return nil, err
	}

	return bucket.GetLedgerStore(name)
}

func (f *Driver) CreateLedgerStore(ctx context.Context, name string, configuration LedgerConfiguration) (*ledgerstore.Store, error) {

	tx, err := f.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	if _, err := f.systemStore.GetLedger(ctx, name); err == nil {
		return nil, ErrLedgerAlreadyExists
	} else if !sqlutils.IsNotFoundError(err) {
		return nil, err
	}

	bucketName := defaultBucket
	if configuration.Bucket != "" {
		bucketName = configuration.Bucket
	}

	bucket, err := f.OpenBucket(bucketName)
	if err != nil {
		return nil, errors.Wrap(err, "opening bucket")
	}

	isInitialized, err := bucket.IsInitialized(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "checking if bucket is initialized")
	}

	if isInitialized {
		isUpToDate, err := bucket.IsUpToDate(ctx)
		if err != nil {
			return nil, errors.Wrap(err, "checking if bucket is up to date")
		}
		if !isUpToDate {
			return nil, ErrNeedUpgradeBucket
		}
	} else {
		if err := ledgerstore.MigrateBucket(ctx, tx, bucketName); err != nil {
			return nil, errors.Wrap(err, "migrating bucket")
		}
	}

	store, err := bucket.GetLedgerStore(name)
	if err != nil {
		return nil, errors.Wrap(err, "getting ledger store")
	}

	_, err = systemstore.RegisterLedger(ctx, tx, &systemstore.Ledger{
		Name:    name,
		AddedAt: ledger.Now(),
		Bucket:  bucketName,
	})
	if err != nil {
		return nil, errors.Wrap(err, "registring ledger on system store")
	}

	return store, errors.Wrap(tx.Commit(), "committing sql transaction")
}

func (d *Driver) Initialize(ctx context.Context) error {
	logging.FromContext(ctx).Debugf("Initialize driver")

	var err error
	d.db, err = bunconnect.OpenSQLDB(d.connectionOptions)
	if err != nil {
		return errors.Wrap(err, "connecting to database")
	}

	if err := systemstore.Migrate(ctx, d.db); err != nil {
		return errors.Wrap(err, "migrating data")
	}

	d.systemStore, err = systemstore.Connect(ctx, d.connectionOptions)
	if err != nil {
		return errors.Wrap(err, "connecting to system store")
	}

	return nil
}

func (d *Driver) UpgradeAllBuckets(ctx context.Context) error {

	systemStore := d.GetSystemStore()

	buckets := collectionutils.Set[string]{}
	err := bunpaginate.Iterate(ctx, systemstore.NewListLedgersQuery(10),
		func(ctx context.Context, q systemstore.ListLedgersQuery) (*api.Cursor[systemstore.Ledger], error) {
			return systemStore.ListLedgers(ctx, q)
		},
		func(cursor *api.Cursor[systemstore.Ledger]) error {
			for _, name := range cursor.Data {
				buckets.Put(name.Name)
			}
			return nil
		})
	if err != nil {
		return err
	}

	for _, bucket := range collectionutils.Keys(buckets) {
		bucket, err := d.OpenBucket(bucket)
		if err != nil {
			return err
		}

		logging.FromContext(ctx).Infof("Upgrading bucket '%s'", bucket)
		if err := bucket.Migrate(ctx); err != nil {
			return err
		}
	}

	return nil
}

func (d *Driver) Close() error {
	if err := d.systemStore.Close(); err != nil {
		return err
	}
	for _, b := range d.buckets {
		if err := b.Close(); err != nil {
			return err
		}
	}
	if err := d.db.Close(); err != nil {
		return err
	}
	return nil
}

func New(connectionOptions bunconnect.ConnectionOptions) *Driver {
	return &Driver{
		connectionOptions: connectionOptions,
		buckets:           make(map[string]*ledgerstore.Bucket),
	}
}
