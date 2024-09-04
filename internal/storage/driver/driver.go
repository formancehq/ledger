package driver

import (
	"context"
	"database/sql"

	systemcontroller "github.com/formancehq/ledger/internal/controller/system"

	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"

	"github.com/formancehq/go-libs/bun/bunpaginate"
	"github.com/formancehq/go-libs/collectionutils"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/storage/bucket"
	ledgerstore "github.com/formancehq/ledger/internal/storage/ledger"
	"github.com/formancehq/ledger/internal/storage/system"
	"github.com/pkg/errors"
	"github.com/uptrace/bun"

	"github.com/formancehq/go-libs/logging"
)

type Driver struct {
	db *bun.DB
}

func (d *Driver) CreateBucket(ctx context.Context, bucketName string) (*bucket.Bucket, error) {
	tx, err := d.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	b := bucket.New(d.db, bucketName)

	isInitialized, err := b.IsInitialized(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "checking if bucket is initialized")
	}

	if isInitialized {
		isUpToDate, err := b.IsUpToDate(ctx)
		if err != nil {
			return nil, errors.Wrap(err, "checking if bucket is up to date")
		}
		if !isUpToDate {
			return nil, systemcontroller.ErrNeedUpgradeBucket
		}
	} else {
		if err := bucket.Migrate(ctx, tx, bucketName); err != nil {
			return nil, errors.Wrap(err, "migrating bucket")
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, errors.Wrap(err, "committing sql transaction to create bucket schema")
	}

	return b, nil
}

func (d *Driver) createLedgerStore(ctx context.Context, db bun.IDB, ledger ledger.Ledger) (*ledgerstore.Store, error) {

	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return nil, errors.Wrap(err, "begin transaction")
	}

	b := bucket.New(tx, ledger.Bucket)
	if err := b.Migrate(ctx); err != nil {
		return nil, errors.Wrap(err, "migrating bucket")
	}

	if err := ledgerstore.Migrate(ctx, tx, ledger); err != nil {
		return nil, errors.Wrap(err, "failed to migrate ledger store")
	}

	if err := tx.Commit(); err != nil {
		return nil, errors.Wrap(err, "committing sql transaction to create ledger and schemas")
	}

	return ledgerstore.New(d.db, ledger), nil
}

func (d *Driver) CreateLedger(ctx context.Context, l *ledger.Ledger) (*ledgerstore.Store, error) {

	tx, err := d.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return nil, errors.Wrap(err, "begin transaction")
	}
	defer func() {
		_ = tx.Rollback()
	}()

	created, err := system.New(tx).CreateLedger(ctx, l)
	if err != nil {
		return nil, errors.Wrap(err, "creating ledger")
	}
	if !created {
		return nil, systemcontroller.ErrLedgerAlreadyExists
	}

	store, err := d.createLedgerStore(ctx, tx, *l)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, errors.Wrap(err, "committing sql transaction to create ledger schema")
	}

	return store, nil
}

func (d *Driver) OpenLedger(ctx context.Context, name string) (*ledgerstore.Store, *ledger.Ledger, error) {
	l, err := system.New(d.db).GetLedger(ctx, name)
	if err != nil {
		return nil, nil, errors.Wrap(err, "opening ledger")
	}

	return ledgerstore.New(d.db, *l), l, nil
}

func (d *Driver) Initialize(ctx context.Context) error {
	logging.FromContext(ctx).Debugf("Initialize driver")
	return errors.Wrap(system.Migrate(ctx, d.db), "migrating system store")
}

func (d *Driver) UpgradeAllBuckets(ctx context.Context) error {

	systemStore := system.New(d.db)

	bucketsNames := collectionutils.Set[string]{}
	err := bunpaginate.Iterate(ctx, ledgercontroller.NewListLedgersQuery(10),
		func(ctx context.Context, q ledgercontroller.ListLedgersQuery) (*bunpaginate.Cursor[ledger.Ledger], error) {
			return systemStore.ListLedgers(ctx, q)
		},
		func(cursor *bunpaginate.Cursor[ledger.Ledger]) error {
			for _, name := range cursor.Data {
				bucketsNames.Put(name.Bucket)
			}
			return nil
		})
	if err != nil {
		return err
	}

	for _, bucketName := range collectionutils.Keys(bucketsNames) {
		b := bucket.New(d.db, bucketName)

		logging.FromContext(ctx).Infof("Upgrading bucket '%s'", bucketName)
		if err := b.Migrate(ctx); err != nil {
			return err
		}
	}

	return nil
}

func (d *Driver) UpgradeBucket(ctx context.Context, name string) error {
	return bucket.New(d.db, name).Migrate(ctx)
}

func New(db *bun.DB) *Driver {
	return &Driver{
		db: db,
	}
}
