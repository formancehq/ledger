package driver

import (
	"context"
	"database/sql"
	"fmt"
	. "github.com/formancehq/go-libs/collectionutils"
	"github.com/formancehq/go-libs/metadata"
	"github.com/formancehq/go-libs/platform/postgres"

	systemcontroller "github.com/formancehq/ledger/internal/controller/system"

	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"

	"github.com/formancehq/go-libs/bun/bunpaginate"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/storage/bucket"
	ledgerstore "github.com/formancehq/ledger/internal/storage/ledger"
	"github.com/uptrace/bun"

	"github.com/formancehq/go-libs/logging"
)

const (
	SchemaSystem = "_system"
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
		return nil, fmt.Errorf("checking if bucket is initialized: %w", err)
	}

	if isInitialized {
		isUpToDate, err := b.IsUpToDate(ctx)
		if err != nil {
			return nil, fmt.Errorf("checking if bucket is up to date: %w", err)
		}
		if !isUpToDate {
			return nil, systemcontroller.ErrNeedUpgradeBucket
		}
	} else {
		if err := bucket.Migrate(ctx, tx, bucketName); err != nil {
			return nil, fmt.Errorf("migrating bucket: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("committing sql transaction to create bucket schema: %w", err)
	}

	return b, nil
}

func (d *Driver) createLedgerStore(ctx context.Context, db bun.IDB, ledger ledger.Ledger) (*ledgerstore.Store, error) {

	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin transaction: %w", err)
	}

	b := bucket.New(tx, ledger.Bucket)
	if err := b.Migrate(ctx); err != nil {
		return nil, fmt.Errorf("migrating bucket: %w", err)
	}

	if err := ledgerstore.Migrate(ctx, tx, ledger); err != nil {
		return nil, fmt.Errorf("failed to migrate ledger store: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("committing sql transaction to create ledger and schemas: %w", err)
	}

	return ledgerstore.New(d.db, ledger), nil
}

func (d *Driver) CreateLedger(ctx context.Context, l *ledger.Ledger) (*ledgerstore.Store, error) {

	// start a transaction because we will need to create the schema and apply ledger migrations
	tx, err := d.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	if l.Metadata == nil {
		l.Metadata = metadata.Metadata{}
	}

	ret, err := d.db.NewInsert().
		Model(l).
		Ignore().
		Returning("id, added_at").
		Exec(ctx)
	if err != nil {
		return nil, postgres.ResolveError(err)
	}

	affected, err := ret.RowsAffected()
	if err != nil {
		return nil, fmt.Errorf("creating ledger: %w", err)
	}
	if affected == 0 {
		return nil, systemcontroller.ErrLedgerAlreadyExists
	}

	store, err := d.createLedgerStore(ctx, tx, *l)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("committing sql transaction to create ledger schema: %w", err)
	}

	return store, nil
}

func (d *Driver) OpenLedger(ctx context.Context, name string) (*ledgerstore.Store, *ledger.Ledger, error) {
	ret := &ledger.Ledger{}
	if err := d.db.NewSelect().
		Model(ret).
		Column("*").
		Where("name = ?", name).
		Scan(ctx); err != nil {
		return nil, nil, postgres.ResolveError(err)
	}

	return ledgerstore.New(d.db, *ret), ret, nil
}

func (d *Driver) Initialize(ctx context.Context) error {
	logging.FromContext(ctx).Debugf("Initialize driver")
	err := Migrate(ctx, d.db)
	if err != nil {
		return fmt.Errorf("migrating system store: %w", err)
	}
	return nil
}

func (d *Driver) UpdateLedgerMetadata(ctx context.Context, name string, m metadata.Metadata) error {
	_, err := d.db.NewUpdate().
		Model(&ledger.Ledger{}).
		Set("metadata = metadata || ?", m).
		Where("name = ?", name).
		Exec(ctx)
	return err
}

func (d *Driver) UpdateLedgerState(ctx context.Context, name string, state string) error {
	_, err := d.db.NewUpdate().
		Model(&ledger.Ledger{}).
		Set("state = ?", state).
		Where("name = ?", name).
		Exec(ctx)
	return err
}

func (d *Driver) DeleteLedgerMetadata(ctx context.Context, name string, key string) error {
	_, err := d.db.NewUpdate().
		Model(&ledger.Ledger{}).
		Set("metadata = metadata - ?", key).
		Where("name = ?", name).
		Exec(ctx)
	return err
}

func (d *Driver) ListLedgers(ctx context.Context, q ledgercontroller.ListLedgersQuery) (*bunpaginate.Cursor[ledger.Ledger], error) {
	query := d.db.NewSelect().
		Model(&ledger.Ledger{}).
		Column("*").
		Order("added_at asc")

	return bunpaginate.UsingOffset[ledgercontroller.PaginatedQueryOptions[struct{}], ledger.Ledger](
		ctx,
		query,
		bunpaginate.OffsetPaginatedQuery[ledgercontroller.PaginatedQueryOptions[struct{}]](q),
	)
}

func (d *Driver) GetLedger(ctx context.Context, name string) (*ledger.Ledger, error) {
	ret := &ledger.Ledger{}
	if err := d.db.NewSelect().
		Model(ret).
		Column("*").
		Where("name = ?", name).
		Scan(ctx); err != nil {
		return nil, postgres.ResolveError(err)
	}

	return ret, nil
}

func (d *Driver) UpgradeBucket(ctx context.Context, name string) error {
	return bucket.New(d.db, name).Migrate(ctx)
}

func (d *Driver) UpgradeAllBuckets(ctx context.Context) error {

	bucketsNames := Set[string]{}
	err := bunpaginate.Iterate(ctx, ledgercontroller.NewListLedgersQuery(10),
		func(ctx context.Context, q ledgercontroller.ListLedgersQuery) (*bunpaginate.Cursor[ledger.Ledger], error) {
			return d.ListLedgers(ctx, q)
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

	for _, bucketName := range Keys(bucketsNames) {
		b := bucket.New(d.db, bucketName)

		logging.FromContext(ctx).Infof("Upgrading bucket '%s'", bucketName)
		if err := b.Migrate(ctx); err != nil {
			return err
		}
	}

	return nil
}

func (d *Driver) UpgradeAllLedgers(ctx context.Context) error {
	err := bunpaginate.Iterate(ctx, ledgercontroller.NewListLedgersQuery(10),
		func(ctx context.Context, q ledgercontroller.ListLedgersQuery) (*bunpaginate.Cursor[ledger.Ledger], error) {
			return d.ListLedgers(ctx, q)
		},
		func(cursor *bunpaginate.Cursor[ledger.Ledger]) error {
			for _, ledger := range cursor.Data {
				if err := ledgerstore.Migrate(ctx, d.db, ledger); err != nil {
					return err
				}
			}
			return nil
		})
	if err != nil {
		return err
	}

	return nil
}

func New(db *bun.DB) *Driver {
	return &Driver{
		db: db,
	}
}
