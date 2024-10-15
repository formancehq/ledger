package driver

import (
	"context"
	"database/sql"
	"fmt"
	. "github.com/formancehq/go-libs/v2/collectionutils"
	"github.com/formancehq/go-libs/v2/metadata"
	"github.com/formancehq/go-libs/v2/platform/postgres"
	"go.opentelemetry.io/otel/metric"
	noopmetrics "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"
	nooptracer "go.opentelemetry.io/otel/trace/noop"

	systemcontroller "github.com/formancehq/ledger/internal/controller/system"

	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"

	"github.com/formancehq/go-libs/v2/bun/bunpaginate"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/storage/bucket"
	ledgerstore "github.com/formancehq/ledger/internal/storage/ledger"
	"github.com/uptrace/bun"

	"github.com/formancehq/go-libs/v2/logging"
)

const (
	SchemaSystem = "_system"
)

type Driver struct {
	db     *bun.DB
	tracer trace.Tracer
	meter  metric.Meter
}

func (d *Driver) createLedgerStore(ctx context.Context, db bun.IDB, ledger ledger.Ledger) (*ledgerstore.Store, error) {

	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin transaction: %w", err)
	}

	b := bucket.New(tx, ledger.Bucket)
	if err := b.Migrate(ctx, d.tracer); err != nil {
		return nil, fmt.Errorf("migrating bucket: %w", err)
	}

	if err := ledgerstore.Migrate(ctx, d.tracer, tx, ledger); err != nil {
		return nil, fmt.Errorf("failed to migrate ledger store: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("committing sql transaction to create ledger and schemas: %w", err)
	}

	return ledgerstore.New(
		d.db,
		ledger,
		ledgerstore.WithMeter(d.meter),
		ledgerstore.WithTracer(d.tracer),
	), nil
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

	return ledgerstore.New(
		d.db,
		*ret,
		ledgerstore.WithMeter(d.meter),
		ledgerstore.WithTracer(d.tracer),
	), ret, nil
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
	return bucket.New(d.db, name).Migrate(ctx, d.tracer)
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
		if err := b.Migrate(ctx, d.tracer); err != nil {
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
				if err := ledgerstore.Migrate(ctx, d.tracer, d.db, ledger); err != nil {
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

func New(db *bun.DB, opts ...Option) *Driver {
	ret := &Driver{
		db: db,
	}
	for _, opt := range append(defaultOptions, opts...) {
		opt(ret)
	}
	return ret
}

type Option func(d *Driver)

func WithMeter(m metric.Meter) Option {
	return func(d *Driver) {
		d.meter = m
	}
}

func WithTracer(tracer trace.Tracer) Option {
	return func(d *Driver) {
		d.tracer = tracer
	}
}

var defaultOptions = []Option{
	WithMeter(noopmetrics.Meter{}),
	WithTracer(nooptracer.Tracer{}),
}
