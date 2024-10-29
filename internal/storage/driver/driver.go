package driver

import (
	"context"
	"errors"
	"fmt"
	"github.com/formancehq/go-libs/v2/metadata"
	"github.com/formancehq/go-libs/v2/platform/postgres"
	systemcontroller "github.com/formancehq/ledger/internal/controller/system"
	"go.opentelemetry.io/otel/metric"
	noopmetrics "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"
	nooptracer "go.opentelemetry.io/otel/trace/noop"

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

func (d *Driver) CreateLedger(ctx context.Context, l *ledger.Ledger) (*ledgerstore.Store, error) {

	if l.Metadata == nil {
		l.Metadata = metadata.Metadata{}
	}

	b := bucket.New(d.db, l.Bucket)
	if err := b.Migrate(ctx, d.tracer); err != nil {
		return nil, fmt.Errorf("migrating bucket: %w", err)
	}

	_, err := d.db.NewInsert().
		Model(l).
		Returning("id, added_at").
		Exec(ctx)
	if err != nil {
		if errors.Is(postgres.ResolveError(err), postgres.ErrConstraintsFailed{}) {
			return nil, systemcontroller.ErrLedgerAlreadyExists
		}
		return nil, postgres.ResolveError(err)
	}

	if err := b.AddLedger(ctx, *l, d.db); err != nil {
		return nil, fmt.Errorf("adding ledger to bucket: %w", err)
	}

	return ledgerstore.New(
		d.db,
		b,
		*l,
		ledgerstore.WithMeter(d.meter),
		ledgerstore.WithTracer(d.tracer),
	), nil
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
		bucket.New(d.db, ret.Bucket),
		*ret,
		ledgerstore.WithMeter(d.meter),
		ledgerstore.WithTracer(d.tracer),
	), ret, nil
}

func (d *Driver) Initialize(ctx context.Context) error {
	logging.FromContext(ctx).Debugf("Initialize driver")
	err := d.detectRollbacks(ctx)
	if err != nil {
		return fmt.Errorf("detecting rollbacks: %w", err)
	}

	err = Migrate(ctx, d.db)
	if err != nil {
		return fmt.Errorf("migrating system store: %w", err)
	}

	return nil
}

func (d *Driver) detectRollbacks(ctx context.Context) error {

	logging.FromContext(ctx).Debugf("Checking for downgrades on system schema")
	if err := detectDowngrades(GetMigrator(d.db), ctx); err != nil {
		return fmt.Errorf("detecting rollbacks of system schema: %w", err)
	}

	type row struct {
		Bucket string `bun:"bucket"`
	}
	rows := make([]row, 0)
	if err := d.db.NewSelect().
		DistinctOn("bucket").
		ModelTableExpr("_system.ledgers").
		Column("bucket").
		Scan(ctx, &rows); err != nil {
		err = postgres.ResolveError(err)
		if errors.Is(err, postgres.ErrMissingTable) {
			return nil
		}
		return err
	}

	for _, r := range rows {
		logging.FromContext(ctx).Debugf("Checking for downgrades on bucket '%s'", r.Bucket)
		if err := detectDowngrades(bucket.GetMigrator(d.db, r.Bucket), ctx); err != nil {
			return fmt.Errorf("detecting rollbacks on bucket '%s': %w", r.Bucket, err)
		}
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

	var buckets []string
	err := d.db.NewSelect().
		DistinctOn("bucket").
		Model(&ledger.Ledger{}).
		Column("bucket").
		Scan(ctx, &buckets)
	if err != nil {
		return fmt.Errorf("getting buckets: %w", err)
	}

	for _, bucketName := range buckets {
		b := bucket.New(d.db, bucketName)

		logging.FromContext(ctx).Infof("Upgrading bucket '%s'", bucketName)
		if err := b.Migrate(ctx, d.tracer); err != nil {
			return err
		}
		logging.FromContext(ctx).Infof("Bucket '%s' up to date", bucketName)
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
