package driver

import (
	"context"
	"errors"
	"fmt"
	"github.com/alitto/pond"
	"github.com/formancehq/go-libs/v2/metadata"
	"github.com/formancehq/go-libs/v2/migrations"
	"github.com/formancehq/go-libs/v2/platform/postgres"
	systemcontroller "github.com/formancehq/ledger/internal/controller/system"
	systemstore "github.com/formancehq/ledger/internal/storage/system"
	"go.opentelemetry.io/otel/metric"
	noopmetrics "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"
	nooptracer "go.opentelemetry.io/otel/trace/noop"
	"time"

	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"

	"github.com/formancehq/go-libs/v2/bun/bunpaginate"
	"github.com/formancehq/go-libs/v2/logging"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/storage/bucket"
	ledgerstore "github.com/formancehq/ledger/internal/storage/ledger"
)

type Driver struct {
	ledgerStoreFactory ledgerstore.Factory
	systemStore        systemstore.Store
	bucketFactory      bucket.Factory
	tracer             trace.Tracer
	meter              metric.Meter

	migrationRetryPeriod      time.Duration
	migratorLockRetryInterval time.Duration
	parallelBucketMigrations  int
}

func (d *Driver) CreateLedger(ctx context.Context, l *ledger.Ledger) (*ledgerstore.Store, error) {

	if l.Metadata == nil {
		l.Metadata = metadata.Metadata{}
	}

	b := d.bucketFactory.Create(l.Bucket)
	if err := b.Migrate(
		ctx,
		make(chan struct{}),
		migrations.WithLockRetryInterval(d.migratorLockRetryInterval),
	); err != nil {
		return nil, fmt.Errorf("migrating bucket: %w", err)
	}

	if err := d.systemStore.CreateLedger(ctx, l); err != nil {
		if errors.Is(postgres.ResolveError(err), postgres.ErrConstraintsFailed{}) {
			return nil, systemcontroller.ErrLedgerAlreadyExists
		}
		return nil, postgres.ResolveError(err)
	}

	if err := b.AddLedger(ctx, *l); err != nil {
		return nil, fmt.Errorf("adding ledger to bucket: %w", err)
	}

	return d.ledgerStoreFactory.Create(b, *l), nil
}

func (d *Driver) OpenLedger(ctx context.Context, name string) (*ledgerstore.Store, *ledger.Ledger, error) {
	ret, err := d.systemStore.GetLedger(ctx, name)
	if err != nil {
		return nil, nil, err
	}

	store := d.ledgerStoreFactory.Create(d.bucketFactory.Create(ret.Bucket), *ret)

	return store, ret, err
}

func (d *Driver) Initialize(ctx context.Context) error {
	logging.FromContext(ctx).Debugf("Initialize driver")
	err := d.detectRollbacks(ctx)
	if err != nil {
		return fmt.Errorf("detecting rollbacks: %w", err)
	}

	err = d.systemStore.Migrate(ctx, migrations.WithLockRetryInterval(d.migratorLockRetryInterval))
	if err != nil {
		constraintsFailed := postgres.ErrConstraintsFailed{}
		if errors.As(err, &constraintsFailed) &&
			constraintsFailed.GetConstraint() == "pg_namespace_nspname_index" {
			// notes(gfyrag): Creating schema concurrently can result in a constraint violation of pg_namespace_nspname_index table.
			// If we have this error, it's because a concurrent instance of the service is actually creating the schema
			// I guess we can ignore the error.
			return nil
		}
		return fmt.Errorf("migrating system store: %w", err)
	}

	return nil
}

func (d *Driver) detectRollbacks(ctx context.Context) error {

	logging.FromContext(ctx).Debugf("Checking for downgrades on system schema")
	if err := detectDowngrades(d.systemStore.GetMigrator(), ctx); err != nil {
		return fmt.Errorf("detecting rollbacks of system schema: %w", err)
	}

	buckets, err := d.systemStore.GetDistinctBuckets(ctx)
	if err != nil {
		if !errors.Is(err, postgres.ErrMissingTable) {
			return fmt.Errorf("getting distinct buckets: %w", err)
		}
		return nil
	}

	for _, b := range buckets {
		logging.FromContext(ctx).Debugf("Checking for downgrades on bucket '%s'", b)
		if err := detectDowngrades(d.bucketFactory.GetMigrator(b), ctx); err != nil {
			return fmt.Errorf("detecting rollbacks on bucket '%s': %w", b, err)
		}
	}

	return nil
}

func (d *Driver) UpdateLedgerMetadata(ctx context.Context, name string, m metadata.Metadata) error {
	return d.systemStore.UpdateLedgerMetadata(ctx, name, m)
}

func (d *Driver) DeleteLedgerMetadata(ctx context.Context, name string, key string) error {
	return d.systemStore.DeleteLedgerMetadata(ctx, name, key)
}

func (d *Driver) ListLedgers(ctx context.Context, q ledgercontroller.ListLedgersQuery) (*bunpaginate.Cursor[ledger.Ledger], error) {
	return d.systemStore.ListLedgers(ctx, q)
}

func (d *Driver) GetLedger(ctx context.Context, name string) (*ledger.Ledger, error) {
	return d.systemStore.GetLedger(ctx, name)
}

func (d *Driver) UpgradeBucket(ctx context.Context, name string) error {
	return d.bucketFactory.Create(name).Migrate(
		ctx,
		make(chan struct{}),
		migrations.WithLockRetryInterval(d.migratorLockRetryInterval),
	)
}

func (d *Driver) UpgradeAllBuckets(ctx context.Context, minimalVersionReached chan struct{}) error {

	buckets, err := d.systemStore.GetDistinctBuckets(ctx)
	if err != nil {
		return fmt.Errorf("getting distinct buckets: %w", err)
	}

	sem := make(chan struct{}, len(buckets))

	wp := pond.New(d.parallelBucketMigrations, len(buckets), pond.Context(ctx))

	for _, bucketName := range buckets {
		wp.Submit(func() {
			logger := logging.FromContext(ctx).WithFields(map[string]any{
				"bucket": bucketName,
			})
			b := d.bucketFactory.Create(bucketName)

			// copy semaphore to be able to nil it
			sem := sem

		l:
			for {
				minimalVersionReached := make(chan struct{})
				errChan := make(chan error, 1)
				go func() {
					logger.Infof("Upgrading...")
					errChan <- b.Migrate(
						logging.ContextWithLogger(ctx, logger),
						minimalVersionReached,
						migrations.WithLockRetryInterval(d.migratorLockRetryInterval),
					)
				}()

				for {
					logger.Infof("Waiting termination")
					select {
					case <-ctx.Done():
						return
					case err := <-errChan:
						if err != nil {
							logger.Errorf("Error upgrading: %s", err)
							select {
							case <-time.After(d.migrationRetryPeriod):
								continue l
							case <-ctx.Done():
								return
							}
						}
						if sem != nil {
							logger.Infof("Reached minimal workable version")
							sem <- struct{}{}
						}

						logger.Info("Upgrade terminated")
						return
					case <-minimalVersionReached:
						minimalVersionReached = nil
						if sem != nil {
							logger.Infof("Reached minimal workable version")
							sem <- struct{}{}
							sem = nil
						}
					}
				}
			}
		})
	}

	for i := 0; i < len(buckets); i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-sem:
		}
	}

	logging.FromContext(ctx).Infof("All buckets have reached minimal workable version")
	close(minimalVersionReached)

	wp.StopAndWait()

	return nil
}

func New(
	ledgerStoreFactory ledgerstore.Factory,
	systemStore systemstore.Store,
	bucketFactory bucket.Factory,
	opts ...Option,
) *Driver {
	ret := &Driver{
		ledgerStoreFactory: ledgerStoreFactory,
		bucketFactory:      bucketFactory,
		systemStore:        systemStore,
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

func WithMigratorLockRetryInterval(interval time.Duration) Option {
	return func(d *Driver) {
		d.migratorLockRetryInterval = interval
	}
}

func WithParallelBucketMigration(p int) Option {
	return func(d *Driver) {
		d.parallelBucketMigrations = p
	}
}

func WithMigrationRetryPeriod(p time.Duration) Option {
	return func(d *Driver) {
		d.migrationRetryPeriod = p
	}
}

var defaultOptions = []Option{
	WithMeter(noopmetrics.Meter{}),
	WithTracer(nooptracer.Tracer{}),
	WithParallelBucketMigration(10),
	WithMigrationRetryPeriod(5 * time.Second),
}
