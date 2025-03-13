package driver

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/alitto/pond"
	"github.com/formancehq/go-libs/v2/metadata"
	"github.com/formancehq/go-libs/v2/platform/postgres"
	systemcontroller "github.com/formancehq/ledger/internal/controller/system"
	systemstore "github.com/formancehq/ledger/internal/storage/system"
	"github.com/uptrace/bun"
	"go.opentelemetry.io/otel/metric"
	noopmetrics "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"
	nooptracer "go.opentelemetry.io/otel/trace/noop"

	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"

	"github.com/formancehq/go-libs/v2/bun/bunpaginate"
	"github.com/formancehq/go-libs/v2/logging"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/storage/bucket"
	ledgerstore "github.com/formancehq/ledger/internal/storage/ledger"
)

type Driver struct {
	ledgerStoreFactory ledgerstore.Factory
	db                 *bun.DB
	bucketFactory      bucket.Factory
	tracer             trace.Tracer
	meter              metric.Meter

	migrationRetryPeriod     time.Duration
	parallelBucketMigrations int
}

func (d *Driver) CreateLedger(ctx context.Context, l *ledger.Ledger) (*ledgerstore.Store, error) {

	var ret *ledgerstore.Store
	err := d.db.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		systemStore := systemstore.New(tx)

		if err := systemStore.CreateLedger(ctx, l); err != nil {
			if errors.Is(postgres.ResolveError(err), postgres.ErrConstraintsFailed{}) {
				return systemcontroller.ErrLedgerAlreadyExists
			}
			return err
		}

		b := d.bucketFactory.Create(l.Bucket, tx)
		isInitialized, err := b.IsInitialized(ctx)
		if err != nil {
			return fmt.Errorf("checking if bucket is initialized: %w", err)
		}
		if isInitialized {
			upToDate, err := b.IsUpToDate(ctx)
			if err != nil {
				return fmt.Errorf("checking if bucket is up to date: %w", err)
			}

			if !upToDate {
				return systemcontroller.ErrBucketOutdated
			}

			if err := b.AddLedger(ctx, *l); err != nil {
				return fmt.Errorf("adding ledger to bucket: %w", err)
			}
		} else {
			if err := b.Migrate(ctx); err != nil {
				return fmt.Errorf("migrating bucket: %w", err)
			}
		}

		ret = d.ledgerStoreFactory.Create(b, *l)

		return nil
	})
	if err != nil {
		return nil, postgres.ResolveError(err)
	}

	return ret, nil
}

func (d *Driver) OpenLedger(ctx context.Context, name string) (*ledgerstore.Store, *ledger.Ledger, error) {
	// todo: keep the ledger in cache somewhere to avoid read the ledger at each request, maybe in the factory
	ret, err := systemstore.New(d.db).GetLedger(ctx, name)
	if err != nil {
		return nil, nil, err
	}

	store := d.ledgerStoreFactory.Create(d.bucketFactory.Create(ret.Bucket, d.db), *ret)

	return store, ret, err
}

func (d *Driver) Initialize(ctx context.Context) error {
	logging.FromContext(ctx).Debugf("Initialize driver")
	err := d.detectRollbacks(ctx)
	if err != nil {
		return fmt.Errorf("detecting rollbacks: %w", err)
	}

	err = systemstore.New(d.db).Migrate(ctx)
	if err != nil {
		return fmt.Errorf("migrating system store: %w", err)
	}

	return nil
}

func (d *Driver) detectRollbacks(ctx context.Context) error {

	systemStore := systemstore.New(d.db)
	logging.FromContext(ctx).Debugf("Checking for downgrades on system schema")

	if err := detectDowngrades(systemStore.GetMigrator(), ctx); err != nil {
		return fmt.Errorf("detecting rollbacks of system schema: %w", err)
	}

	buckets, err := systemStore.GetDistinctBuckets(ctx)
	if err != nil {
		if !errors.Is(err, postgres.ErrMissingTable) {
			return fmt.Errorf("getting distinct buckets: %w", err)
		}
		return nil
	}

	parallelWorkers := d.parallelBucketMigrations
	if parallelWorkers <= 0 {
		parallelWorkers = 100
	}
	wp := pond.New(parallelWorkers, len(buckets), pond.Context(ctx))
	var (
		mu   sync.Mutex
		errs []error
	)

	for _, b := range buckets {
		wp.Submit(func() {
			logger := logging.FromContext(ctx).WithFields(map[string]any{
				"bucket": b,
			})
			logger.Debugf("Checking for downgrades on bucket '%s'", b)

			if err := detectDowngrades(d.bucketFactory.GetMigrator(b, d.db), ctx); err != nil {
				mu.Lock()
				errs = append(errs, fmt.Errorf("detecting rollbacks on bucket '%s': %w", b, err))
				mu.Unlock()
			}
		})
	}

	wp.StopAndWait()

	if len(errs) > 0 {
		if len(errs) == 1 {
			return errs[0]
		}
		var combinedErr error
		for _, err := range errs {
			if combinedErr == nil {
				combinedErr = err
			} else {
				combinedErr = fmt.Errorf("%v; %w", combinedErr, err)
			}
		}
		return combinedErr
	}

	return nil
}

func (d *Driver) UpdateLedgerMetadata(ctx context.Context, name string, m metadata.Metadata) error {
	return systemstore.New(d.db).UpdateLedgerMetadata(ctx, name, m)
}

func (d *Driver) DeleteLedgerMetadata(ctx context.Context, name string, key string) error {
	return systemstore.New(d.db).DeleteLedgerMetadata(ctx, name, key)
}

func (d *Driver) ListLedgers(ctx context.Context, q ledgercontroller.ListLedgersQuery) (*bunpaginate.Cursor[ledger.Ledger], error) {
	return systemstore.New(d.db).ListLedgers(ctx, q)
}

func (d *Driver) GetLedger(ctx context.Context, name string) (*ledger.Ledger, error) {
	return systemstore.New(d.db).GetLedger(ctx, name)
}

func (d *Driver) UpgradeBucket(ctx context.Context, name string) error {
	return d.bucketFactory.Create(name, d.db).Migrate(ctx)
}

func (d *Driver) UpgradeAllBuckets(ctx context.Context) error {

	buckets, err := systemstore.New(d.db).GetDistinctBuckets(ctx)
	if err != nil {
		return fmt.Errorf("getting distinct buckets: %w", err)
	}

	wp := pond.New(d.parallelBucketMigrations, len(buckets), pond.Context(ctx))

	for _, bucketName := range buckets {
		wp.Submit(func() {
			logger := logging.FromContext(ctx).WithFields(map[string]any{
				"bucket": bucketName,
			})
			b := d.bucketFactory.Create(bucketName, d.db)

		l:
			for {
				errChan := make(chan error, 1)
				go func() {
					logger.Infof("Upgrading...")
					errChan <- b.Migrate(logging.ContextWithLogger(ctx, logger))
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

						logger.Info("Upgrade terminated")
						return
					}
				}
			}
		})
	}

	wp.StopAndWait()

	return nil
}

func (d *Driver) HasReachMinimalVersion(ctx context.Context) (bool, error) {
	systemStore := systemstore.New(d.db)

	isUpToDate, err := systemStore.IsUpToDate(ctx)
	if err != nil {
		return false, fmt.Errorf("checking if system store is up to date: %w", err)
	}
	if !isUpToDate {
		return false, nil
	}

	buckets, err := systemStore.GetDistinctBuckets(ctx)
	if err != nil {
		return false, fmt.Errorf("getting distinct buckets: %w", err)
	}

	for _, b := range buckets {
		hasMinimalVersion, err := d.bucketFactory.Create(b, d.db).HasMinimalVersion(ctx)
		if err != nil {
			return false, fmt.Errorf("checking if bucket '%s' is up to date: %w", b, err)
		}
		if !hasMinimalVersion {
			return false, nil
		}
	}

	return true, nil
}

func New(
	db *bun.DB,
	ledgerStoreFactory ledgerstore.Factory,
	bucketFactory bucket.Factory,
	opts ...Option,
) *Driver {
	ret := &Driver{
		db:                 db,
		ledgerStoreFactory: ledgerStoreFactory,
		bucketFactory:      bucketFactory,
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
