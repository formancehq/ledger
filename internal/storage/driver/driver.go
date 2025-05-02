package driver

import (
	"context"
	"errors"
	"fmt"
	"github.com/formancehq/ledger/internal/storage/common"
	systemstore "github.com/formancehq/ledger/internal/storage/system"
	"github.com/formancehq/ledger/internal/tracing"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
	"sync"
	stdtime "time"

	"github.com/alitto/pond"
	"github.com/formancehq/go-libs/v3/bun/bunpaginate"
	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/metadata"
	"github.com/formancehq/go-libs/v3/platform/postgres"
	"github.com/formancehq/go-libs/v3/time"
	ledger "github.com/formancehq/ledger/internal"
	systemcontroller "github.com/formancehq/ledger/internal/controller/system"
	"github.com/formancehq/ledger/internal/storage/bucket"
	ledgerstore "github.com/formancehq/ledger/internal/storage/ledger"
	"github.com/uptrace/bun"
)

type Driver struct {
	ledgerStoreFactory ledgerstore.Factory
	db                 *bun.DB
	bucketFactory      bucket.Factory
	systemStoreFactory systemstore.StoreFactory
	tracer             trace.Tracer

	migrationRetryPeriod     time.Duration
	parallelBucketMigrations int
}

func (d *Driver) CreateLedger(ctx context.Context, l *ledger.Ledger) (*ledgerstore.Store, error) {

	var ret *ledgerstore.Store
	err := d.db.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		systemStore := d.systemStoreFactory.Create(d.db)

		if err := systemStore.CreateLedger(ctx, l); err != nil {
			if errors.Is(postgres.ResolveError(err), postgres.ErrConstraintsFailed{}) {
				return systemcontroller.ErrLedgerAlreadyExists
			}
			return err
		}

		b := d.bucketFactory.Create(l.Bucket)
		isInitialized, err := b.IsInitialized(ctx, tx)
		if err != nil {
			return fmt.Errorf("checking if bucket is initialized: %w", err)
		}
		if isInitialized {
			upToDate, err := b.IsUpToDate(ctx, tx)
			if err != nil {
				return fmt.Errorf("checking if bucket is up to date: %w", err)
			}

			if !upToDate {
				return systemcontroller.ErrBucketOutdated
			}

			if err := b.AddLedger(ctx, tx, *l); err != nil {
				return fmt.Errorf("adding ledger to bucket: %w", err)
			}
		} else {
			if err := b.Migrate(ctx, tx); err != nil {
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
	ret, err := d.systemStoreFactory.Create(d.db).GetLedger(ctx, name)
	if err != nil {
		return nil, nil, err
	}

	if ret.DeletedAt != nil {
		return nil, nil, systemcontroller.ErrLedgerNotFound
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

	err = d.systemStoreFactory.Create(d.db).Migrate(ctx)
	if err != nil {
		return fmt.Errorf("migrating system store: %w", err)
	}

	return nil
}

func (d *Driver) detectRollbacks(ctx context.Context) error {

	systemStore := d.systemStoreFactory.Create(d.db)
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
	return d.systemStoreFactory.Create(d.db).UpdateLedgerMetadata(ctx, name, m)
}

func (d *Driver) DeleteLedgerMetadata(ctx context.Context, name string, key string) error {
	return d.systemStoreFactory.Create(d.db).DeleteLedgerMetadata(ctx, name, key)
}

func (d *Driver) ListLedgers(ctx context.Context, q common.ColumnPaginatedQuery[any]) (*bunpaginate.Cursor[ledger.Ledger], error) {
	return d.systemStoreFactory.Create(d.db).Ledgers().Paginate(ctx, q)
}

func (d *Driver) GetLedger(ctx context.Context, name string) (*ledger.Ledger, error) {
	return d.systemStoreFactory.Create(d.db).GetLedger(ctx, name)
}

func (d *Driver) UpgradeBucket(ctx context.Context, name string) error {
	return d.bucketFactory.Create(name).Migrate(ctx, d.db)
}

func (d *Driver) UpgradeAllBuckets(ctx context.Context) error {
	_, err := tracing.Trace(ctx, d.tracer, "UpgradeAllBuckets", tracing.NoResult(func(ctx context.Context) error {
		buckets, err := d.systemStoreFactory.Create(d.db).GetDistinctBuckets(ctx)
		if err != nil {
			return fmt.Errorf("getting distinct buckets: %w", err)
		}

		wp := pond.New(d.parallelBucketMigrations, len(buckets), pond.Context(ctx))

		for _, bucketName := range buckets {
			wp.Submit(func() {
				logger := logging.FromContext(ctx).WithFields(map[string]any{
					"bucket": bucketName,
				})
			l:
				for {
					if err := d.upgradeBucket(ctx, logger, bucketName); err != nil {
						logger.Errorf("Error upgrading: %s", err)
						select {
						case <-time.After(d.migrationRetryPeriod):
							continue l
						case <-ctx.Done():
							return
						}
					}
					logger.Info("Upgrade terminated")
					break
				}
			})
		}

		wp.StopAndWait()

		return nil
	}))

	return err
}

func (d *Driver) upgradeBucket(ctx context.Context, logger logging.Logger, bucketName string) error {
	ctx, span := d.tracer.Start(ctx, "UpgradeBucket",
		trace.WithNewRoot(),
		trace.WithLinks(
			trace.Link{
				SpanContext: trace.SpanFromContext(ctx).SpanContext(),
			},
		),
	)
	defer span.End()

	logger.Infof("Upgrading...")
	b := d.bucketFactory.Create(bucketName)

	err := b.Migrate(logging.ContextWithLogger(ctx, logger), d.db)
	if err != nil {
		return err
	}

	return nil
}

func (d *Driver) HasReachMinimalVersion(ctx context.Context) (bool, error) {
	systemStore := d.systemStoreFactory.Create(d.db)

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
		hasMinimalVersion, err := d.bucketFactory.Create(b).HasMinimalVersion(ctx, d.db)
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
	systemStoreFactory systemstore.StoreFactory,
	opts ...Option,
) *Driver {
	ret := &Driver{
		db:                 db,
		ledgerStoreFactory: ledgerStoreFactory,
		bucketFactory:      bucketFactory,
		systemStoreFactory: systemStoreFactory,
	}
	for _, opt := range append(defaultOptions, opts...) {
		opt(ret)
	}
	return ret
}

type Option func(d *Driver)

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

func WithTracer(tracer trace.Tracer) Option {
	return func(d *Driver) {
		d.tracer = tracer
	}
}

func (d *Driver) GetBucketsMarkedForDeletion(ctx context.Context, days int) ([]string, error) {
	if days < 0 {
		return []string{}, nil
	}
	
	var buckets []string
	cutoffDate := time.Now().UTC().AddDate(0, 0, -days)
	
	err := d.db.NewSelect().
		DistinctOn("bucket").
		Model(&ledger.Ledger{}).
		Column("bucket").
		Where("deleted_at IS NOT NULL").
		Where("deleted_at <= ?", cutoffDate).
		Scan(ctx, &buckets)
	
	if err != nil {
		return nil, fmt.Errorf("getting buckets marked for deletion: %w", postgres.ResolveError(err))
	}
	
	return buckets, nil
}

func (d *Driver) PhysicallyDeleteBucket(ctx context.Context, bucketName string) error {
	_, err := d.db.ExecContext(ctx, fmt.Sprintf(`DROP SCHEMA IF EXISTS "%s" CASCADE`, bucketName))
	if err != nil {
		return fmt.Errorf("dropping bucket schema: %w", postgres.ResolveError(err))
	}
	
	_, err = d.db.NewDelete().
		Model(&ledger.Ledger{}).
		Where("bucket = ?", bucketName).
		Exec(ctx)
	
	if err != nil {
		return fmt.Errorf("deleting ledger entries for bucket: %w", postgres.ResolveError(err))
	}
	
	return nil
}

func (d *Driver) ListBucketsWithStatus(ctx context.Context, query common.ColumnPaginatedQuery[any]) (*bunpaginate.Cursor[systemcontroller.BucketWithStatus], error) {
	return d.systemStoreFactory.Create(d.db).ListBucketsWithStatus(ctx, query)
}

func (d *Driver) RestoreBucket(ctx context.Context, bucketName string) error {
	return d.systemStoreFactory.Create(d.db).RestoreBucket(ctx, bucketName)
}

func (d *Driver) MarkBucketAsDeleted(ctx context.Context, bucketName string) error {
	return d.systemStoreFactory.Create(d.db).MarkBucketAsDeleted(ctx, bucketName)
}

var defaultOptions = []Option{
	WithParallelBucketMigration(10),
	WithMigrationRetryPeriod(5 * time.Second),
	WithTracer(noop.Tracer{}),
}
