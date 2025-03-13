package driver

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/alitto/pond"
	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/formancehq/go-libs/v2/metadata"
	"github.com/formancehq/go-libs/v2/migrations"
	"github.com/formancehq/go-libs/v2/platform/postgres"
	systemcontroller "github.com/formancehq/ledger/internal/controller/system"
	systemstore "github.com/formancehq/ledger/internal/storage/system"
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
	systemStore        systemstore.Store
	bucketFactory      bucket.Factory
	tracer             trace.Tracer
	meter              metric.Meter

	migrationRetryPeriod      time.Duration
	migratorLockRetryInterval time.Duration
	parallelBucketMigrations  int
}

/*
CreateLedger creates a new ledger in the system and sets up all necessary database objects.

The function follows these steps:
 1. Create a ledger record in the system store (_system.ledgers table)
 2. Get the bucket (database schema) for this ledger
 3. Check if the bucket is already initialized:
    a. If initialized: Verify it's up to date and add ledger-specific objects to it
    b. If not initialized: Create the bucket schema with all necessary tables
 4. Return a ledger store that provides an interface to interact with the ledger

Note: This process is not atomic. If context cancellation occurs between creating
the ledger record and setting up the bucket, the system could be left in an
inconsistent state with a ledger record but no corresponding database objects.
*/
func (d *Driver) CreateLedger(ctx context.Context, l *ledger.Ledger) (*ledgerstore.Store, error) {
	// Track whether we've successfully created the ledger in the system store
	ledgerCreatedInSystemStore := false
	bucketSetupComplete := false

	// Defer an assertion check that will run when the function exits
	defer func() {
		// The invariant we want to check:
		// Either (1) both steps completed successfully OR (2) neither step completed
		// If only the ledger was created but bucket setup failed, that's a problem
		invariantMaintained := (!ledgerCreatedInSystemStore && !bucketSetupComplete) ||
			(ledgerCreatedInSystemStore && bucketSetupComplete)

		assert.Always(invariantMaintained, "ledger_creation_atomic", map[string]any{
			"ledger_name":                    l.Name,
			"bucket":                         l.Bucket,
			"ledger_created_in_system_store": ledgerCreatedInSystemStore,
			"bucket_setup_complete":          bucketSetupComplete,
		})
	}()

	// Create ledger record in system store
	if err := d.systemStore.CreateLedger(ctx, l); err != nil {
		if errors.Is(postgres.ResolveError(err), postgres.ErrConstraintsFailed{}) {
			return nil, systemcontroller.ErrLedgerAlreadyExists
		}
		return nil, postgres.ResolveError(err)
	}

	// Mark that we've successfully created the ledger in the system store
	ledgerCreatedInSystemStore = true

	// Get the bucket for this ledger
	b := d.bucketFactory.Create(l.Bucket)
	isInitialized, err := b.IsInitialized(ctx)
	if err != nil {
		return nil, fmt.Errorf("checking if bucket is initialized: %w", err)
	}

	if isInitialized {
		// Bucket exists - check if it's up to date
		upToDate, err := b.IsUpToDate(ctx)
		if err != nil {
			return nil, fmt.Errorf("checking if bucket is up to date: %w", err)
		}

		if !upToDate {
			return nil, systemcontroller.ErrBucketOutdated
		}

		// Add ledger-specific objects to the bucket
		if err := b.AddLedger(ctx, *l); err != nil {
			return nil, fmt.Errorf("adding ledger to bucket: %w", err)
		}
	} else {
		// Bucket doesn't exist - create it
		if err := b.Migrate(
			ctx,
			migrations.WithLockRetryInterval(d.migratorLockRetryInterval),
		); err != nil {
			return nil, fmt.Errorf("migrating bucket: %w", err)
		}
	}

	// Mark that bucket setup is complete
	bucketSetupComplete = true

	// Create and return a store for interacting with the ledger
	return d.ledgerStoreFactory.Create(b, *l), nil
}

func (d *Driver) OpenLedger(ctx context.Context, name string) (*ledgerstore.Store, *ledger.Ledger, error) {
	// todo: keep the ledger in cache somewhere to avoid read the ledger at each request, maybe in the factory
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
		migrations.WithLockRetryInterval(d.migratorLockRetryInterval),
	)
}

func (d *Driver) UpgradeAllBuckets(ctx context.Context) error {

	buckets, err := d.systemStore.GetDistinctBuckets(ctx)
	if err != nil {
		return fmt.Errorf("getting distinct buckets: %w", err)
	}

	wp := pond.New(d.parallelBucketMigrations, len(buckets), pond.Context(ctx))

	for _, bucketName := range buckets {
		wp.Submit(func() {
			logger := logging.FromContext(ctx).WithFields(map[string]any{
				"bucket": bucketName,
			})
			b := d.bucketFactory.Create(bucketName)

		l:
			for {
				errChan := make(chan error, 1)
				go func() {
					logger.Infof("Upgrading...")
					errChan <- b.Migrate(
						logging.ContextWithLogger(ctx, logger),
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
	isUpToDate, err := d.systemStore.IsUpToDate(ctx)
	if err != nil {
		return false, fmt.Errorf("checking if system store is up to date: %w", err)
	}
	if !isUpToDate {
		return false, nil
	}

	buckets, err := d.systemStore.GetDistinctBuckets(ctx)
	if err != nil {
		return false, fmt.Errorf("getting distinct buckets: %w", err)
	}

	for _, b := range buckets {
		hasMinimalVersion, err := d.bucketFactory.Create(b).HasMinimalVersion(ctx)
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
