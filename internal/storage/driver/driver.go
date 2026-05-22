package driver

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/alitto/pond"
	"github.com/uptrace/bun"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
	"golang.org/x/sync/singleflight"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	"github.com/formancehq/go-libs/v5/pkg/storage/bun/paginate"
	"github.com/formancehq/go-libs/v5/pkg/storage/postgres"
	"github.com/formancehq/go-libs/v5/pkg/types/metadata"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/storage/bucket"
	"github.com/formancehq/ledger/internal/storage/common"
	ledgerstore "github.com/formancehq/ledger/internal/storage/ledger"
	systemstore "github.com/formancehq/ledger/internal/storage/system"
	"github.com/formancehq/ledger/internal/tracing"
)

var ErrBucketOutdated = errors.New("bucket is outdated, you need to upgrade it before adding a new ledger")

type cachedLedger struct {
	ledger    ledger.Ledger
	expiresAt time.Time
}

type Driver struct {
	ledgerStoreFactory ledgerstore.Factory
	db                 *bun.DB
	bucketFactory      bucket.Factory
	systemStoreFactory systemstore.StoreFactory
	tracer             trace.Tracer

	migrationRetryPeriod     time.Duration
	parallelBucketMigrations int

	mu          sync.RWMutex
	ledgerCache map[string]cachedLedger
	cacheGens   map[string]uint64 // invalidation generation per ledger; bumped on every eviction
	cacheTTL    time.Duration
	group       singleflight.Group
}

func (d *Driver) CreateLedger(ctx context.Context, l *ledger.Ledger) (*ledgerstore.Store, error) {

	var ret *ledgerstore.Store
	err := d.db.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		b := d.bucketFactory.Create(l.Bucket)

		// Bring the bucket up to date before inserting the _system.ledgers row.
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
				return ErrBucketOutdated
			}
		} else {
			if err := b.Migrate(ctx, tx); err != nil {
				return fmt.Errorf("migrating bucket: %w", err)
			}
		}

		systemStore := d.systemStoreFactory.Create(tx)
		if err := systemStore.CreateLedger(ctx, l); err != nil {
			if errors.Is(postgres.ResolveError(err), postgres.ErrConstraintsFailed{}) {
				return systemstore.ErrLedgerAlreadyExists
			}
			return postgres.ResolveError(err)
		}

		if err := b.AddLedger(ctx, tx, *l); err != nil {
			return fmt.Errorf("adding ledger to bucket: %w", err)
		}

		count, err := systemStore.CountLedgersInBucket(ctx, l.Bucket)
		if err != nil {
			return fmt.Errorf("counting ledgers in bucket: %w", err)
		}

		ret = d.ledgerStoreFactory.Create(b, *l)
		ret.SetAloneInBucket(count == 1)

		return nil
	})
	if err != nil {
		return nil, postgres.ResolveError(err)
	}

	return ret, nil
}

func (d *Driver) getCachedLedger(name string) (ledger.Ledger, bool) {
	d.mu.RLock()
	entry, ok := d.ledgerCache[name]
	d.mu.RUnlock()

	if !ok {
		return ledger.Ledger{}, false
	}

	if time.Now().After(entry.expiresAt) {
		d.mu.Lock()
		// Double-check: another goroutine may have refreshed the entry.
		if e, still := d.ledgerCache[name]; still && time.Now().After(e.expiresAt) {
			delete(d.ledgerCache, name)
		}
		d.mu.Unlock()
		return ledger.Ledger{}, false
	}

	return entry.ledger, true
}

func (d *Driver) setCachedLedger(l ledger.Ledger) {
	if d.cacheTTL <= 0 {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.ledgerCache[l.Name] = cachedLedger{
		ledger:    l,
		expiresAt: time.Now().Add(d.cacheTTL),
	}
}

// setCachedLedgerGen stores l only if cacheGens[l.Name] still equals gen.
// A mismatched generation means evictCachedLedger ran while the DB query
// was in-flight; in that case the snapshot is stale and must be dropped.
func (d *Driver) setCachedLedgerGen(l ledger.Ledger, gen uint64) {
	if d.cacheTTL <= 0 {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.cacheGens[l.Name] != gen {
		return
	}
	d.ledgerCache[l.Name] = cachedLedger{
		ledger:    l,
		expiresAt: time.Now().Add(d.cacheTTL),
	}
}

func (d *Driver) evictCachedLedger(name string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.ledgerCache, name)
	d.cacheGens[name]++
}

type openLedgerResult struct {
	ledger        ledger.Ledger
	aloneInBucket *bool // non-nil only when derived from a count query; nil → shared atomic already correct
}

func (d *Driver) OpenLedger(ctx context.Context, name string) (*ledgerstore.Store, *ledger.Ledger, error) {
	// aloneInBucket is shared per bucket via the Factory — all stores in the same
	// bucket see updates immediately, so no count query is needed on cache hits.
	if l, ok := d.getCachedLedger(name); ok {
		store := d.ledgerStoreFactory.Create(d.bucketFactory.Create(l.Bucket), l)
		return store, &l, nil
	}

	// Collapse concurrent cache-miss requests for the same ledger into one DB call.
	// DoChan is used so each caller can honour its own ctx while the shared DB
	// work runs under an independent context — a single request cancellation
	// must not abort the in-flight query for all other waiters.
	ch := d.group.DoChan(name, func() (any, error) {
		// Re-check the cache: a previous waiter may have already populated it.
		if l, ok := d.getCachedLedger(name); ok {
			return &openLedgerResult{ledger: l}, nil
		}

		// Snapshot the invalidation generation before hitting the DB.
		// If evictCachedLedger runs while our query is in-flight, the generation
		// will have advanced and we must not write the now-stale snapshot back.
		d.mu.RLock()
		gen := d.cacheGens[name]
		d.mu.RUnlock()

		dbCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		systemStore := d.systemStoreFactory.Create(d.db)

		ret, err := systemStore.GetLedger(dbCtx, name)
		if err != nil {
			return nil, err
		}

		count, err := systemStore.CountLedgersInBucket(dbCtx, ret.Bucket)
		if err != nil {
			return nil, fmt.Errorf("counting ledgers in bucket: %w", err)
		}

		// Write back only if no eviction occurred during the DB queries.
		d.setCachedLedgerGen(*ret, gen)

		alone := count == 1
		return &openLedgerResult{ledger: *ret, aloneInBucket: &alone}, nil
	})

	select {
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	case result := <-ch:
		if result.Err != nil {
			return nil, nil, result.Err
		}
		res := result.Val.(*openLedgerResult)
		l := res.ledger
		store := d.ledgerStoreFactory.Create(d.bucketFactory.Create(l.Bucket), l)
		if res.aloneInBucket != nil {
			store.SetAloneInBucket(*res.aloneInBucket)
		}
		return store, &l, nil
	}
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
	if err := d.systemStoreFactory.Create(d.db).UpdateLedgerMetadata(ctx, name, m); err != nil {
		return err
	}
	d.evictCachedLedger(name)
	return nil
}

func (d *Driver) DeleteLedgerMetadata(ctx context.Context, name string, key string) error {
	if err := d.systemStoreFactory.Create(d.db).DeleteLedgerMetadata(ctx, name, key); err != nil {
		return err
	}
	d.evictCachedLedger(name)
	return nil
}

func (d *Driver) ListLedgers(ctx context.Context, q common.PaginatedQuery[systemstore.ListLedgersQueryPayload]) (*paginate.Cursor[ledger.Ledger], error) {
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
		ledgerCache:        make(map[string]cachedLedger),
		cacheGens:          make(map[string]uint64),
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

func WithCacheTTL(ttl time.Duration) Option {
	return func(d *Driver) {
		d.cacheTTL = ttl
	}
}

var defaultOptions = []Option{
	WithParallelBucketMigration(10),
	WithMigrationRetryPeriod(5 * time.Second),
	WithTracer(noop.Tracer{}),
	WithCacheTTL(60 * time.Second),
}
