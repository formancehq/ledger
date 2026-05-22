//go:build it

package driver_test

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	"github.com/formancehq/go-libs/v5/pkg/query"
	"github.com/formancehq/go-libs/v5/pkg/types/metadata"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/storage/bucket"
	storagecommon "github.com/formancehq/ledger/internal/storage/common"
	"github.com/formancehq/ledger/internal/storage/driver"
	ledgerstore "github.com/formancehq/ledger/internal/storage/ledger"
	systemstore "github.com/formancehq/ledger/internal/storage/system"
)

// queryCounter counts bun queries issued through a specific *bun.DB.
type queryCounter struct{ n atomic.Int64 }

func (h *queryCounter) BeforeQuery(_ context.Context, _ *bun.QueryEvent) context.Context {
	return context.Background()
}
func (h *queryCounter) AfterQuery(_ context.Context, _ *bun.QueryEvent) { h.n.Add(1) }

// countingDB wraps the shared sql.DB in a fresh bun.DB with a query counter attached.
func countingDB(t *testing.T) (*bun.DB, *queryCounter) {
	t.Helper()
	cdb := bun.NewDB(db.DB, pgdialect.New(), bun.WithDiscardUnknownColumns())
	h := &queryCounter{}
	cdb.AddQueryHook(h)
	return cdb, h
}

func TestLedgersCreate(t *testing.T) {
	t.Parallel()
	ctx := logging.TestingContext()

	d := driver.New(
		db,
		ledgerstore.NewFactory(db),
		bucket.NewDefaultFactory(),
		systemstore.NewStoreFactory(),
	)

	buckets := []string{"bucket1", "bucket2"}
	const countLedgers = 30

	wg := sync.WaitGroup{}
	wg.Add(countLedgers)
	errors := make(chan error, countLedgers)
	for i := range countLedgers {
		go func() {
			defer wg.Done()

			l, err := ledger.New(fmt.Sprintf("ledger%d", i), ledger.Configuration{
				Bucket: buckets[rand.Int31n(int32(len(buckets)))],
			})
			if err != nil {
				errors <- err
				return
			}

			_, err = d.CreateLedger(ctx, l)
			if err != nil {
				errors <- err
				return
			}
		}()
	}
	wg.Wait()

	close(errors)

	for err := range errors {
		require.NoError(t, err)
	}

	hasReachMinimalVersion, err := d.HasReachMinimalVersion(ctx)
	require.NoError(t, err)
	require.True(t, hasReachMinimalVersion)

	err = d.UpgradeAllBuckets(ctx)
	require.NoError(t, err)
}

func TestLedgersList(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()

	d := driver.New(
		db,
		ledgerstore.NewFactory(db),
		bucket.NewDefaultFactory(),
		systemstore.NewStoreFactory(),
	)

	bucket := uuid.NewString()[:8]

	l1, err := ledger.New(uuid.NewString(), ledger.Configuration{
		Bucket: bucket,
	})
	require.NoError(t, err)

	_, err = d.CreateLedger(ctx, l1)
	require.NoError(t, err)

	l2, err := ledger.New(uuid.NewString(), ledger.Configuration{
		Bucket: bucket,
	})
	require.NoError(t, err)

	_, err = d.CreateLedger(ctx, l2)
	require.NoError(t, err)

	q := storagecommon.InitialPaginatedQuery[systemstore.ListLedgersQueryPayload]{
		Options: storagecommon.ResourceQuery[systemstore.ListLedgersQueryPayload]{
			Builder: query.Match("bucket", bucket),
		},
	}

	cursor, err := d.ListLedgers(ctx, q)
	require.NoError(t, err)

	require.Len(t, cursor.Data, 2)
}

func TestLedgerUpdateMetadata(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()

	d := driver.New(
		db,
		ledgerstore.NewFactory(db),
		bucket.NewDefaultFactory(),
		systemstore.NewStoreFactory(),
	)

	l := ledger.MustNewWithDefault(uuid.NewString())
	_, err := d.CreateLedger(ctx, &l)
	require.NoError(t, err)

	addedMetadata := metadata.Metadata{
		"foo": "bar",
	}
	err = d.UpdateLedgerMetadata(ctx, l.Name, addedMetadata)
	require.NoError(t, err)
}

func TestLedgerDeleteMetadata(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	d := driver.New(
		db,
		ledgerstore.NewFactory(db),
		bucket.NewDefaultFactory(),
		systemstore.NewStoreFactory(),
	)

	l := ledger.MustNewWithDefault(uuid.NewString()).WithMetadata(metadata.Metadata{
		"foo": "bar",
	})
	_, err := d.CreateLedger(ctx, &l)
	require.NoError(t, err)

	err = d.DeleteLedgerMetadata(ctx, l.Name, "foo")
	require.NoError(t, err)
}

func TestOpenLedgerCacheHit(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	d := driver.New(
		db,
		ledgerstore.NewFactory(db),
		bucket.NewDefaultFactory(),
		systemstore.NewStoreFactory(),
	)

	l := ledger.MustNewWithDefault(uuid.NewString())
	_, err := d.CreateLedger(ctx, &l)
	require.NoError(t, err)

	_, got1, err := d.OpenLedger(ctx, l.Name)
	require.NoError(t, err)
	require.Equal(t, l.Name, got1.Name)

	_, got2, err := d.OpenLedger(ctx, l.Name)
	require.NoError(t, err)
	require.Equal(t, l.Name, got2.Name)
}

func TestOpenLedgerCacheEvictionOnMetadataUpdate(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	d := driver.New(
		db,
		ledgerstore.NewFactory(db),
		bucket.NewDefaultFactory(),
		systemstore.NewStoreFactory(),
	)

	l := ledger.MustNewWithDefault(uuid.NewString())
	_, err := d.CreateLedger(ctx, &l)
	require.NoError(t, err)

	_, _, err = d.OpenLedger(ctx, l.Name)
	require.NoError(t, err)

	err = d.UpdateLedgerMetadata(ctx, l.Name, metadata.Metadata{"env": "prod"})
	require.NoError(t, err)

	_, got, err := d.OpenLedger(ctx, l.Name)
	require.NoError(t, err)
	require.Equal(t, "prod", got.Metadata["env"])
}

func TestOpenLedgerCacheEvictionOnMetadataDelete(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	d := driver.New(
		db,
		ledgerstore.NewFactory(db),
		bucket.NewDefaultFactory(),
		systemstore.NewStoreFactory(),
	)

	l := ledger.MustNewWithDefault(uuid.NewString()).WithMetadata(metadata.Metadata{"env": "staging"})
	_, err := d.CreateLedger(ctx, &l)
	require.NoError(t, err)

	_, _, err = d.OpenLedger(ctx, l.Name)
	require.NoError(t, err)

	err = d.DeleteLedgerMetadata(ctx, l.Name, "env")
	require.NoError(t, err)

	_, got, err := d.OpenLedger(ctx, l.Name)
	require.NoError(t, err)
	require.Empty(t, got.Metadata["env"])
}

// TestOpenLedgerCacheHitNoDBQueries verifies that a warm-cache OpenLedger call
// issues zero DB queries. It uses a separate bun.DB with a query counter so the
// measurement is isolated from other tests sharing the same sql.DB.
func TestOpenLedgerCacheHitNoDBQueries(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()

	// Create the ledger using the shared DB (migration state is shared).
	setup := driver.New(db, ledgerstore.NewFactory(db), bucket.NewDefaultFactory(), systemstore.NewStoreFactory())
	l := ledger.MustNewWithDefault(uuid.NewString())
	_, err := setup.CreateLedger(ctx, &l)
	require.NoError(t, err)

	// Build a fresh driver with a query-counting bun.DB.
	cdb, counter := countingDB(t)
	d := driver.New(cdb, ledgerstore.NewFactory(cdb), bucket.NewDefaultFactory(), systemstore.NewStoreFactory())

	// Cold call — populates the cache.
	_, _, err = d.OpenLedger(ctx, l.Name)
	require.NoError(t, err)

	before := counter.n.Load()

	// Warm call — must not touch the DB.
	_, got, err := d.OpenLedger(ctx, l.Name)
	require.NoError(t, err)
	require.Equal(t, l.Name, got.Name)
	require.Equal(t, before, counter.n.Load(), "cache hit should issue zero DB queries")
}

// TestOpenLedgerConcurrentColdCacheCoalesces fires 50 goroutines simultaneously
// against a cold cache and checks that all get a valid result. With singleflight
// the DB is queried once; without it queries would fan out to all 50.
func TestOpenLedgerConcurrentColdCacheCoalesces(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()

	setup := driver.New(db, ledgerstore.NewFactory(db), bucket.NewDefaultFactory(), systemstore.NewStoreFactory())
	l := ledger.MustNewWithDefault(uuid.NewString())
	_, err := setup.CreateLedger(ctx, &l)
	require.NoError(t, err)

	cdb, counter := countingDB(t)
	d := driver.New(cdb, ledgerstore.NewFactory(cdb), bucket.NewDefaultFactory(), systemstore.NewStoreFactory())

	const goroutines = 50
	var wg sync.WaitGroup
	errs := make(chan error, goroutines)

	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			_, got, err := d.OpenLedger(ctx, l.Name)
			if err != nil {
				errs <- err
				return
			}
			if got.Name != l.Name {
				errs <- fmt.Errorf("unexpected ledger name: %s", got.Name)
			}
		}()
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}

	// Singleflight collapses concurrent cache misses: DB query count should be
	// well below goroutines (ideally 2 — GetLedger + CountLedgersInBucket).
	t.Logf("DB queries for %d concurrent cold-cache OpenLedger calls: %d", goroutines, counter.n.Load())
	require.Less(t, counter.n.Load(), int64(goroutines),
		"singleflight should collapse concurrent cache misses")
}

// TestOpenLedgerCacheTTLExpiry verifies that after the TTL elapses, OpenLedger
// re-reads from the DB and reflects changes made between the two calls.
func TestOpenLedgerCacheTTLExpiry(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	d := driver.New(
		db,
		ledgerstore.NewFactory(db),
		bucket.NewDefaultFactory(),
		systemstore.NewStoreFactory(),
		driver.WithCacheTTL(50*time.Millisecond),
	)

	l := ledger.MustNewWithDefault(uuid.NewString())
	_, err := d.CreateLedger(ctx, &l)
	require.NoError(t, err)

	_, _, err = d.OpenLedger(ctx, l.Name)
	require.NoError(t, err)

	// Bypass the driver cache by updating metadata directly via a separate driver.
	other := driver.New(db, ledgerstore.NewFactory(db), bucket.NewDefaultFactory(), systemstore.NewStoreFactory())
	require.NoError(t, other.UpdateLedgerMetadata(ctx, l.Name, metadata.Metadata{"ttl-test": "yes"}))

	// Wait for the TTL to expire so the next call re-reads from DB.
	time.Sleep(100 * time.Millisecond)

	_, got, err := d.OpenLedger(ctx, l.Name)
	require.NoError(t, err)
	require.Equal(t, "yes", got.Metadata["ttl-test"], "stale cache entry should have expired")
}
}
