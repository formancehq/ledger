//go:build it

package driver_test

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/metadata"
	"github.com/formancehq/go-libs/v3/query"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/storage/bucket"
	storagecommon "github.com/formancehq/ledger/internal/storage/common"
	"github.com/formancehq/ledger/internal/storage/driver"
	ledgerstore "github.com/formancehq/ledger/internal/storage/ledger"
	systemstore "github.com/formancehq/ledger/internal/storage/system"
)

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
