//go:build it

package ledger_test

import (
	"context"
	"database/sql"
	"golang.org/x/sync/errgroup"
	"math/big"
	"testing"

	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/pkg/errors"

	"github.com/formancehq/go-libs/bun/bunpaginate"
	"github.com/formancehq/go-libs/time"

	"github.com/formancehq/go-libs/logging"

	"github.com/formancehq/go-libs/metadata"
	"github.com/formancehq/go-libs/query"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/stretchr/testify/require"
)

func TestInsertLog(t *testing.T) {
	t.Parallel()

	store := newLedgerStore(t)
	ctx := logging.TestingContext()

	t.Run("duplicate IK", func(t *testing.T) {
		// Insert a first tx (we don't have any previous hash to use at this moment)
		logTx := ledger.NewTransactionLog(ledger.NewTransaction(), map[string]metadata.Metadata{}).
			WithIdempotencyKey("foo")

		err := store.InsertLog(ctx, &logTx)
		require.NoError(t, err)

		require.NotZero(t, logTx.ID)
		require.NotZero(t, logTx.Hash)

		// Create a new log with the same IK as previous should fail
		logTx = ledger.NewTransactionLog(ledger.NewTransaction(), map[string]metadata.Metadata{}).
			WithIdempotencyKey("foo")
		err = store.InsertLog(ctx, &logTx)
		require.Error(t, err)
		require.True(t, errors.Is(err, ledgercontroller.ErrIdempotencyKeyConflict{}))
	})

	t.Run("hash consistency over high concurrency", func(t *testing.T) {
		errGroup, _ := errgroup.WithContext(ctx)
		const countLogs = 50
		for range countLogs {
			errGroup.Go(func() error {
				tx, err := store.GetDB().BeginTx(ctx, &sql.TxOptions{})
				if err != nil {
					return err
				}
				defer func() {
					_ = tx.Rollback()
				}()
				store := store.WithDB(tx)

				logTx := ledger.NewTransactionLog(ledger.NewTransaction(), map[string]metadata.Metadata{})
				err = store.InsertLog(ctx, &logTx)
				if err != nil {
					return err
				}
				return tx.Commit()
			})
		}
		err := errGroup.Wait()
		require.NoError(t, err)

		logs, err := store.ListLogs(ctx, ledgercontroller.NewListLogsQuery(ledgercontroller.PaginatedQueryOptions[any]{
			PageSize: countLogs,
		}).WithOrder(bunpaginate.OrderAsc))
		require.NoError(t, err)

		var previous *ledger.Log
		for _, log := range logs.Data {
			expectedHash := log.Hash
			expectedID := log.ID
			log.Hash = nil
			log.ID = 0
			chainedLog := log.ChainLog(previous)
			require.Equal(t, expectedHash, chainedLog.Hash, "checking log hash %d", expectedID)
			previous = &chainedLog
		}
	})
}

func TestReadLogWithIdempotencyKey(t *testing.T) {
	t.Parallel()

	store := newLedgerStore(t)
	ctx := logging.TestingContext()

	logTx := ledger.NewTransactionLog(
		ledger.NewTransaction().
			WithPostings(
				ledger.NewPosting("world", "bank", "USD", big.NewInt(100)),
			),
		map[string]metadata.Metadata{},
	)
	log := logTx.WithIdempotencyKey("test")
	err := store.InsertLog(ctx, &log)
	require.NoError(t, err)

	lastLog, err := store.ReadLogWithIdempotencyKey(context.Background(), "test")
	require.NoError(t, err)
	require.NotNil(t, lastLog)
	require.Equal(t, log, *lastLog)
}

func TestGetLogs(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)
	now := time.Now()
	ctx := logging.TestingContext()

	for i := 1; i <= 3; i++ {
		newLog := ledger.NewTransactionLog(ledger.NewTransaction(), map[string]metadata.Metadata{})
		newLog.Date = now.Add(-time.Duration(i) * time.Hour)

		err := store.InsertLog(ctx, &newLog)
		require.NoError(t, err)
	}

	cursor, err := store.ListLogs(context.Background(), ledgercontroller.NewListLogsQuery(ledgercontroller.NewPaginatedQueryOptions[any](nil)))
	require.NoError(t, err)
	require.Equal(t, bunpaginate.QueryDefaultPageSize, cursor.PageSize)

	require.Equal(t, 3, len(cursor.Data))
	require.EqualValues(t, 3, cursor.Data[0].ID)

	cursor, err = store.ListLogs(context.Background(), ledgercontroller.NewListLogsQuery(ledgercontroller.NewPaginatedQueryOptions[any](nil).WithPageSize(1)))
	require.NoError(t, err)
	// Should get only the first log.
	require.Equal(t, 1, cursor.PageSize)
	require.EqualValues(t, 3, cursor.Data[0].ID)

	cursor, err = store.ListLogs(context.Background(), ledgercontroller.NewListLogsQuery(ledgercontroller.NewPaginatedQueryOptions[any](nil).
		WithQueryBuilder(query.And(
			query.Gte("date", now.Add(-2*time.Hour)),
			query.Lt("date", now.Add(-time.Hour)),
		)).
		WithPageSize(10),
	))
	require.NoError(t, err)
	require.Equal(t, 10, cursor.PageSize)
	// Should get only the second log, as StartTime is inclusive and EndTime exclusive.
	require.Len(t, cursor.Data, 1)
	require.EqualValues(t, 2, cursor.Data[0].ID)
}
