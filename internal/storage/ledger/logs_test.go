//go:build it

package ledger_test

import (
	"context"
	"database/sql"
	"github.com/formancehq/go-libs/v2/metadata"
	"github.com/formancehq/go-libs/v2/pointer"
	"golang.org/x/sync/errgroup"
	"math/big"
	"testing"

	"errors"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"

	"github.com/formancehq/go-libs/v2/bun/bunpaginate"
	"github.com/formancehq/go-libs/v2/time"

	"github.com/formancehq/go-libs/v2/logging"

	"github.com/formancehq/go-libs/v2/query"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/stretchr/testify/require"
)

func TestLogsInsert(t *testing.T) {
	t.Parallel()

	store := newLedgerStore(t)
	ctx := logging.TestingContext()

	t.Run("check hash against core", func(t *testing.T) {
		// Insert a first tx (we don't have any previous hash to use at this moment)
		log1 := ledger.NewLog(ledger.CreatedTransaction{
			Transaction: ledger.NewTransaction().WithMetadata(metadata.Metadata{
				"foo": "<nil>",
				"bar": "?/\\'>",
			}),
			AccountMetadata: ledger.AccountMetadata{},
		})
		log1Copy := log1

		err := store.InsertLog(ctx, &log1)
		require.NoError(t, err)

		require.Equal(t, 1, *log1.ID)
		require.NotZero(t, log1.Hash)
		require.NotEmpty(t, log1.Date)

		// Ensure than the database hashing is the same as the go hashing
		log1Copy.Date = log1.Date
		chainedLog1 := log1Copy.ChainLog(nil)
		require.Equal(t, chainedLog1.Hash, log1.Hash)

		// Insert a new log to test the hash when a previous hash exists
		// We also addi an idempotency key to check for conflicts
		log2 := ledger.NewLog(ledger.CreatedTransaction{
			Transaction: ledger.NewTransaction().WithID(1).WithMetadata(metadata.Metadata{
				"foo": "<nil>",
			}),
			AccountMetadata: ledger.AccountMetadata{},
		})
		log2Copy := log2
		err = store.InsertLog(ctx, &log2)
		require.NoError(t, err)
		require.Equal(t, 2, *log2.ID)
		require.NotZero(t, log2.Hash)
		require.NotZero(t, log2.Date)

		// Ensure than the database hashing is the same as the go hashing
		log2Copy.Date = log2.Date
		chainedLog2 := log2Copy.ChainLog(&log1)
		require.Equal(t, chainedLog2.Hash, log2.Hash)
	})

	t.Run("duplicate IK", func(t *testing.T) {
		// Insert a first tx (we don't have any previous hash to use at this moment)
		logTx := ledger.NewLog(ledger.CreatedTransaction{
			Transaction:     ledger.NewTransaction(),
			AccountMetadata: ledger.AccountMetadata{},
		}).
			WithIdempotencyKey("foo")

		err := store.InsertLog(ctx, &logTx)
		require.NoError(t, err)

		require.NotZero(t, logTx.ID)
		require.NotZero(t, logTx.Hash)

		// Create a new log with the same IK as previous should fail
		logTx = ledger.NewLog(ledger.CreatedTransaction{
			Transaction:     ledger.NewTransaction(),
			AccountMetadata: ledger.AccountMetadata{},
		}).
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

				logTx := ledger.NewLog(ledger.CreatedTransaction{
					Transaction:     ledger.NewTransaction(),
					AccountMetadata: ledger.AccountMetadata{},
				})
				err = store.InsertLog(ctx, &logTx)
				if err != nil {
					return err
				}
				return tx.Commit()
			})
		}
		err := errGroup.Wait()
		require.NoError(t, err)

		logs, err := store.Logs().Paginate(ctx, ledgercontroller.ColumnPaginatedQuery[any]{
			PageSize: countLogs,
			Order:    pointer.For(bunpaginate.Order(bunpaginate.OrderAsc)),
		})
		require.NoError(t, err)

		var previous *ledger.Log
		for _, log := range logs.Data {
			expectedHash := log.Hash
			expectedID := *log.ID
			log.Hash = nil
			log.ID = pointer.For(0)
			chainedLog := log.ChainLog(previous)
			require.Equal(t, expectedHash, chainedLog.Hash, "checking log hash %d", expectedID)
			previous = &chainedLog
		}
	})

	t.Run("insert with special characters", func(t *testing.T) {

		type testCase struct {
			name     string
			metadata map[string]string
		}

		testCases := []testCase{
			{name: "with escaped quotes", metadata: map[string]string{"key": "value with \"quotes\""}},
			{name: "with utf-8 characters", metadata: map[string]string{"rate": "½"}},
		}

		for _, testCase := range testCases {
			t.Run(testCase.name, func(t *testing.T) {
				t.Parallel()

				log := ledger.NewLog(ledger.CreatedTransaction{
					Transaction: ledger.NewTransaction().
						WithPostings(ledger.NewPosting("world", "bank", "USD", big.NewInt(100))).
						WithMetadata(testCase.metadata),
				})

				err := store.InsertLog(ctx, &log)
				require.NoError(t, err)
			})
		}
	})
}

func TestLogsReadWithIdempotencyKey(t *testing.T) {
	t.Parallel()

	store := newLedgerStore(t)
	ctx := logging.TestingContext()

	logTx := ledger.NewLog(
		ledger.CreatedTransaction{
			Transaction: ledger.NewTransaction().
				WithPostings(
					ledger.NewPosting("world", "bank", "USD", big.NewInt(100)),
				),
			AccountMetadata: ledger.AccountMetadata{},
		},
	)
	log := logTx.WithIdempotencyKey("test")
	err := store.InsertLog(ctx, &log)
	require.NoError(t, err)

	lastLog, err := store.ReadLogWithIdempotencyKey(context.Background(), "test")
	require.NoError(t, err)
	require.NotNil(t, lastLog)
	require.Equal(t, log, *lastLog)
}

func TestLogsList(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)
	now := time.Now()
	ctx := logging.TestingContext()

	for i := 1; i <= 3; i++ {
		newLog := ledger.NewLog(ledger.CreatedTransaction{
			Transaction:     ledger.NewTransaction(),
			AccountMetadata: ledger.AccountMetadata{},
		})
		newLog.Date = now.Add(-time.Duration(i) * time.Hour)

		err := store.InsertLog(ctx, &newLog)
		require.NoError(t, err)
	}

	cursor, err := store.Logs().Paginate(context.Background(), ledgercontroller.ColumnPaginatedQuery[any]{})
	require.NoError(t, err)
	require.Equal(t, bunpaginate.QueryDefaultPageSize, cursor.PageSize)

	require.Equal(t, 3, len(cursor.Data))
	require.EqualValues(t, 3, *cursor.Data[0].ID)

	cursor, err = store.Logs().Paginate(context.Background(), ledgercontroller.ColumnPaginatedQuery[any]{
		PageSize: 1,
	})
	require.NoError(t, err)
	// Should get only the first log.
	require.Equal(t, 1, cursor.PageSize)
	require.EqualValues(t, 3, *cursor.Data[0].ID)

	cursor, err = store.Logs().Paginate(context.Background(), ledgercontroller.ColumnPaginatedQuery[any]{
		PageSize: 10,
		Options: ledgercontroller.ResourceQuery[any]{
			Builder: query.And(
				query.Gte("date", now.Add(-2*time.Hour)),
				query.Lt("date", now.Add(-time.Hour)),
			),
		},
	})
	require.NoError(t, err)
	require.Equal(t, 10, cursor.PageSize)
	// Should get only the second log, as StartTime is inclusive and EndTime exclusive.
	require.Len(t, cursor.Data, 1)
	require.EqualValues(t, 2, *cursor.Data[0].ID)

	cursor, err = store.Logs().Paginate(context.Background(), ledgercontroller.ColumnPaginatedQuery[any]{
		PageSize: 10,
		Options: ledgercontroller.ResourceQuery[any]{
			Builder: query.Lt("id", 3),
		},
	})
	require.NoError(t, err)
	require.Equal(t, 2, len(cursor.Data))
}
