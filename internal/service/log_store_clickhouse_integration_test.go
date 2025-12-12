//go:build it
// +build it

package service

import (
	"io"
	"math/big"
	"testing"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/testing/docker"
	"github.com/formancehq/go-libs/v3/testing/platform/clickhousetesting"
	ledger "github.com/formancehq/ledger-v3-poc/internal"
	"github.com/stretchr/testify/require"
)

func TestClickHouseLogStoreIntegration(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	pool := docker.NewPool(t, logger)
	clickhouseServer := clickhousetesting.CreateServer(pool)

	ledgerName := "test-ledger"

	t.Run("InsertLogs", func(t *testing.T) {
		t.Parallel()
		database := clickhouseServer.NewDatabase(t)
		dsn := database.ConnString()
		store := createClickHouseStore(t, dsn)

		testLogs := createTestLogs(t, ledgerName)
		err := store.InsertLogs(ctx, testLogs...)
		require.NoError(t, err)
	})

	t.Run("GetLogWithIdempotencyKey", func(t *testing.T) {
		t.Parallel()
		database := clickhouseServer.NewDatabase(t)
		dsn := database.ConnString()
		store := createClickHouseStore(t, dsn)

		testLogs := createTestLogs(t, ledgerName)
		err := store.InsertLogs(ctx, testLogs...)
		require.NoError(t, err)

		// Test with existing idempotency key
		log, err := store.GetLogWithIdempotencyKey(ctx, ledgerName, "idempotency-key-1")
		require.NoError(t, err)
		require.NotNil(t, log)
		require.Equal(t, "idempotency-key-1", log.IdempotencyKey)
		require.Equal(t, ledgerName, log.Ledger)

		// Test with non-existing idempotency key
		log, err = store.GetLogWithIdempotencyKey(ctx, ledgerName, "non-existing-key")
		require.Error(t, err)

		// Test with different ledger
		log, err = store.GetLogWithIdempotencyKey(ctx, "other-ledger", "idempotency-key-1")
		require.Error(t, err)
	})

	t.Run("GetLastLog", func(t *testing.T) {
		t.Parallel()
		database := clickhouseServer.NewDatabase(t)
		dsn := database.ConnString()
		store := createClickHouseStore(t, dsn)

		testLogs := createTestLogs(t, ledgerName)
		err := store.InsertLogs(ctx, testLogs...)
		require.NoError(t, err)

		// Get last log
		lastLog, err := store.GetLastLog(ctx, ledgerName)
		require.NoError(t, err)
		require.NotNil(t, lastLog)
		require.Equal(t, ledgerName, lastLog.Ledger)
		// The last log should be the one with the highest ID
		if len(testLogs) > 0 {
			expectedID := testLogs[len(testLogs)-1].ID
			if expectedID != nil {
				require.Equal(t, *expectedID, *lastLog.ID)
			}
		}

		// Test with non-existing ledger
		lastLog, err = store.GetLastLog(ctx, "non-existing-ledger")
		require.NoError(t, err)
		require.Nil(t, lastLog)
	})

	t.Run("GetAllLogs", func(t *testing.T) {
		t.Parallel()
		database := clickhouseServer.NewDatabase(t)
		dsn := database.ConnString()
		store := createClickHouseStore(t, dsn)

		testLogs := createTestLogs(t, ledgerName)
		err := store.InsertLogs(ctx, testLogs...)
		require.NoError(t, err)

		// Get all logs
		cursorPtr, err := store.GetAllLogs(ctx, 0)
		require.NoError(t, err)
		require.NotNil(t, cursorPtr)
		cursor := *cursorPtr
		t.Cleanup(func() { _ = cursor.Close() })

		// Read all logs and filter by ledger
		var logs []ledger.Log
		for {
			log, err := cursor.Next(ctx)
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			// Filter by ledger name
			if log.Ledger == ledgerName {
				logs = append(logs, log)
			}
		}

		// Verify we got all logs for this ledger
		require.Equal(t, len(testLogs), len(logs))

		// Verify logs are in ascending order by sequence
		for i := 0; i < len(logs)-1; i++ {
			require.LessOrEqual(t, logs[i].Sequence, logs[i+1].Sequence)
		}
	})

	t.Run("InsertLogsEmpty", func(t *testing.T) {
		t.Parallel()
		database := clickhouseServer.NewDatabase(t)
		dsn := database.ConnString()
		store := createClickHouseStore(t, dsn)

		err := store.InsertLogs(ctx)
		require.NoError(t, err)
	})

	t.Run("BalancesCalculation", func(t *testing.T) {
		t.Parallel()
		database := clickhouseServer.NewDatabase(t)
		dsn := database.ConnString()
		store := createClickHouseStore(t, dsn)
		testLogs := createTestLogs(t, ledgerName)

		// Insert logs
		err := store.InsertLogs(ctx, testLogs...)
		require.NoError(t, err)

		balances, err := store.GetBalances(ctx, ledgerName, map[string][]string{
			"world": {"USD"},
			"bank":  {"USD"},
			"user":  {"USD"},
		})
		require.NoError(t, err)
		require.Equal(t, big.NewInt(0), balances["world"]["USD"])
		require.Equal(t, big.NewInt(-50), balances["bank"]["USD"])
		require.Equal(t, big.NewInt(50), balances["user"]["USD"])
	})
}

func createClickHouseStore(t *testing.T, dsn string) *ClickHouseLogStore {
	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	store, err := NewClickHouseLogStore(ctx, dsn, logger)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	return store
}
