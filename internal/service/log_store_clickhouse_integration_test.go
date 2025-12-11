//go:build it
// +build it

package service

import (
	"io"
	"testing"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/testing/docker"
	"github.com/formancehq/go-libs/v3/testing/platform/clickhousetesting"
	ledger "github.com/formancehq/ledger-v3-poc/internal"
	"github.com/stretchr/testify/assert"
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
		store, cleanup := createClickHouseStore(t, dsn)
		t.Cleanup(func() { _ = cleanup() })

		testLogs := createTestLogs(t, ledgerName)
		err := store.InsertLogs(ctx, testLogs...)
		require.NoError(t, err)
	})

	t.Run("GetLogWithIdempotencyKey", func(t *testing.T) {
		t.Parallel()
		database := clickhouseServer.NewDatabase(t)
		dsn := database.ConnString()
		store, cleanup := createClickHouseStore(t, dsn)
		t.Cleanup(func() { _ = cleanup() })

		testLogs := createTestLogs(t, ledgerName)
		err := store.InsertLogs(ctx, testLogs...)
		require.NoError(t, err)

		// Test with existing idempotency key
		log, err := store.GetLogWithIdempotencyKey(ctx, ledgerName, "idempotency-key-1")
		require.NoError(t, err)
		require.NotNil(t, log)
		assert.Equal(t, "idempotency-key-1", log.IdempotencyKey)
		assert.Equal(t, ledgerName, log.Ledger)

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
		store, cleanup := createClickHouseStore(t, dsn)
		t.Cleanup(func() { _ = cleanup() })

		testLogs := createTestLogs(t, ledgerName)
		err := store.InsertLogs(ctx, testLogs...)
		require.NoError(t, err)

		// Get last log
		lastLog, err := store.GetLastLog(ctx, ledgerName)
		require.NoError(t, err)
		require.NotNil(t, lastLog)
		assert.Equal(t, ledgerName, lastLog.Ledger)
		// The last log should be the one with the highest ID
		if len(testLogs) > 0 {
			expectedID := testLogs[len(testLogs)-1].ID
			if expectedID != nil {
				assert.Equal(t, *expectedID, *lastLog.ID)
			}
		}

		// Test with non-existing ledger
		lastLog, err = store.GetLastLog(ctx, "non-existing-ledger")
		require.NoError(t, err)
		assert.Nil(t, lastLog)
	})

	t.Run("GetAllLogs", func(t *testing.T) {
		t.Parallel()
		database := clickhouseServer.NewDatabase(t)
		dsn := database.ConnString()
		store, cleanup := createClickHouseStore(t, dsn)
		t.Cleanup(func() { _ = cleanup() })

		testLogs := createTestLogs(t, ledgerName)
		err := store.InsertLogs(ctx, testLogs...)
		require.NoError(t, err)

		// Get all logs
		cursorPtr, err := store.GetAllLogs(ctx, ledgerName)
		require.NoError(t, err)
		require.NotNil(t, cursorPtr)
		cursor := *cursorPtr
		t.Cleanup(func() { _ = cursor.Close() })

		// Read all logs
		var logs []ledger.Log
		for {
			log, err := cursor.Next(ctx)
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			logs = append(logs, log)
		}

		// Verify we got all logs
		assert.Equal(t, len(testLogs), len(logs))

		// Verify logs are in ascending order by ID
		for i := 0; i < len(logs)-1; i++ {
			if logs[i].ID != nil && logs[i+1].ID != nil {
				assert.LessOrEqual(t, *logs[i].ID, *logs[i+1].ID)
			}
		}

		// Test with non-existing ledger
		cursorPtr, err = store.GetAllLogs(ctx, "non-existing-ledger")
		require.NoError(t, err)
		require.NotNil(t, cursorPtr)
		cursor = *cursorPtr
		t.Cleanup(func() { _ = cursor.Close() })

		log, err := cursor.Next(ctx)
		assert.Equal(t, io.EOF, err)
		assert.Equal(t, ledger.Log{}, log)
	})

	t.Run("InsertLogsEmpty", func(t *testing.T) {
		t.Parallel()
		database := clickhouseServer.NewDatabase(t)
		dsn := database.ConnString()
		store, cleanup := createClickHouseStore(t, dsn)
		t.Cleanup(func() { _ = cleanup() })

		err := store.InsertLogs(ctx)
		require.NoError(t, err)
	})

	t.Run("MultipleLedgers", func(t *testing.T) {
		t.Parallel()
		database := clickhouseServer.NewDatabase(t)
		dsn := database.ConnString()
		store, cleanup := createClickHouseStore(t, dsn)
		t.Cleanup(func() { _ = cleanup() })

		ledger1 := "ledger-1"
		ledger2 := "ledger-2"

		logs1 := createTestLogs(t, ledger1)
		logs2 := createTestLogsWithPrefix(t, ledger2, "ledger2-")

		err := store.InsertLogs(ctx, logs1...)
		require.NoError(t, err)

		err = store.InsertLogs(ctx, logs2...)
		require.NoError(t, err)

		// Verify logs are isolated by ledger
		lastLog1, err := store.GetLastLog(ctx, ledger1)
		require.NoError(t, err)
		require.NotNil(t, lastLog1)
		assert.Equal(t, ledger1, lastLog1.Ledger)

		lastLog2, err := store.GetLastLog(ctx, ledger2)
		require.NoError(t, err)
		require.NotNil(t, lastLog2)
		assert.Equal(t, ledger2, lastLog2.Ledger)

		// Verify GetAllLogs returns only logs for the specified ledger
		cursorPtr, err := store.GetAllLogs(ctx, ledger1)
		require.NoError(t, err)
		require.NotNil(t, cursorPtr)
		cursor := *cursorPtr
		t.Cleanup(func() { _ = cursor.Close() })

		var count int
		for {
			log, err := cursor.Next(ctx)
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			assert.Equal(t, ledger1, log.Ledger)
			count++
		}
		assert.Equal(t, len(logs1), count)
	})
}

func createClickHouseStore(t *testing.T, dsn string) (LogStore, func() error) {
	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	store, err := NewClickHouseLogStore(ctx, dsn, logger)
	require.NoError(t, err)
	return store, store.Close
}
