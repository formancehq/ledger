//go:build it
// +build it

package service

import (
	"io"
	"path/filepath"
	"testing"

	"github.com/formancehq/go-libs/v3/logging"
	ledger "github.com/formancehq/ledger-v3-poc/internal"
	"github.com/stretchr/testify/require"
)

func TestFileLogStoreIntegration(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	ledgerName := "test-ledger"

	t.Run("InsertLogs", func(t *testing.T) {
		t.Parallel()
		store, cleanup := createFileStore(t)
		t.Cleanup(func() { _ = cleanup() })

		testLogs := createTestLogs(t, ledgerName)
		err := store.InsertLogs(ctx, testLogs...)
		require.NoError(t, err)
	})

	t.Run("GetLogWithIdempotencyKey", func(t *testing.T) {
		t.Parallel()
		store, cleanup := createFileStore(t)
		t.Cleanup(func() { _ = cleanup() })

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
		require.NoError(t, err)
		require.Nil(t, log)

		// Test with different ledger
		log, err = store.GetLogWithIdempotencyKey(ctx, "other-ledger", "idempotency-key-1")
		require.NoError(t, err)
		require.Nil(t, log)
	})

	t.Run("GetLastLog", func(t *testing.T) {
		t.Parallel()
		store, cleanup := createFileStore(t)
		t.Cleanup(func() { _ = cleanup() })

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
		store, cleanup := createFileStore(t)
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
		require.Equal(t, len(testLogs), len(logs))

		// Verify logs are in ascending order by ID
		for i := 0; i < len(logs)-1; i++ {
			if logs[i].ID != nil && logs[i+1].ID != nil {
				require.LessOrEqual(t, *logs[i].ID, *logs[i+1].ID)
			}
		}

		// Test with non-existing ledger
		cursorPtr, err = store.GetAllLogs(ctx, "non-existing-ledger")
		require.NoError(t, err)
		require.NotNil(t, cursorPtr)
		cursor = *cursorPtr
		t.Cleanup(func() { _ = cursor.Close() })

		log, err := cursor.Next(ctx)
		require.Equal(t, io.EOF, err)
		require.Equal(t, ledger.Log{}, log)
	})

	t.Run("InsertLogsEmpty", func(t *testing.T) {
		t.Parallel()
		store, cleanup := createFileStore(t)
		t.Cleanup(func() { _ = cleanup() })

		err := store.InsertLogs(ctx)
		require.NoError(t, err)
	})
}

func createFileStore(t *testing.T) (LogStore, func() error) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "logs.jsonl")
	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	store, err := NewFileLogStore(filePath, logger)
	require.NoError(t, err)
	return store, store.Close
}
