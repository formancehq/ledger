//go:build it

package service

import (
	"io"
	"math/big"
	"testing"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	libtime "github.com/formancehq/go-libs/v3/time"
	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
	"github.com/stretchr/testify/require"
)

// createTestLogs creates a minimal set of logs needed by log store tests.
func createTestLogs(t *testing.T) []*ledgerpb.Log {
	now := libtime.New(time.Now())

	logs := []*ledgerpb.Log{
		func() *ledgerpb.Log {
			payload, _ := ledgerpb.LogPayloadToProtobuf(&ledgerpb.CreatedTransaction{
				Transaction: ledgerpb.NewTransaction().
					WithPostings(
						ledgerpb.NewPosting("world", "bank", "USD", big.NewInt(100)),
					).
					WithID(1).
					WithTimestamp(now),
			})
			return ledgerpb.NewLog(payload).
				WithID(1).
				WithDate(now)
		}(),

		func() *ledgerpb.Log {
			payload, _ := ledgerpb.LogPayloadToProtobuf(&ledgerpb.CreatedTransaction{
				Transaction: ledgerpb.NewTransaction().
					WithPostings(
						ledgerpb.NewPosting("bank", "user", "USD", big.NewInt(50)),
					).
					WithID(2).
					WithTimestamp(now),
			})
			return ledgerpb.NewLog(payload).
				WithID(2).
				WithDate(now.Add(time.Second))
		}(),
	}

	return logs
}

// TestLogStoreIntegrationCommon runs common tests for log stores (LogWriter + LogReader)
func TestLogStoreIntegrationCommon(t *testing.T, createStore func(*testing.T) LogStore) {
	t.Parallel()

	ctx := logging.TestingContext()

	t.Run("InsertLogs", func(t *testing.T) {
		t.Parallel()
		store := createStore(t)

		testLogs := createTestLogs(t)
		err := store.InsertLogs(ctx, testLogs...)
		require.NoError(t, err)
	})

	t.Run("GetAllLogs", func(t *testing.T) {
		t.Parallel()
		store := createStore(t)

		testLogs := createTestLogs(t)
		err := store.InsertLogs(ctx, testLogs...)
		require.NoError(t, err)

		// Get all logs
		cursor, err := store.GetAllLogs(ctx, 0, 0)
		require.NoError(t, err)
		require.NotNil(t, cursor)
		t.Cleanup(func() { _ = cursor.Close() })

		// Read all logs
		var logs []*ledgerpb.Log
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

		// Verify logs are in ascending order by id
		for i := 0; i < len(logs)-1; i++ {
			require.LessOrEqual(t, logs[i].Id, logs[i+1].Id)
		}
	})

	t.Run("GetLogByID", func(t *testing.T) {
		t.Parallel()
		store := createStore(t)

		testLogs := createTestLogs(t)
		err := store.InsertLogs(ctx, testLogs...)
		require.NoError(t, err)

		log, err := store.GetLogByID(ctx, 1)
		require.NoError(t, err)
		require.NotNil(t, log)
		require.Equal(t, uint64(1), log.Id)

		log, err = store.GetLogByID(ctx, 999)
		require.NoError(t, err)
		require.Nil(t, log)
	})

	t.Run("InsertLogsEmpty", func(t *testing.T) {
		t.Parallel()
		store := createStore(t)

		err := store.InsertLogs(ctx)
		require.NoError(t, err)
	})
}







