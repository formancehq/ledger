//go:build it

package service

import (
	"context"
	"io"
	"math/big"
	"testing"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/metadata"
	libtime "github.com/formancehq/go-libs/v3/time"
	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
	"github.com/stretchr/testify/require"
)

// createTestLogs creates a set of test logs with different types
func createTestLogs(t *testing.T, ledgerName string) []*ledgerpb.Log {
	now := libtime.New(time.Now())

	logs := []*ledgerpb.Log{
		// Log 1: CreatedTransaction
		func() *ledgerpb.Log {
			payload, _ := ledgerpb.LogPayloadToProtobuf(&ledgerpb.CreatedTransaction{
				Transaction: ledgerpb.NewTransaction().
					WithPostings(
						ledgerpb.NewPosting("world", "bank", "USD", big.NewInt(100)),
					).
					WithID(1).
					WithTimestamp(now),
				AccountMetadata: map[string]*ledgerpb.Metadata{
					"bank": {Entries: metadata.Metadata{
						"account_type": "asset",
					}},
				},
			})
			log := ledgerpb.NewLog(payload).
				WithID(1).
				WithIdempotencyKey("idempotency-key-1").
				WithDate(now)
			log.IdempotencyHash = "hash-1"
			return log
		}(),

		// Log 2: CreatedTransaction with different idempotency key
		func() *ledgerpb.Log {
			payload, _ := ledgerpb.LogPayloadToProtobuf(&ledgerpb.CreatedTransaction{
				Transaction: ledgerpb.NewTransaction().
					WithPostings(
						ledgerpb.NewPosting("bank", "user", "USD", big.NewInt(50)),
					).
					WithID(2).
					WithTimestamp(now),
			})
			log := ledgerpb.NewLog(payload).
				WithID(2).
				WithIdempotencyKey("idempotency-key-2").
				WithDate(now.Add(time.Second))
			log.IdempotencyHash = "hash-2"
			return log
		}(),

		// Log 3: SavedMetadata
		func() *ledgerpb.Log {
			payload, _ := ledgerpb.LogPayloadToProtobuf(&ledgerpb.SavedMetadata{
				TargetType: "ACCOUNT",
				TargetId:   &ledgerpb.SavedMetadata_AccountId{AccountId: "bank"},
				Metadata: metadata.Metadata{
					"label": "Bank Account",
				},
			})
			return ledgerpb.NewLog(payload).
				WithID(3).
				WithDate(now.Add(2 * time.Second))
		}(),

		// Log 4: DeletedMetadata
		func() *ledgerpb.Log {
			payload, _ := ledgerpb.LogPayloadToProtobuf(&ledgerpb.DeletedMetadata{
				TargetType: "ACCOUNT",
				TargetId:   &ledgerpb.DeletedMetadata_AccountId{AccountId: "bank"},
				Key:        "old_key",
			})
			return ledgerpb.NewLog(payload).
				WithID(4).
				WithDate(now.Add(3 * time.Second))
		}(),

		// Log 5: RevertedTransaction
		func() *ledgerpb.Log {
			payload, _ := ledgerpb.LogPayloadToProtobuf(&ledgerpb.RevertedTransaction{
				RevertedTransaction: ledgerpb.NewTransaction().
					WithPostings(
						ledgerpb.NewPosting("world", "bank", "USD", big.NewInt(100)),
					).
					WithID(1).
					WithTimestamp(now),
				RevertTransaction: ledgerpb.NewTransaction().
					WithPostings(
						ledgerpb.NewPosting("bank", "world", "USD", big.NewInt(100)),
					).
					WithID(5).
					WithTimestamp(now.Add(4 * time.Second)),
			})
			return ledgerpb.NewLog(payload).
				WithID(5).
				WithDate(now.Add(4 * time.Second))
		}(),
	}

	return logs
}

// TestLogStoreIntegrationCommon runs common tests for log stores (LogWriter + LogReader)
func TestLogStoreIntegrationCommon(t *testing.T, createStore func(*testing.T) interface {
	LogWriter
	LogReader
	GetLogWithID(ctx context.Context, id uint64) (*ledgerpb.Log, error)
	GetLogWithIdempotencyKey(ctx context.Context, idempotencyKey string) (*ledgerpb.Log, error)
	GetLastLog(ctx context.Context) (*ledgerpb.Log, error)
}) {
	t.Parallel()

	ctx := logging.TestingContext()
	ledgerName := "test-ledger"

	t.Run("InsertLogs", func(t *testing.T) {
		t.Parallel()
		store := createStore(t)

		testLogs := createTestLogs(t, ledgerName)
		err := store.InsertLogs(ctx, testLogs...)
		require.NoError(t, err)
	})

	t.Run("GetAllLogs", func(t *testing.T) {
		t.Parallel()
		store := createStore(t)

		testLogs := createTestLogs(t, ledgerName)
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

	t.Run("GetLogWithID", func(t *testing.T) {
		t.Parallel()
		store := createStore(t)

		testLogs := createTestLogs(t, ledgerName)
		err := store.InsertLogs(ctx, testLogs...)
		require.NoError(t, err)

		// Test with existing log ID
		log, err := store.GetLogWithID(ctx, 1)
		require.NoError(t, err)
		require.NotNil(t, log)
		require.Equal(t, uint64(1), log.Id)

		// Test with non-existing log ID
		log, err = store.GetLogWithID(ctx, 999)
		require.NoError(t, err)
		require.Nil(t, log)
	})

	t.Run("GetLogWithIdempotencyKey", func(t *testing.T) {
		t.Parallel()
		store := createStore(t)

		testLogs := createTestLogs(t, ledgerName)
		err := store.InsertLogs(ctx, testLogs...)
		require.NoError(t, err)

		// Test with existing idempotency key
		log, err := store.GetLogWithIdempotencyKey(ctx, "idempotency-key-1")
		require.NoError(t, err)
		require.NotNil(t, log)
		require.Equal(t, "idempotency-key-1", log.IdempotencyKey)

		// Test with non-existing idempotency key
		log, err = store.GetLogWithIdempotencyKey(ctx, "non-existing-key")
		require.NoError(t, err)
		require.Nil(t, log)
	})

	t.Run("GetLastLog", func(t *testing.T) {
		t.Parallel()
		store := createStore(t)

		testLogs := createTestLogs(t, ledgerName)
		err := store.InsertLogs(ctx, testLogs...)
		require.NoError(t, err)

		lastLog, err := store.GetLastLog(ctx)
		require.NoError(t, err)
		require.NotNil(t, lastLog)
		require.Equal(t, testLogs[len(testLogs)-1].Id, lastLog.Id)
	})

	t.Run("InsertLogsEmpty", func(t *testing.T) {
		t.Parallel()
		store := createStore(t)

		err := store.InsertLogs(ctx)
		require.NoError(t, err)
	})
}






