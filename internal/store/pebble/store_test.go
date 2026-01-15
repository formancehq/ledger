//go:build it

package pebble

import (
	"io"
	"math/big"
	"testing"

	"github.com/formancehq/go-libs/v3/time"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/metadata"
	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
	"github.com/formancehq/ledger-v3-poc/internal/store"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
)

func TestPebbleStore(t *testing.T) {
	testStoreCommon(t, func(t *testing.T) store.Store {
		tmpDir := t.TempDir()
		ctx := logging.TestingContext()
		logger := logging.FromContext(ctx)
		meter := noop.NewMeterProvider().Meter("test")

		s, err := NewStore(tmpDir, logger, meter)
		require.NoError(t, err)
		t.Cleanup(func() { _ = s.Close(ctx) })

		return s
	})
}

func testStoreCommon(t *testing.T, createStore func(*testing.T) store.Store) {
	t.Parallel()

	ctx := logging.TestingContext()
	testLedger := "test-ledger"

	t.Run("InsertLogs", func(t *testing.T) {
		t.Parallel()
		s := createStore(t)

		testLogs := createTestLogs(testLedger)
		err := s.InsertLogs(ctx, testLogs...)
		require.NoError(t, err)
	})

	t.Run("BalancesCalculation", func(t *testing.T) {
		t.Parallel()
		s := createStore(t)
		testLogs := createTestLogs(testLedger)

		err := s.InsertLogs(ctx, testLogs...)
		require.NoError(t, err)

		balances, err := s.GetBalances(ctx, testLedger, map[string][]string{
			"world": {"USD"},
			"bank":  {"USD"},
			"user":  {"USD"},
		})
		require.NoError(t, err)
		require.Equal(t, big.NewInt(-100), balances["world"]["USD"])
		require.Equal(t, big.NewInt(50), balances["bank"]["USD"])
		require.Equal(t, big.NewInt(50), balances["user"]["USD"])
	})

	t.Run("GetAllLogs", func(t *testing.T) {
		t.Parallel()
		s := createStore(t)

		testLogs := createTestLogs(testLedger)
		err := s.InsertLogs(ctx, testLogs...)
		require.NoError(t, err)

		cursor, err := s.GetAllLogs(ctx, testLedger, 0, 0)
		require.NoError(t, err)
		require.NotNil(t, cursor)
		t.Cleanup(func() { _ = cursor.Close() })

		var logs []*ledgerpb.Log
		for {
			log, err := cursor.Next(ctx)
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			logs = append(logs, log)
		}

		require.Equal(t, len(testLogs), len(logs))

		for i := 0; i < len(logs)-1; i++ {
			require.LessOrEqual(t, logs[i].Id, logs[i+1].Id)
		}
	})

	t.Run("GetLogByID", func(t *testing.T) {
		t.Parallel()
		s := createStore(t)

		testLogs := createTestLogs(testLedger)
		err := s.InsertLogs(ctx, testLogs...)
		require.NoError(t, err)

		log, err := s.GetLogByID(ctx, testLedger, 1)
		require.NoError(t, err)
		require.NotNil(t, log)
		require.Equal(t, uint64(1), log.Id)

		log, err = s.GetLogByID(ctx, testLedger, 999)
		require.NoError(t, err)
		require.Nil(t, log)
	})

	t.Run("GetAccountMetadata", func(t *testing.T) {
		t.Parallel()
		s := createStore(t)
		testLogs := createTestLogs(testLedger)

		err := s.InsertLogs(ctx, testLogs...)
		require.NoError(t, err)

		accountsMetadata, err := s.GetAccountMetadata(ctx, testLedger, []string{"bank", "user", "world", "non-existing"})
		require.NoError(t, err)
		require.NotNil(t, accountsMetadata)

		bankMetadata, exists := accountsMetadata["bank"]
		require.True(t, exists)
		require.Equal(t, "asset", bankMetadata["account_type"])
		require.Equal(t, "Bank Account", bankMetadata["label"])

		userMetadata, exists := accountsMetadata["user"]
		require.True(t, exists)
		require.Empty(t, userMetadata)

		worldMetadata, exists := accountsMetadata["world"]
		require.True(t, exists)
		require.Empty(t, worldMetadata)

		nonExistingMetadata, exists := accountsMetadata["non-existing"]
		require.True(t, exists)
		require.Empty(t, nonExistingMetadata)

		emptyMetadata, err := s.GetAccountMetadata(ctx, testLedger, []string{})
		require.NoError(t, err)
		require.NotNil(t, emptyMetadata)
		require.Empty(t, emptyMetadata)
	})

	t.Run("GetLogForIdempotencyKey", func(t *testing.T) {
		t.Parallel()
		s := createStore(t)
		testLogs := createTestLogs(testLedger)

		err := s.InsertLogs(ctx, testLogs...)
		require.NoError(t, err)

		logID, err := s.GetLogIDForIdempotencyKey(ctx, testLedger, "idempotency-key-1")
		require.NoError(t, err)
		require.Equal(t, uint64(1), logID)

		logID, err = s.GetLogIDForIdempotencyKey(ctx, testLedger, "non-existing-key")
		require.NoError(t, err)
		require.Equal(t, uint64(0), logID)

		logID, err = s.GetLogIDForIdempotencyKey(ctx, testLedger, "")
		require.NoError(t, err)
		require.Equal(t, uint64(0), logID)
	})

	t.Run("InsertLogsEmpty", func(t *testing.T) {
		t.Parallel()
		s := createStore(t)

		err := s.InsertLogs(ctx)
		require.NoError(t, err)
	})
}

func createTestLogs(ledger string) []*ledgerpb.Log {
	now := time.Now()

	logs := []*ledgerpb.Log{
		ledgerpb.NewLog(&ledgerpb.LogPayload{
			Payload: &ledgerpb.LogPayload_CreatedTransaction{
				CreatedTransaction: &ledgerpb.CreatedTransaction{
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
				},
			},
		}).
			WithLedger(ledger).
			WithID(1).
			WithIdempotency("idempotency-key-1", []byte("hash-1")).
			WithDate(now),
		ledgerpb.NewLog(&ledgerpb.LogPayload{
			Payload: &ledgerpb.LogPayload_CreatedTransaction{
				CreatedTransaction: &ledgerpb.CreatedTransaction{
					Transaction: ledgerpb.NewTransaction().
						WithPostings(
							ledgerpb.NewPosting("bank", "user", "USD", big.NewInt(50)),
						).
						WithID(2).
						WithTimestamp(now),
				},
			},
		}).
			WithLedger(ledger).
			WithID(2).
			WithIdempotency("idempotency-key-2", []byte("hash-2")).
			WithDate(now.Add(time.Second)),
		ledgerpb.NewLog(&ledgerpb.LogPayload{
			Payload: &ledgerpb.LogPayload_SavedMetadata{
				SavedMetadata: &ledgerpb.SavedMetadata{
					Target: &ledgerpb.Target{
						Target: &ledgerpb.Target_Account{Account: &ledgerpb.TargetAccount{
							Addr: "bank",
						}},
					},
					Metadata: metadata.Metadata{
						"label": "Bank Account",
					},
				},
			},
		}).
			WithLedger(ledger).
			WithID(3).
			WithDate(now.Add(2 * time.Second)),
		ledgerpb.NewLog(&ledgerpb.LogPayload{
			Payload: &ledgerpb.LogPayload_DeletedMetadata{
				DeletedMetadata: &ledgerpb.DeletedMetadata{
					Target: &ledgerpb.Target{
						Target: &ledgerpb.Target_Account{Account: &ledgerpb.TargetAccount{
							Addr: "bank",
						}},
					},
					Key: "old_key",
				},
			},
		}).
			WithLedger(ledger).
			WithID(4).
			WithDate(now.Add(3 * time.Second)),
	}

	return logs
}

func TestPebbleStoreSnapshots(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	store, err := NewStore(tmpDir, logger, meter)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close(ctx) })

	now := time.Now()
	ledger := "default"
	for i := range uint64(10) {
		err := store.InsertLogs(ctx,
			ledgerpb.NewLog(&ledgerpb.LogPayload{
				Payload: &ledgerpb.LogPayload_CreatedTransaction{
					CreatedTransaction: &ledgerpb.CreatedTransaction{
						Transaction: ledgerpb.NewTransaction().
							WithPostings(
								ledgerpb.NewPosting("world", "bank", "USD", big.NewInt(100)),
							).
							WithID(i).
							WithTimestamp(now),
					},
				},
			}).
				WithLedger(ledger).
				WithID(i).
				WithDate(now),
		)
		require.NoError(t, err)
	}

	require.NoError(t, store.CreateSnapshot(ctx))

	for i := range uint64(5) {
		err := store.InsertLogs(ctx,
			ledgerpb.NewLog(&ledgerpb.LogPayload{
				Payload: &ledgerpb.LogPayload_CreatedTransaction{
					CreatedTransaction: &ledgerpb.CreatedTransaction{
						Transaction: ledgerpb.NewTransaction().
							WithPostings(
								ledgerpb.NewPosting("world", "bank", "USD", big.NewInt(100)),
							).
							WithID(10 + i).
							WithTimestamp(now),
					},
				},
			}).
				WithLedger(ledger).
				WithID(10+i).
				WithDate(now),
		)
		require.NoError(t, err)
	}

	cursor, err := store.GetAllLogs(ctx, ledger, 0, 0)
	require.NoError(t, err)

	count := 0
	for {
		_, err := cursor.Next(ctx)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		count++
	}

	require.Equal(t, 15, count)
	require.NoError(t, cursor.Close())
	require.NoError(t, store.Close(ctx))

	store, err = NewStore(tmpDir, logger, meter)
	require.NoError(t, err)

	cursor, err = store.GetAllLogs(ctx, ledger, 0, 0)
	require.NoError(t, err)

	count = 0
	for {
		_, err := cursor.Next(ctx)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		count++
	}

	// Should have restored the last snapshot
	require.Equal(t, 10, count)
	require.NoError(t, cursor.Close())

}
