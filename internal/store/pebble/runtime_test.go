//go:build it

package pebble

import (
	"io"
	"math/big"
	"testing"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/metadata"
	libtime "github.com/formancehq/go-libs/v3/time"
	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
	"github.com/formancehq/ledger-v3-poc/internal/store"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
)

func TestPebbleRuntimeStore(t *testing.T) {
	testRuntimeStoreCommon(t, func(t *testing.T) store.Runtime {
		tmpDir := t.TempDir()
		ctx := logging.TestingContext()
		logger := logging.FromContext(ctx)
		meter := noop.NewMeterProvider().Meter("test")

		s, err := NewRuntimeStore(tmpDir, logger, meter)
		require.NoError(t, err)
		t.Cleanup(func() { _ = s.Close(ctx) })

		return s
	})
}

func testRuntimeStoreCommon(t *testing.T, createStore func(*testing.T) store.Runtime) {
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
				AccountMetadata: map[string]*ledgerpb.Metadata{
					"bank": {Entries: metadata.Metadata{
						"account_type": "asset",
					}},
				},
			})
			return ledgerpb.NewLog(payload).
				WithLedger(ledger).
				WithID(1).
				WithIdempotency("idempotency-key-1", []byte("hash-1")).
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
				WithLedger(ledger).
				WithID(2).
				WithIdempotency("idempotency-key-2", []byte("hash-2")).
				WithDate(now.Add(time.Second))
		}(),
		func() *ledgerpb.Log {
			payload, _ := ledgerpb.LogPayloadToProtobuf(&ledgerpb.SavedMetadata{
				Target: &ledgerpb.Target{
					Target: &ledgerpb.Target_Account{Account: &ledgerpb.TargetAccount{
						Addr: "bank",
					}},
				},
				Metadata: metadata.Metadata{
					"label": "Bank Account",
				},
			})
			return ledgerpb.NewLog(payload).
				WithLedger(ledger).
				WithID(3).
				WithDate(now.Add(2 * time.Second))
		}(),
		func() *ledgerpb.Log {
			payload, _ := ledgerpb.LogPayloadToProtobuf(&ledgerpb.DeletedMetadata{
				Target: &ledgerpb.Target{
					Target: &ledgerpb.Target_Account{Account: &ledgerpb.TargetAccount{
						Addr: "bank",
					}},
				},
				Key: "old_key",
			})
			return ledgerpb.NewLog(payload).
				WithLedger(ledger).
				WithID(4).
				WithDate(now.Add(3 * time.Second))
		}(),
	}

	return logs
}
