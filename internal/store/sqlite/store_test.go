package sqlite

import (
	"context"
	"fmt"
	"io"
	"math/big"
	"testing"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/metadata"
	"github.com/formancehq/go-libs/v3/time"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/store"
	"github.com/stretchr/testify/require"
)

// collectLedgers collects all ledgers from a cursor into a slice
func collectLedgers(ctx context.Context, cursor store.Cursor[*commonpb.LedgerInfo]) ([]*commonpb.LedgerInfo, error) {
	defer func() { _ = cursor.Close() }()
	var ledgers []*commonpb.LedgerInfo
	for {
		ledger, err := cursor.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		ledgers = append(ledgers, ledger)
	}
	return ledgers, nil
}

func TestSQLiteMattnStore(t *testing.T) {
	testStoreCommon(t, func(t *testing.T) store.Store {
		tmpDir := t.TempDir()
		runtimeDSN := fmt.Sprintf("file:%s/test-runtime.db", tmpDir)
		ctx := logging.TestingContext()
		logger := logging.FromContext(ctx)

		store, err := NewMattnStore(runtimeDSN, logger)
		require.NoError(t, err)
		t.Cleanup(func() { _ = store.Close(ctx) })

		return store
	})
}

func TestSQLiteModernStore(t *testing.T) {
	testStoreCommon(t, func(t *testing.T) store.Store {
		tmpDir := t.TempDir()
		runtimeDSN := fmt.Sprintf("file:%s/test-runtime.db", tmpDir)
		ctx := logging.TestingContext()
		logger := logging.FromContext(ctx)

		store, err := NewModernStore(runtimeDSN, logger)
		require.NoError(t, err)
		t.Cleanup(func() { _ = store.Close(ctx) })

		return store
	})
}

// registerLedger is a helper function to register a ledger and return its ID
func registerLedger(ctx context.Context, t *testing.T, s store.Store, name string, id uint32) {
	t.Helper()
	batch := s.NewBatch(0)
	err := batch.RegisterLedger(ctx, &commonpb.LedgerInfo{
		Id:        id,
		Name:      name,
		CreatedAt: commonpb.NewTimestamp(time.Now()),
	})
	require.NoError(t, err)
	err = batch.Commit(ctx)
	require.NoError(t, err)
}

// appendLogs is a helper function to append logs using the batch pattern
func appendLogs(ctx context.Context, t *testing.T, s store.Store, lastAppliedIndex uint64, logs ...*commonpb.Log) {
	t.Helper()
	batch := s.NewBatch(lastAppliedIndex)
	err := batch.AppendLogs(ctx, logs...)
	require.NoError(t, err)
	err = batch.Commit(ctx)
	require.NoError(t, err)
}

func testStoreCommon(t *testing.T, createStore func(*testing.T) store.Store) {
	t.Parallel()

	ctx := logging.TestingContext()
	var testLedgerID uint32 = 1

	t.Run("AppendLogs", func(t *testing.T) {
		t.Parallel()
		s := createStore(t)

		registerLedger(ctx, t, s, "test-ledger", testLedgerID)
		testLogs := createTestLogs(testLedgerID)
		appendLogs(ctx, t, s, 0, testLogs...)
	})

	t.Run("BalancesCalculation", func(t *testing.T) {
		t.Parallel()
		s := createStore(t)

		registerLedger(ctx, t, s, "test-ledger", testLedgerID)
		batch := s.NewBatch(0)
		require.NoError(t, batch.AppendBalanceDiff(ctx, testLedgerID, "world", "USD", commonpb.NewBigInt(big.NewInt(-100)), 1))
		require.NoError(t, batch.AppendBalanceDiff(ctx, testLedgerID, "bank", "USD", commonpb.NewBigInt(big.NewInt(100)), 1))
		require.NoError(t, batch.AppendBalanceDiff(ctx, testLedgerID, "user", "USD", commonpb.NewBigInt(big.NewInt(50)), 2))
		require.NoError(t, batch.AppendBalanceDiff(ctx, testLedgerID, "bank", "USD", commonpb.NewBigInt(big.NewInt(-50)), 2))
		require.NoError(t, batch.Commit(ctx))

		balances, err := s.GetBalances(ctx, testLedgerID, map[string][]string{
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

		registerLedger(ctx, t, s, "test-ledger", testLedgerID)
		testLogs := createTestLogs(testLedgerID)
		appendLogs(ctx, t, s, 0, testLogs...)

		// Test GetAllLogs (global logs by sequence)
		cursor, err := s.GetAllLogs(ctx, 0, 0)
		require.NoError(t, err)
		require.NotNil(t, cursor)
		t.Cleanup(func() { _ = cursor.Close() })

		var logs []*commonpb.Log
		for {
			log, err := cursor.Next(ctx)
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			logs = append(logs, log)
		}

		require.Equal(t, len(testLogs), len(logs))

		// Verify logs are in sequence order
		for i := 0; i < len(logs)-1; i++ {
			require.LessOrEqual(t, logs[i].Sequence, logs[i+1].Sequence)
		}
	})

	t.Run("GetLogBySequence", func(t *testing.T) {
		t.Parallel()
		s := createStore(t)

		registerLedger(ctx, t, s, "test-ledger", testLedgerID)
		testLogs := createTestLogs(testLedgerID)
		appendLogs(ctx, t, s, 0, testLogs...)

		log, err := s.GetLogBySequence(ctx, 1)
		require.NoError(t, err)
		require.NotNil(t, log)
		require.Equal(t, uint64(1), log.Sequence)

		log, err = s.GetLogBySequence(ctx, 999)
		require.NoError(t, err)
		require.Nil(t, log)
	})

	t.Run("GetAccountMetadata", func(t *testing.T) {
		t.Parallel()
		s := createStore(t)

		registerLedger(ctx, t, s, "test-ledger", testLedgerID)
		batch := s.NewBatch(0)
		require.NoError(t, batch.SaveAccountMetadata(ctx, testLedgerID, "bank", &commonpb.Metadata{
			Entries: metadata.Metadata{
				"account_type": "asset",
			},
		}))
		require.NoError(t, batch.SaveAccountMetadata(ctx, testLedgerID, "bank", &commonpb.Metadata{
			Entries: metadata.Metadata{
				"label": "Bank Account",
			},
		}))
		require.NoError(t, batch.DeleteAccountMetadata(ctx, testLedgerID, "bank", []string{"old_key"}))
		require.NoError(t, batch.Commit(ctx))

		accountsMetadata, err := s.GetAccountMetadata(ctx, testLedgerID, []string{"bank", "user", "world", "non-existing"})
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

		emptyMetadata, err := s.GetAccountMetadata(ctx, testLedgerID, []string{})
		require.NoError(t, err)
		require.NotNil(t, emptyMetadata)
		require.Empty(t, emptyMetadata)
	})

	t.Run("GetSequenceForIdempotencyKey", func(t *testing.T) {
		t.Parallel()
		s := createStore(t)

		registerLedger(ctx, t, s, "test-ledger", testLedgerID)
		testLogs := createTestLogs(testLedgerID)
		appendLogs(ctx, t, s, 0, testLogs...)

		sequence, err := s.GetSequenceForIdempotencyKey(ctx, "idempotency-key-1")
		require.NoError(t, err)
		require.Equal(t, uint64(1), sequence)

		sequence, err = s.GetSequenceForIdempotencyKey(ctx, "non-existing-key")
		require.NoError(t, err)
		require.Equal(t, uint64(0), sequence)

		sequence, err = s.GetSequenceForIdempotencyKey(ctx, "")
		require.NoError(t, err)
		require.Equal(t, uint64(0), sequence)
	})

	t.Run("AppendLogsEmpty", func(t *testing.T) {
		t.Parallel()
		s := createStore(t)

		appendLogs(ctx, t, s, 0)
	})

	t.Run("GetLastSequence", func(t *testing.T) {
		t.Parallel()
		s := createStore(t)

		registerLedger(ctx, t, s, "test-ledger", testLedgerID)

		// Test with no logs - should return 0
		lastSequence, err := s.GetLastSequence(ctx)
		require.NoError(t, err)
		require.Equal(t, uint64(0), lastSequence)

		// Insert logs and verify last sequence
		testLogs := createTestLogs(testLedgerID)
		appendLogs(ctx, t, s, 0, testLogs...)

		lastSequence, err = s.GetLastSequence(ctx)
		require.NoError(t, err)
		require.Equal(t, uint64(4), lastSequence) // Last log has sequence 4
	})

	t.Run("ListLedgers", func(t *testing.T) {
		t.Parallel()
		s := createStore(t)

		// Initially no ledgers
		cursor, err := s.ListLedgers(ctx)
		require.NoError(t, err)
		ledgers, err := collectLedgers(ctx, cursor)
		require.NoError(t, err)
		require.Empty(t, ledgers)

		// Register first ledger
		registerLedger(ctx, t, s, "ledger-1", 1)
		cursor, err = s.ListLedgers(ctx)
		require.NoError(t, err)
		ledgers, err = collectLedgers(ctx, cursor)
		require.NoError(t, err)
		require.Len(t, ledgers, 1)
		require.Equal(t, "ledger-1", ledgers[0].Name)
		require.Equal(t, uint32(1), ledgers[0].Id)

		// Register second ledger
		registerLedger(ctx, t, s, "ledger-2", 2)
		cursor, err = s.ListLedgers(ctx)
		require.NoError(t, err)
		ledgers, err = collectLedgers(ctx, cursor)
		require.NoError(t, err)
		require.Len(t, ledgers, 2)
	})

	t.Run("GetLedgerByName", func(t *testing.T) {
		t.Parallel()
		s := createStore(t)

		registerLedger(ctx, t, s, "my-ledger", 42)

		ledger, err := s.GetLedgerByName(ctx, "my-ledger")
		require.NoError(t, err)
		require.NotNil(t, ledger)
		require.Equal(t, "my-ledger", ledger.Name)
		require.Equal(t, uint32(42), ledger.Id)

		ledger, err = s.GetLedgerByName(ctx, "non-existing")
		require.Error(t, err)
		require.Nil(t, ledger)
	})
}

// createTestLogs creates test logs wrapped in Log with ApplyLog payload
func createTestLogs(ledgerID uint32) []*commonpb.Log {
	now := time.Now()

	logs := []*commonpb.Log{
		{
			Sequence: 1,
			Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{
				Apply: &commonpb.ApplyLedgerLog{
					LedgerId: ledgerID,
					Log: commonpb.NewLedgerLog(&commonpb.LedgerLogPayload{
						Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
							CreatedTransaction: &commonpb.CreatedTransaction{
								Transaction: commonpb.NewTransaction().
									WithPostings(
										commonpb.NewPosting("world", "bank", "USD", big.NewInt(100)),
									).
									WithID(1).
									WithTimestamp(now),
								AccountMetadata: map[string]*commonpb.Metadata{
									"bank": {Entries: metadata.Metadata{
										"account_type": "asset",
									}},
								},
							},
						},
					}).
						WithID(1).
						WithDate(now),
				},
			}},
			Idempotency: &commonpb.Idempotency{
				Key:  "idempotency-key-1",
				Hash: []byte("hash-1"),
			},
		},
		{
			Sequence: 2,
			Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{
				Apply: &commonpb.ApplyLedgerLog{
					LedgerId: ledgerID,
					Log: commonpb.NewLedgerLog(&commonpb.LedgerLogPayload{
						Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
							CreatedTransaction: &commonpb.CreatedTransaction{
								Transaction: commonpb.NewTransaction().
									WithPostings(
										commonpb.NewPosting("bank", "user", "USD", big.NewInt(50)),
									).
									WithID(2).
									WithTimestamp(now),
							},
						},
					}).
						WithID(2).
						WithDate(now.Add(time.Second)),
				},
			}},
			Idempotency: &commonpb.Idempotency{
				Key:  "idempotency-key-2",
				Hash: []byte("hash-2"),
			},
		},
		{
			Sequence: 3,
			Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{
				Apply: &commonpb.ApplyLedgerLog{
					LedgerId: ledgerID,
					Log: commonpb.NewLedgerLog(&commonpb.LedgerLogPayload{
						Payload: &commonpb.LedgerLogPayload_SavedMetadata{
							SavedMetadata: &commonpb.SavedMetadata{
								Target: &commonpb.Target{
									Target: &commonpb.Target_Account{Account: &commonpb.TargetAccount{
										Addr: "bank",
									}},
								},
								Metadata: &commonpb.Metadata{Entries: metadata.Metadata{
									"label": "Bank Account",
								}},
							},
						},
					}).
						WithID(3).
						WithDate(now.Add(2 * time.Second)),
				},
			}},
		},
		{
			Sequence: 4,
			Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{
				Apply: &commonpb.ApplyLedgerLog{
					LedgerId: ledgerID,
					Log: commonpb.NewLedgerLog(&commonpb.LedgerLogPayload{
						Payload: &commonpb.LedgerLogPayload_DeletedMetadata{
							DeletedMetadata: &commonpb.DeletedMetadata{
								Target: &commonpb.Target{
									Target: &commonpb.Target_Account{Account: &commonpb.TargetAccount{
										Addr: "bank",
									}},
								},
								Key: "old_key",
							},
						},
					}).
						WithID(4).
						WithDate(now.Add(3 * time.Second)),
				},
			}},
		},
	}

	return logs
}
