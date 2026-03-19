package dal_test

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/formancehq/go-libs/v4/logging"
	"github.com/formancehq/go-libs/v4/metadata"
	"github.com/formancehq/go-libs/v4/time"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/infra/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/infra/state"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

func TestPebbleStore(t *testing.T) {
	testStoreCommon(t, func(t *testing.T) *dal.Store {
		tmpDir := t.TempDir()
		ctx := logging.TestingContext()
		logger := logging.FromContext(ctx)
		meter := noop.NewMeterProvider().Meter("test")

		s, err := dal.NewStore(tmpDir, logger, meter, dal.DefaultConfig())
		require.NoError(t, err)
		t.Cleanup(func() { _ = s.Close() })

		return s
	})
}

// registerLedger is a helper function to register a ledger.
func registerLedger(t *testing.T, s *dal.Store, name string) {
	t.Helper()

	batch := s.NewBatch()
	err := state.SaveLedger(batch, &commonpb.LedgerInfo{
		Name:      name,
		CreatedAt: commonpb.NewTimestamp(time.Now()),
	})
	require.NoError(t, err)
	err = batch.Commit()
	require.NoError(t, err)
}

// appendLogs is a helper function to append logs using the batch pattern.
func appendLogs(t *testing.T, s *dal.Store, lastAppliedIndex uint64, logs ...*commonpb.Log) {
	t.Helper()

	batch := s.NewBatch()
	err := state.AppendLogs(batch, logs...)
	require.NoError(t, err)
	require.NoError(t, state.SetAppliedIndex(batch, lastAppliedIndex))
	require.NoError(t, batch.Commit())
}

func testStoreCommon(t *testing.T, createStore func(*testing.T) *dal.Store) {
	t.Parallel()

	const testLedgerName = "test-ledger"

	t.Run("AppendLogs", func(t *testing.T) {
		t.Parallel()
		s := createStore(t)

		registerLedger(t, s, testLedgerName)
		testLogs := createTestLogs(testLedgerName)
		appendLogs(t, s, 0, testLogs...)
	})

	t.Run("InputOutputCalculation", func(t *testing.T) {
		t.Parallel()
		s := createStore(t)
		attrs := attributes.New()

		registerLedger(t, s, testLedgerName)
		batch := s.NewBatch()

		// Index 1: world sends 100 to bank
		worldKey := domain.VolumeKey{AccountKey: domain.AccountKey{Ledger: testLedgerName, Account: "world"}, Asset: "USD"}
		worldCanonicalKey := worldKey.Bytes()
		require.NoError(t, attrs.Volume.Set(batch, 1, worldCanonicalKey, &raftcmdpb.VolumePair{
			Output: commonpb.NewUint256FromUint64(100),
		}))

		bankKey := domain.VolumeKey{AccountKey: domain.AccountKey{Ledger: testLedgerName, Account: "bank"}, Asset: "USD"}
		bankCanonicalKey := bankKey.Bytes()
		require.NoError(t, attrs.Volume.Set(batch, 1, bankCanonicalKey, &raftcmdpb.VolumePair{
			Input: commonpb.NewUint256FromUint64(100),
		}))

		// Index 2: bank sends 50 to user (bank cumulative: input=100, output=50)
		userKey := domain.VolumeKey{AccountKey: domain.AccountKey{Ledger: testLedgerName, Account: "user"}, Asset: "USD"}
		userCanonicalKey := userKey.Bytes()

		require.NoError(t, attrs.Volume.Set(batch, 2, bankCanonicalKey, &raftcmdpb.VolumePair{
			Input:  commonpb.NewUint256FromUint64(100),
			Output: commonpb.NewUint256FromUint64(50),
		}))
		require.NoError(t, attrs.Volume.Set(batch, 2, userCanonicalKey, &raftcmdpb.VolumePair{
			Input: commonpb.NewUint256FromUint64(50),
		}))

		require.NoError(t, batch.Commit())

		// world: input=0, output=100 → balance = -100
		worldVolume, _, err := attrs.Volume.ComputeValue(s, 100, worldCanonicalKey)
		require.NoError(t, err)
		require.Equal(t, big.NewInt(0), worldVolume.GetInput().ToBigInt())
		require.Equal(t, big.NewInt(100), worldVolume.GetOutput().ToBigInt())

		// bank: input=100, output=50 → balance = 50
		bankVolume, _, err := attrs.Volume.ComputeValue(s, 100, bankCanonicalKey)
		require.NoError(t, err)
		require.Equal(t, big.NewInt(100), bankVolume.GetInput().ToBigInt())
		require.Equal(t, big.NewInt(50), bankVolume.GetOutput().ToBigInt())

		// user: input=50, output=0 → balance = 50
		userVolume, _, err := attrs.Volume.ComputeValue(s, 100, userCanonicalKey)
		require.NoError(t, err)
		require.Equal(t, big.NewInt(50), userVolume.GetInput().ToBigInt())
		require.Equal(t, big.NewInt(0), userVolume.GetOutput().ToBigInt())
	})

	t.Run("AppendLogsEmpty", func(t *testing.T) {
		t.Parallel()
		s := createStore(t)

		appendLogs(t, s, 0)
	})
}

// createTestLogs creates test logs wrapped in Log with ApplyLog payload.
func createTestLogs(ledgerName string) []*commonpb.Log {
	return createTestLogsForLedger(ledgerName, 1)
}

// createTestLogsForLedger creates test logs with custom starting sequence.
func createTestLogsForLedger(ledgerName string, startSequence uint64) []*commonpb.Log {
	now := time.Now()

	logs := []*commonpb.Log{
		{
			Sequence: startSequence,
			Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{
				Apply: &commonpb.ApplyLedgerLog{
					LedgerName: ledgerName,
					Log: commonpb.NewLedgerLog(&commonpb.LedgerLogPayload{
						Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
							CreatedTransaction: &commonpb.CreatedTransaction{
								Transaction: commonpb.NewTransaction().
									WithPostings(
										commonpb.NewPosting("world", "bank", "USD", big.NewInt(100)),
									).
									WithID(1).
									WithTimestamp(now),
								AccountMetadata: map[string]*commonpb.MetadataSet{
									"bank": commonpb.MetadataSetFromMap(metadata.Metadata{
										"account_type": "asset",
									}),
								},
							},
						},
					}).
						WithID(1).
						WithDate(now),
				},
			}},
			Idempotency: &commonpb.Idempotency{
				Key: "idempotency-key-1",
			},
		},
		{
			Sequence: startSequence + 1,
			Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{
				Apply: &commonpb.ApplyLedgerLog{
					LedgerName: ledgerName,
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
				Key: "idempotency-key-2",
			},
		},
		{
			Sequence: startSequence + 2,
			Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{
				Apply: &commonpb.ApplyLedgerLog{
					LedgerName: ledgerName,
					Log: commonpb.NewLedgerLog(&commonpb.LedgerLogPayload{
						Payload: &commonpb.LedgerLogPayload_SavedMetadata{
							SavedMetadata: &commonpb.SavedMetadata{
								Target: &commonpb.Target{
									Target: &commonpb.Target_Account{Account: &commonpb.TargetAccount{
										Addr: "bank",
									}},
								},
								Metadata: commonpb.MetadataSetFromMap(metadata.Metadata{
									"label": "Bank Account",
								}),
							},
						},
					}).
						WithID(3).
						WithDate(now.Add(2 * time.Second)),
				},
			}},
		},
		{
			Sequence: startSequence + 3,
			Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{
				Apply: &commonpb.ApplyLedgerLog{
					LedgerName: ledgerName,
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

func TestVolume(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	tmpDir := t.TempDir()
	s, err := dal.NewStore(tmpDir, logger, meter, dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	attrs := attributes.New()

	const ledgerName = "test-ledger"
	registerLedger(t, s, ledgerName)

	bankUSD := domain.VolumeKey{AccountKey: domain.AccountKey{Ledger: ledgerName, Account: "bank"}, Asset: "USD"}
	userUSD := domain.VolumeKey{AccountKey: domain.AccountKey{Ledger: ledgerName, Account: "user"}, Asset: "USD"}
	bankEUR := domain.VolumeKey{AccountKey: domain.AccountKey{Ledger: ledgerName, Account: "bank"}, Asset: "EUR"}

	bankUSDKey := bankUSD.Bytes()
	userUSDKey := userUSD.Bytes()
	bankEURKey := bankEUR.Bytes()

	getVolume := func(canonicalKey []byte, index uint64) *raftcmdpb.VolumePair {
		result, _, err := attrs.Volume.ComputeValue(s, index, canonicalKey)
		require.NoError(t, err)

		return result
	}

	// Initially volume should be {input: 0, output: 0}
	v := getVolume(bankUSDKey, 100)
	require.Equal(t, big.NewInt(0), v.GetInput().ToBigInt())
	require.Equal(t, big.NewInt(0), v.GetOutput().ToBigInt())

	// Set cumulative volume snapshots at successive raft indices.
	// Each Set stores the absolute cumulative value at that point.
	batch := s.NewBatch()
	require.NoError(t, attrs.Volume.Set(batch, 1, bankUSDKey, &raftcmdpb.VolumePair{
		Input: commonpb.NewUint256FromUint64(100),
	}))
	require.NoError(t, attrs.Volume.Set(batch, 2, bankUSDKey, &raftcmdpb.VolumePair{
		Input: commonpb.NewUint256FromUint64(150),
	}))
	require.NoError(t, attrs.Volume.Set(batch, 3, bankUSDKey, &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(150),
		Output: commonpb.NewUint256FromUint64(30),
	}))
	require.NoError(t, batch.Commit())

	// At boundary 100: latest entry at index 3 → input=150, output=30
	v = getVolume(bankUSDKey, 100)
	require.Equal(t, big.NewInt(150), v.GetInput().ToBigInt())
	require.Equal(t, big.NewInt(30), v.GetOutput().ToBigInt())

	// At boundary 2: latest entry at index 2 → input=150, output=0
	v = getVolume(bankUSDKey, 2)
	require.Equal(t, big.NewInt(150), v.GetInput().ToBigInt())
	require.Equal(t, big.NewInt(0), v.GetOutput().ToBigInt())

	// At boundary 1: entry at index 1 → input=100, output=0
	v = getVolume(bankUSDKey, 1)
	require.Equal(t, big.NewInt(100), v.GetInput().ToBigInt())
	require.Equal(t, big.NewInt(0), v.GetOutput().ToBigInt())

	// At boundary 0: no entries → input=0, output=0
	v = getVolume(bankUSDKey, 0)
	require.Equal(t, big.NewInt(0), v.GetInput().ToBigInt())
	require.Equal(t, big.NewInt(0), v.GetOutput().ToBigInt())

	// Set a snapshot at index 10 (cumulative state: input=1000, output=30)
	batch = s.NewBatch()
	require.NoError(t, attrs.Volume.Set(batch, 10, bankUSDKey, &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(1000),
		Output: commonpb.NewUint256FromUint64(30),
	}))
	require.NoError(t, batch.Commit())

	// At boundary 100: latest entry at index 10 → input=1000, output=30
	v = getVolume(bankUSDKey, 100)
	require.Equal(t, big.NewInt(1000), v.GetInput().ToBigInt())
	require.Equal(t, big.NewInt(30), v.GetOutput().ToBigInt())

	// At boundary 10: entry at index 10 → input=1000, output=30
	v = getVolume(bankUSDKey, 10)
	require.Equal(t, big.NewInt(1000), v.GetInput().ToBigInt())
	require.Equal(t, big.NewInt(30), v.GetOutput().ToBigInt())

	// At boundary 9: latest entry at index 3 → input=150, output=30
	v = getVolume(bankUSDKey, 9)
	require.Equal(t, big.NewInt(150), v.GetInput().ToBigInt())
	require.Equal(t, big.NewInt(30), v.GetOutput().ToBigInt())

	// Set another snapshot at index 11 (cumulative: input=1200, output=80)
	batch = s.NewBatch()
	require.NoError(t, attrs.Volume.Set(batch, 11, bankUSDKey, &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(1200),
		Output: commonpb.NewUint256FromUint64(80),
	}))
	require.NoError(t, batch.Commit())

	// At boundary 100: latest entry at index 11 → input=1200, output=80
	v = getVolume(bankUSDKey, 100)
	require.Equal(t, big.NewInt(1200), v.GetInput().ToBigInt())
	require.Equal(t, big.NewInt(80), v.GetOutput().ToBigInt())

	// At boundary 11: entry at index 11 → input=1200, output=80
	v = getVolume(bankUSDKey, 11)
	require.Equal(t, big.NewInt(1200), v.GetInput().ToBigInt())
	require.Equal(t, big.NewInt(80), v.GetOutput().ToBigInt())

	// Set a newer snapshot at index 20
	batch = s.NewBatch()
	require.NoError(t, attrs.Volume.Set(batch, 20, bankUSDKey, &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(5000),
		Output: commonpb.NewUint256FromUint64(80),
	}))
	require.NoError(t, batch.Commit())

	// At boundary 100: latest entry at index 20 → input=5000, output=80
	v = getVolume(bankUSDKey, 100)
	require.Equal(t, big.NewInt(5000), v.GetInput().ToBigInt())
	require.Equal(t, big.NewInt(80), v.GetOutput().ToBigInt())

	// At boundary 15: latest entry at index 11 → input=1200, output=80
	v = getVolume(bankUSDKey, 15)
	require.Equal(t, big.NewInt(1200), v.GetInput().ToBigInt())
	require.Equal(t, big.NewInt(80), v.GetOutput().ToBigInt())

	// Different account should have 0 volume
	v = getVolume(userUSDKey, 100)
	require.Equal(t, big.NewInt(0), v.GetInput().ToBigInt())
	require.Equal(t, big.NewInt(0), v.GetOutput().ToBigInt())

	// Different asset should have 0 volume
	v = getVolume(bankEURKey, 100)
	require.Equal(t, big.NewInt(0), v.GetInput().ToBigInt())
	require.Equal(t, big.NewInt(0), v.GetOutput().ToBigInt())

	// Non-existing ledger should have 0 volume
	nonExistingKey := domain.VolumeKey{AccountKey: domain.AccountKey{Ledger: "nonexistent", Account: "bank"}, Asset: "USD"}
	nonExistingCanonicalKey := nonExistingKey.Bytes()
	v = getVolume(nonExistingCanonicalKey, 100)
	require.Equal(t, big.NewInt(0), v.GetInput().ToBigInt())
	require.Equal(t, big.NewInt(0), v.GetOutput().ToBigInt())
}
