package dal_test

import (
	"math/big"
	"testing"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/metadata"
	"github.com/formancehq/go-libs/v3/time"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/service/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/service/state"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
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

// registerLedger is a helper function to register a ledger and return its ID
func registerLedger(t *testing.T, s *dal.Store, name string, id uint32) {
	t.Helper()
	batch := s.NewBatch()
	err := state.SaveLedger(batch, &commonpb.LedgerInfo{
		Id:        id,
		Name:      name,
		CreatedAt: commonpb.NewTimestamp(time.Now()),
	})
	require.NoError(t, err)
	err = batch.Commit()
	require.NoError(t, err)
}

// appendLogs is a helper function to append logs using the batch pattern
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

	const (
		testLedgerName = "test-ledger"
		testLedgerID   = uint32(1)
	)

	t.Run("AppendLogs", func(t *testing.T) {
		t.Parallel()
		s := createStore(t)

		registerLedger(t, s, testLedgerName, testLedgerID)
		testLogs := createTestLogs(testLedgerName)
		appendLogs(t, s, 0, testLogs...)
	})

	t.Run("InputOutputCalculation", func(t *testing.T) {
		t.Parallel()
		s := createStore(t)
		attrs := attributes.New()

		registerLedger(t, s, testLedgerName, testLedgerID)
		batch := s.NewBatch()

		// Index 1: world sends 100 to bank
		worldKey := dal.VolumeKey{AccountKey: dal.AccountKey{LedgerID: testLedgerID, Account: "world"}, Asset: "USD"}
		worldCanonicalKey := worldKey.Bytes()
		require.NoError(t, attrs.Volume.AddDiff(batch, 1, worldCanonicalKey, &raftcmdpb.VolumePair{
			OutputKnown: commonpb.NewUint256FromUint64(100),
		}))

		bankKey := dal.VolumeKey{AccountKey: dal.AccountKey{LedgerID: testLedgerID, Account: "bank"}, Asset: "USD"}
		bankCanonicalKey := bankKey.Bytes()
		require.NoError(t, attrs.Volume.AddDiff(batch, 1, bankCanonicalKey, &raftcmdpb.VolumePair{
			InputKnown: commonpb.NewUint256FromUint64(100),
		}))

		// Index 2: bank sends 50 to user (bank cumulative: input=100, output=50)
		userKey := dal.VolumeKey{AccountKey: dal.AccountKey{LedgerID: testLedgerID, Account: "user"}, Asset: "USD"}
		userCanonicalKey := userKey.Bytes()
		require.NoError(t, attrs.Volume.AddDiff(batch, 2, bankCanonicalKey, &raftcmdpb.VolumePair{
			InputKnown:  commonpb.NewUint256FromUint64(100),
			OutputKnown: commonpb.NewUint256FromUint64(50),
		}))
		require.NoError(t, attrs.Volume.AddDiff(batch, 2, userCanonicalKey, &raftcmdpb.VolumePair{
			InputKnown: commonpb.NewUint256FromUint64(50),
		}))

		require.NoError(t, batch.Commit())

		// world: input=0, output=100 → balance = -100
		worldVolume, err := attrs.Volume.ComputeValue(s, 100, worldCanonicalKey)
		require.NoError(t, err)
		require.Equal(t, big.NewInt(0), worldVolume.InputKnown.ToBigInt())
		require.Equal(t, big.NewInt(100), worldVolume.OutputKnown.ToBigInt())

		// bank: input=100, output=50 → balance = 50
		bankVolume, err := attrs.Volume.ComputeValue(s, 100, bankCanonicalKey)
		require.NoError(t, err)
		require.Equal(t, big.NewInt(100), bankVolume.InputKnown.ToBigInt())
		require.Equal(t, big.NewInt(50), bankVolume.OutputKnown.ToBigInt())

		// user: input=50, output=0 → balance = 50
		userVolume, err := attrs.Volume.ComputeValue(s, 100, userCanonicalKey)
		require.NoError(t, err)
		require.Equal(t, big.NewInt(50), userVolume.InputKnown.ToBigInt())
		require.Equal(t, big.NewInt(0), userVolume.OutputKnown.ToBigInt())
	})

	t.Run("AppendLogsEmpty", func(t *testing.T) {
		t.Parallel()
		s := createStore(t)

		appendLogs(t, s, 0)
	})
}

// createTestLogs creates test logs wrapped in Log with ApplyLog payload
func createTestLogs(ledgerName string) []*commonpb.Log {
	return createTestLogsForLedger(ledgerName, 1)
}

// createTestLogsForLedger creates test logs with custom starting sequence
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

	const (
		ledgerName = "test-ledger"
		ledgerID   = uint32(1)
	)
	registerLedger(t, s, ledgerName, ledgerID)

	bankUSD := dal.VolumeKey{AccountKey: dal.AccountKey{LedgerID: ledgerID, Account: "bank"}, Asset: "USD"}
	userUSD := dal.VolumeKey{AccountKey: dal.AccountKey{LedgerID: ledgerID, Account: "user"}, Asset: "USD"}
	bankEUR := dal.VolumeKey{AccountKey: dal.AccountKey{LedgerID: ledgerID, Account: "bank"}, Asset: "EUR"}

	bankUSDKey := bankUSD.Bytes()
	userUSDKey := userUSD.Bytes()
	bankEURKey := bankEUR.Bytes()

	getVolume := func(canonicalKey []byte, index uint64) *raftcmdpb.VolumePair {
		result, err := attrs.Volume.ComputeValue(s, index, canonicalKey)
		require.NoError(t, err)
		return result
	}

	// Initially volume should be {input: 0, output: 0}
	v := getVolume(bankUSDKey, 100)
	require.Equal(t, big.NewInt(0), v.InputKnown.ToBigInt())
	require.Equal(t, big.NewInt(0), v.OutputKnown.ToBigInt())

	// Add cumulative diffs. Each diff is cumulative since the last base.
	// In the merged Volume model, each diff carries both input and output sides.
	batch := s.NewBatch()
	require.NoError(t, attrs.Volume.AddDiff(batch, 1, bankUSDKey, &raftcmdpb.VolumePair{
		InputKnown: commonpb.NewUint256FromUint64(100),
	}))
	require.NoError(t, attrs.Volume.AddDiff(batch, 2, bankUSDKey, &raftcmdpb.VolumePair{
		InputKnown: commonpb.NewUint256FromUint64(150),
	}))
	require.NoError(t, attrs.Volume.AddDiff(batch, 3, bankUSDKey, &raftcmdpb.VolumePair{
		InputKnown:  commonpb.NewUint256FromUint64(150),
		OutputKnown: commonpb.NewUint256FromUint64(30),
	}))
	require.NoError(t, batch.Commit())

	// At boundary 100: lastDiff at index 3 → input=150, output=30
	v = getVolume(bankUSDKey, 100)
	require.Equal(t, big.NewInt(150), v.InputKnown.ToBigInt())
	require.Equal(t, big.NewInt(30), v.OutputKnown.ToBigInt())

	// At boundary 2: lastDiff at index 2 → input=150, output=0
	v = getVolume(bankUSDKey, 2)
	require.Equal(t, big.NewInt(150), v.InputKnown.ToBigInt())
	require.Equal(t, big.NewInt(0), v.OutputKnown.ToBigInt())

	// At boundary 1: lastDiff at index 1 → input=100, output=0
	v = getVolume(bankUSDKey, 1)
	require.Equal(t, big.NewInt(100), v.InputKnown.ToBigInt())
	require.Equal(t, big.NewInt(0), v.OutputKnown.ToBigInt())

	// At boundary 0: no diffs → input=0, output=0
	v = getVolume(bankUSDKey, 0)
	require.Equal(t, big.NewInt(0), v.InputKnown.ToBigInt())
	require.Equal(t, big.NewInt(0), v.OutputKnown.ToBigInt())

	// Add a base at index 10 (captures cumulative state: input=1000, output=30)
	batch = s.NewBatch()
	require.NoError(t, attrs.Volume.SetBase(batch, 10, bankUSDKey, &raftcmdpb.VolumePair{
		InputKnown:  commonpb.NewUint256FromUint64(1000),
		OutputKnown: commonpb.NewUint256FromUint64(30),
	}))
	require.NoError(t, batch.Commit())

	// At boundary 100: base at 10, no diffs after → input=1000, output=30
	v = getVolume(bankUSDKey, 100)
	require.Equal(t, big.NewInt(1000), v.InputKnown.ToBigInt())
	require.Equal(t, big.NewInt(30), v.OutputKnown.ToBigInt())

	// At boundary 10: base → input=1000, output=30
	v = getVolume(bankUSDKey, 10)
	require.Equal(t, big.NewInt(1000), v.InputKnown.ToBigInt())
	require.Equal(t, big.NewInt(30), v.OutputKnown.ToBigInt())

	// At boundary 9: base not visible, diffs only → lastDiff at 3: input=150, output=30
	v = getVolume(bankUSDKey, 9)
	require.Equal(t, big.NewInt(150), v.InputKnown.ToBigInt())
	require.Equal(t, big.NewInt(30), v.OutputKnown.ToBigInt())

	// Add cumulative diffs after the base
	batch = s.NewBatch()
	require.NoError(t, attrs.Volume.AddDiff(batch, 11, bankUSDKey, &raftcmdpb.VolumePair{
		InputKnown:  commonpb.NewUint256FromUint64(200),
		OutputKnown: commonpb.NewUint256FromUint64(50),
	}))
	require.NoError(t, batch.Commit())

	// At boundary 100: base(1000,30) + diff(200,50) → input=1200, output=80
	v = getVolume(bankUSDKey, 100)
	require.Equal(t, big.NewInt(1200), v.InputKnown.ToBigInt())
	require.Equal(t, big.NewInt(80), v.OutputKnown.ToBigInt())

	// At boundary 11: base(1000,30) + diff(200,50) → input=1200, output=80
	v = getVolume(bankUSDKey, 11)
	require.Equal(t, big.NewInt(1200), v.InputKnown.ToBigInt())
	require.Equal(t, big.NewInt(80), v.OutputKnown.ToBigInt())

	// Add a newer base at index 20
	batch = s.NewBatch()
	require.NoError(t, attrs.Volume.SetBase(batch, 20, bankUSDKey, &raftcmdpb.VolumePair{
		InputKnown:  commonpb.NewUint256FromUint64(5000),
		OutputKnown: commonpb.NewUint256FromUint64(80),
	}))
	require.NoError(t, batch.Commit())

	// At boundary 100: newer base → input=5000, output=80
	v = getVolume(bankUSDKey, 100)
	require.Equal(t, big.NewInt(5000), v.InputKnown.ToBigInt())
	require.Equal(t, big.NewInt(80), v.OutputKnown.ToBigInt())

	// At boundary 15: older base + diffs → input=1200, output=80
	v = getVolume(bankUSDKey, 15)
	require.Equal(t, big.NewInt(1200), v.InputKnown.ToBigInt())
	require.Equal(t, big.NewInt(80), v.OutputKnown.ToBigInt())

	// Different account should have 0 volume
	v = getVolume(userUSDKey, 100)
	require.Equal(t, big.NewInt(0), v.InputKnown.ToBigInt())
	require.Equal(t, big.NewInt(0), v.OutputKnown.ToBigInt())

	// Different asset should have 0 volume
	v = getVolume(bankEURKey, 100)
	require.Equal(t, big.NewInt(0), v.InputKnown.ToBigInt())
	require.Equal(t, big.NewInt(0), v.OutputKnown.ToBigInt())

	// Non-existing ledger should have 0 volume
	nonExistingKey := dal.VolumeKey{AccountKey: dal.AccountKey{LedgerID: 999, Account: "bank"}, Asset: "USD"}
	nonExistingCanonicalKey := nonExistingKey.Bytes()
	v = getVolume(nonExistingCanonicalKey, 100)
	require.Equal(t, big.NewInt(0), v.InputKnown.ToBigInt())
	require.Equal(t, big.NewInt(0), v.OutputKnown.ToBigInt())
}
