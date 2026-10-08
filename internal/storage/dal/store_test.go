package dal_test

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	"github.com/formancehq/go-libs/v5/pkg/types/metadata"
	"github.com/formancehq/go-libs/v5/pkg/types/time"
	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/protohelpers"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
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

	batch := s.OpenWriteSession()
	err := state.SaveLedger(batch, name, &ledgerpb.LedgerInfo{
		Name:      name,
		CreatedAt: ledgerpb.NewTimestamp(time.Now()),
	})
	require.NoError(t, err)
	err = batch.Commit()
	require.NoError(t, err)
}

// appendLogs is a helper function to append logs using the batch pattern.
func appendLogs(t *testing.T, s *dal.Store, lastAppliedIndex uint64, logs ...*ledgerpb.Log) {
	t.Helper()

	batch := s.OpenWriteSession()
	err := state.AppendLogs(batch, logs)
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
		batch := s.OpenWriteSession()

		// Index 1: world sends 100 to bank
		worldKey := domain.VolumeKey{AccountKey: domain.AccountKey{LedgerName: "test-ledger", Account: "world"}, Asset: "USD"}
		worldCanonicalKey := worldKey.Bytes()
		_, err := attrs.Volume.Set(batch, worldCanonicalKey, &raftcmdpb.VolumePair{
			Output: ledgerpb.NewUint256FromUint64(100),
		})
		require.NoError(t, err)

		bankKey := domain.VolumeKey{AccountKey: domain.AccountKey{LedgerName: "test-ledger", Account: "bank"}, Asset: "USD"}
		bankCanonicalKey := bankKey.Bytes()
		_, err = attrs.Volume.Set(batch, bankCanonicalKey, &raftcmdpb.VolumePair{
			Input:  ledgerpb.NewUint256FromUint64(100),
			Output: ledgerpb.NewUint256FromUint64(50),
		})
		require.NoError(t, err)

		userKey := domain.VolumeKey{AccountKey: domain.AccountKey{LedgerName: "test-ledger", Account: "user"}, Asset: "USD"}
		userCanonicalKey := userKey.Bytes()

		_, err = attrs.Volume.Set(batch, userCanonicalKey, &raftcmdpb.VolumePair{
			Input: ledgerpb.NewUint256FromUint64(50),
		})
		require.NoError(t, err)

		require.NoError(t, batch.Commit())

		// world: input=0, output=100 → balance = -100
		worldVolume, err := attrs.Volume.Get(s, worldCanonicalKey)
		require.NoError(t, err)
		require.Equal(t, big.NewInt(0), worldVolume.GetInput().ToBigInt())
		require.Equal(t, big.NewInt(100), worldVolume.GetOutput().ToBigInt())

		// bank: input=100, output=50 → balance = 50
		bankVolume, err := attrs.Volume.Get(s, bankCanonicalKey)
		require.NoError(t, err)
		require.Equal(t, big.NewInt(100), bankVolume.GetInput().ToBigInt())
		require.Equal(t, big.NewInt(50), bankVolume.GetOutput().ToBigInt())

		// user: input=50, output=0 → balance = 50
		userVolume, err := attrs.Volume.Get(s, userCanonicalKey)
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
func createTestLogs(ledgerName string) []*ledgerpb.Log {
	return createTestLogsForLedger(ledgerName, 1)
}

// createTestLogsForLedger creates test logs with custom starting sequence.
func createTestLogsForLedger(ledgerName string, startSequence uint64) []*ledgerpb.Log {
	now := time.Now()

	logs := []*ledgerpb.Log{
		{
			Sequence: startSequence,
			Payload: &ledgerpb.LogPayload{Type: &ledgerpb.LogPayload_Apply{
				Apply: &ledgerpb.ApplyLedgerLog{
					LedgerName: ledgerName,
					Log: protohelpers.WithLedgerLogDate(protohelpers.WithLedgerLogID(protohelpers.NewLedgerLog(&ledgerpb.LedgerLogPayload{
						Payload: &ledgerpb.LedgerLogPayload_CreatedTransaction{
							CreatedTransaction: &ledgerpb.CreatedTransaction{
								Transaction: protohelpers.WithTransactionTimestamp(protohelpers.WithTransactionID(protohelpers.WithTransactionPostings(protohelpers.NewTransaction(), protohelpers.NewPosting("world", "bank", "USD", big.NewInt(100))), 1), now),
								AccountMetadata: map[string]*ledgerpb.MetadataMap{
									"bank": protohelpers.MetadataMapFromGoMap(metadata.Metadata{
										"account_type": "asset",
									}),
								},
							},
						},
					}), 1), now),
				},
			}},
		},
		{
			Sequence: startSequence + 1,
			Payload: &ledgerpb.LogPayload{Type: &ledgerpb.LogPayload_Apply{
				Apply: &ledgerpb.ApplyLedgerLog{
					LedgerName: ledgerName,
					Log: protohelpers.WithLedgerLogDate(protohelpers.WithLedgerLogID(protohelpers.NewLedgerLog(&ledgerpb.LedgerLogPayload{
						Payload: &ledgerpb.LedgerLogPayload_CreatedTransaction{
							CreatedTransaction: &ledgerpb.CreatedTransaction{
								Transaction: protohelpers.WithTransactionTimestamp(protohelpers.WithTransactionID(protohelpers.WithTransactionPostings(protohelpers.NewTransaction(), protohelpers.NewPosting("bank", "user", "USD", big.NewInt(50))), 2), now),
							},
						},
					}), 2), now.Add(time.Second)),
				},
			}},
		},
		{
			Sequence: startSequence + 2,
			Payload: &ledgerpb.LogPayload{Type: &ledgerpb.LogPayload_Apply{
				Apply: &ledgerpb.ApplyLedgerLog{
					LedgerName: ledgerName,
					Log: protohelpers.WithLedgerLogDate(protohelpers.WithLedgerLogID(protohelpers.NewLedgerLog(&ledgerpb.LedgerLogPayload{
						Payload: &ledgerpb.LedgerLogPayload_SavedMetadata{
							SavedMetadata: &ledgerpb.SavedMetadata{
								Target: &ledgerpb.Target{
									Target: &ledgerpb.Target_Account{Account: &ledgerpb.TargetAccount{
										Addr: "bank",
									}},
								},
								Metadata: protohelpers.MetadataFromGoMap(metadata.Metadata{
									"label": "Bank Account",
								}),
							},
						},
					}), 3), now.Add(2*time.Second)),
				},
			}},
		},
		{
			Sequence: startSequence + 3,
			Payload: &ledgerpb.LogPayload{Type: &ledgerpb.LogPayload_Apply{
				Apply: &ledgerpb.ApplyLedgerLog{
					LedgerName: ledgerName,
					Log: protohelpers.WithLedgerLogDate(protohelpers.WithLedgerLogID(protohelpers.NewLedgerLog(&ledgerpb.LedgerLogPayload{
						Payload: &ledgerpb.LedgerLogPayload_DeletedMetadata{
							DeletedMetadata: &ledgerpb.DeletedMetadata{
								Target: &ledgerpb.Target{
									Target: &ledgerpb.Target_Account{Account: &ledgerpb.TargetAccount{
										Addr: "bank",
									}},
								},
								Key: "old_key",
							},
						},
					}), 4), now.Add(3*time.Second)),
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

	bankUSD := domain.VolumeKey{AccountKey: domain.AccountKey{LedgerName: "test-ledger", Account: "bank"}, Asset: "USD"}
	userUSD := domain.VolumeKey{AccountKey: domain.AccountKey{LedgerName: "test-ledger", Account: "user"}, Asset: "USD"}
	bankEUR := domain.VolumeKey{AccountKey: domain.AccountKey{LedgerName: "test-ledger", Account: "bank"}, Asset: "EUR"}

	bankUSDKey := bankUSD.Bytes()
	userUSDKey := userUSD.Bytes()
	bankEURKey := bankEUR.Bytes()

	getVolume := func(canonicalKey []byte) *raftcmdpb.VolumePair {
		result, err := attrs.Volume.Get(s, canonicalKey)
		require.NoError(t, err)

		return result
	}

	// Initially volume should be {input: 0, output: 0}
	v := getVolume(bankUSDKey)
	require.Equal(t, big.NewInt(0), v.GetInput().ToBigInt())
	require.Equal(t, big.NewInt(0), v.GetOutput().ToBigInt())

	// Set cumulative volume for bank USD.
	batch := s.OpenWriteSession()
	_, err = attrs.Volume.Set(batch, bankUSDKey, &raftcmdpb.VolumePair{
		Input:  ledgerpb.NewUint256FromUint64(150),
		Output: ledgerpb.NewUint256FromUint64(30),
	})
	require.NoError(t, err)
	require.NoError(t, batch.Commit())

	// Read back: input=150, output=30
	v = getVolume(bankUSDKey)
	require.Equal(t, big.NewInt(150), v.GetInput().ToBigInt())
	require.Equal(t, big.NewInt(30), v.GetOutput().ToBigInt())

	// Overwrite with a new cumulative value
	batch = s.OpenWriteSession()
	_, err = attrs.Volume.Set(batch, bankUSDKey, &raftcmdpb.VolumePair{
		Input:  ledgerpb.NewUint256FromUint64(1000),
		Output: ledgerpb.NewUint256FromUint64(30),
	})
	require.NoError(t, err)
	require.NoError(t, batch.Commit())

	// Latest value: input=1000, output=30
	v = getVolume(bankUSDKey)
	require.Equal(t, big.NewInt(1000), v.GetInput().ToBigInt())
	require.Equal(t, big.NewInt(30), v.GetOutput().ToBigInt())

	// Overwrite again
	batch = s.OpenWriteSession()
	_, err = attrs.Volume.Set(batch, bankUSDKey, &raftcmdpb.VolumePair{
		Input:  ledgerpb.NewUint256FromUint64(5000),
		Output: ledgerpb.NewUint256FromUint64(80),
	})
	require.NoError(t, err)
	require.NoError(t, batch.Commit())

	// Latest value: input=5000, output=80
	v = getVolume(bankUSDKey)
	require.Equal(t, big.NewInt(5000), v.GetInput().ToBigInt())
	require.Equal(t, big.NewInt(80), v.GetOutput().ToBigInt())

	// Different account should have 0 volume
	v = getVolume(userUSDKey)
	require.Equal(t, big.NewInt(0), v.GetInput().ToBigInt())
	require.Equal(t, big.NewInt(0), v.GetOutput().ToBigInt())

	// Different asset should have 0 volume
	v = getVolume(bankEURKey)
	require.Equal(t, big.NewInt(0), v.GetInput().ToBigInt())
	require.Equal(t, big.NewInt(0), v.GetOutput().ToBigInt())

	// Non-existing ledger should have 0 volume
	nonExistingKey := domain.VolumeKey{AccountKey: domain.AccountKey{LedgerName: "other-ledger", Account: "bank"}, Asset: "USD"}
	nonExistingCanonicalKey := nonExistingKey.Bytes()
	v = getVolume(nonExistingCanonicalKey)
	require.Equal(t, big.NewInt(0), v.GetInput().ToBigInt())
	require.Equal(t, big.NewInt(0), v.GetOutput().ToBigInt())
}
