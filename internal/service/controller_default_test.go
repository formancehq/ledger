package service

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/metadata"
	libtime "github.com/formancehq/go-libs/v3/time"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/internal/store"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"
)

// applyAction wraps a LedgerApplyAction in an Action for testing
func applyAction(action *servicepb.LedgerApplyRequest) *servicepb.Request {
	return applyActionWithIdempotency(action, "")
}

func applyActionWithIdempotency(action *servicepb.LedgerApplyRequest, idempotencyKey string) *servicepb.Request {
	return &servicepb.Request{
		IdempotencyKey: idempotencyKey,
		Type: &servicepb.Request_Apply{
			Apply: action,
		},
	}
}

func TestDefaultLedger_SaveAccountMetadata(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	var testLedgerID uint32 = 1

	t.Run("SaveAccountMetadata", func(t *testing.T) {
		t.Parallel()
		ledgerService, _, engine := newTestLedgerService(t, ctx)

		expectApplyWithSequentialIDs(engine, 1)

		// Save account metadata
		md := metadata.Metadata{
			"account_type": "asset",
			"label":        "Test Account",
		}

		log, err := ledgerService.Apply(ctx, applyAction(&servicepb.LedgerApplyRequest{
			Ledger: servicepb.LedgerID(testLedgerID),
			Data: &servicepb.LedgerApplyRequest_AddMetadata{
				AddMetadata: &commonpb.SaveMetadataCommand{
					Target: &commonpb.Target{
						Target: &commonpb.Target_Account{
							Account: &commonpb.TargetAccount{Addr: "test-account"},
						},
					},
					Metadata: &commonpb.Metadata{Entries: md},
				},
			},
		}))
		require.NoError(t, err)
		require.NotNil(t, log)
	})

	t.Run("SaveAccountMetadata_ValidationErrors", func(t *testing.T) {
		t.Parallel()
		ledgerService, _, _ := newTestLedgerService(t, ctx)

		// Test empty address
		md1 := metadata.Metadata{"key": "value"}
		log, err := ledgerService.Apply(ctx, applyAction(&servicepb.LedgerApplyRequest{
			Ledger: servicepb.LedgerID(testLedgerID),
			Data: &servicepb.LedgerApplyRequest_AddMetadata{
				AddMetadata: &commonpb.SaveMetadataCommand{
					Target: &commonpb.Target{
						Target: &commonpb.Target_Account{
							Account: &commonpb.TargetAccount{Addr: ""},
						},
					},
					Metadata: &commonpb.Metadata{Entries: md1},
				},
			},
		}))
		require.Error(t, err)
		require.Nil(t, log)
		require.Contains(t, err.Error(), "account address is required")

		// Test empty metadata
		log, err = ledgerService.Apply(ctx, applyAction(&servicepb.LedgerApplyRequest{
			Ledger: servicepb.LedgerID(testLedgerID),
			Data: &servicepb.LedgerApplyRequest_AddMetadata{
				AddMetadata: &commonpb.SaveMetadataCommand{
					Target: &commonpb.Target{
						Target: &commonpb.Target_Account{
							Account: &commonpb.TargetAccount{Addr: "test-account"},
						},
					},
					Metadata: nil,
				},
			},
		}))
		require.Error(t, err)
		require.Nil(t, log)
		require.Contains(t, err.Error(), "metadata is required")
	})
}

func TestDefaultLedger_SaveTransactionMetadata(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	var testLedgerID uint32 = 1

	t.Run("SaveTransactionMetadata", func(t *testing.T) {
		t.Parallel()
		ledgerService, _, engine := newTestLedgerService(t, ctx)

		expectApplyWithSequentialIDs(engine, 1)

		md := commonpb.Metadata{
			Entries: metadata.Metadata{
				"tx_label": "Test Transaction",
			},
		}

		log, err := ledgerService.Apply(ctx, applyAction(&servicepb.LedgerApplyRequest{
			Ledger: servicepb.LedgerID(testLedgerID),
			Data: &servicepb.LedgerApplyRequest_AddMetadata{
				AddMetadata: &commonpb.SaveMetadataCommand{
					Target: &commonpb.Target{
						Target: &commonpb.Target_Transaction{
							Transaction: &commonpb.TargetTransaction{Id: 42},
						},
					},
					Metadata: &md,
				},
			},
		}))
		require.NoError(t, err)
		require.NotNil(t, log)
	})

	t.Run("SaveTransactionMetadata_ValidationErrors", func(t *testing.T) {
		t.Parallel()
		ledgerService, _, _ := newTestLedgerService(t, ctx)

		md := &commonpb.Metadata{
			Entries: metadata.Metadata{"key": "value"},
		}
		log, err := ledgerService.Apply(ctx, applyAction(&servicepb.LedgerApplyRequest{
			Ledger: servicepb.LedgerID(testLedgerID),
			Data: &servicepb.LedgerApplyRequest_AddMetadata{
				AddMetadata: &commonpb.SaveMetadataCommand{
					Target: &commonpb.Target{
						Target: &commonpb.Target_Transaction{
							Transaction: &commonpb.TargetTransaction{Id: 0},
						},
					},
					Metadata: md,
				},
			},
		}))
		require.Error(t, err)
		require.Nil(t, log)
		require.Contains(t, err.Error(), "transaction id is required")

		log, err = ledgerService.Apply(ctx, applyAction(&servicepb.LedgerApplyRequest{
			Ledger: servicepb.LedgerID(testLedgerID),
			Data: &servicepb.LedgerApplyRequest_AddMetadata{
				AddMetadata: &commonpb.SaveMetadataCommand{
					Target: &commonpb.Target{
						Target: &commonpb.Target_Transaction{
							Transaction: &commonpb.TargetTransaction{Id: 42},
						},
					},
					Metadata: nil,
				},
			},
		}))
		require.Error(t, err)
		require.Nil(t, log)
		require.Contains(t, err.Error(), "metadata is required")
	})
}

func TestDefaultLedger_DeleteAccountMetadata(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	var testLedgerID uint32 = 1

	t.Run("DeleteAccountMetadata", func(t *testing.T) {
		t.Parallel()
		ledgerService, _, engine := newTestLedgerService(t, ctx)

		expectApplyWithSequentialIDs(engine, 1)

		log, err := ledgerService.Apply(ctx, applyAction(&servicepb.LedgerApplyRequest{
			Ledger: servicepb.LedgerID(testLedgerID),
			Data: &servicepb.LedgerApplyRequest_DeleteMetadata{
				DeleteMetadata: &commonpb.DeleteMetadataCommand{
					Target: &commonpb.Target{
						Target: &commonpb.Target_Account{
							Account: &commonpb.TargetAccount{Addr: "test-account"},
						},
					},
					Key: "key1",
				},
			},
		}))
		require.NoError(t, err)
		require.NotNil(t, log)
	})

	t.Run("DeleteAccountMetadata_ValidationErrors", func(t *testing.T) {
		t.Parallel()
		ledgerService, _, _ := newTestLedgerService(t, ctx)

		log, err := ledgerService.Apply(ctx, applyAction(&servicepb.LedgerApplyRequest{
			Ledger: servicepb.LedgerID(testLedgerID),
			Data: &servicepb.LedgerApplyRequest_DeleteMetadata{
				DeleteMetadata: &commonpb.DeleteMetadataCommand{
					Target: &commonpb.Target{
						Target: &commonpb.Target_Account{
							Account: &commonpb.TargetAccount{Addr: ""},
						},
					},
					Key: "key1",
				},
			},
		}))
		require.Error(t, err)
		require.Nil(t, log)
		require.Contains(t, err.Error(), "account address is required")

		log, err = ledgerService.Apply(ctx, applyAction(&servicepb.LedgerApplyRequest{
			Ledger: servicepb.LedgerID(testLedgerID),
			Data: &servicepb.LedgerApplyRequest_DeleteMetadata{
				DeleteMetadata: &commonpb.DeleteMetadataCommand{
					Target: &commonpb.Target{
						Target: &commonpb.Target_Account{
							Account: &commonpb.TargetAccount{Addr: "test-account"},
						},
					},
					Key: "",
				},
			},
		}))
		require.Error(t, err)
		require.Nil(t, log)
		require.Contains(t, err.Error(), "metadata key is required")
	})
}

func TestDefaultLedger_DeleteTransactionMetadata(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	var testLedgerID uint32 = 1

	t.Run("DeleteTransactionMetadata", func(t *testing.T) {
		t.Parallel()
		ledgerService, _, engine := newTestLedgerService(t, ctx)

		expectApplyWithSequentialIDs(engine, 1)

		log, err := ledgerService.Apply(ctx, applyAction(&servicepb.LedgerApplyRequest{
			Ledger: servicepb.LedgerID(testLedgerID),
			Data: &servicepb.LedgerApplyRequest_DeleteMetadata{
				DeleteMetadata: &commonpb.DeleteMetadataCommand{
					Target: &commonpb.Target{
						Target: &commonpb.Target_Transaction{
							Transaction: &commonpb.TargetTransaction{Id: 42},
						},
					},
					Key: "key1",
				},
			},
		}))
		require.NoError(t, err)
		require.NotNil(t, log)
	})

	t.Run("DeleteTransactionMetadata_ValidationErrors", func(t *testing.T) {
		t.Parallel()
		ledgerService, _, _ := newTestLedgerService(t, ctx)

		log, err := ledgerService.Apply(ctx, applyAction(&servicepb.LedgerApplyRequest{
			Ledger: servicepb.LedgerID(testLedgerID),
			Data: &servicepb.LedgerApplyRequest_DeleteMetadata{
				DeleteMetadata: &commonpb.DeleteMetadataCommand{
					Target: &commonpb.Target{
						Target: &commonpb.Target_Transaction{
							Transaction: &commonpb.TargetTransaction{Id: 0},
						},
					},
					Key: "key1",
				},
			},
		}))
		require.Error(t, err)
		require.Nil(t, log)
		require.Contains(t, err.Error(), "transaction id is required")

		log, err = ledgerService.Apply(ctx, applyAction(&servicepb.LedgerApplyRequest{
			Ledger: servicepb.LedgerID(testLedgerID),
			Data: &servicepb.LedgerApplyRequest_DeleteMetadata{
				DeleteMetadata: &commonpb.DeleteMetadataCommand{
					Target: &commonpb.Target{
						Target: &commonpb.Target_Transaction{
							Transaction: &commonpb.TargetTransaction{Id: 42},
						},
					},
					Key: "",
				},
			},
		}))
		require.Error(t, err)
		require.Nil(t, log)
		require.Contains(t, err.Error(), "metadata key is required")
	})
}

func newTestLedgerService(t *testing.T, ctx context.Context) (*DefaultController, *store.StoreInterceptor, *MockEngine) {
	t.Helper()

	ctrl := gomock.NewController(t)
	engine := NewMockEngine(ctrl)
	logger := logging.FromContext(ctx)

	// Create a temporary pebble store
	dataDir := t.TempDir()
	meterProvider := noop.NewMeterProvider()
	pebbleStore, err := store.NewStore(dataDir, logger, meterProvider.Meter("test"), store.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = pebbleStore.Close()
	})

	storeInterceptor := store.NewStoreInterceptor(pebbleStore)

	ledgerService := NewDefaultController(engine, pebbleStore, logger)
	return ledgerService, storeInterceptor, engine
}

func expectApplyWithSequentialIDs(engine *MockEngine, times int) {
	var counter uint64
	engine.EXPECT().
		Apply(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, actions ...*raftcmdpb.Action) ([]*commonpb.Log, error) {
			counter++
			// Return []*commonpb.Log with an ApplyLog payload containing the LedgerLog
			return []*commonpb.Log{{
				Sequence: counter,
				Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{
					Apply: &commonpb.ApplyLedgerLog{
						LedgerId: 1, // testLedgerID
						Log: &commonpb.LedgerLog{
							Id: counter,
						},
					},
				}},
			}}, nil
		}).
		Times(times)
}

// wrapLedgerLogInLog wraps a LedgerLog in a Log with ApplyLog payload
func wrapLedgerLogInLog(sequence uint64, ledgerID uint32, ledgerLog *commonpb.LedgerLog) *commonpb.Log {
	return &commonpb.Log{
		Sequence: sequence,
		Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{
			Apply: &commonpb.ApplyLedgerLog{
				LedgerId: ledgerID,
				Log:      ledgerLog,
			},
		}},
	}
}

// wrapLedgerLogInLogWithIdempotency wraps a LedgerLog in a Log with ApplyLog payload and idempotency info
func wrapLedgerLogInLogWithIdempotency(sequence uint64, ledgerID uint32, ledgerLog *commonpb.LedgerLog, idempotencyKey string, input proto.Message) *commonpb.Log {
	return &commonpb.Log{
		Sequence: sequence,
		Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{
			Apply: &commonpb.ApplyLedgerLog{
				LedgerId: ledgerID,
				Log:      ledgerLog,
			},
		}},
		Idempotency: &commonpb.Idempotency{
			Key:  idempotencyKey,
			Hash: commonpb.ComputeIdempotencyHash(input),
		},
	}
}

func TestDefaultLedger_RevertTransaction(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	var testLedgerID uint32 = 1

	t.Run("RevertTransaction_ValidationErrors", func(t *testing.T) {
		t.Parallel()
		ledgerService, _, _ := newTestLedgerService(t, ctx)

		log, err := ledgerService.Apply(ctx, applyAction(&servicepb.LedgerApplyRequest{
			Ledger: servicepb.LedgerID(testLedgerID),
			Data: &servicepb.LedgerApplyRequest_RevertTransaction{
				RevertTransaction: &servicepb.RevertTransactionPayload{
					TransactionId: 0,
				},
			},
		}))
		require.Error(t, err)
		require.Nil(t, log)
		require.Contains(t, err.Error(), "transaction id is required")
	})

	t.Run("RevertTransaction_NotFound", func(t *testing.T) {
		t.Parallel()
		ledgerService, _, _ := newTestLedgerService(t, ctx)

		transactionID := uint64(999)

		// With a real store, the transaction won't be found
		log, err := ledgerService.Apply(ctx, applyAction(&servicepb.LedgerApplyRequest{
			Ledger: servicepb.LedgerID(testLedgerID),
			Data: &servicepb.LedgerApplyRequest_RevertTransaction{
				RevertTransaction: &servicepb.RevertTransactionPayload{
					TransactionId: transactionID,
				},
			},
		}))
		require.Error(t, err)
		require.Nil(t, log)
		require.Contains(t, err.Error(), "not found")
	})
}

// These helper types and methods are preserved for reference
var _ = libtime.Time{}
var _ = time.Time{}
var _ = big.Int{}
var _ = store.BalanceDiffsResult{}
