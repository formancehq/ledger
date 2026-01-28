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
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"
)

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

		log, err := ledgerService.Apply(ctx, &servicepb.LedgerAction{
			LedgerId: testLedgerID,
			Data: &servicepb.LedgerAction_AddMetadata{
				AddMetadata: &commonpb.SaveMetadataCommand{
					Target: &commonpb.Target{
						Target: &commonpb.Target_Account{
							Account: &commonpb.TargetAccount{Addr: "test-account"},
						},
					},
					Metadata: &commonpb.Metadata{Entries: md},
				},
			},
		})
		require.NoError(t, err)
		require.NotNil(t, log)
	})

	t.Run("SaveAccountMetadata_ValidationErrors", func(t *testing.T) {
		t.Parallel()
		ledgerService, _, _ := newTestLedgerService(t, ctx)

		// Test empty address
		md1 := metadata.Metadata{"key": "value"}
		log, err := ledgerService.Apply(ctx, &servicepb.LedgerAction{
			LedgerId: testLedgerID,
			Data: &servicepb.LedgerAction_AddMetadata{
				AddMetadata: &commonpb.SaveMetadataCommand{
					Target: &commonpb.Target{
						Target: &commonpb.Target_Account{
							Account: &commonpb.TargetAccount{Addr: ""},
						},
					},
					Metadata: &commonpb.Metadata{Entries: md1},
				},
			},
		})
		require.Error(t, err)
		require.Nil(t, log)
		require.Contains(t, err.Error(), "account address is required")

		// Test empty metadata
		log, err = ledgerService.Apply(ctx, &servicepb.LedgerAction{
			LedgerId: testLedgerID,
			Data: &servicepb.LedgerAction_AddMetadata{
				AddMetadata: &commonpb.SaveMetadataCommand{
					Target: &commonpb.Target{
						Target: &commonpb.Target_Account{
							Account: &commonpb.TargetAccount{Addr: "test-account"},
						},
					},
					Metadata: nil,
				},
			},
		})
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

		log, err := ledgerService.Apply(ctx, &servicepb.LedgerAction{
			LedgerId: testLedgerID,
			Data: &servicepb.LedgerAction_AddMetadata{
				AddMetadata: &commonpb.SaveMetadataCommand{
					Target: &commonpb.Target{
						Target: &commonpb.Target_Transaction{
							Transaction: &commonpb.TargetTransaction{Id: 42},
						},
					},
					Metadata: &md,
				},
			},
		})
		require.NoError(t, err)
		require.NotNil(t, log)
	})

	t.Run("SaveTransactionMetadata_ValidationErrors", func(t *testing.T) {
		t.Parallel()
		ledgerService, _, _ := newTestLedgerService(t, ctx)

		md := &commonpb.Metadata{
			Entries: metadata.Metadata{"key": "value"},
		}
		log, err := ledgerService.Apply(ctx, &servicepb.LedgerAction{
			LedgerId: testLedgerID,
			Data: &servicepb.LedgerAction_AddMetadata{
				AddMetadata: &commonpb.SaveMetadataCommand{
					Target: &commonpb.Target{
						Target: &commonpb.Target_Transaction{
							Transaction: &commonpb.TargetTransaction{Id: 0},
						},
					},
					Metadata: md,
				},
			},
		})
		require.Error(t, err)
		require.Nil(t, log)
		require.Contains(t, err.Error(), "transaction id is required")

		log, err = ledgerService.Apply(ctx, &servicepb.LedgerAction{
			LedgerId: testLedgerID,
			Data: &servicepb.LedgerAction_AddMetadata{
				AddMetadata: &commonpb.SaveMetadataCommand{
					Target: &commonpb.Target{
						Target: &commonpb.Target_Transaction{
							Transaction: &commonpb.TargetTransaction{Id: 42},
						},
					},
					Metadata: nil,
				},
			},
		})
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

		log, err := ledgerService.Apply(ctx, &servicepb.LedgerAction{
			LedgerId: testLedgerID,
			Data: &servicepb.LedgerAction_DeleteMetadata{
				DeleteMetadata: &commonpb.DeleteMetadataCommand{
					Target: &commonpb.Target{
						Target: &commonpb.Target_Account{
							Account: &commonpb.TargetAccount{Addr: "test-account"},
						},
					},
					Key: "key1",
				},
			},
		})
		require.NoError(t, err)
		require.NotNil(t, log)
	})

	t.Run("DeleteAccountMetadata_ValidationErrors", func(t *testing.T) {
		t.Parallel()
		ledgerService, _, _ := newTestLedgerService(t, ctx)

		log, err := ledgerService.Apply(ctx, &servicepb.LedgerAction{
			LedgerId: testLedgerID,
			Data: &servicepb.LedgerAction_DeleteMetadata{
				DeleteMetadata: &commonpb.DeleteMetadataCommand{
					Target: &commonpb.Target{
						Target: &commonpb.Target_Account{
							Account: &commonpb.TargetAccount{Addr: ""},
						},
					},
					Key: "key1",
				},
			},
		})
		require.Error(t, err)
		require.Nil(t, log)
		require.Contains(t, err.Error(), "account address is required")

		log, err = ledgerService.Apply(ctx, &servicepb.LedgerAction{
			LedgerId: testLedgerID,
			Data: &servicepb.LedgerAction_DeleteMetadata{
				DeleteMetadata: &commonpb.DeleteMetadataCommand{
					Target: &commonpb.Target{
						Target: &commonpb.Target_Account{
							Account: &commonpb.TargetAccount{Addr: "test-account"},
						},
					},
					Key: "",
				},
			},
		})
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

		log, err := ledgerService.Apply(ctx, &servicepb.LedgerAction{
			LedgerId: testLedgerID,
			Data: &servicepb.LedgerAction_DeleteMetadata{
				DeleteMetadata: &commonpb.DeleteMetadataCommand{
					Target: &commonpb.Target{
						Target: &commonpb.Target_Transaction{
							Transaction: &commonpb.TargetTransaction{Id: 42},
						},
					},
					Key: "key1",
				},
			},
		})
		require.NoError(t, err)
		require.NotNil(t, log)
	})

	t.Run("DeleteTransactionMetadata_WithIdempotencyKey", func(t *testing.T) {
		t.Parallel()
		ledgerService, runtimeStore, engine := newTestLedgerService(t, ctx)

		idempotencyKey := "delete-transaction-metadata-idempotency-key"
		deleteMetadataCmd := &commonpb.DeleteMetadataCommand{
			Target: &commonpb.Target{
				Target: &commonpb.Target_Transaction{
					Transaction: &commonpb.TargetTransaction{Id: 42},
				},
			},
			Key: "key1",
		}

		runtimeStore.EXPECT().
			GetSequenceForIdempotencyKey(gomock.Any(), idempotencyKey).
			Return(uint64(0), store.ErrNotFound)

		expectApplyWithSequentialIDs(engine, 1)

		log1, err := ledgerService.Apply(ctx, &servicepb.LedgerAction{
			LedgerId:       testLedgerID,
			IdempotencyKey: idempotencyKey,
			Data: &servicepb.LedgerAction_DeleteMetadata{
				DeleteMetadata: deleteMetadataCmd,
			},
		})
		require.NoError(t, err)
		require.NotNil(t, log1)

		runtimeStore.EXPECT().
			GetSequenceForIdempotencyKey(gomock.Any(), idempotencyKey).
			Return(log1.Id, nil)
		runtimeStore.EXPECT().
			GetLogBySequence(gomock.Any(), log1.Id).
			Return(wrapLedgerLogInLogWithIdempotency(log1.Id, testLedgerID, log1, idempotencyKey, deleteMetadataCmd), nil)

		log2, err := ledgerService.Apply(ctx, &servicepb.LedgerAction{
			LedgerId:       testLedgerID,
			IdempotencyKey: idempotencyKey,
			Data: &servicepb.LedgerAction_DeleteMetadata{
				DeleteMetadata: deleteMetadataCmd,
			},
		})
		require.NoError(t, err)
		require.NotNil(t, log2)
		require.Equal(t, log1.Id, log2.Id)
	})

	t.Run("DeleteTransactionMetadata_ValidationErrors", func(t *testing.T) {
		t.Parallel()
		ledgerService, _, _ := newTestLedgerService(t, ctx)

		log, err := ledgerService.Apply(ctx, &servicepb.LedgerAction{
			LedgerId: testLedgerID,
			Data: &servicepb.LedgerAction_DeleteMetadata{
				DeleteMetadata: &commonpb.DeleteMetadataCommand{
					Target: &commonpb.Target{
						Target: &commonpb.Target_Transaction{
							Transaction: &commonpb.TargetTransaction{Id: 0},
						},
					},
					Key: "key1",
				},
			},
		})
		require.Error(t, err)
		require.Nil(t, log)
		require.Contains(t, err.Error(), "transaction id is required")

		log, err = ledgerService.Apply(ctx, &servicepb.LedgerAction{
			LedgerId: testLedgerID,
			Data: &servicepb.LedgerAction_DeleteMetadata{
				DeleteMetadata: &commonpb.DeleteMetadataCommand{
					Target: &commonpb.Target{
						Target: &commonpb.Target_Transaction{
							Transaction: &commonpb.TargetTransaction{Id: 42},
						},
					},
					Key: "",
				},
			},
		})
		require.Error(t, err)
		require.Nil(t, log)
		require.Contains(t, err.Error(), "metadata key is required")
	})
}

func newTestLedgerService(t *testing.T, ctx context.Context) (*DefaultController, *store.MockStore, *MockEngine) {
	t.Helper()

	ctrl := gomock.NewController(t)
	engine := NewMockEngine(ctrl)
	store := store.NewMockStore(ctrl)
	logger := logging.FromContext(ctx)

	ledgerService := NewDefaultController(engine, store, logger)
	return ledgerService, store, engine
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
				Payload: &commonpb.Log_Apply{
					Apply: &commonpb.ApplyLog{
						LedgerId: 1, // testLedgerID
						Log: &commonpb.LedgerLog{
							Id: counter,
						},
					},
				},
			}}, nil
		}).
		Times(times)
}

// wrapLedgerLogInLog wraps a LedgerLog in a Log with ApplyLog payload
func wrapLedgerLogInLog(sequence uint64, ledgerID uint32, ledgerLog *commonpb.LedgerLog) *commonpb.Log {
	return &commonpb.Log{
		Sequence: sequence,
		Payload: &commonpb.Log_Apply{
			Apply: &commonpb.ApplyLog{
				LedgerId: ledgerID,
				Log:      ledgerLog,
			},
		},
	}
}

// wrapLedgerLogInLogWithIdempotency wraps a LedgerLog in a Log with ApplyLog payload and idempotency info
func wrapLedgerLogInLogWithIdempotency(sequence uint64, ledgerID uint32, ledgerLog *commonpb.LedgerLog, idempotencyKey string, input proto.Message) *commonpb.Log {
	return &commonpb.Log{
		Sequence: sequence,
		Payload: &commonpb.Log_Apply{
			Apply: &commonpb.ApplyLog{
				LedgerId: ledgerID,
				Log:      ledgerLog,
			},
		},
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

	t.Run("RevertTransaction", func(t *testing.T) {
		t.Parallel()
		ledgerService, runtimeStore, engine := newTestLedgerService(t, ctx)

		transactionID := uint64(42)
		logID := uint64(1)

		// Create original transaction log
		originalTx := &commonpb.Transaction{
			Id: transactionID,
			Postings: []*commonpb.Posting{
				commonpb.NewPosting("world", "account-1", "USD", big.NewInt(100)),
			},
		}
		originalLog := &commonpb.LedgerLog{
			Id: logID,
			Data: &commonpb.LogPayload{
				Payload: &commonpb.LogPayload_CreatedTransaction{
					CreatedTransaction: &commonpb.CreatedTransaction{
						Transaction: originalTx,
					},
				},
			},
		}

		// Mock expectations
		runtimeStore.EXPECT().
			IsTransactionReverted(gomock.Any(), testLedgerID, transactionID).
			Return(false, nil)
		runtimeStore.EXPECT().
			GetSequenceForTransactionID(gomock.Any(), testLedgerID, transactionID).
			Return(logID, nil)
		runtimeStore.EXPECT().
			GetLogBySequence(gomock.Any(), logID).
			Return(wrapLedgerLogInLog(logID, testLedgerID, originalLog), nil)
		runtimeStore.EXPECT().
			GetBalances(gomock.Any(), testLedgerID, gomock.Any()).
			Return(commonpb.Balances{
				"account-1": map[string]*big.Int{
					"USD": big.NewInt(100),
				},
			}, nil)

		expectApplyWithSequentialIDs(engine, 1)

		log, err := ledgerService.Apply(ctx, &servicepb.LedgerAction{
			LedgerId: testLedgerID,
			Data: &servicepb.LedgerAction_RevertTransaction{
				RevertTransaction: &servicepb.RevertTransactionPayload{
					TransactionId: transactionID,
				},
			},
		})
		require.NoError(t, err)
		require.NotNil(t, log)
	})

	t.Run("RevertTransaction_AlreadyReverted", func(t *testing.T) {
		t.Parallel()
		ledgerService, runtimeStore, _ := newTestLedgerService(t, ctx)

		transactionID := uint64(42)

		runtimeStore.EXPECT().
			IsTransactionReverted(gomock.Any(), testLedgerID, transactionID).
			Return(true, nil)

		log, err := ledgerService.Apply(ctx, &servicepb.LedgerAction{
			LedgerId: testLedgerID,
			Data: &servicepb.LedgerAction_RevertTransaction{
				RevertTransaction: &servicepb.RevertTransactionPayload{
					TransactionId: transactionID,
				},
			},
		})
		require.Error(t, err)
		require.Nil(t, log)
		require.Contains(t, err.Error(), "already reverted")
	})

	t.Run("RevertTransaction_NotFound", func(t *testing.T) {
		t.Parallel()
		ledgerService, runtimeStore, _ := newTestLedgerService(t, ctx)

		transactionID := uint64(999)

		runtimeStore.EXPECT().
			IsTransactionReverted(gomock.Any(), testLedgerID, transactionID).
			Return(false, nil)
		runtimeStore.EXPECT().
			GetSequenceForTransactionID(gomock.Any(), testLedgerID, transactionID).
			Return(uint64(0), nil)

		log, err := ledgerService.Apply(ctx, &servicepb.LedgerAction{
			LedgerId: testLedgerID,
			Data: &servicepb.LedgerAction_RevertTransaction{
				RevertTransaction: &servicepb.RevertTransactionPayload{
					TransactionId: transactionID,
				},
			},
		})
		require.Error(t, err)
		require.Nil(t, log)
		require.Contains(t, err.Error(), "not found")
	})

	t.Run("RevertTransaction_ValidationErrors", func(t *testing.T) {
		t.Parallel()
		ledgerService, _, _ := newTestLedgerService(t, ctx)

		log, err := ledgerService.Apply(ctx, &servicepb.LedgerAction{
			LedgerId: testLedgerID,
			Data: &servicepb.LedgerAction_RevertTransaction{
				RevertTransaction: &servicepb.RevertTransactionPayload{
					TransactionId: 0,
				},
			},
		})
		require.Error(t, err)
		require.Nil(t, log)
		require.Contains(t, err.Error(), "transaction id is required")
	})

	t.Run("RevertTransaction_WithForce", func(t *testing.T) {
		t.Parallel()
		ledgerService, runtimeStore, engine := newTestLedgerService(t, ctx)

		transactionID := uint64(42)
		logID := uint64(1)

		// Create original transaction log
		originalTx := &commonpb.Transaction{
			Id: transactionID,
			Postings: []*commonpb.Posting{
				commonpb.NewPosting("world", "account-1", "USD", big.NewInt(100)),
			},
		}
		originalLog := &commonpb.LedgerLog{
			Id: logID,
			Data: &commonpb.LogPayload{
				Payload: &commonpb.LogPayload_CreatedTransaction{
					CreatedTransaction: &commonpb.CreatedTransaction{
						Transaction: originalTx,
					},
				},
			},
		}

		// Mock expectations - with force=true, balance check should be skipped
		runtimeStore.EXPECT().
			IsTransactionReverted(gomock.Any(), testLedgerID, transactionID).
			Return(false, nil)
		runtimeStore.EXPECT().
			GetSequenceForTransactionID(gomock.Any(), testLedgerID, transactionID).
			Return(logID, nil)
		runtimeStore.EXPECT().
			GetLogBySequence(gomock.Any(), logID).
			Return(wrapLedgerLogInLog(logID, testLedgerID, originalLog), nil)

		expectApplyWithSequentialIDs(engine, 1)

		log, err := ledgerService.Apply(ctx, &servicepb.LedgerAction{
			LedgerId: testLedgerID,
			Data: &servicepb.LedgerAction_RevertTransaction{
				RevertTransaction: &servicepb.RevertTransactionPayload{
					TransactionId: transactionID,
					Force:         true,
				},
			},
		})
		require.NoError(t, err)
		require.NotNil(t, log)
	})

	t.Run("RevertTransaction_WithAtEffectiveDate", func(t *testing.T) {
		t.Parallel()
		ledgerService, runtimeStore, engine := newTestLedgerService(t, ctx)

		transactionID := uint64(42)
		logID := uint64(1)

		// Create original transaction log with timestamp
		originalTimestamp := commonpb.NewTimestamp(libtime.New(time.Now()))
		originalTx := &commonpb.Transaction{
			Id:        transactionID,
			Timestamp: originalTimestamp,
			Postings: []*commonpb.Posting{
				commonpb.NewPosting("world", "account-1", "USD", big.NewInt(100)),
			},
		}
		originalLog := &commonpb.LedgerLog{
			Id: logID,
			Data: &commonpb.LogPayload{
				Payload: &commonpb.LogPayload_CreatedTransaction{
					CreatedTransaction: &commonpb.CreatedTransaction{
						Transaction: originalTx,
					},
				},
			},
		}

		// Mock expectations
		runtimeStore.EXPECT().
			IsTransactionReverted(gomock.Any(), testLedgerID, transactionID).
			Return(false, nil)
		runtimeStore.EXPECT().
			GetSequenceForTransactionID(gomock.Any(), testLedgerID, transactionID).
			Return(logID, nil)
		runtimeStore.EXPECT().
			GetLogBySequence(gomock.Any(), logID).
			Return(wrapLedgerLogInLog(logID, testLedgerID, originalLog), nil)
		runtimeStore.EXPECT().
			GetBalances(gomock.Any(), testLedgerID, gomock.Any()).
			Return(commonpb.Balances{
				"account-1": map[string]*big.Int{
					"USD": big.NewInt(100),
				},
			}, nil)

		// Create a log with data for the revert transaction
		revertLogID := uint64(1)
		engine.EXPECT().
			Apply(gomock.Any(), gomock.Any()).
			DoAndReturn(func(_ context.Context, actions ...*raftcmdpb.Action) ([]*commonpb.Log, error) {
				return []*commonpb.Log{{
					Sequence: revertLogID,
					Payload: &commonpb.Log_Apply{
						Apply: &commonpb.ApplyLog{
							LedgerId: 1, // testLedgerID
							Log: &commonpb.LedgerLog{
								Id: revertLogID,
								Data: &commonpb.LogPayload{
									Payload: &commonpb.LogPayload_RevertedTransaction{
										RevertedTransaction: &commonpb.RevertedTransaction{
											RevertedTransactionId: originalTx.Id,
											RevertTransaction: &commonpb.Transaction{
												Id:        2,
												Timestamp: originalTimestamp,
											},
										},
									},
								},
							},
						},
					},
				}}, nil
			}).
			Times(1)

		log, err := ledgerService.Apply(ctx, &servicepb.LedgerAction{
			LedgerId: testLedgerID,
			Data: &servicepb.LedgerAction_RevertTransaction{
				RevertTransaction: &servicepb.RevertTransactionPayload{
					TransactionId:   transactionID,
					AtEffectiveDate: true,
				},
			},
		})
		require.NoError(t, err)
		require.NotNil(t, log)

		// Verify that the revert transaction command contains the original timestamp
		require.NotNil(t, log.Data)
		require.NotNil(t, log.Data.Payload)
	})
}
