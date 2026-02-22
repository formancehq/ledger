package processing

import (
	"testing"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestProcessCreateTransaction(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1, LedgerId: 1}

	sourceKey := dal.VolumeKey{
		AccountKey: dal.AccountKey{LedgerID: 1, Account: "bank"},
		Asset:      "USD",
	}
	destKey := dal.VolumeKey{
		AccountKey: dal.AccountKey{LedgerID: 1, Account: "users:123"},
		Asset:      "USD",
	}

	// Source has 1000 input, 0 output -> balance = 1000
	sourceVolume := &raftcmdpb.VolumePair{
		InputKnown:  commonpb.NewUint256FromUint64(1000),
		OutputKnown: commonpb.NewUint256FromUint64(0),
	}

	// Destination starts with 0
	destVolume := &raftcmdpb.VolumePair{
		InputKnown:  commonpb.NewUint256FromUint64(0),
		OutputKnown: commonpb.NewUint256FromUint64(0),
	}

	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries, true)
	mockStore.EXPECT().GetLedger("test-ledger").Return(nil, false)
	mockStore.EXPECT().GetDate().Return(now).Times(4) // Called for: ledger log date, timestamp fallback, InsertedAt, UpdatedAt
	mockStore.EXPECT().GetCurrentOpenPeriod().Return(nil, false)
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())
	mockStore.EXPECT().GetVolume(sourceKey).Return(sourceVolume, nil)
	mockStore.EXPECT().PutVolume(sourceKey, gomock.Any()).Do(
		func(key dal.VolumeKey, value *raftcmdpb.VolumePair) {
			// Output should increase by 100
			require.Equal(t, int64(100), value.OutputKnown.ToBigInt().Int64())
		},
	)
	mockStore.EXPECT().GetVolume(destKey).Return(destVolume, nil)
	mockStore.EXPECT().PutVolume(destKey, gomock.Any()).Do(
		func(key dal.VolumeKey, value *raftcmdpb.VolumePair) {
			// Input should increase by 100
			require.Equal(t, int64(100), value.InputKnown.ToBigInt().Int64())
		},
	)
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(1))
	mockStore.EXPECT().AddTransactionUpdate(dal.TransactionKey{LedgerID: 1, ID: 1}, gomock.Any())

	request := &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: "test-ledger",
				Data: &servicepb.LedgerApplyRequest_CreateTransaction{
					CreateTransaction: &servicepb.CreateTransactionPayload{
						Postings: []*commonpb.Posting{
							{
								Source:      "bank",
								Destination: "users:123",
								Amount:      commonpb.NewUint256FromUint64(100),
								Asset:       "USD",
							},
						},
					},
				},
			},
		},
	}

	result, err := processor.ProcessOrder(requestToOrder(request), mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	applyLog := result.GetApply()
	require.NotNil(t, applyLog)

	createdTx := applyLog.Log.Data.GetCreatedTransaction()
	require.NotNil(t, createdTx)
	require.Equal(t, uint64(1), createdTx.Transaction.Id)
	require.Len(t, createdTx.Transaction.Postings, 1)
}

func TestProcessCreateTransaction_InsufficientFunds(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1, LedgerId: 1}

	sourceKey := dal.VolumeKey{
		AccountKey: dal.AccountKey{LedgerID: 1, Account: "users:123"},
		Asset:      "USD",
	}

	// Source has only 50 balance (100 input - 50 output)
	sourceVolume := &raftcmdpb.VolumePair{
		InputKnown:  commonpb.NewUint256FromUint64(100),
		OutputKnown: commonpb.NewUint256FromUint64(50),
	}

	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries, true)
	mockStore.EXPECT().GetVolume(sourceKey).Return(sourceVolume, nil)

	request := &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: "test-ledger",
				Data: &servicepb.LedgerApplyRequest_CreateTransaction{
					CreateTransaction: &servicepb.CreateTransactionPayload{
						Postings: []*commonpb.Posting{
							{
								Source:      "users:123",
								Destination: "merchant",
								Amount:      commonpb.NewUint256FromUint64(100), // Wants 100, has only 50
								Asset:       "USD",
							},
						},
					},
				},
			},
		},
	}

	result, err := processor.ProcessOrder(requestToOrder(request), mockStore)
	require.Error(t, err)
	require.Nil(t, result)
	require.Contains(t, err.Error(), "insufficient funds")
}

func TestProcessCreateTransaction_WorldSource(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1, LedgerId: 1}

	worldKey := dal.VolumeKey{
		AccountKey: dal.AccountKey{LedgerID: 1, Account: "world"},
		Asset:      "USD",
	}
	destKey := dal.VolumeKey{
		AccountKey: dal.AccountKey{LedgerID: 1, Account: "users:123"},
		Asset:      "USD",
	}

	// World has negative balance (but "world" bypasses balance check)
	worldVolume := &raftcmdpb.VolumePair{
		InputKnown:  commonpb.NewUint256FromUint64(0),
		OutputKnown: commonpb.NewUint256FromUint64(1000000),
	}
	destVolume := &raftcmdpb.VolumePair{
		InputKnown:  commonpb.NewUint256FromUint64(0),
		OutputKnown: commonpb.NewUint256FromUint64(0),
	}

	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries, true)
	mockStore.EXPECT().GetLedger("test-ledger").Return(nil, false)
	mockStore.EXPECT().GetDate().Return(now).Times(4)
	mockStore.EXPECT().GetCurrentOpenPeriod().Return(nil, false)
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())
	mockStore.EXPECT().GetVolume(worldKey).Return(worldVolume, nil)
	mockStore.EXPECT().PutVolume(worldKey, gomock.Any())
	mockStore.EXPECT().GetVolume(destKey).Return(destVolume, nil)
	mockStore.EXPECT().PutVolume(destKey, gomock.Any())
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(1))
	mockStore.EXPECT().AddTransactionUpdate(dal.TransactionKey{LedgerID: 1, ID: 1}, gomock.Any())

	request := &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: "test-ledger",
				Data: &servicepb.LedgerApplyRequest_CreateTransaction{
					CreateTransaction: &servicepb.CreateTransactionPayload{
						Postings: []*commonpb.Posting{
							{
								Source:      "world", // Can go negative
								Destination: "users:123",
								Amount:      commonpb.NewUint256FromUint64(1000),
								Asset:       "USD",
							},
						},
					},
				},
			},
		},
	}

	result, err := processor.ProcessOrder(requestToOrder(request), mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)
}

func TestProcessApply_LedgerNotFound(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	mockStore.EXPECT().GetBoundaries("nonexistent").Return(nil, false)

	request := &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: "nonexistent",
				Data: &servicepb.LedgerApplyRequest_AddMetadata{
					AddMetadata: &commonpb.SaveMetadataCommand{
						Target: &commonpb.Target{
							Target: &commonpb.Target_Account{
								Account: &commonpb.TargetAccount{Addr: "test"},
							},
						},
						Metadata: &commonpb.MetadataSet{},
					},
				},
			},
		},
	}

	result, err := processor.ProcessOrder(requestToOrder(request), mockStore)
	require.Error(t, err)
	require.Nil(t, result)
	require.Contains(t, err.Error(), "ledger does not exist")
}

// setupNumscriptVolumeMocks sets up the common volume mock expectations for Numscript tests.
// All Numscript tests use flexible (AnyTimes) volume mocking with zero balances.
func setupNumscriptVolumeMocks(mockStore *MockInMemoryStore) {
	mockStore.EXPECT().GetVolume(gomock.Any()).Return(&raftcmdpb.VolumePair{
		InputKnown:  commonpb.NewUint256FromUint64(0),
		OutputKnown: commonpb.NewUint256FromUint64(0),
	}, nil).AnyTimes()
	mockStore.EXPECT().PutVolume(gomock.Any(), gomock.Any()).AnyTimes()
}

func TestProcessCreateTransaction_Numscript_WorldSource(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1, LedgerId: 1}

	// Use flexible mocking for volume operations
	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries, true)
	mockStore.EXPECT().GetLedger("test-ledger").Return(nil, false)
	mockStore.EXPECT().GetDate().Return(now).AnyTimes()
	mockStore.EXPECT().GetCurrentOpenPeriod().Return(nil, false)
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())
	setupNumscriptVolumeMocks(mockStore)
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(1))
	mockStore.EXPECT().AddTransactionUpdate(dal.TransactionKey{LedgerID: 1, ID: 1}, gomock.Any())

	request := &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: "test-ledger",
				Data: &servicepb.LedgerApplyRequest_CreateTransaction{
					CreateTransaction: &servicepb.CreateTransactionPayload{
						Script: &commonpb.Script{
							Plain: `
								send [USD/2 10000] (
									source = @world
									destination = @users:alice
								)
							`,
						},
					},
				},
			},
		},
	}

	result, err := processor.ProcessOrder(requestToOrder(request), mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	applyLog := result.GetApply()
	require.NotNil(t, applyLog)

	createdTx := applyLog.Log.Data.GetCreatedTransaction()
	require.NotNil(t, createdTx)
	require.Equal(t, uint64(1), createdTx.Transaction.Id)
	require.Len(t, createdTx.Transaction.Postings, 1)

	posting := createdTx.Transaction.Postings[0]
	require.Equal(t, "world", posting.Source)
	require.Equal(t, "users:alice", posting.Destination)
	require.Equal(t, int64(10000), posting.Amount.ToBigInt().Int64())
	require.Equal(t, "USD/2", posting.Asset)
}

func TestProcessCreateTransaction_Numscript_WithVariables(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1, LedgerId: 1}

	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries, true)
	mockStore.EXPECT().GetLedger("test-ledger").Return(nil, false)
	mockStore.EXPECT().GetDate().Return(now).AnyTimes()
	mockStore.EXPECT().GetCurrentOpenPeriod().Return(nil, false)
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())
	setupNumscriptVolumeMocks(mockStore)
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(1))
	mockStore.EXPECT().AddTransactionUpdate(dal.TransactionKey{LedgerID: 1, ID: 1}, gomock.Any())

	request := &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: "test-ledger",
				Data: &servicepb.LedgerApplyRequest_CreateTransaction{
					CreateTransaction: &servicepb.CreateTransactionPayload{
						Script: &commonpb.Script{
							Plain: `
								vars {
									monetary $amount
									account $destination
								}
								send $amount (
									source = @world
									destination = $destination
								)
							`,
							Vars: map[string]string{
								"amount":      "EUR/2 5000",
								"destination": "merchants:shop1",
							},
						},
					},
				},
			},
		},
	}

	result, err := processor.ProcessOrder(requestToOrder(request), mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	applyLog := result.GetApply()
	require.NotNil(t, applyLog)

	createdTx := applyLog.Log.Data.GetCreatedTransaction()
	require.NotNil(t, createdTx)
	require.Len(t, createdTx.Transaction.Postings, 1)

	posting := createdTx.Transaction.Postings[0]
	require.Equal(t, "world", posting.Source)
	require.Equal(t, "merchants:shop1", posting.Destination)
	require.Equal(t, int64(5000), posting.Amount.ToBigInt().Int64())
	require.Equal(t, "EUR/2", posting.Asset)
}

func TestProcessCreateTransaction_Numscript_MultiplePostings(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1, LedgerId: 1}

	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries, true)
	mockStore.EXPECT().GetLedger("test-ledger").Return(nil, false)
	mockStore.EXPECT().GetDate().Return(now).AnyTimes()
	mockStore.EXPECT().GetCurrentOpenPeriod().Return(nil, false)
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())
	setupNumscriptVolumeMocks(mockStore)
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(1))
	mockStore.EXPECT().AddTransactionUpdate(dal.TransactionKey{LedgerID: 1, ID: 1}, gomock.Any())

	request := &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: "test-ledger",
				Data: &servicepb.LedgerApplyRequest_CreateTransaction{
					CreateTransaction: &servicepb.CreateTransactionPayload{
						Script: &commonpb.Script{
							Plain: `
								send [USD/2 10000] (
									source = @world
									destination = @users:alice
								)
								send [USD/2 5000] (
									source = @world
									destination = @users:bob
								)
							`,
						},
					},
				},
			},
		},
	}

	result, err := processor.ProcessOrder(requestToOrder(request), mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	applyLog := result.GetApply()
	require.NotNil(t, applyLog)

	createdTx := applyLog.Log.Data.GetCreatedTransaction()
	require.NotNil(t, createdTx)
	require.Len(t, createdTx.Transaction.Postings, 2)

	// Verify first posting
	require.Equal(t, "world", createdTx.Transaction.Postings[0].Source)
	require.Equal(t, "users:alice", createdTx.Transaction.Postings[0].Destination)
	require.Equal(t, int64(10000), createdTx.Transaction.Postings[0].Amount.ToBigInt().Int64())

	// Verify second posting
	require.Equal(t, "world", createdTx.Transaction.Postings[1].Source)
	require.Equal(t, "users:bob", createdTx.Transaction.Postings[1].Destination)
	require.Equal(t, int64(5000), createdTx.Transaction.Postings[1].Amount.ToBigInt().Int64())
}

func TestProcessCreateTransaction_Numscript_UnboundedOverdraft(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1, LedgerId: 1}

	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries, true)
	mockStore.EXPECT().GetLedger("test-ledger").Return(nil, false)
	mockStore.EXPECT().GetDate().Return(now).AnyTimes()
	mockStore.EXPECT().GetCurrentOpenPeriod().Return(nil, false)
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())
	// Bank starts with 0 balance but can go negative with unbounded overdraft
	setupNumscriptVolumeMocks(mockStore)
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(1))
	mockStore.EXPECT().AddTransactionUpdate(dal.TransactionKey{LedgerID: 1, ID: 1}, gomock.Any())

	request := &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: "test-ledger",
				Data: &servicepb.LedgerApplyRequest_CreateTransaction{
					CreateTransaction: &servicepb.CreateTransactionPayload{
						Script: &commonpb.Script{
							Plain: `
								send [USD/2 100000] (
									source = @bank:main allowing unbounded overdraft
									destination = @users:alice
								)
							`,
						},
					},
				},
			},
		},
	}

	result, err := processor.ProcessOrder(requestToOrder(request), mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	applyLog := result.GetApply()
	require.NotNil(t, applyLog)

	createdTx := applyLog.Log.Data.GetCreatedTransaction()
	require.NotNil(t, createdTx)
	require.Len(t, createdTx.Transaction.Postings, 1)

	posting := createdTx.Transaction.Postings[0]
	require.Equal(t, "bank:main", posting.Source)
	require.Equal(t, "users:alice", posting.Destination)
	require.Equal(t, int64(100000), posting.Amount.ToBigInt().Int64())
}

func TestProcessCreateTransaction_Numscript_ParseError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1, LedgerId: 1}

	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries, true)

	request := &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: "test-ledger",
				Data: &servicepb.LedgerApplyRequest_CreateTransaction{
					CreateTransaction: &servicepb.CreateTransactionPayload{
						Script: &commonpb.Script{
							Plain: `
								send [USD/2 invalid] (
									source = @world
									destination = @users:alice
								)
							`,
						},
					},
				},
			},
		},
	}

	result, err := processor.ProcessOrder(requestToOrder(request), mockStore)
	require.Error(t, err)
	require.Nil(t, result)
	require.Contains(t, err.Error(), "numscript parse error")
}

func TestProcessCreateTransaction_Numscript_EmptyScript(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1, LedgerId: 1}

	// Note: When Script.Plain is empty, the processor falls back to stdPostingProducer
	// which creates an empty transaction. This test verifies that behavior.
	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries, true)
	mockStore.EXPECT().GetLedger("test-ledger").Return(nil, false)
	mockStore.EXPECT().GetDate().Return(now).AnyTimes()
	mockStore.EXPECT().GetCurrentOpenPeriod().Return(nil, false)
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(1))
	mockStore.EXPECT().AddTransactionUpdate(dal.TransactionKey{LedgerID: 1, ID: 1}, gomock.Any())

	request := &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: "test-ledger",
				Data: &servicepb.LedgerApplyRequest_CreateTransaction{
					CreateTransaction: &servicepb.CreateTransactionPayload{
						Script: &commonpb.Script{
							Plain: "",
						},
					},
				},
			},
		},
	}

	// Empty script falls back to standard posting producer, creating an empty transaction
	result, err := processor.ProcessOrder(requestToOrder(request), mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	applyLog := result.GetApply()
	require.NotNil(t, applyLog)

	createdTx := applyLog.Log.Data.GetCreatedTransaction()
	require.NotNil(t, createdTx)
	require.Len(t, createdTx.Transaction.Postings, 0) // Empty transaction
}

func TestProcessCreateTransaction_Numscript_SendToMultipleDestinations(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1, LedgerId: 1}

	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries, true)
	mockStore.EXPECT().GetLedger("test-ledger").Return(nil, false)
	mockStore.EXPECT().GetDate().Return(now).AnyTimes()
	mockStore.EXPECT().GetCurrentOpenPeriod().Return(nil, false)
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())
	setupNumscriptVolumeMocks(mockStore)
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(1))
	mockStore.EXPECT().AddTransactionUpdate(dal.TransactionKey{LedgerID: 1, ID: 1}, gomock.Any())

	// Test allotment to multiple destinations
	request := &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: "test-ledger",
				Data: &servicepb.LedgerApplyRequest_CreateTransaction{
					CreateTransaction: &servicepb.CreateTransactionPayload{
						Script: &commonpb.Script{
							Plain: `
								send [USD/2 10000] (
									source = @world
									destination = {
										1/2 to @users:alice
										1/2 to @users:bob
									}
								)
							`,
						},
					},
				},
			},
		},
	}

	result, err := processor.ProcessOrder(requestToOrder(request), mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	applyLog := result.GetApply()
	require.NotNil(t, applyLog)

	createdTx := applyLog.Log.Data.GetCreatedTransaction()
	require.NotNil(t, createdTx)
	require.Len(t, createdTx.Transaction.Postings, 2)

	// Should split 10000 into 5000 each
	require.Equal(t, "world", createdTx.Transaction.Postings[0].Source)
	require.Equal(t, "users:alice", createdTx.Transaction.Postings[0].Destination)
	require.Equal(t, int64(5000), createdTx.Transaction.Postings[0].Amount.ToBigInt().Int64())

	require.Equal(t, "world", createdTx.Transaction.Postings[1].Source)
	require.Equal(t, "users:bob", createdTx.Transaction.Postings[1].Destination)
	require.Equal(t, int64(5000), createdTx.Transaction.Postings[1].Amount.ToBigInt().Int64())
}

func TestProcessCreateTransaction_Numscript_SetTxMeta(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1, LedgerId: 1}

	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries, true)
	mockStore.EXPECT().GetLedger("test-ledger").Return(nil, false)
	mockStore.EXPECT().GetDate().Return(now).AnyTimes()
	mockStore.EXPECT().GetCurrentOpenPeriod().Return(nil, false)
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())
	setupNumscriptVolumeMocks(mockStore)
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(1))
	mockStore.EXPECT().AddTransactionUpdate(dal.TransactionKey{LedgerID: 1, ID: 1}, gomock.Any())

	// Test set_tx_meta
	request := &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: "test-ledger",
				Data: &servicepb.LedgerApplyRequest_CreateTransaction{
					CreateTransaction: &servicepb.CreateTransactionPayload{
						Script: &commonpb.Script{
							Plain: `
								set_tx_meta("type", "payment")
								set_tx_meta("category", "purchase")
								send [USD/2 100] (
									source = @world
									destination = @users:alice
								)
							`,
						},
					},
				},
			},
		},
	}

	result, err := processor.ProcessOrder(requestToOrder(request), mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	applyLog := result.GetApply()
	require.NotNil(t, applyLog)

	createdTx := applyLog.Log.Data.GetCreatedTransaction()
	require.NotNil(t, createdTx)
	require.NotNil(t, createdTx.Transaction.Metadata)

	// Verify metadata was set
	metaMap := createdTx.Transaction.Metadata.ToMap()
	require.Equal(t, "payment", metaMap["type"])
	require.Equal(t, "purchase", metaMap["category"])
}

func TestProcessCreateTransaction_Numscript_SetAccountMeta(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1, LedgerId: 1}

	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries, true)
	mockStore.EXPECT().GetLedger("test-ledger").Return(nil, false)
	mockStore.EXPECT().GetDate().Return(now).AnyTimes()
	mockStore.EXPECT().GetCurrentOpenPeriod().Return(nil, false)
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())
	setupNumscriptVolumeMocks(mockStore)
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(1))
	mockStore.EXPECT().AddTransactionUpdate(dal.TransactionKey{LedgerID: 1, ID: 1}, gomock.Any())

	// Expect account metadata to be set (called from numscript adapter + re-pushed after enforceSchema)
	mockStore.EXPECT().PutAccountMetadata(
		dal.MetadataKey{
			AccountKey: dal.AccountKey{LedgerID: 1, Account: "users:alice"},
			Key:        "account_type",
		},
		commonpb.NewStringValue("savings"),
	).Times(2)
	mockStore.EXPECT().PutAccountMetadata(
		dal.MetadataKey{
			AccountKey: dal.AccountKey{LedgerID: 1, Account: "users:alice"},
			Key:        "created_by",
		},
		commonpb.NewStringValue("numscript"),
	).Times(2)

	// Test set_account_meta
	request := &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: "test-ledger",
				Data: &servicepb.LedgerApplyRequest_CreateTransaction{
					CreateTransaction: &servicepb.CreateTransactionPayload{
						Script: &commonpb.Script{
							Plain: `
								set_account_meta(@users:alice, "account_type", "savings")
								set_account_meta(@users:alice, "created_by", "numscript")
								send [USD/2 100] (
									source = @world
									destination = @users:alice
								)
							`,
						},
					},
				},
			},
		},
	}

	result, err := processor.ProcessOrder(requestToOrder(request), mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	applyLog := result.GetApply()
	require.NotNil(t, applyLog)

	createdTx := applyLog.Log.Data.GetCreatedTransaction()
	require.NotNil(t, createdTx)
	require.Len(t, createdTx.Transaction.Postings, 1)

	// Verify account metadata was returned in the log payload
	require.NotNil(t, createdTx.AccountMetadata, "AccountMetadata should not be nil")
	require.Contains(t, createdTx.AccountMetadata, "users:alice", "AccountMetadata should contain users:alice")
	aliceMeta := createdTx.AccountMetadata["users:alice"]
	require.NotNil(t, aliceMeta)
	metaMap := aliceMeta.ToMap()
	require.Equal(t, "savings", metaMap["account_type"])
	require.Equal(t, "numscript", metaMap["created_by"])
}

func TestProcessCreateTransaction_Force_InsufficientFunds(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1, LedgerId: 1}

	sourceKey := dal.VolumeKey{
		AccountKey: dal.AccountKey{LedgerID: 1, Account: "users:123"},
		Asset:      "USD",
	}
	destKey := dal.VolumeKey{
		AccountKey: dal.AccountKey{LedgerID: 1, Account: "merchant"},
		Asset:      "USD",
	}

	// Source has only 50 balance (100 input - 50 output) - not enough for 100
	sourceVolume := &raftcmdpb.VolumePair{
		InputKnown:  commonpb.NewUint256FromUint64(100),
		OutputKnown: commonpb.NewUint256FromUint64(50),
	}
	destVolume := &raftcmdpb.VolumePair{
		InputKnown:  commonpb.NewUint256FromUint64(0),
		OutputKnown: commonpb.NewUint256FromUint64(0),
	}

	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries, true)
	mockStore.EXPECT().GetLedger("test-ledger").Return(nil, false)
	mockStore.EXPECT().GetDate().Return(now).Times(4)
	mockStore.EXPECT().GetCurrentOpenPeriod().Return(nil, false)
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())
	mockStore.EXPECT().GetVolume(sourceKey).Return(sourceVolume, nil)
	// With force=true, balance check is skipped and output is updated
	mockStore.EXPECT().PutVolume(sourceKey, gomock.Any()).Do(
		func(key dal.VolumeKey, value *raftcmdpb.VolumePair) {
			// Output should increase by 100 (50 + 100 = 150)
			require.Equal(t, int64(150), value.OutputKnown.ToBigInt().Int64())
		},
	)
	mockStore.EXPECT().GetVolume(destKey).Return(destVolume, nil)
	mockStore.EXPECT().PutVolume(destKey, gomock.Any()).Do(
		func(key dal.VolumeKey, value *raftcmdpb.VolumePair) {
			// Input should increase by 100
			require.Equal(t, int64(100), value.InputKnown.ToBigInt().Int64())
		},
	)
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(1))
	mockStore.EXPECT().AddTransactionUpdate(dal.TransactionKey{LedgerID: 1, ID: 1}, gomock.Any())

	request := &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: "test-ledger",
				Data: &servicepb.LedgerApplyRequest_CreateTransaction{
					CreateTransaction: &servicepb.CreateTransactionPayload{
						Force: true, // Force flag bypasses balance check
						Postings: []*commonpb.Posting{
							{
								Source:      "users:123",
								Destination: "merchant",
								Amount:      commonpb.NewUint256FromUint64(100), // Wants 100, has only 50
								Asset:       "USD",
							},
						},
					},
				},
			},
		},
	}

	// With force=true, the transaction should succeed despite insufficient funds
	result, err := processor.ProcessOrder(requestToOrder(request), mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	applyLog := result.GetApply()
	require.NotNil(t, applyLog)

	createdTx := applyLog.Log.Data.GetCreatedTransaction()
	require.NotNil(t, createdTx)
	require.Equal(t, uint64(1), createdTx.Transaction.Id)
}

func TestProcessCreateTransaction_Force_ZeroBalance(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1, LedgerId: 1}

	sourceKey := dal.VolumeKey{
		AccountKey: dal.AccountKey{LedgerID: 1, Account: "users:new"},
		Asset:      "USD",
	}
	destKey := dal.VolumeKey{
		AccountKey: dal.AccountKey{LedgerID: 1, Account: "merchant"},
		Asset:      "USD",
	}

	// Source has no volume at all (nil) - would fail balance check normally
	// With force=true, this should succeed

	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries, true)
	mockStore.EXPECT().GetLedger("test-ledger").Return(nil, false)
	mockStore.EXPECT().GetDate().Return(now).Times(4)
	mockStore.EXPECT().GetCurrentOpenPeriod().Return(nil, false)
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())
	// Source returns ErrNotFound (no volume)
	mockStore.EXPECT().GetVolume(sourceKey).Return(nil, dal.ErrNotFound)
	mockStore.EXPECT().PutVolume(sourceKey, gomock.Any()).Do(
		func(key dal.VolumeKey, value *raftcmdpb.VolumePair) {
			// Output should use OutputDiff since we don't know the absolute value
			require.Nil(t, value.OutputKnown)
			require.Equal(t, int64(100), value.OutputDiff.ToBigInt().Int64())
		},
	)
	mockStore.EXPECT().GetVolume(destKey).Return(nil, dal.ErrNotFound)
	mockStore.EXPECT().PutVolume(destKey, gomock.Any()).Do(
		func(key dal.VolumeKey, value *raftcmdpb.VolumePair) {
			// Input should use InputDiff since we don't know the absolute value
			require.Nil(t, value.InputKnown)
			require.Equal(t, int64(100), value.InputDiff.ToBigInt().Int64())
		},
	)
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(1))
	mockStore.EXPECT().AddTransactionUpdate(dal.TransactionKey{LedgerID: 1, ID: 1}, gomock.Any())

	request := &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: "test-ledger",
				Data: &servicepb.LedgerApplyRequest_CreateTransaction{
					CreateTransaction: &servicepb.CreateTransactionPayload{
						Force: true,
						Postings: []*commonpb.Posting{
							{
								Source:      "users:new",
								Destination: "merchant",
								Amount:      commonpb.NewUint256FromUint64(100),
								Asset:       "USD",
							},
						},
					},
				},
			},
		},
	}

	result, err := processor.ProcessOrder(requestToOrder(request), mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)
}

func TestProcessCreateTransaction_Numscript_Force_InsufficientFunds(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1, LedgerId: 1}

	// Account has 0 balance, but with force=true, Numscript should see unlimited balance
	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries, true)
	mockStore.EXPECT().GetLedger("test-ledger").Return(nil, false)
	mockStore.EXPECT().GetDate().Return(now).AnyTimes()
	mockStore.EXPECT().GetCurrentOpenPeriod().Return(nil, false)
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())
	// Note: GetVolume might be called for volume updates but not for balance queries
	// when force=true (store adapter returns unlimited balance)
	setupNumscriptVolumeMocks(mockStore)
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(1))
	mockStore.EXPECT().AddTransactionUpdate(dal.TransactionKey{LedgerID: 1, ID: 1}, gomock.Any())

	request := &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: "test-ledger",
				Data: &servicepb.LedgerApplyRequest_CreateTransaction{
					CreateTransaction: &servicepb.CreateTransactionPayload{
						Force: true, // Force bypasses balance checks in Numscript
						Script: &commonpb.Script{
							Plain: `
								send [USD/2 100000] (
									source = @users:broke
									destination = @users:alice
								)
							`,
						},
					},
				},
			},
		},
	}

	// With force=true, Numscript should succeed even though users:broke has 0 balance
	result, err := processor.ProcessOrder(requestToOrder(request), mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	applyLog := result.GetApply()
	require.NotNil(t, applyLog)

	createdTx := applyLog.Log.Data.GetCreatedTransaction()
	require.NotNil(t, createdTx)
	require.Len(t, createdTx.Transaction.Postings, 1)

	posting := createdTx.Transaction.Postings[0]
	require.Equal(t, "users:broke", posting.Source)
	require.Equal(t, "users:alice", posting.Destination)
	require.Equal(t, int64(100000), posting.Amount.ToBigInt().Int64())
}

func TestProcessCreateTransaction_Numscript_OverflowUint256(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1, LedgerId: 1}

	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries, true)
	setupNumscriptVolumeMocks(mockStore)

	// 2^256 = 115792089237316195423570985008687907853269984665640564039457584007913129639936
	// This exceeds the uint256 max (2^256 - 1) and must be rejected.
	overflow256 := "115792089237316195423570985008687907853269984665640564039457584007913129639936"

	request := &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: "test-ledger",
				Data: &servicepb.LedgerApplyRequest_CreateTransaction{
					CreateTransaction: &servicepb.CreateTransactionPayload{
						Script: &commonpb.Script{
							Plain: `
								vars {
									monetary $amount
								}
								send $amount (
									source = @world
									destination = @users:alice
								)
							`,
							Vars: map[string]string{
								"amount": "USD/2 " + overflow256,
							},
						},
					},
				},
			},
		},
	}

	result, err := processor.ProcessOrder(requestToOrder(request), mockStore)
	require.Error(t, err)
	require.Nil(t, result)
	require.Contains(t, err.Error(), "exceeds 256 bits")
}

func TestProcessCreateTransaction_Numscript_NegativeAmount(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1, LedgerId: 1}

	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries, true)
	setupNumscriptVolumeMocks(mockStore)

	request := &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: "test-ledger",
				Data: &servicepb.LedgerApplyRequest_CreateTransaction{
					CreateTransaction: &servicepb.CreateTransactionPayload{
						Script: &commonpb.Script{
							Plain: `
								vars {
									monetary $amount
								}
								send $amount (
									source = @world
									destination = @users:alice
								)
							`,
							Vars: map[string]string{
								"amount": "USD/2 -100",
							},
						},
					},
				},
			},
		},
	}

	result, err := processor.ProcessOrder(requestToOrder(request), mockStore)
	require.Error(t, err)
	require.Nil(t, result)
}

func TestProcessCreateTransaction_PeriodIdInCreatedTransaction(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1, LedgerId: 1}

	sourceKey := dal.VolumeKey{
		AccountKey: dal.AccountKey{LedgerID: 1, Account: "world"},
		Asset:      "USD",
	}
	destKey := dal.VolumeKey{
		AccountKey: dal.AccountKey{LedgerID: 1, Account: "users:alice"},
		Asset:      "USD",
	}

	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries, true)
	mockStore.EXPECT().GetLedger("test-ledger").Return(nil, false)
	mockStore.EXPECT().GetVolume(sourceKey).Return(nil, dal.ErrNotFound)
	mockStore.EXPECT().PutVolume(sourceKey, gomock.Any())
	mockStore.EXPECT().GetVolume(destKey).Return(nil, dal.ErrNotFound)
	mockStore.EXPECT().PutVolume(destKey, gomock.Any())
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(10))
	mockStore.EXPECT().AddTransactionUpdate(dal.TransactionKey{LedgerID: 1, ID: 1}, gomock.Any())
	mockStore.EXPECT().GetDate().Return(now).Times(4)
	mockStore.EXPECT().GetCurrentOpenPeriod().Return(&commonpb.Period{Id: 5, Status: commonpb.PeriodStatus_PERIOD_OPEN}, true)
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())

	request := &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: "test-ledger",
				Data: &servicepb.LedgerApplyRequest_CreateTransaction{
					CreateTransaction: &servicepb.CreateTransactionPayload{
						Postings: []*commonpb.Posting{
							{
								Source:      "world",
								Destination: "users:alice",
								Amount:      commonpb.NewUint256FromUint64(uint64(100)),
								Asset:       "USD",
							},
						},
						Force: true,
					},
				},
			},
		},
	}

	result, err := processor.ProcessOrder(requestToOrder(request), mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	applyLog := result.GetApply()
	require.NotNil(t, applyLog)

	createdTx := applyLog.Log.Data.GetCreatedTransaction()
	require.NotNil(t, createdTx)
	require.Equal(t, uint64(5), createdTx.PeriodId, "PeriodId should match the open period")
	require.Equal(t, uint64(1), createdTx.Transaction.Id)
}

func TestProcessCreateTransaction_PeriodIdZeroWhenNoPeriod(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1, LedgerId: 1}

	sourceKey := dal.VolumeKey{
		AccountKey: dal.AccountKey{LedgerID: 1, Account: "world"},
		Asset:      "USD",
	}
	destKey := dal.VolumeKey{
		AccountKey: dal.AccountKey{LedgerID: 1, Account: "users:bob"},
		Asset:      "USD",
	}

	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries, true)
	mockStore.EXPECT().GetLedger("test-ledger").Return(nil, false)
	mockStore.EXPECT().GetVolume(sourceKey).Return(nil, dal.ErrNotFound)
	mockStore.EXPECT().PutVolume(sourceKey, gomock.Any())
	mockStore.EXPECT().GetVolume(destKey).Return(nil, dal.ErrNotFound)
	mockStore.EXPECT().PutVolume(destKey, gomock.Any())
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(10))
	mockStore.EXPECT().AddTransactionUpdate(dal.TransactionKey{LedgerID: 1, ID: 1}, gomock.Any())
	mockStore.EXPECT().GetDate().Return(now).Times(4)
	mockStore.EXPECT().GetCurrentOpenPeriod().Return(nil, false)
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())

	request := &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: "test-ledger",
				Data: &servicepb.LedgerApplyRequest_CreateTransaction{
					CreateTransaction: &servicepb.CreateTransactionPayload{
						Postings: []*commonpb.Posting{
							{
								Source:      "world",
								Destination: "users:bob",
								Amount:      commonpb.NewUint256FromUint64(uint64(50)),
								Asset:       "USD",
							},
						},
						Force: true,
					},
				},
			},
		},
	}

	result, err := processor.ProcessOrder(requestToOrder(request), mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	applyLog := result.GetApply()
	require.NotNil(t, applyLog)

	createdTx := applyLog.Log.Data.GetCreatedTransaction()
	require.NotNil(t, createdTx)
	require.Equal(t, uint64(0), createdTx.PeriodId, "PeriodId should be 0 when no open period exists")
}
