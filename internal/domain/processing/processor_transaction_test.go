package processing

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func TestProcessCreateTransaction(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}

	sourceKey := domain.NewVolumeKey("test-ledger", "bank", "USD")
	destKey := domain.NewVolumeKey("test-ledger", "users:123", "USD")

	// Source has 1000 input, 0 output -> balance = 1000
	sourceVolume := &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(1000),
		Output: commonpb.NewUint256FromUint64(0),
	}

	// Destination starts with 0
	destVolume := &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(0),
		Output: commonpb.NewUint256FromUint64(0),
	}

	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries.AsReader(), nil)
	mockStore.EXPECT().GetLedger("test-ledger").Return(&commonpb.LedgerInfo{Name: "test-ledger", Id: 1}, nil).AnyTimes()
	mockStore.EXPECT().GetDate().Return(now).Times(4) // Called for: ledger log date, timestamp fallback, InsertedAt, UpdatedAt
	mockStore.EXPECT().GetCurrentOpenPeriod().Return(nil, false)
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())
	mockStore.EXPECT().GetVolume(sourceKey).Return(sourceVolume.AsReader(), nil)
	mockStore.EXPECT().PutVolume(sourceKey, gomock.Any()).Do(
		func(key domain.VolumeKey, value *raftcmdpb.VolumePair) {
			// Output should increase by 100
			require.Equal(t, int64(100), value.GetOutput().ToBigInt().Int64())
		},
	)
	mockStore.EXPECT().GetVolume(destKey).Return(destVolume.AsReader(), nil)
	mockStore.EXPECT().PutVolume(destKey, gomock.Any()).Do(
		func(key domain.VolumeKey, value *raftcmdpb.VolumePair) {
			// Input should increase by 100
			require.Equal(t, int64(100), value.GetInput().ToBigInt().Int64())
		},
	)
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(1))
	mockStore.EXPECT().PutTransactionState(domain.TransactionKey{LedgerName: "test-ledger", ID: 1}, gomock.Any())

	request := &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: "test-ledger",
				Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
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
				}},
			},
		},
	}

	result, err := processor.ProcessOrder(requestToOrder(request), mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	applyLog := result.GetApply()
	require.NotNil(t, applyLog)

	createdTx := applyLog.GetLog().GetData().GetCreatedTransaction()
	require.NotNil(t, createdTx)
	require.Equal(t, uint64(1), createdTx.GetTransaction().GetId())
	require.Len(t, createdTx.GetTransaction().GetPostings(), 1)
}

func TestProcessCreateTransaction_InsufficientFunds(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}

	sourceKey := domain.NewVolumeKey("test-ledger", "users:123", "USD")

	// Source has only 50 balance (100 input - 50 output)
	sourceVolume := &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(100),
		Output: commonpb.NewUint256FromUint64(50),
	}

	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries.AsReader(), nil)
	mockStore.EXPECT().GetLedger("test-ledger").Return(&commonpb.LedgerInfo{Name: "test-ledger", Id: 1}, nil).AnyTimes()
	mockStore.EXPECT().GetVolume(sourceKey).Return(sourceVolume.AsReader(), nil)

	request := &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: "test-ledger",
				Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
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
				}},
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

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}

	worldKey := domain.NewVolumeKey("test-ledger", "world", "USD")
	destKey := domain.NewVolumeKey("test-ledger", "users:123", "USD")

	// World has negative balance (but "world" bypasses balance check)
	worldVolume := &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(0),
		Output: commonpb.NewUint256FromUint64(1000000),
	}
	destVolume := &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(0),
		Output: commonpb.NewUint256FromUint64(0),
	}

	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries.AsReader(), nil)
	mockStore.EXPECT().GetLedger("test-ledger").Return(&commonpb.LedgerInfo{Name: "test-ledger", Id: 1}, nil).AnyTimes()
	mockStore.EXPECT().GetDate().Return(now).Times(4)
	mockStore.EXPECT().GetCurrentOpenPeriod().Return(nil, false)
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())
	mockStore.EXPECT().GetVolume(worldKey).Return(worldVolume.AsReader(), nil)
	mockStore.EXPECT().PutVolume(worldKey, gomock.Any())
	mockStore.EXPECT().GetVolume(destKey).Return(destVolume.AsReader(), nil)
	mockStore.EXPECT().PutVolume(destKey, gomock.Any())
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(1))
	mockStore.EXPECT().PutTransactionState(domain.TransactionKey{LedgerName: "test-ledger", ID: 1}, gomock.Any())

	request := &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: "test-ledger",
				Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
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
				}},
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

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	mockStore.EXPECT().GetLedger("nonexistent").Return(nil, domain.ErrNotFound)
	mockStore.EXPECT().GetBoundaries("nonexistent").Return(nil, domain.ErrNotFound)

	request := &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: "nonexistent",
				Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_AddMetadata{
					AddMetadata: &commonpb.SaveMetadataCommand{
						Target: &commonpb.Target{
							Target: &commonpb.Target_Account{
								Account: &commonpb.TargetAccount{Addr: "test"},
							},
						},
						Metadata: map[string]*commonpb.MetadataValue{},
					},
				}},
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
func setupNumscriptVolumeMocks(mockStore *MockScope) {
	mockStore.EXPECT().GetVolume(gomock.Any()).Return((&raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(0),
		Output: commonpb.NewUint256FromUint64(0),
	}).AsReader(), nil).AnyTimes()
	mockStore.EXPECT().PutVolume(gomock.Any(), gomock.Any()).AnyTimes()
}

func TestProcessCreateTransaction_Numscript_WorldSource(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}

	// Use flexible mocking for volume operations
	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries.AsReader(), nil)
	mockStore.EXPECT().GetLedger("test-ledger").Return(&commonpb.LedgerInfo{Name: "test-ledger", Id: 1}, nil).AnyTimes()
	mockStore.EXPECT().GetDate().Return(now).AnyTimes()
	mockStore.EXPECT().GetCurrentOpenPeriod().Return(nil, false)
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())
	setupNumscriptVolumeMocks(mockStore)
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(1))
	mockStore.EXPECT().PutTransactionState(domain.TransactionKey{LedgerName: "test-ledger", ID: 1}, gomock.Any())

	request := &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: "test-ledger",
				Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
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
				}},
			},
		},
	}

	result, err := processor.ProcessOrder(requestToOrder(request), mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	applyLog := result.GetApply()
	require.NotNil(t, applyLog)

	createdTx := applyLog.GetLog().GetData().GetCreatedTransaction()
	require.NotNil(t, createdTx)
	require.Equal(t, uint64(1), createdTx.GetTransaction().GetId())
	require.Len(t, createdTx.GetTransaction().GetPostings(), 1)

	posting := createdTx.GetTransaction().GetPostings()[0]
	require.Equal(t, "world", posting.GetSource())
	require.Equal(t, "users:alice", posting.GetDestination())
	require.Equal(t, int64(10000), posting.GetAmount().ToBigInt().Int64())
	require.Equal(t, "USD/2", posting.GetAsset())
}

func TestProcessCreateTransaction_Numscript_WithVariables(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}

	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries.AsReader(), nil)
	mockStore.EXPECT().GetLedger("test-ledger").Return(&commonpb.LedgerInfo{Name: "test-ledger", Id: 1}, nil).AnyTimes()
	mockStore.EXPECT().GetDate().Return(now).AnyTimes()
	mockStore.EXPECT().GetCurrentOpenPeriod().Return(nil, false)
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())
	setupNumscriptVolumeMocks(mockStore)
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(1))
	mockStore.EXPECT().PutTransactionState(domain.TransactionKey{LedgerName: "test-ledger", ID: 1}, gomock.Any())

	request := &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: "test-ledger",
				Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
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
				}},
			},
		},
	}

	result, err := processor.ProcessOrder(requestToOrder(request), mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	applyLog := result.GetApply()
	require.NotNil(t, applyLog)

	createdTx := applyLog.GetLog().GetData().GetCreatedTransaction()
	require.NotNil(t, createdTx)
	require.Len(t, createdTx.GetTransaction().GetPostings(), 1)

	posting := createdTx.GetTransaction().GetPostings()[0]
	require.Equal(t, "world", posting.GetSource())
	require.Equal(t, "merchants:shop1", posting.GetDestination())
	require.Equal(t, int64(5000), posting.GetAmount().ToBigInt().Int64())
	require.Equal(t, "EUR/2", posting.GetAsset())
}

func TestProcessCreateTransaction_Numscript_MultiplePostings(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}

	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries.AsReader(), nil)
	mockStore.EXPECT().GetLedger("test-ledger").Return(&commonpb.LedgerInfo{Name: "test-ledger", Id: 1}, nil).AnyTimes()
	mockStore.EXPECT().GetDate().Return(now).AnyTimes()
	mockStore.EXPECT().GetCurrentOpenPeriod().Return(nil, false)
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())
	setupNumscriptVolumeMocks(mockStore)
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(1))
	mockStore.EXPECT().PutTransactionState(domain.TransactionKey{LedgerName: "test-ledger", ID: 1}, gomock.Any())

	request := &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: "test-ledger",
				Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
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
				}},
			},
		},
	}

	result, err := processor.ProcessOrder(requestToOrder(request), mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	applyLog := result.GetApply()
	require.NotNil(t, applyLog)

	createdTx := applyLog.GetLog().GetData().GetCreatedTransaction()
	require.NotNil(t, createdTx)
	require.Len(t, createdTx.GetTransaction().GetPostings(), 2)

	// Verify first posting
	require.Equal(t, "world", createdTx.GetTransaction().GetPostings()[0].GetSource())
	require.Equal(t, "users:alice", createdTx.GetTransaction().GetPostings()[0].GetDestination())
	require.Equal(t, int64(10000), createdTx.GetTransaction().GetPostings()[0].GetAmount().ToBigInt().Int64())

	// Verify second posting
	require.Equal(t, "world", createdTx.GetTransaction().GetPostings()[1].GetSource())
	require.Equal(t, "users:bob", createdTx.GetTransaction().GetPostings()[1].GetDestination())
	require.Equal(t, int64(5000), createdTx.GetTransaction().GetPostings()[1].GetAmount().ToBigInt().Int64())
}

func TestProcessCreateTransaction_Numscript_UnboundedOverdraft(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}

	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries.AsReader(), nil)
	mockStore.EXPECT().GetLedger("test-ledger").Return(&commonpb.LedgerInfo{Name: "test-ledger", Id: 1}, nil).AnyTimes()
	mockStore.EXPECT().GetDate().Return(now).AnyTimes()
	mockStore.EXPECT().GetCurrentOpenPeriod().Return(nil, false)
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())
	// Bank starts with 0 balance but can go negative with unbounded overdraft
	setupNumscriptVolumeMocks(mockStore)
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(1))
	mockStore.EXPECT().PutTransactionState(domain.TransactionKey{LedgerName: "test-ledger", ID: 1}, gomock.Any())

	request := &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: "test-ledger",
				Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
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
				}},
			},
		},
	}

	result, err := processor.ProcessOrder(requestToOrder(request), mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	applyLog := result.GetApply()
	require.NotNil(t, applyLog)

	createdTx := applyLog.GetLog().GetData().GetCreatedTransaction()
	require.NotNil(t, createdTx)
	require.Len(t, createdTx.GetTransaction().GetPostings(), 1)

	posting := createdTx.GetTransaction().GetPostings()[0]
	require.Equal(t, "bank:main", posting.GetSource())
	require.Equal(t, "users:alice", posting.GetDestination())
	require.Equal(t, int64(100000), posting.GetAmount().ToBigInt().Int64())
}

func TestProcessCreateTransaction_Numscript_ParseError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}

	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries.AsReader(), nil)
	mockStore.EXPECT().GetLedger("test-ledger").Return(&commonpb.LedgerInfo{Name: "test-ledger", Id: 1}, nil).AnyTimes()

	request := &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: "test-ledger",
				Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
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
				}},
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

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}

	// A payload with an empty Script.Plain and no postings used to silently
	// produce a zero-posting transaction (#452). The structural gate at
	// admission catches inputs with no content source; this FSM-side check
	// catches anything that slipped past it AND the symmetric case where a
	// numscript runs cleanly but emits no `send`. The processor rejects
	// before bumping boundaries.
	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries.AsReader(), nil)
	mockStore.EXPECT().GetLedger("test-ledger").Return(&commonpb.LedgerInfo{Name: "test-ledger", Id: 1}, nil).AnyTimes()

	request := &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: "test-ledger",
				Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
					CreateTransaction: &servicepb.CreateTransactionPayload{
						Script: &commonpb.Script{
							Plain: "",
						},
					},
				}},
			},
		},
	}

	result, err := processor.ProcessOrder(requestToOrder(request), mockStore)
	require.Nil(t, result)
	require.ErrorIs(t, err, domain.ErrEmptyTransaction)
}

// TestProcessCreateTransaction_Numscript_NoSendStillRejected exercises the
// post-producer guard via a numscript that parses and runs cleanly but emits
// no `send`. Without the FSM-side check, the producer returns an empty
// result and the transaction would be committed with zero postings (#452).
func TestProcessCreateTransaction_Numscript_NoSendStillRejected(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}

	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries.AsReader(), nil)
	mockStore.EXPECT().GetLedger("test-ledger").Return(&commonpb.LedgerInfo{Name: "test-ledger", Id: 1}, nil).AnyTimes()

	// A numscript that declares a variable but never emits a `send`.
	request := &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: "test-ledger",
				Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
					CreateTransaction: &servicepb.CreateTransactionPayload{
						Script: &commonpb.Script{
							Plain: "vars { account $a }",
							Vars:  map[string]string{"a": "users:alice"},
						},
					},
				}},
			},
		},
	}

	result, err := processor.ProcessOrder(requestToOrder(request), mockStore)
	require.Nil(t, result)
	require.ErrorIs(t, err, domain.ErrEmptyTransaction)
}

func TestProcessCreateTransaction_Numscript_SendToMultipleDestinations(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}

	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries.AsReader(), nil)
	mockStore.EXPECT().GetLedger("test-ledger").Return(&commonpb.LedgerInfo{Name: "test-ledger", Id: 1}, nil).AnyTimes()
	mockStore.EXPECT().GetDate().Return(now).AnyTimes()
	mockStore.EXPECT().GetCurrentOpenPeriod().Return(nil, false)
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())
	setupNumscriptVolumeMocks(mockStore)
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(1))
	mockStore.EXPECT().PutTransactionState(domain.TransactionKey{LedgerName: "test-ledger", ID: 1}, gomock.Any())

	// Test allotment to multiple destinations
	request := &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: "test-ledger",
				Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
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
				}},
			},
		},
	}

	result, err := processor.ProcessOrder(requestToOrder(request), mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	applyLog := result.GetApply()
	require.NotNil(t, applyLog)

	createdTx := applyLog.GetLog().GetData().GetCreatedTransaction()
	require.NotNil(t, createdTx)
	require.Len(t, createdTx.GetTransaction().GetPostings(), 2)

	// Should split 10000 into 5000 each
	require.Equal(t, "world", createdTx.GetTransaction().GetPostings()[0].GetSource())
	require.Equal(t, "users:alice", createdTx.GetTransaction().GetPostings()[0].GetDestination())
	require.Equal(t, int64(5000), createdTx.GetTransaction().GetPostings()[0].GetAmount().ToBigInt().Int64())

	require.Equal(t, "world", createdTx.GetTransaction().GetPostings()[1].GetSource())
	require.Equal(t, "users:bob", createdTx.GetTransaction().GetPostings()[1].GetDestination())
	require.Equal(t, int64(5000), createdTx.GetTransaction().GetPostings()[1].GetAmount().ToBigInt().Int64())
}

func TestProcessCreateTransaction_Numscript_SetTxMeta(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}

	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries.AsReader(), nil)
	mockStore.EXPECT().GetLedger("test-ledger").Return(&commonpb.LedgerInfo{Name: "test-ledger", Id: 1}, nil).AnyTimes()
	mockStore.EXPECT().GetDate().Return(now).AnyTimes()
	mockStore.EXPECT().GetCurrentOpenPeriod().Return(nil, false)
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())
	setupNumscriptVolumeMocks(mockStore)
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(1))
	mockStore.EXPECT().PutTransactionState(domain.TransactionKey{LedgerName: "test-ledger", ID: 1}, gomock.Any())

	// Test set_tx_meta
	request := &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: "test-ledger",
				Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
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
				}},
			},
		},
	}

	result, err := processor.ProcessOrder(requestToOrder(request), mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	applyLog := result.GetApply()
	require.NotNil(t, applyLog)

	createdTx := applyLog.GetLog().GetData().GetCreatedTransaction()
	require.NotNil(t, createdTx)
	require.NotNil(t, createdTx.GetTransaction().GetMetadata())

	// Verify metadata was set
	metaMap := commonpb.MetadataToGoMap(createdTx.GetTransaction().GetMetadata())
	require.Equal(t, "payment", metaMap["type"])
	require.Equal(t, "purchase", metaMap["category"])
}

// TestProcessCreateTransaction_Numscript_RejectsEmptyMetadataKey pins
// the fix for #322 (second prong): Numscript-produced metadata keys
// never passed through admission's ValidateMetadataKey, so a script
// could write empty / NUL-bearing keys straight into the canonical
// Pebble layout. We now revalidate after Numscript resolution, mirroring
// the post-resolution address validation.
func TestProcessCreateTransaction_Numscript_RejectsEmptyMetadataKey(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}

	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries.AsReader(), nil).AnyTimes()
	mockStore.EXPECT().GetLedger("test-ledger").Return(&commonpb.LedgerInfo{Name: "test-ledger", Id: 1}, nil).AnyTimes()
	mockStore.EXPECT().GetDate().Return(now).AnyTimes()
	mockStore.EXPECT().GetCurrentOpenPeriod().Return(nil, false).AnyTimes()
	setupNumscriptVolumeMocks(mockStore)

	request := &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: "test-ledger",
				Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
					CreateTransaction: &servicepb.CreateTransactionPayload{
						Script: &commonpb.Script{
							Plain: `
								set_tx_meta("", "ghost")
								send [USD/2 100] (
									source = @world
									destination = @users:alice
								)
							`,
						},
					},
				}},
			},
		},
	}

	_, err = processor.ProcessOrder(requestToOrder(request), mockStore)
	require.Error(t, err)
	require.ErrorIs(t, err, domain.ErrMetadataKeyEmpty,
		"empty metadata key produced by Numscript must surface ErrMetadataKeyEmpty (#322)")
}

func TestProcessCreateTransaction_Numscript_RejectsNullByteMetadataValue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		script string
	}{
		{
			name: "transaction metadata",
			script: `
				vars {
					string $poison
				}
				set_tx_meta("kind", $poison)
				send [USD/2 100] (
					source = @world
					destination = @users:alice
				)
			`,
		},
		{
			name: "account metadata",
			script: `
				vars {
					string $poison
				}
				set_account_meta(@users:alice, "role", $poison)
				send [USD/2 100] (
					source = @world
					destination = @users:alice
				)
			`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockStore := NewMockScope(ctrl)
			processor, err := NewRequestProcessor(nil, 0)
			require.NoError(t, err)

			now := &commonpb.Timestamp{Data: 1234567890}
			boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}

			mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries.AsReader(), nil).AnyTimes()
			mockStore.EXPECT().GetLedger("test-ledger").Return(&commonpb.LedgerInfo{Name: "test-ledger", Id: 1}, nil).AnyTimes()
			mockStore.EXPECT().GetDate().Return(now).AnyTimes()
			mockStore.EXPECT().GetCurrentOpenPeriod().Return(nil, false).AnyTimes()
			setupNumscriptVolumeMocks(mockStore)

			request := &servicepb.Request{
				Type: &servicepb.Request_Apply{
					Apply: &servicepb.LedgerApplyRequest{
						Ledger: "test-ledger",
						Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
							CreateTransaction: &servicepb.CreateTransactionPayload{
								Script: &commonpb.Script{
									Plain: tt.script,
									Vars: map[string]string{
										"poison": "safe\x00poison",
									},
								},
							},
						}},
					},
				},
			}

			_, err = processor.ProcessOrder(requestToOrder(request), mockStore)
			require.Error(t, err)
			require.ErrorIs(t, err, domain.ErrMetadataValueContainsNullByte)
		})
	}
}

func TestProcessCreateTransaction_Numscript_SetAccountMeta(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}

	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries.AsReader(), nil)
	mockStore.EXPECT().GetLedger("test-ledger").Return(&commonpb.LedgerInfo{Name: "test-ledger", Id: 1}, nil).AnyTimes()
	mockStore.EXPECT().GetDate().Return(now).AnyTimes()
	mockStore.EXPECT().GetCurrentOpenPeriod().Return(nil, false)
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())
	setupNumscriptVolumeMocks(mockStore)
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(1))
	mockStore.EXPECT().PutTransactionState(domain.TransactionKey{LedgerName: "test-ledger", ID: 1}, gomock.Any())

	acctTypeKey := domain.MetadataKey{
		AccountKey: domain.AccountKey{LedgerName: "test-ledger", Account: "users:alice"},
		Key:        "account_type",
	}
	createdByKey := domain.MetadataKey{
		AccountKey: domain.AccountKey{LedgerName: "test-ledger", Account: "users:alice"},
		Key:        "created_by",
	}

	// After #186: PutAccountMetadata is only called once per key, by the
	// caller (processCreateTransaction), so the previous value capture sees
	// the real prior state. The numscript adapter no longer pre-writes.
	mockStore.EXPECT().PutAccountMetadata(acctTypeKey, commonpb.NewStringValue("savings"))
	mockStore.EXPECT().PutAccountMetadata(createdByKey, commonpb.NewStringValue("numscript"))
	// New account → no prior metadata; GetAccountMetadata returns ErrNotFound.
	mockStore.EXPECT().GetAccountMetadata(acctTypeKey).Return(nil, domain.ErrNotFound)
	mockStore.EXPECT().GetAccountMetadata(createdByKey).Return(nil, domain.ErrNotFound)

	// Test set_account_meta
	request := &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: "test-ledger",
				Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
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
				}},
			},
		},
	}

	result, err := processor.ProcessOrder(requestToOrder(request), mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	applyLog := result.GetApply()
	require.NotNil(t, applyLog)

	createdTx := applyLog.GetLog().GetData().GetCreatedTransaction()
	require.NotNil(t, createdTx)
	require.Len(t, createdTx.GetTransaction().GetPostings(), 1)

	// Verify account metadata was returned in the log payload
	require.NotNil(t, createdTx.GetAccountMetadata(), "AccountMetadata should not be nil")
	require.Contains(t, createdTx.GetAccountMetadata(), "users:alice", "AccountMetadata should contain users:alice")
	aliceMeta := createdTx.GetAccountMetadata()["users:alice"]
	require.NotNil(t, aliceMeta)
	metaMap := commonpb.MetadataMapToGoMap(aliceMeta)
	require.Equal(t, "savings", metaMap["account_type"])
	require.Equal(t, "numscript", metaMap["created_by"])

	// New account → log's PreviousAccountMetadata must be empty for these
	// keys, not equal to the new values (the #186 regression).
	prev := createdTx.GetPreviousAccountMetadata()["users:alice"]
	if prev != nil {
		require.NotContains(t, prev.GetValues(), "account_type",
			"PreviousAccountMetadata must not contain the newly-written key for a fresh account")
		require.NotContains(t, prev.GetValues(), "created_by",
			"PreviousAccountMetadata must not contain the newly-written key for a fresh account")
	}
}

// TestProcessCreateTransaction_Numscript_SetAccountMeta_OverwriteCapturesOldValue
// is the #186 regression. Before the fix, the Numscript adapter pre-wrote
// new metadata into the overlay, so by the time
// processCreateTransaction called GetAccountMetadata to capture the prior
// value, it observed the just-written new value. The log's
// PreviousAccountMetadata then matched the current metadata and the
// indexbuilder failed to remove stale index entries for value A.
func TestProcessCreateTransaction_Numscript_SetAccountMeta_OverwriteCapturesOldValue(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}

	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries.AsReader(), nil)
	mockStore.EXPECT().GetLedger("test-ledger").Return(&commonpb.LedgerInfo{Name: "test-ledger", Id: 1}, nil).AnyTimes()
	mockStore.EXPECT().GetDate().Return(now).AnyTimes()
	mockStore.EXPECT().GetCurrentOpenPeriod().Return(nil, false)
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())
	setupNumscriptVolumeMocks(mockStore)
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(1))
	mockStore.EXPECT().PutTransactionState(domain.TransactionKey{LedgerName: "test-ledger", ID: 1}, gomock.Any())

	roleKey := domain.MetadataKey{
		AccountKey: domain.AccountKey{LedgerName: "test-ledger", Account: "users:alice"},
		Key:        "role",
	}

	// Existing value "admin" must be observed at capture time.
	mockStore.EXPECT().GetAccountMetadata(roleKey).Return(commonpb.NewStringValue("admin"), nil)
	// Only one write — by processCreateTransaction itself, not by the
	// numscript adapter pre-writing.
	mockStore.EXPECT().PutAccountMetadata(roleKey, commonpb.NewStringValue("viewer"))

	request := &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: "test-ledger",
				Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
					CreateTransaction: &servicepb.CreateTransactionPayload{
						Script: &commonpb.Script{
							Plain: `
								set_account_meta(@users:alice, "role", "viewer")
								send [USD/2 100] (
									source = @world
									destination = @users:alice
								)
							`,
						},
					},
				}},
			},
		},
	}

	result, err := processor.ProcessOrder(requestToOrder(request), mockStore)
	require.NoError(t, err)
	createdTx := result.GetApply().GetLog().GetData().GetCreatedTransaction()
	require.NotNil(t, createdTx)

	// New metadata in the log.
	aliceMeta := createdTx.GetAccountMetadata()["users:alice"]
	require.NotNil(t, aliceMeta)
	require.Equal(t, "viewer", commonpb.MetadataMapToGoMap(aliceMeta)["role"])

	// The fix's specific claim: PreviousAccountMetadata holds "admin", not
	// "viewer". The indexbuilder reads this to remove the stale index entry
	// for "admin".
	prev := createdTx.GetPreviousAccountMetadata()["users:alice"]
	require.NotNil(t, prev, "PreviousAccountMetadata must capture the value before the script ran")
	prevValue := prev.GetValues()["role"]
	require.NotNil(t, prevValue)
	require.Equal(t, "admin", prevValue.GetStringValue(),
		"previous value must be the pre-script value, not the newly-written one")
}

func TestProcessCreateTransaction_Force_InsufficientFunds(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}

	sourceKey := domain.NewVolumeKey("test-ledger", "users:123", "USD")
	destKey := domain.NewVolumeKey("test-ledger", "merchant", "USD")

	// Source has only 50 balance (100 input - 50 output) - not enough for 100
	sourceVolume := &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(100),
		Output: commonpb.NewUint256FromUint64(50),
	}
	destVolume := &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(0),
		Output: commonpb.NewUint256FromUint64(0),
	}

	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries.AsReader(), nil)
	mockStore.EXPECT().GetLedger("test-ledger").Return(&commonpb.LedgerInfo{Name: "test-ledger", Id: 1}, nil).AnyTimes()
	mockStore.EXPECT().GetDate().Return(now).Times(4)
	mockStore.EXPECT().GetCurrentOpenPeriod().Return(nil, false)
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())
	mockStore.EXPECT().GetVolume(sourceKey).Return(sourceVolume.AsReader(), nil)
	// With force=true, balance check is skipped and output is updated
	mockStore.EXPECT().PutVolume(sourceKey, gomock.Any()).Do(
		func(key domain.VolumeKey, value *raftcmdpb.VolumePair) {
			// Output should increase by 100 (50 + 100 = 150)
			require.Equal(t, int64(150), value.GetOutput().ToBigInt().Int64())
		},
	)
	mockStore.EXPECT().GetVolume(destKey).Return(destVolume.AsReader(), nil)
	mockStore.EXPECT().PutVolume(destKey, gomock.Any()).Do(
		func(key domain.VolumeKey, value *raftcmdpb.VolumePair) {
			// Input should increase by 100
			require.Equal(t, int64(100), value.GetInput().ToBigInt().Int64())
		},
	)
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(1))
	mockStore.EXPECT().PutTransactionState(domain.TransactionKey{LedgerName: "test-ledger", ID: 1}, gomock.Any())

	request := &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: "test-ledger",
				Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
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
				}},
			},
		},
	}

	// With force=true, the transaction should succeed despite insufficient funds
	result, err := processor.ProcessOrder(requestToOrder(request), mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	applyLog := result.GetApply()
	require.NotNil(t, applyLog)

	createdTx := applyLog.GetLog().GetData().GetCreatedTransaction()
	require.NotNil(t, createdTx)
	require.Equal(t, uint64(1), createdTx.GetTransaction().GetId())
}

func TestProcessCreateTransaction_Force_ZeroBalance(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}

	sourceKey := domain.NewVolumeKey("test-ledger", "users:new", "USD")
	destKey := domain.NewVolumeKey("test-ledger", "merchant", "USD")

	// Source has zero balance, force=true skips balance check
	zeroVol := &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(0),
		Output: commonpb.NewUint256FromUint64(0),
	}

	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries.AsReader(), nil)
	mockStore.EXPECT().GetLedger("test-ledger").Return(&commonpb.LedgerInfo{Name: "test-ledger", Id: 1}, nil).AnyTimes()
	mockStore.EXPECT().GetDate().Return(now).Times(4)
	mockStore.EXPECT().GetCurrentOpenPeriod().Return(nil, false)
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())
	mockStore.EXPECT().GetVolume(sourceKey).Return(zeroVol.AsReader(), nil)
	mockStore.EXPECT().PutVolume(sourceKey, gomock.Any()).Do(
		func(key domain.VolumeKey, value *raftcmdpb.VolumePair) {
			require.Equal(t, int64(100), value.GetOutput().ToBigInt().Int64())
		},
	)
	mockStore.EXPECT().GetVolume(destKey).Return(zeroVol.AsReader(), nil)
	mockStore.EXPECT().PutVolume(destKey, gomock.Any()).Do(
		func(key domain.VolumeKey, value *raftcmdpb.VolumePair) {
			require.Equal(t, int64(100), value.GetInput().ToBigInt().Int64())
		},
	)
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(1))
	mockStore.EXPECT().PutTransactionState(domain.TransactionKey{LedgerName: "test-ledger", ID: 1}, gomock.Any())

	request := &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: "test-ledger",
				Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
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
				}},
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

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}

	// Account has 0 balance, but with force=true, Numscript should see unlimited balance
	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries.AsReader(), nil)
	mockStore.EXPECT().GetLedger("test-ledger").Return(&commonpb.LedgerInfo{Name: "test-ledger", Id: 1}, nil).AnyTimes()
	mockStore.EXPECT().GetDate().Return(now).AnyTimes()
	mockStore.EXPECT().GetCurrentOpenPeriod().Return(nil, false)
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())
	// Note: GetVolume might be called for volume updates but not for balance queries
	// when force=true (store adapter returns unlimited balance)
	setupNumscriptVolumeMocks(mockStore)
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(1))
	mockStore.EXPECT().PutTransactionState(domain.TransactionKey{LedgerName: "test-ledger", ID: 1}, gomock.Any())

	request := &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: "test-ledger",
				Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
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
				}},
			},
		},
	}

	// With force=true, Numscript should succeed even though users:broke has 0 balance
	result, err := processor.ProcessOrder(requestToOrder(request), mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	applyLog := result.GetApply()
	require.NotNil(t, applyLog)

	createdTx := applyLog.GetLog().GetData().GetCreatedTransaction()
	require.NotNil(t, createdTx)
	require.Len(t, createdTx.GetTransaction().GetPostings(), 1)

	posting := createdTx.GetTransaction().GetPostings()[0]
	require.Equal(t, "users:broke", posting.GetSource())
	require.Equal(t, "users:alice", posting.GetDestination())
	require.Equal(t, int64(100000), posting.GetAmount().ToBigInt().Int64())
}

func TestProcessCreateTransaction_Numscript_OverflowUint256(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}

	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries.AsReader(), nil)
	mockStore.EXPECT().GetLedger("test-ledger").Return(&commonpb.LedgerInfo{Name: "test-ledger", Id: 1}, nil).AnyTimes()
	setupNumscriptVolumeMocks(mockStore)

	// 2^256 = 115792089237316195423570985008687907853269984665640564039457584007913129639936
	// This exceeds the uint256 max (2^256 - 1) and must be rejected.
	overflow256 := "115792089237316195423570985008687907853269984665640564039457584007913129639936"

	request := &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: "test-ledger",
				Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
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
				}},
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

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}

	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries.AsReader(), nil)
	mockStore.EXPECT().GetLedger("test-ledger").Return(&commonpb.LedgerInfo{Name: "test-ledger", Id: 1}, nil).AnyTimes()
	setupNumscriptVolumeMocks(mockStore)

	request := &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: "test-ledger",
				Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
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
				}},
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

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}

	sourceKey := domain.NewVolumeKey("test-ledger", "world", "USD")
	destKey := domain.NewVolumeKey("test-ledger", "users:alice", "USD")

	zeroVol := &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(0),
		Output: commonpb.NewUint256FromUint64(0),
	}

	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries.AsReader(), nil)
	mockStore.EXPECT().GetLedger("test-ledger").Return(&commonpb.LedgerInfo{Name: "test-ledger", Id: 1}, nil).AnyTimes()
	mockStore.EXPECT().GetVolume(sourceKey).Return(zeroVol.AsReader(), nil)
	mockStore.EXPECT().PutVolume(sourceKey, gomock.Any())
	mockStore.EXPECT().GetVolume(destKey).Return(zeroVol.AsReader(), nil)
	mockStore.EXPECT().PutVolume(destKey, gomock.Any())
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(10))
	mockStore.EXPECT().PutTransactionState(domain.TransactionKey{LedgerName: "test-ledger", ID: 1}, gomock.Any())
	mockStore.EXPECT().GetDate().Return(now).Times(4)
	mockStore.EXPECT().GetCurrentOpenPeriod().Return(&commonpb.Period{Id: 5, Status: commonpb.PeriodStatus_PERIOD_OPEN}, true)
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())

	request := &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: "test-ledger",
				Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
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
				}},
			},
		},
	}

	result, err := processor.ProcessOrder(requestToOrder(request), mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	applyLog := result.GetApply()
	require.NotNil(t, applyLog)

	createdTx := applyLog.GetLog().GetData().GetCreatedTransaction()
	require.NotNil(t, createdTx)
	require.Equal(t, uint64(5), createdTx.GetPeriodId(), "PeriodId should match the open period")
	require.Equal(t, uint64(1), createdTx.GetTransaction().GetId())
}

func TestProcessCreateTransaction_PeriodIdZeroWhenNoPeriod(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}

	sourceKey := domain.NewVolumeKey("test-ledger", "world", "USD")
	destKey := domain.NewVolumeKey("test-ledger", "users:bob", "USD")

	zeroVol := &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(0),
		Output: commonpb.NewUint256FromUint64(0),
	}

	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries.AsReader(), nil)
	mockStore.EXPECT().GetLedger("test-ledger").Return(&commonpb.LedgerInfo{Name: "test-ledger", Id: 1}, nil).AnyTimes()
	mockStore.EXPECT().GetVolume(sourceKey).Return(zeroVol.AsReader(), nil)
	mockStore.EXPECT().PutVolume(sourceKey, gomock.Any())
	mockStore.EXPECT().GetVolume(destKey).Return(zeroVol.AsReader(), nil)
	mockStore.EXPECT().PutVolume(destKey, gomock.Any())
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(10))
	mockStore.EXPECT().PutTransactionState(domain.TransactionKey{LedgerName: "test-ledger", ID: 1}, gomock.Any())
	mockStore.EXPECT().GetDate().Return(now).Times(4)
	mockStore.EXPECT().GetCurrentOpenPeriod().Return(nil, false)
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())

	request := &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: "test-ledger",
				Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
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
				}},
			},
		},
	}

	result, err := processor.ProcessOrder(requestToOrder(request), mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	applyLog := result.GetApply()
	require.NotNil(t, applyLog)

	createdTx := applyLog.GetLog().GetData().GetCreatedTransaction()
	require.NotNil(t, createdTx)
	require.Equal(t, uint64(0), createdTx.GetPeriodId(), "PeriodId should be 0 when no open period exists")
}

func TestProcessCreateTransaction_CoercesPreviousAccountMetadata(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}

	// "age" is declared INT64 and still converting; the account's stored value is
	// the raw string written before the type was declared.
	ledgerInfo := &commonpb.LedgerInfo{
		Name: "test-ledger",
		Id:   1,
		MetadataSchema: &commonpb.MetadataSchema{
			AccountFields: map[string]*commonpb.MetadataFieldSchema{
				"age": {Type: commonpb.MetadataType_METADATA_TYPE_INT64},
			},
		},
	}

	worldKey := domain.NewVolumeKey("test-ledger", "world", "USD")
	destKey := domain.NewVolumeKey("test-ledger", "users:123", "USD")
	zero := &raftcmdpb.VolumePair{Input: commonpb.NewUint256FromUint64(0), Output: commonpb.NewUint256FromUint64(0)}

	metaKey := domain.MetadataKey{
		AccountKey: domain.AccountKey{LedgerName: "test-ledger", Account: "users:123"},
		Key:        "age",
	}

	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries.AsReader(), nil)
	mockStore.EXPECT().GetLedger("test-ledger").Return(ledgerInfo, nil).AnyTimes()
	mockStore.EXPECT().GetDate().Return(now).Times(4)
	mockStore.EXPECT().GetCurrentOpenPeriod().Return(nil, false)
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())
	mockStore.EXPECT().GetVolume(worldKey).Return(zero.AsReader(), nil)
	mockStore.EXPECT().PutVolume(worldKey, gomock.Any())
	mockStore.EXPECT().GetVolume(destKey).Return(zero.AsReader(), nil)
	mockStore.EXPECT().PutVolume(destKey, gomock.Any())
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(1))
	mockStore.EXPECT().PutTransactionState(domain.TransactionKey{LedgerName: "test-ledger", ID: 1}, gomock.Any())
	// Stored value is still the raw string (conversion has not rewritten it yet).
	mockStore.EXPECT().GetAccountMetadata(metaKey).Return(commonpb.NewStringValue("30"), nil)
	mockStore.EXPECT().PutAccountMetadata(metaKey, gomock.Any())

	request := &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: "test-ledger",
				Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
					CreateTransaction: &servicepb.CreateTransactionPayload{
						Postings: []*commonpb.Posting{
							{Source: "world", Destination: "users:123", Amount: commonpb.NewUint256FromUint64(1000), Asset: "USD"},
						},
						AccountMetadata: map[string]*commonpb.MetadataMap{
							"users:123": {Values: map[string]*commonpb.MetadataValue{"age": commonpb.NewStringValue("40")}},
						},
					},
				}},
			},
		},
	}

	result, err := processor.ProcessOrder(requestToOrder(request), mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	created := result.GetApply().GetLog().GetData().GetCreatedTransaction()
	require.NotNil(t, created)
	prev := created.GetPreviousAccountMetadata()["users:123"].GetValues()["age"]
	require.NotNil(t, prev)
	require.Equal(t, int64(30), prev.GetIntValue(),
		"previous account metadata must be coerced to the declared INT64 type, not left as raw string")
}
