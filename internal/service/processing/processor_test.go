package processing

import (
	"math/big"
	"testing"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/data"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestProcessProposal_WithIdempotencyKey_NewRequest(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockStore(ctrl)
	processor, err := NewRequestProcessor(nil)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_CreateLedger{
			CreateLedger: &raftcmdpb.CreateLedgerOrder{
				Name: "test-ledger",
			},
		},
		Idempotency: &commonpb.Idempotency{
			Key: "unique-key-123",
		},
	}

	proposal := &raftcmdpb.Proposal{
		Id:     1,
		Orders: []*raftcmdpb.Order{order},
	}

	// Idempotency key not found
	mockStore.EXPECT().GetIdempotencyKey(data.IdempotencyKey{Key: "unique-key-123"}).Return(nil, data.ErrNotFound)

	// Process the order normally
	mockStore.EXPECT().GetLedger("test-ledger").Return(nil, false)
	mockStore.EXPECT().IncrementNextLedgerID().Return(uint32(1))
	mockStore.EXPECT().GetDate().Return(now)
	mockStore.EXPECT().PutLedger("test-ledger", gomock.Any())
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())

	// Increment sequence ID, hash chaining, and store idempotency key
	mockStore.EXPECT().IncrementNextSequenceID().Return(uint64(100))
	mockStore.EXPECT().GetLastLogHash().Return(nil)
	mockStore.EXPECT().SetLastLogHash(gomock.Any())
	mockStore.EXPECT().PutIdempotencyKey(
		data.IdempotencyKey{Key: "unique-key-123"},
		gomock.Any(),
	).Do(func(key data.IdempotencyKey, value *commonpb.IdempotencyKeyValue) {
		require.Equal(t, uint64(100), value.LogSequence)
		require.NotEmpty(t, value.Hash)
	})

	response, err := processor.ProcessProposal(proposal, mockStore)
	require.NoError(t, err)
	require.NotNil(t, response)
	require.Len(t, response.Logs, 1)

	// Should be a created log, not a reference
	createdLog := response.Logs[0].GetCreatedLog()
	require.NotNil(t, createdLog)
	require.Equal(t, uint64(100), createdLog.Sequence)
	require.NotNil(t, createdLog.Idempotency)
	require.Equal(t, "unique-key-123", createdLog.Idempotency.Key)
	require.NotEmpty(t, createdLog.Hash, "log should have a hash")
}

func TestProcessProposal_WithIdempotencyKey_DuplicateRequest(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockStore(ctrl)
	processor, err := NewRequestProcessor(nil)
	require.NoError(t, err)

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_CreateLedger{
			CreateLedger: &raftcmdpb.CreateLedgerOrder{
				Name: "test-ledger",
			},
		},
		Idempotency: &commonpb.Idempotency{
			Key: "unique-key-123",
		},
	}

	// Compute the expected hash for this order
	expectedHash := computeOrderHash(order)

	proposal := &raftcmdpb.Proposal{
		Id:     1,
		Orders: []*raftcmdpb.Order{order},
	}

	// Idempotency key found with matching hash
	mockStore.EXPECT().GetIdempotencyKey(data.IdempotencyKey{Key: "unique-key-123"}).Return(
		&commonpb.IdempotencyKeyValue{
			LogSequence: 42,
			Hash:        expectedHash,
		},
		nil,
	)

	// No other calls should be made - the order should not be processed

	response, err := processor.ProcessProposal(proposal, mockStore)
	require.NoError(t, err)
	require.NotNil(t, response)
	require.Len(t, response.Logs, 1)

	// Should be a reference, not a created log
	refSequence := response.Logs[0].GetReferenceSequence()
	require.Equal(t, uint64(42), refSequence)
}

func TestProcessProposal_WithIdempotencyKey_Conflict(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockStore(ctrl)
	processor, err := NewRequestProcessor(nil)
	require.NoError(t, err)

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_CreateLedger{
			CreateLedger: &raftcmdpb.CreateLedgerOrder{
				Name: "test-ledger",
			},
		},
		Idempotency: &commonpb.Idempotency{
			Key: "unique-key-123",
		},
	}

	proposal := &raftcmdpb.Proposal{
		Id:     1,
		Orders: []*raftcmdpb.Order{order},
	}

	// Idempotency key found with DIFFERENT hash (conflict)
	mockStore.EXPECT().GetIdempotencyKey(data.IdempotencyKey{Key: "unique-key-123"}).Return(
		&commonpb.IdempotencyKeyValue{
			LogSequence: 42,
			Hash:        []byte("different-hash"),
		},
		nil,
	)

	// No other calls should be made - should fail immediately

	response, err := processor.ProcessProposal(proposal, mockStore)
	require.Error(t, err)
	require.Nil(t, response)
	require.ErrorIs(t, err, ErrIdempotencyKeyConflict)
}

func TestProcessProposal_WithoutIdempotencyKey(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockStore(ctrl)
	processor, err := NewRequestProcessor(nil)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_CreateLedger{
			CreateLedger: &raftcmdpb.CreateLedgerOrder{
				Name: "test-ledger",
			},
		},
		// No idempotency key
	}

	proposal := &raftcmdpb.Proposal{
		Id:     1,
		Orders: []*raftcmdpb.Order{order},
	}

	// No idempotency check should be made
	// Process the order normally
	mockStore.EXPECT().GetLedger("test-ledger").Return(nil, false)
	mockStore.EXPECT().IncrementNextLedgerID().Return(uint32(1))
	mockStore.EXPECT().GetDate().Return(now)
	mockStore.EXPECT().PutLedger("test-ledger", gomock.Any())
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())
	mockStore.EXPECT().IncrementNextSequenceID().Return(uint64(100))
	mockStore.EXPECT().GetLastLogHash().Return(nil)
	mockStore.EXPECT().SetLastLogHash(gomock.Any())

	response, err := processor.ProcessProposal(proposal, mockStore)
	require.NoError(t, err)
	require.NotNil(t, response)
	require.Len(t, response.Logs, 1)

	createdLog := response.Logs[0].GetCreatedLog()
	require.NotNil(t, createdLog)
	require.Equal(t, uint64(100), createdLog.Sequence)
	require.NotEmpty(t, createdLog.Hash, "log should have a hash")
}

func TestProcessCreateLedger(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockStore(ctrl)
	processor, err := NewRequestProcessor(nil)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}

	// Setup expectations
	mockStore.EXPECT().GetLedger("test-ledger").Return(nil, false)
	mockStore.EXPECT().IncrementNextLedgerID().Return(uint32(1))
	mockStore.EXPECT().GetDate().Return(now)
	mockStore.EXPECT().PutLedger("test-ledger", gomock.Any()).Do(
		func(name string, info *commonpb.LedgerInfo) {
			require.Equal(t, "test-ledger", info.Name)
			require.Equal(t, uint32(1), info.Id)
			require.Equal(t, now, info.CreatedAt)
		},
	)
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any()).Do(
		func(ledger string, boundaries *raftcmdpb.LedgerBoundaries) {
			require.Equal(t, uint64(1), boundaries.NextTransactionId)
			require.Equal(t, uint64(1), boundaries.NextLogId)
		},
	)

	request := &servicepb.Request{
		Type: &servicepb.Request_CreateLedger{
			CreateLedger: &servicepb.CreateLedgerRequest{
				Name: "test-ledger",
			},
		},
	}

	result, err := processor.ProcessOrder(requestToOrder(request), mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	createLedgerLog := result.GetCreateLedger()
	require.NotNil(t, createLedgerLog)
	require.Equal(t, "test-ledger", createLedgerLog.Info.Name)
	require.Equal(t, uint32(1), createLedgerLog.Info.Id)
}

func TestProcessCreateLedger_AlreadyExists(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockStore(ctrl)
	processor, err := NewRequestProcessor(nil)
	require.NoError(t, err)

	existingLedger := &commonpb.LedgerInfo{Name: "test-ledger", Id: 1}
	mockStore.EXPECT().GetLedger("test-ledger").Return(existingLedger, true)

	request := &servicepb.Request{
		Type: &servicepb.Request_CreateLedger{
			CreateLedger: &servicepb.CreateLedgerRequest{
				Name: "test-ledger",
			},
		},
	}

	result, err := processor.ProcessOrder(requestToOrder(request), mockStore)
	require.Error(t, err)
	require.Nil(t, result)
	require.Contains(t, err.Error(), "ledger already exists")
}

func TestProcessCreateLedger_WithMetadata(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockStore(ctrl)
	processor, err := NewRequestProcessor(nil)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}

	mockStore.EXPECT().GetLedger("test-ledger").Return(nil, false)
	mockStore.EXPECT().IncrementNextLedgerID().Return(uint32(1))
	mockStore.EXPECT().GetDate().Return(now)
	mockStore.EXPECT().PutLedger("test-ledger", gomock.Any())
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())
	mockStore.EXPECT().PutLedgerMetadata(
		data.LedgerMetadataKey{LedgerID: 1, Key: "env"},
		&commonpb.MetadataValue{Value: "production"},
	)

	request := &servicepb.Request{
		Type: &servicepb.Request_CreateLedger{
			CreateLedger: &servicepb.CreateLedgerRequest{
				Name: "test-ledger",
				Metadata: &commonpb.MetadataSet{
					Metadata: []*commonpb.Metadata{
						{Key: "env", Value: &commonpb.MetadataValue{Value: "production"}},
					},
				},
			},
		},
	}

	result, err := processor.ProcessOrder(requestToOrder(request), mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)
}

func TestProcessDeleteLedger(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockStore(ctrl)
	processor, err := NewRequestProcessor(nil)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	existingLedger := &commonpb.LedgerInfo{Name: "test-ledger", Id: 1}

	mockStore.EXPECT().GetLedger("test-ledger").Return(existingLedger, true)
	mockStore.EXPECT().GetDate().Return(now)
	mockStore.EXPECT().PutLedger("test-ledger", gomock.Any())

	request := &servicepb.Request{
		Type: &servicepb.Request_DeleteLedger{
			DeleteLedger: &servicepb.DeleteLedgerRequest{
				Name: "test-ledger",
			},
		},
	}

	result, err := processor.ProcessOrder(requestToOrder(request), mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	deleteLedgerLog := result.GetDeleteLedger()
	require.NotNil(t, deleteLedgerLog)
	require.Equal(t, "test-ledger", deleteLedgerLog.Info.Name)
	require.Equal(t, now, deleteLedgerLog.Info.DeletedAt)
}

func TestProcessDeleteLedger_NotFound(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockStore(ctrl)
	processor, err := NewRequestProcessor(nil)
	require.NoError(t, err)

	mockStore.EXPECT().GetLedger("test-ledger").Return(nil, false)

	request := &servicepb.Request{
		Type: &servicepb.Request_DeleteLedger{
			DeleteLedger: &servicepb.DeleteLedgerRequest{
				Name: "test-ledger",
			},
		},
	}

	result, err := processor.ProcessOrder(requestToOrder(request), mockStore)
	require.Error(t, err)
	require.Nil(t, result)
	require.Contains(t, err.Error(), "ledger does not exist")
}

func TestProcessAddMetadata_Account(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockStore(ctrl)
	processor, err := NewRequestProcessor(nil)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}

	mockStore.EXPECT().GetLedger("test-ledger").Return(&commonpb.LedgerInfo{Name: "test-ledger", Id: 1}, true)
	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries, true)
	mockStore.EXPECT().GetDate().Return(now)
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())
	mockStore.EXPECT().PutAccountMetadata(
		data.MetadataKey{
			AccountKey: data.AccountKey{LedgerID: 1, Account: "users:123"},
			Key:        "status",
		},
		&commonpb.MetadataValue{Value: "active"},
	)

	request := &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: "test-ledger",
				Data: &servicepb.LedgerApplyRequest_AddMetadata{
					AddMetadata: &commonpb.SaveMetadataCommand{
						Target: &commonpb.Target{
							Target: &commonpb.Target_Account{
								Account: &commonpb.TargetAccount{Addr: "users:123"},
							},
						},
						Metadata: &commonpb.MetadataSet{
							Metadata: []*commonpb.Metadata{
								{Key: "status", Value: &commonpb.MetadataValue{Value: "active"}},
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
	require.Equal(t, "test-ledger", applyLog.LedgerName)

	savedMetadata := applyLog.Log.Data.GetSavedMetadata()
	require.NotNil(t, savedMetadata)
}

func TestProcessAddMetadata_Transaction(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockStore(ctrl)
	processor, err := NewRequestProcessor(nil)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 10, NextLogId: 5}

	mockStore.EXPECT().GetLedger("test-ledger").Return(&commonpb.LedgerInfo{Name: "test-ledger", Id: 1}, true)
	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries, true)
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(42))
	mockStore.EXPECT().GetDate().Return(now)
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())
	mockStore.EXPECT().AddTransactionUpdate(data.TransactionKey{LedgerID: 1, ID: 5}, gomock.Any()).Do(
		func(key data.TransactionKey, update *commonpb.TransactionUpdate) {
			require.Equal(t, uint64(42), update.ByLog) // Global sequence ID
			require.Len(t, update.Updates, 1)
			addMeta := update.Updates[0].GetTransactionModificationAddMetadata()
			require.NotNil(t, addMeta)
			require.Equal(t, "category", addMeta.Metadata.Key)
		},
	)

	request := &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: "test-ledger",
				Data: &servicepb.LedgerApplyRequest_AddMetadata{
					AddMetadata: &commonpb.SaveMetadataCommand{
						Target: &commonpb.Target{
							Target: &commonpb.Target_Transaction{
								Transaction: &commonpb.TargetTransaction{Id: 5},
							},
						},
						Metadata: &commonpb.MetadataSet{
							Metadata: []*commonpb.Metadata{
								{Key: "category", Value: &commonpb.MetadataValue{Value: "payment"}},
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

func TestProcessDeleteMetadata_Account(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockStore(ctrl)
	processor, err := NewRequestProcessor(nil)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}

	mockStore.EXPECT().GetLedger("test-ledger").Return(&commonpb.LedgerInfo{Name: "test-ledger", Id: 1}, true)
	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries, true)
	mockStore.EXPECT().GetDate().Return(now)
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())
	mockStore.EXPECT().DeleteAccountMetadata(data.MetadataKey{
		AccountKey: data.AccountKey{LedgerID: 1, Account: "users:123"},
		Key:        "status",
	})

	request := &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: "test-ledger",
				Data: &servicepb.LedgerApplyRequest_DeleteMetadata{
					DeleteMetadata: &commonpb.DeleteMetadataCommand{
						Target: &commonpb.Target{
							Target: &commonpb.Target_Account{
								Account: &commonpb.TargetAccount{Addr: "users:123"},
							},
						},
						Key: "status",
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

	deletedMetadata := applyLog.Log.Data.GetDeletedMetadata()
	require.NotNil(t, deletedMetadata)
	require.Equal(t, "status", deletedMetadata.Key)
}

func TestProcessCreateTransaction(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockStore(ctrl)
	processor, err := NewRequestProcessor(nil)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}

	sourceKey := data.VolumeKey{
		AccountKey: data.AccountKey{LedgerID: 1, Account: "bank"},
		Asset:      "USD",
	}
	destKey := data.VolumeKey{
		AccountKey: data.AccountKey{LedgerID: 1, Account: "users:123"},
		Asset:      "USD",
	}

	// Source has 1000 input, 0 output -> balance = 1000
	sourceInput := &raftcmdpb.VolumeHolder{Known: commonpb.NewBigInt(big.NewInt(1000))}
	sourceOutput := &raftcmdpb.VolumeHolder{Known: commonpb.NewBigInt(big.NewInt(0))}

	// Destination starts with 0
	destInput := &raftcmdpb.VolumeHolder{Known: commonpb.NewBigInt(big.NewInt(0))}

	mockStore.EXPECT().GetLedger("test-ledger").Return(&commonpb.LedgerInfo{Name: "test-ledger", Id: 1}, true)
	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries, true)
	mockStore.EXPECT().GetDate().Return(now).Times(4) // Called for: ledger log date, timestamp fallback, InsertedAt, UpdatedAt
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())
	mockStore.EXPECT().GetInput(sourceKey).Return(sourceInput, nil)
	mockStore.EXPECT().GetOutput(sourceKey).Return(sourceOutput, nil)
	mockStore.EXPECT().PutOutput(sourceKey, gomock.Any()).Do(
		func(key data.VolumeKey, value *raftcmdpb.VolumeHolder) {
			// Output should increase by 100
			require.Equal(t, int64(100), value.Known.Value().Int64())
		},
	)
	mockStore.EXPECT().GetInput(destKey).Return(destInput, nil)
	mockStore.EXPECT().PutInput(destKey, gomock.Any()).Do(
		func(key data.VolumeKey, value *raftcmdpb.VolumeHolder) {
			// Input should increase by 100
			require.Equal(t, int64(100), value.Known.Value().Int64())
		},
	)
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(1))
	mockStore.EXPECT().AddTransactionUpdate(data.TransactionKey{LedgerID: 1, ID: 1}, gomock.Any())

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
								Amount:      commonpb.NewBigInt(big.NewInt(100)),
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

	mockStore := NewMockStore(ctrl)
	processor, err := NewRequestProcessor(nil)
	require.NoError(t, err)

	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}

	sourceKey := data.VolumeKey{
		AccountKey: data.AccountKey{LedgerID: 1, Account: "users:123"},
		Asset:      "USD",
	}

	// Source has only 50 balance (100 input - 50 output)
	sourceInput := &raftcmdpb.VolumeHolder{Known: commonpb.NewBigInt(big.NewInt(100))}
	sourceOutput := &raftcmdpb.VolumeHolder{Known: commonpb.NewBigInt(big.NewInt(50))}

	mockStore.EXPECT().GetLedger("test-ledger").Return(&commonpb.LedgerInfo{Name: "test-ledger", Id: 1}, true)
	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries, true)
	mockStore.EXPECT().GetInput(sourceKey).Return(sourceInput, nil)
	mockStore.EXPECT().GetOutput(sourceKey).Return(sourceOutput, nil)

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
								Amount:      commonpb.NewBigInt(big.NewInt(100)), // Wants 100, has only 50
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

	mockStore := NewMockStore(ctrl)
	processor, err := NewRequestProcessor(nil)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}

	worldKey := data.VolumeKey{
		AccountKey: data.AccountKey{LedgerID: 1, Account: "world"},
		Asset:      "USD",
	}
	destKey := data.VolumeKey{
		AccountKey: data.AccountKey{LedgerID: 1, Account: "users:123"},
		Asset:      "USD",
	}

	// World has negative balance (but "world" bypasses balance check)
	worldInput := &raftcmdpb.VolumeHolder{Known: commonpb.NewBigInt(big.NewInt(0))}
	worldOutput := &raftcmdpb.VolumeHolder{Known: commonpb.NewBigInt(big.NewInt(1000000))}
	destInput := &raftcmdpb.VolumeHolder{Known: commonpb.NewBigInt(big.NewInt(0))}

	mockStore.EXPECT().GetLedger("test-ledger").Return(&commonpb.LedgerInfo{Name: "test-ledger", Id: 1}, true)
	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries, true)
	mockStore.EXPECT().GetDate().Return(now).Times(4)
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())
	mockStore.EXPECT().GetInput(worldKey).Return(worldInput, nil)
	mockStore.EXPECT().GetOutput(worldKey).Return(worldOutput, nil)
	mockStore.EXPECT().PutOutput(worldKey, gomock.Any())
	mockStore.EXPECT().GetInput(destKey).Return(destInput, nil)
	mockStore.EXPECT().PutInput(destKey, gomock.Any())
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(1))
	mockStore.EXPECT().AddTransactionUpdate(data.TransactionKey{LedgerID: 1, ID: 1}, gomock.Any())

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
								Amount:      commonpb.NewBigInt(big.NewInt(1000)),
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

	mockStore := NewMockStore(ctrl)
	processor, err := NewRequestProcessor(nil)
	require.NoError(t, err)

	mockStore.EXPECT().GetLedger("nonexistent").Return(nil, false)

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

func TestProcessCreateTransaction_Numscript_WorldSource(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockStore(ctrl)
	processor, err := NewRequestProcessor(nil)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}

	// Use flexible mocking for volume operations
	mockStore.EXPECT().GetLedger("test-ledger").Return(&commonpb.LedgerInfo{Name: "test-ledger", Id: 1}, true)
	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries, true)
	mockStore.EXPECT().GetDate().Return(now).AnyTimes()
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())
	mockStore.EXPECT().GetInput(gomock.Any()).Return(&raftcmdpb.VolumeHolder{Known: commonpb.NewBigInt(big.NewInt(0))}, nil).AnyTimes()
	mockStore.EXPECT().GetOutput(gomock.Any()).Return(&raftcmdpb.VolumeHolder{Known: commonpb.NewBigInt(big.NewInt(0))}, nil).AnyTimes()
	mockStore.EXPECT().PutOutput(gomock.Any(), gomock.Any()).AnyTimes()
	mockStore.EXPECT().PutInput(gomock.Any(), gomock.Any()).AnyTimes()
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(1))
	mockStore.EXPECT().AddTransactionUpdate(data.TransactionKey{LedgerID: 1, ID: 1}, gomock.Any())

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
	require.Equal(t, int64(10000), posting.Amount.Value().Int64())
	require.Equal(t, "USD/2", posting.Asset)
}

func TestProcessCreateTransaction_Numscript_WithVariables(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockStore(ctrl)
	processor, err := NewRequestProcessor(nil)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}

	mockStore.EXPECT().GetLedger("test-ledger").Return(&commonpb.LedgerInfo{Name: "test-ledger", Id: 1}, true)
	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries, true)
	mockStore.EXPECT().GetDate().Return(now).AnyTimes()
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())
	mockStore.EXPECT().GetInput(gomock.Any()).Return(&raftcmdpb.VolumeHolder{Known: commonpb.NewBigInt(big.NewInt(0))}, nil).AnyTimes()
	mockStore.EXPECT().GetOutput(gomock.Any()).Return(&raftcmdpb.VolumeHolder{Known: commonpb.NewBigInt(big.NewInt(0))}, nil).AnyTimes()
	mockStore.EXPECT().PutOutput(gomock.Any(), gomock.Any()).AnyTimes()
	mockStore.EXPECT().PutInput(gomock.Any(), gomock.Any()).AnyTimes()
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(1))
	mockStore.EXPECT().AddTransactionUpdate(data.TransactionKey{LedgerID: 1, ID: 1}, gomock.Any())

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
	require.Equal(t, int64(5000), posting.Amount.Value().Int64())
	require.Equal(t, "EUR/2", posting.Asset)
}

func TestProcessCreateTransaction_Numscript_MultiplePostings(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockStore(ctrl)
	processor, err := NewRequestProcessor(nil)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}

	mockStore.EXPECT().GetLedger("test-ledger").Return(&commonpb.LedgerInfo{Name: "test-ledger", Id: 1}, true)
	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries, true)
	mockStore.EXPECT().GetDate().Return(now).AnyTimes()
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())
	mockStore.EXPECT().GetInput(gomock.Any()).Return(&raftcmdpb.VolumeHolder{Known: commonpb.NewBigInt(big.NewInt(0))}, nil).AnyTimes()
	mockStore.EXPECT().GetOutput(gomock.Any()).Return(&raftcmdpb.VolumeHolder{Known: commonpb.NewBigInt(big.NewInt(0))}, nil).AnyTimes()
	mockStore.EXPECT().PutOutput(gomock.Any(), gomock.Any()).AnyTimes()
	mockStore.EXPECT().PutInput(gomock.Any(), gomock.Any()).AnyTimes()
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(1))
	mockStore.EXPECT().AddTransactionUpdate(data.TransactionKey{LedgerID: 1, ID: 1}, gomock.Any())

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
	require.Equal(t, int64(10000), createdTx.Transaction.Postings[0].Amount.Value().Int64())

	// Verify second posting
	require.Equal(t, "world", createdTx.Transaction.Postings[1].Source)
	require.Equal(t, "users:bob", createdTx.Transaction.Postings[1].Destination)
	require.Equal(t, int64(5000), createdTx.Transaction.Postings[1].Amount.Value().Int64())
}

func TestProcessCreateTransaction_Numscript_UnboundedOverdraft(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockStore(ctrl)
	processor, err := NewRequestProcessor(nil)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}

	mockStore.EXPECT().GetLedger("test-ledger").Return(&commonpb.LedgerInfo{Name: "test-ledger", Id: 1}, true)
	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries, true)
	mockStore.EXPECT().GetDate().Return(now).AnyTimes()
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())
	// Bank starts with 0 balance but can go negative with unbounded overdraft
	mockStore.EXPECT().GetInput(gomock.Any()).Return(&raftcmdpb.VolumeHolder{Known: commonpb.NewBigInt(big.NewInt(0))}, nil).AnyTimes()
	mockStore.EXPECT().GetOutput(gomock.Any()).Return(&raftcmdpb.VolumeHolder{Known: commonpb.NewBigInt(big.NewInt(0))}, nil).AnyTimes()
	mockStore.EXPECT().PutOutput(gomock.Any(), gomock.Any()).AnyTimes()
	mockStore.EXPECT().PutInput(gomock.Any(), gomock.Any()).AnyTimes()
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(1))
	mockStore.EXPECT().AddTransactionUpdate(data.TransactionKey{LedgerID: 1, ID: 1}, gomock.Any())

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
	require.Equal(t, int64(100000), posting.Amount.Value().Int64())
}

func TestProcessCreateTransaction_Numscript_ParseError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockStore(ctrl)
	processor, err := NewRequestProcessor(nil)
	require.NoError(t, err)

	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}

	mockStore.EXPECT().GetLedger("test-ledger").Return(&commonpb.LedgerInfo{Name: "test-ledger", Id: 1}, true)
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

	mockStore := NewMockStore(ctrl)
	processor, err := NewRequestProcessor(nil)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}

	// Note: When Script.Plain is empty, the processor falls back to stdPostingProducer
	// which creates an empty transaction. This test verifies that behavior.
	mockStore.EXPECT().GetLedger("test-ledger").Return(&commonpb.LedgerInfo{Name: "test-ledger", Id: 1}, true)
	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries, true)
	mockStore.EXPECT().GetDate().Return(now).AnyTimes()
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(1))
	mockStore.EXPECT().AddTransactionUpdate(data.TransactionKey{LedgerID: 1, ID: 1}, gomock.Any())

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

	mockStore := NewMockStore(ctrl)
	processor, err := NewRequestProcessor(nil)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}

	mockStore.EXPECT().GetLedger("test-ledger").Return(&commonpb.LedgerInfo{Name: "test-ledger", Id: 1}, true)
	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries, true)
	mockStore.EXPECT().GetDate().Return(now).AnyTimes()
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())
	mockStore.EXPECT().GetInput(gomock.Any()).Return(&raftcmdpb.VolumeHolder{Known: commonpb.NewBigInt(big.NewInt(0))}, nil).AnyTimes()
	mockStore.EXPECT().GetOutput(gomock.Any()).Return(&raftcmdpb.VolumeHolder{Known: commonpb.NewBigInt(big.NewInt(0))}, nil).AnyTimes()
	mockStore.EXPECT().PutOutput(gomock.Any(), gomock.Any()).AnyTimes()
	mockStore.EXPECT().PutInput(gomock.Any(), gomock.Any()).AnyTimes()
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(1))
	mockStore.EXPECT().AddTransactionUpdate(data.TransactionKey{LedgerID: 1, ID: 1}, gomock.Any())

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
	require.Equal(t, int64(5000), createdTx.Transaction.Postings[0].Amount.Value().Int64())

	require.Equal(t, "world", createdTx.Transaction.Postings[1].Source)
	require.Equal(t, "users:bob", createdTx.Transaction.Postings[1].Destination)
	require.Equal(t, int64(5000), createdTx.Transaction.Postings[1].Amount.Value().Int64())
}

func TestProcessCreateTransaction_Numscript_SetTxMeta(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockStore(ctrl)
	processor, err := NewRequestProcessor(nil)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}

	mockStore.EXPECT().GetLedger("test-ledger").Return(&commonpb.LedgerInfo{Name: "test-ledger", Id: 1}, true)
	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries, true)
	mockStore.EXPECT().GetDate().Return(now).AnyTimes()
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())
	mockStore.EXPECT().GetInput(gomock.Any()).Return(&raftcmdpb.VolumeHolder{Known: commonpb.NewBigInt(big.NewInt(0))}, nil).AnyTimes()
	mockStore.EXPECT().GetOutput(gomock.Any()).Return(&raftcmdpb.VolumeHolder{Known: commonpb.NewBigInt(big.NewInt(0))}, nil).AnyTimes()
	mockStore.EXPECT().PutOutput(gomock.Any(), gomock.Any()).AnyTimes()
	mockStore.EXPECT().PutInput(gomock.Any(), gomock.Any()).AnyTimes()
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(1))
	mockStore.EXPECT().AddTransactionUpdate(data.TransactionKey{LedgerID: 1, ID: 1}, gomock.Any())

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

	mockStore := NewMockStore(ctrl)
	processor, err := NewRequestProcessor(nil)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}

	mockStore.EXPECT().GetLedger("test-ledger").Return(&commonpb.LedgerInfo{Name: "test-ledger", Id: 1}, true)
	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries, true)
	mockStore.EXPECT().GetDate().Return(now).AnyTimes()
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())
	mockStore.EXPECT().GetInput(gomock.Any()).Return(&raftcmdpb.VolumeHolder{Known: commonpb.NewBigInt(big.NewInt(0))}, nil).AnyTimes()
	mockStore.EXPECT().GetOutput(gomock.Any()).Return(&raftcmdpb.VolumeHolder{Known: commonpb.NewBigInt(big.NewInt(0))}, nil).AnyTimes()
	mockStore.EXPECT().PutOutput(gomock.Any(), gomock.Any()).AnyTimes()
	mockStore.EXPECT().PutInput(gomock.Any(), gomock.Any()).AnyTimes()
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(1))
	mockStore.EXPECT().AddTransactionUpdate(data.TransactionKey{LedgerID: 1, ID: 1}, gomock.Any())

	// Expect account metadata to be set
	mockStore.EXPECT().PutAccountMetadata(
		data.MetadataKey{
			AccountKey: data.AccountKey{LedgerID: 1, Account: "users:alice"},
			Key:        "account_type",
		},
		&commonpb.MetadataValue{Value: "savings"},
	)
	mockStore.EXPECT().PutAccountMetadata(
		data.MetadataKey{
			AccountKey: data.AccountKey{LedgerID: 1, Account: "users:alice"},
			Key:        "created_by",
		},
		&commonpb.MetadataValue{Value: "numscript"},
	)

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

	mockStore := NewMockStore(ctrl)
	processor, err := NewRequestProcessor(nil)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}

	sourceKey := data.VolumeKey{
		AccountKey: data.AccountKey{LedgerID: 1, Account: "users:123"},
		Asset:      "USD",
	}
	destKey := data.VolumeKey{
		AccountKey: data.AccountKey{LedgerID: 1, Account: "merchant"},
		Asset:      "USD",
	}

	// Source has only 50 balance (100 input - 50 output) - not enough for 100
	sourceInput := &raftcmdpb.VolumeHolder{Known: commonpb.NewBigInt(big.NewInt(100))}
	sourceOutput := &raftcmdpb.VolumeHolder{Known: commonpb.NewBigInt(big.NewInt(50))}
	destInput := &raftcmdpb.VolumeHolder{Known: commonpb.NewBigInt(big.NewInt(0))}

	mockStore.EXPECT().GetLedger("test-ledger").Return(&commonpb.LedgerInfo{Name: "test-ledger", Id: 1}, true)
	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries, true)
	mockStore.EXPECT().GetDate().Return(now).Times(4)
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())
	mockStore.EXPECT().GetInput(sourceKey).Return(sourceInput, nil)
	mockStore.EXPECT().GetOutput(sourceKey).Return(sourceOutput, nil)
	// With force=true, balance check is skipped and output is updated
	mockStore.EXPECT().PutOutput(sourceKey, gomock.Any()).Do(
		func(key data.VolumeKey, value *raftcmdpb.VolumeHolder) {
			// Output should increase by 100 (50 + 100 = 150)
			require.Equal(t, int64(150), value.Known.Value().Int64())
		},
	)
	mockStore.EXPECT().GetInput(destKey).Return(destInput, nil)
	mockStore.EXPECT().PutInput(destKey, gomock.Any()).Do(
		func(key data.VolumeKey, value *raftcmdpb.VolumeHolder) {
			// Input should increase by 100
			require.Equal(t, int64(100), value.Known.Value().Int64())
		},
	)
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(1))
	mockStore.EXPECT().AddTransactionUpdate(data.TransactionKey{LedgerID: 1, ID: 1}, gomock.Any())

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
								Amount:      commonpb.NewBigInt(big.NewInt(100)), // Wants 100, has only 50
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

	mockStore := NewMockStore(ctrl)
	processor, err := NewRequestProcessor(nil)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}

	sourceKey := data.VolumeKey{
		AccountKey: data.AccountKey{LedgerID: 1, Account: "users:new"},
		Asset:      "USD",
	}
	destKey := data.VolumeKey{
		AccountKey: data.AccountKey{LedgerID: 1, Account: "merchant"},
		Asset:      "USD",
	}

	// Source has no input at all (nil) - would fail balance check normally
	// With force=true, this should succeed

	mockStore.EXPECT().GetLedger("test-ledger").Return(&commonpb.LedgerInfo{Name: "test-ledger", Id: 1}, true)
	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries, true)
	mockStore.EXPECT().GetDate().Return(now).Times(4)
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())
	// Source returns nil (no balance)
	mockStore.EXPECT().GetInput(sourceKey).Return(nil, data.ErrNotFound)
	mockStore.EXPECT().GetOutput(sourceKey).Return(nil, data.ErrNotFound)
	mockStore.EXPECT().PutOutput(sourceKey, gomock.Any()).Do(
		func(key data.VolumeKey, value *raftcmdpb.VolumeHolder) {
			// Output should use DiffSinceBaseIndex since we don't know the absolute value
			require.Nil(t, value.Known)
			require.Equal(t, int64(100), value.DiffSinceBaseIndex.Value().Int64())
		},
	)
	mockStore.EXPECT().GetInput(destKey).Return(nil, data.ErrNotFound)
	mockStore.EXPECT().PutInput(destKey, gomock.Any()).Do(
		func(key data.VolumeKey, value *raftcmdpb.VolumeHolder) {
			// Input should use DiffSinceBaseIndex since we don't know the absolute value
			require.Nil(t, value.Known)
			require.Equal(t, int64(100), value.DiffSinceBaseIndex.Value().Int64())
		},
	)
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(1))
	mockStore.EXPECT().AddTransactionUpdate(data.TransactionKey{LedgerID: 1, ID: 1}, gomock.Any())

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
								Amount:      commonpb.NewBigInt(big.NewInt(100)),
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

	mockStore := NewMockStore(ctrl)
	processor, err := NewRequestProcessor(nil)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}

	// Account has 0 balance, but with force=true, Numscript should see unlimited balance
	mockStore.EXPECT().GetLedger("test-ledger").Return(&commonpb.LedgerInfo{Name: "test-ledger", Id: 1}, true)
	mockStore.EXPECT().GetBoundaries("test-ledger").Return(boundaries, true)
	mockStore.EXPECT().GetDate().Return(now).AnyTimes()
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())
	// Note: GetInput/GetOutput might be called for volume updates but not for balance queries
	// when force=true (store adapter returns unlimited balance)
	mockStore.EXPECT().GetInput(gomock.Any()).Return(&raftcmdpb.VolumeHolder{Known: commonpb.NewBigInt(big.NewInt(0))}, nil).AnyTimes()
	mockStore.EXPECT().GetOutput(gomock.Any()).Return(&raftcmdpb.VolumeHolder{Known: commonpb.NewBigInt(big.NewInt(0))}, nil).AnyTimes()
	mockStore.EXPECT().PutOutput(gomock.Any(), gomock.Any()).AnyTimes()
	mockStore.EXPECT().PutInput(gomock.Any(), gomock.Any()).AnyTimes()
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(1))
	mockStore.EXPECT().AddTransactionUpdate(data.TransactionKey{LedgerID: 1, ID: 1}, gomock.Any())

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
	require.Equal(t, int64(100000), posting.Amount.Value().Int64())
}

func TestProcessProposal_HashChaining(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockStore(ctrl)
	processor, err := NewRequestProcessor(nil)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}

	// Create a proposal with 3 orders to verify hash chaining across logs
	proposal := &raftcmdpb.Proposal{
		Id: 1,
		Orders: []*raftcmdpb.Order{
			{Type: &raftcmdpb.Order_CreateLedger{CreateLedger: &raftcmdpb.CreateLedgerOrder{Name: "ledger-1"}}},
			{Type: &raftcmdpb.Order_CreateLedger{CreateLedger: &raftcmdpb.CreateLedgerOrder{Name: "ledger-2"}}},
			{Type: &raftcmdpb.Order_CreateLedger{CreateLedger: &raftcmdpb.CreateLedgerOrder{Name: "ledger-3"}}},
		},
	}

	// Track the hash chain as SetLastLogHash is called
	var capturedHashes [][]byte

	// For each of the 3 orders: GetLedger, IncrementNextLedgerID, GetDate, PutLedger, PutBoundaries, IncrementNextSequenceID
	for i, name := range []string{"ledger-1", "ledger-2", "ledger-3"} {
		mockStore.EXPECT().GetLedger(name).Return(nil, false)
		mockStore.EXPECT().IncrementNextLedgerID().Return(uint32(i + 1))
		mockStore.EXPECT().GetDate().Return(now)
		mockStore.EXPECT().PutLedger(name, gomock.Any())
		mockStore.EXPECT().PutBoundaries(name, gomock.Any())
		mockStore.EXPECT().IncrementNextSequenceID().Return(uint64(i + 1))
	}

	// Use gomock.InOrder for hash chain operations since they must happen sequentially
	gomock.InOrder(
		mockStore.EXPECT().GetLastLogHash().Return(nil),
		mockStore.EXPECT().SetLastLogHash(gomock.Any()).Do(func(hash []byte) {
			capturedHashes = append(capturedHashes, hash)
		}),
		mockStore.EXPECT().GetLastLogHash().DoAndReturn(func() []byte {
			return capturedHashes[0]
		}),
		mockStore.EXPECT().SetLastLogHash(gomock.Any()).Do(func(hash []byte) {
			capturedHashes = append(capturedHashes, hash)
		}),
		mockStore.EXPECT().GetLastLogHash().DoAndReturn(func() []byte {
			return capturedHashes[1]
		}),
		mockStore.EXPECT().SetLastLogHash(gomock.Any()).Do(func(hash []byte) {
			capturedHashes = append(capturedHashes, hash)
		}),
	)

	response, err := processor.ProcessProposal(proposal, mockStore)
	require.NoError(t, err)
	require.NotNil(t, response)
	require.Len(t, response.Logs, 3)

	// Verify all logs have hashes
	for i, logOrRef := range response.Logs {
		createdLog := logOrRef.GetCreatedLog()
		require.NotNil(t, createdLog, "log %d should be a created log", i)
		require.NotEmpty(t, createdLog.Hash, "log %d should have a hash", i)
	}

	// Verify hashes are all different (chaining produces unique hashes)
	hash1 := response.Logs[0].GetCreatedLog().Hash
	hash2 := response.Logs[1].GetCreatedLog().Hash
	hash3 := response.Logs[2].GetCreatedLog().Hash
	require.NotEqual(t, hash1, hash2, "consecutive log hashes should differ")
	require.NotEqual(t, hash2, hash3, "consecutive log hashes should differ")
	require.NotEqual(t, hash1, hash3, "non-consecutive log hashes should differ")

	// Verify determinism: same inputs produce same hashes
	processor2, err := NewRequestProcessor(nil)
	require.NoError(t, err)

	mockStore2 := NewMockStore(ctrl)
	var capturedHashes2 [][]byte

	for i, name := range []string{"ledger-1", "ledger-2", "ledger-3"} {
		mockStore2.EXPECT().GetLedger(name).Return(nil, false)
		mockStore2.EXPECT().IncrementNextLedgerID().Return(uint32(i + 1))
		mockStore2.EXPECT().GetDate().Return(now)
		mockStore2.EXPECT().PutLedger(name, gomock.Any())
		mockStore2.EXPECT().PutBoundaries(name, gomock.Any())
		mockStore2.EXPECT().IncrementNextSequenceID().Return(uint64(i + 1))
	}

	gomock.InOrder(
		mockStore2.EXPECT().GetLastLogHash().Return(nil),
		mockStore2.EXPECT().SetLastLogHash(gomock.Any()).Do(func(hash []byte) {
			capturedHashes2 = append(capturedHashes2, hash)
		}),
		mockStore2.EXPECT().GetLastLogHash().DoAndReturn(func() []byte {
			return capturedHashes2[0]
		}),
		mockStore2.EXPECT().SetLastLogHash(gomock.Any()).Do(func(hash []byte) {
			capturedHashes2 = append(capturedHashes2, hash)
		}),
		mockStore2.EXPECT().GetLastLogHash().DoAndReturn(func() []byte {
			return capturedHashes2[1]
		}),
		mockStore2.EXPECT().SetLastLogHash(gomock.Any()).Do(func(hash []byte) {
			capturedHashes2 = append(capturedHashes2, hash)
		}),
	)

	response2, err := processor2.ProcessProposal(proposal, mockStore2)
	require.NoError(t, err)

	// Hashes should be identical for same inputs
	for i := range response.Logs {
		require.Equal(t,
			response.Logs[i].GetCreatedLog().Hash,
			response2.Logs[i].GetCreatedLog().Hash,
			"hash %d should be deterministic", i,
		)
	}
}
