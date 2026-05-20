package processing

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
)

func TestProcessOrders_WithIdempotencyKey_NewRequest(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
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
	mockStore.EXPECT().GetIdempotencyKey(domain.IdempotencyKey{Key: "unique-key-123"}).Return(nil, domain.ErrNotFound)

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
		domain.IdempotencyKey{Key: "unique-key-123"},
		gomock.Any(),
	).Do(func(key domain.IdempotencyKey, value *commonpb.IdempotencyKeyValue) {
		require.Equal(t, uint64(100), value.GetLogSequence())
		require.NotEmpty(t, value.GetHash())
	})

	response, err := processor.ProcessOrders(proposal.GetOrders(), mockStore)
	require.NoError(t, err)
	require.NotNil(t, response)
	require.Len(t, response, 1)

	// Should be a created log, not a reference
	createdLog := response[0].GetCreatedLog()
	require.NotNil(t, createdLog)
	require.Equal(t, uint64(100), createdLog.GetSequence())
	require.NotNil(t, createdLog.GetIdempotency())
	require.Equal(t, "unique-key-123", createdLog.GetIdempotency().GetKey())
	require.NotEmpty(t, createdLog.GetHash(), "log should have a hash")
}

func TestProcessOrders_WithIdempotencyKey_DuplicateRequest(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
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
	expectedHash := processor.computeOrderHash(order)

	proposal := &raftcmdpb.Proposal{
		Id:     1,
		Orders: []*raftcmdpb.Order{order},
	}

	// Idempotency key found with matching hash
	mockStore.EXPECT().GetIdempotencyKey(domain.IdempotencyKey{Key: "unique-key-123"}).Return(
		&commonpb.IdempotencyKeyValue{
			LogSequence: 42,
			Hash:        expectedHash,
		},
		nil,
	)

	// No other calls should be made - the order should not be processed

	response, err := processor.ProcessOrders(proposal.GetOrders(), mockStore)
	require.NoError(t, err)
	require.NotNil(t, response)
	require.Len(t, response, 1)

	// Should be a reference, not a created log
	refSequence := response[0].GetReferenceSequence()
	require.Equal(t, uint64(42), refSequence)
}

func TestProcessOrders_WithIdempotencyKey_Conflict(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
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
	mockStore.EXPECT().GetIdempotencyKey(domain.IdempotencyKey{Key: "unique-key-123"}).Return(
		&commonpb.IdempotencyKeyValue{
			LogSequence: 42,
			Hash:        []byte("different-hash"),
		},
		nil,
	)

	// No other calls should be made - should fail immediately

	response, err := processor.ProcessOrders(proposal.GetOrders(), mockStore)
	require.Error(t, err)
	require.Nil(t, response)
	require.ErrorAs(t, err, new(*domain.ErrIdempotencyKeyConflict))
}

func TestProcessOrders_WithoutIdempotencyKey(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
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

	response, err := processor.ProcessOrders(proposal.GetOrders(), mockStore)
	require.NoError(t, err)
	require.NotNil(t, response)
	require.Len(t, response, 1)

	createdLog := response[0].GetCreatedLog()
	require.NotNil(t, createdLog)
	require.Equal(t, uint64(100), createdLog.GetSequence())
	require.NotEmpty(t, createdLog.GetHash(), "log should have a hash")
}

// TestCreateLedgerAndTransactInSameBatch verifies that a batch containing
// CreateLedger + CreateTransaction (on the same ledger) succeeds and that
// the transaction uses the correct LedgerID in its volume keys.
func TestCreateLedgerAndTransactInSameBatch(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}

	// Track the ledger info stored by CreateLedger so GetLedger can return it.
	var storedLedgerInfo *commonpb.LedgerInfo

	// Order 1: CreateLedger("myled")
	mockStore.EXPECT().GetLedger("myled").Return(nil, false) // does not exist yet
	mockStore.EXPECT().IncrementNextLedgerID().Return(uint32(1))
	mockStore.EXPECT().GetDate().Return(now).AnyTimes()
	mockStore.EXPECT().PutLedger("myled", gomock.Any()).Do(
		func(_ string, info *commonpb.LedgerInfo) {
			storedLedgerInfo = info
		},
	)
	mockStore.EXPECT().PutBoundaries("myled", gomock.Any())

	// Order 2: CreateTransaction on "myled"
	// After order 1 runs, GetLedger should return the stored info.
	mockStore.EXPECT().GetLedger("myled").DoAndReturn(func(_ string) (*commonpb.LedgerInfo, bool) {
		return storedLedgerInfo, storedLedgerInfo != nil
	}).AnyTimes()
	mockStore.EXPECT().GetBoundaries("myled").Return(&raftcmdpb.LedgerBoundaries{
		NextTransactionId: 1,
		NextLogId:         1,
	}, true)
	mockStore.EXPECT().GetCurrentOpenPeriod().Return(nil, false)
	mockStore.EXPECT().PutBoundaries("myled", gomock.Any())

	// Volume operations: the LedgerID should be 1 (assigned by CreateLedger).
	srcKey := domain.VolumeKey{
		AccountKey: domain.AccountKey{LedgerID: 1, Account: "world"},
		Asset:      "USD",
	}
	dstKey := domain.VolumeKey{
		AccountKey: domain.AccountKey{LedgerID: 1, Account: "users:bob"},
		Asset:      "USD",
	}

	zeroVol := &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(0),
		Output: commonpb.NewUint256FromUint64(0),
	}

	mockStore.EXPECT().GetVolume(srcKey).Return(zeroVol, nil)
	mockStore.EXPECT().PutVolume(srcKey, gomock.Any())
	mockStore.EXPECT().GetVolume(dstKey).Return(zeroVol, nil)
	mockStore.EXPECT().PutVolume(dstKey, gomock.Any())
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(1))
	mockStore.EXPECT().PutTransactionState(domain.TransactionKey{LedgerID: 1, ID: 1}, gomock.Any())

	// Hash chaining for 2 logs.
	var capturedHashes [][]byte

	mockStore.EXPECT().IncrementNextSequenceID().Return(uint64(1))
	mockStore.EXPECT().IncrementNextSequenceID().Return(uint64(2))

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
	)

	orders := []*raftcmdpb.Order{
		{Type: &raftcmdpb.Order_CreateLedger{CreateLedger: &raftcmdpb.CreateLedgerOrder{Name: "myled"}}},
		{Type: &raftcmdpb.Order_Apply{Apply: &raftcmdpb.LedgerApplyOrder{
			Ledger: "myled",
			Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{
				CreateTransaction: &raftcmdpb.CreateTransactionOrder{
					Postings: []*commonpb.Posting{
						{
							Source:      "world",
							Destination: "users:bob",
							Amount:      commonpb.NewUint256FromUint64(100),
							Asset:       "USD",
						},
					},
					Force: true,
				},
			},
		}}},
	}

	response, err := processor.ProcessOrders(orders, mockStore)
	require.NoError(t, err)
	require.Len(t, response, 2)

	// Verify order 1: CreateLedger log with Id=1.
	createLog := response[0].GetCreatedLog()
	require.NotNil(t, createLog)
	require.Equal(t, uint32(1), createLog.GetPayload().GetCreateLedger().GetId())

	// Verify order 2: CreateTransaction succeeded.
	txLog := response[1].GetCreatedLog()
	require.NotNil(t, txLog)
	applyLog := txLog.GetPayload().GetApply()
	require.NotNil(t, applyLog)
	createdTx := applyLog.GetLog().GetData().GetCreatedTransaction()
	require.NotNil(t, createdTx)
	require.Equal(t, uint64(1), createdTx.GetTransaction().GetId())
}

func TestProcessOrders_HashChaining(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
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

	// For each of the 3 orders: GetLedger, GetDate, PutLedger, PutBoundaries, IncrementNextSequenceID
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

	response, err := processor.ProcessOrders(proposal.GetOrders(), mockStore)
	require.NoError(t, err)
	require.NotNil(t, response)
	require.Len(t, response, 3)

	// Verify all logs have hashes
	for i, logOrRef := range response {
		createdLog := logOrRef.GetCreatedLog()
		require.NotNil(t, createdLog, "log %d should be a created log", i)
		require.NotEmpty(t, createdLog.GetHash(), "log %d should have a hash", i)
	}

	// Verify hashes are all different (chaining produces unique hashes)
	hash1 := response[0].GetCreatedLog().GetHash()
	hash2 := response[1].GetCreatedLog().GetHash()
	hash3 := response[2].GetCreatedLog().GetHash()

	require.NotEqual(t, hash1, hash2, "consecutive log hashes should differ")
	require.NotEqual(t, hash2, hash3, "consecutive log hashes should differ")
	require.NotEqual(t, hash1, hash3, "non-consecutive log hashes should differ")

	// Verify determinism: same inputs produce same hashes
	processor2, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	mockStore2 := NewMockInMemoryStore(ctrl)

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

	response2, err := processor2.ProcessOrders(proposal.GetOrders(), mockStore2)
	require.NoError(t, err)

	// Hashes should be identical for same inputs
	for i := range response {
		require.Equal(t,
			response[i].GetCreatedLog().GetHash(),
			response2[i].GetCreatedLog().GetHash(),
			"hash %d should be deterministic", i,
		)
	}
}
