package processing

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// Per-order idempotency dedup/replay/conflict has moved to the FSM apply path
// (covered by internal/infra/state/idempotency_apply_test.go); ProcessOrders no
// longer performs it, so those tests were removed.

// TestComputeOrderHash_ExcludesCoverageBits pins the contract that the
// idempotency hash is computed over only the user-supplied request content —
// never CoverageBits, which admission rebuilds from the proposal-wide
// ExecutionPlan. The same logical order in a different batch (and therefore a
// different CoverageBits) MUST hash identically; otherwise the idempotency
// check would reject a legitimate retry.
func TestHashOrders_ExcludesCoverageBits(t *testing.T) {
	t.Parallel()

	base := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: "L",
				Payload: &raftcmdpb.LedgerScopedOrder_CreateLedger{
					CreateLedger: &raftcmdpb.CreateLedgerOrder{},
				},
			},
		},
	}
	baseHash := HashOrders([]*raftcmdpb.Order{base})

	withCoverage := &raftcmdpb.Order{
		Type:         base.GetType(),
		CoverageBits: []byte{0b0101},
	}
	require.Equal(t, baseHash, HashOrders([]*raftcmdpb.Order{withCoverage}),
		"CoverageBits must not change the idempotency hash")
}

// TestHashOrders_MatchesHashProposal pins the equivalence the integrity checker
// relies on: re-deriving a proposal's frozen hash via HashOrders (from the
// audit orders) must be byte-identical to the hot-path HashProposal.
func TestHashOrders_MatchesHashProposal(t *testing.T) {
	t.Parallel()

	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	orders := []*raftcmdpb.Order{
		{
			Type: &raftcmdpb.Order_LedgerScoped{
				LedgerScoped: &raftcmdpb.LedgerScopedOrder{
					Ledger:  "L",
					Payload: &raftcmdpb.LedgerScopedOrder_CreateLedger{CreateLedger: &raftcmdpb.CreateLedgerOrder{}},
				},
			},
			CoverageBits: []byte{0b1},
		},
		{
			Type: &raftcmdpb.Order_LedgerScoped{
				LedgerScoped: &raftcmdpb.LedgerScopedOrder{Ledger: "M"},
			},
		},
	}

	require.Equal(t, processor.HashProposal(&raftcmdpb.Proposal{Orders: orders}), HashOrders(orders),
		"HashOrders must be byte-identical to HashProposal so the checker re-derives the frozen hash")
}

func TestProcessOrders_WithoutIdempotencyKey(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: "test-ledger",
				Payload: &raftcmdpb.LedgerScopedOrder_CreateLedger{
					CreateLedger: &raftcmdpb.CreateLedgerOrder{},
				},
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
	mockStore.EXPECT().GetLedger("test-ledger").Return(nil, domain.ErrNotFound)
	mockStore.EXPECT().IncrementNextLedgerID().Return(uint32(1))
	mockStore.EXPECT().GetDate().Return(now.AsReader())
	mockStore.EXPECT().PutLedger("test-ledger", gomock.Any())
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any())
	mockStore.EXPECT().IncrementNextSequenceID().Return(uint64(100))

	response, err := processor.ProcessOrders(proposal.GetOrders(), mockFactory(mockStore))
	require.NoError(t, err)
	require.NotNil(t, response)
	require.Len(t, response, 1)

	createdLog := response[0].GetCreatedLog()
	require.NotNil(t, createdLog)
	require.Equal(t, uint64(100), createdLog.GetSequence())
}

// TestCreateLedgerAndTransactInSameBatch verifies that a batch containing
// CreateLedger + CreateTransaction (on the same ledger) succeeds and that
// the transaction uses the correct LedgerID in its volume keys.
func TestCreateLedgerAndTransactInSameBatch(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}

	// Track the ledger info stored by CreateLedger so GetLedger can return it.
	var storedLedgerInfo *commonpb.LedgerInfo

	// Order 1: CreateLedger("myled")
	mockStore.EXPECT().GetLedger("myled").Return(nil, domain.ErrNotFound) // does not exist yet
	mockStore.EXPECT().IncrementNextLedgerID().Return(uint32(1))
	mockStore.EXPECT().GetDate().Return(now.AsReader()).AnyTimes()
	mockStore.EXPECT().PutLedger("myled", gomock.Any()).Do(
		func(_ string, info *commonpb.LedgerInfo) {
			storedLedgerInfo = info
		},
	)
	mockStore.EXPECT().PutBoundaries("myled", gomock.Any())

	// Order 2: CreateTransaction on "myled"
	// After order 1 runs, GetLedger should return the stored info.
	mockStore.EXPECT().GetLedger("myled").DoAndReturn(func(_ string) (commonpb.LedgerInfoReader, error) {
		if storedLedgerInfo == nil {
			return nil, domain.ErrNotFound
		}

		return storedLedgerInfo.AsReader(), nil
	}).AnyTimes()
	mockStore.EXPECT().GetBoundaries("myled").Return((&raftcmdpb.LedgerBoundaries{
		NextTransactionId: 1,
		NextLogId:         1,
	}).AsReader(), nil)
	mockStore.EXPECT().GetCurrentOpenChapter().Return(nil, false)
	mockStore.EXPECT().PutBoundaries("myled", gomock.Any())

	// Volume operations: the LedgerID should be 1 (assigned by CreateLedger).
	srcKey := domain.NewVolumeKey("myled", "world", "USD")
	dstKey := domain.NewVolumeKey("myled", "users:bob", "USD")

	zeroVol := &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(0),
		Output: commonpb.NewUint256FromUint64(0),
	}

	mockStore.EXPECT().GetVolume(srcKey).Return(zeroVol.AsReader(), nil)
	mockStore.EXPECT().PutVolume(srcKey, gomock.Any())
	mockStore.EXPECT().GetVolume(dstKey).Return(zeroVol.AsReader(), nil)
	mockStore.EXPECT().PutVolume(dstKey, gomock.Any())
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(1))
	mockStore.EXPECT().PutTransactionState(domain.TransactionKey{LedgerName: "myled", ID: 1}, gomock.Any())

	mockStore.EXPECT().IncrementNextSequenceID().Return(uint64(1))
	mockStore.EXPECT().IncrementNextSequenceID().Return(uint64(2))

	orders := []*raftcmdpb.Order{
		{Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: "myled",
				Payload: &raftcmdpb.LedgerScopedOrder_CreateLedger{
					CreateLedger: &raftcmdpb.CreateLedgerOrder{},
				},
			},
		}},
		{Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: "myled",
				Payload: &raftcmdpb.LedgerScopedOrder_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{
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
					},
				},
			},
		}},
	}

	response, err := processor.ProcessOrders(orders, mockFactory(mockStore))
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
