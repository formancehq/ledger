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

func TestProcessCreateLedger(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0, false)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}

	// Setup expectations
	expectGetLedger(mockStore, domain.LedgerKey{Name: "test-ledger"}, nil, domain.ErrNotFound)
	mockStore.EXPECT().IncrementNextLedgerID().Return(uint32(1))
	mockStore.EXPECT().GetDate().Return(now.AsReader())
	expectPutLedger(t, mockStore, domain.LedgerKey{Name: "test-ledger"}, nil, func(name string, info *commonpb.LedgerInfo) {
		require.Equal(t, "test-ledger", info.GetName())
		require.Equal(t, now, info.GetCreatedAt())
		require.Equal(t, uint32(1), info.GetId(), "LedgerInfo should have Id == 1")
	})
	expectPutBoundaries(t, mockStore, domain.LedgerKey{Name: "test-ledger"}, nil, func(ledger string, boundaries *raftcmdpb.LedgerBoundaries) {
		require.Equal(t, uint64(1), boundaries.GetNextTransactionId())
		require.Equal(t, uint64(1), boundaries.GetNextLogId())
	})

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
	require.Equal(t, "test-ledger", createLedgerLog.GetName())
	require.Equal(t, uint32(1), createLedgerLog.GetId(), "CreatedLedgerLog should have Id == 1")
}

// TestProcessCreateLedger_InvalidPatternSelectionDeterministic pins EN-1521:
// when several initial account types have invalid patterns, the first-reported
// invalid pattern (chain-bound ErrInvalidPattern → AuditFailure) must be the
// same on every replica. The processor iterates the account-types map in sorted
// key order, so the lexicographically-first name's pattern is always chosen.
func TestProcessCreateLedger_InvalidPatternSelectionDeterministic(t *testing.T) {
	t.Parallel()

	const runs = 64
	for range runs {
		ctrl := gomock.NewController(t)
		mockStore := NewMockScope(ctrl)
		expectGetLedger(mockStore, domain.LedgerKey{Name: "l"}, nil, domain.ErrNotFound)

		order := &raftcmdpb.CreateLedgerOrder{
			AccountTypes: map[string]*commonpb.AccountType{
				"zzz": {Pattern: "z b"},  // invalid: space in a fixed segment
				"aaa": {Pattern: "a::x"}, // invalid: empty segment
			},
		}

		_, derr := processCreateLedger("l", order, &Context{Scope: mockStore})
		require.NotNil(t, derr)

		var invalid *domain.ErrInvalidPattern
		require.ErrorAs(t, derr, &invalid)
		require.Equal(t, "a::x", invalid.Pattern,
			"the first invalid pattern reported must be the lexicographically-first name's, deterministically")

		ctrl.Finish()
	}
}

// TestProcessCreateLedger_DoesNotMutateOrderAccountTypes pins EN-1533: the
// accepted order is captured verbatim for the audit chain, so processCreateLedger
// MUST NOT mutate any AccountType message reachable from
// CreateLedgerOrder.AccountTypes — not even to normalize an embedded Name to its
// map key. Derived state (LedgerInfo) and the created-ledger log must instead use
// the map key as the canonical name, on their own cloned messages.
func TestProcessCreateLedger_DoesNotMutateOrderAccountTypes(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	now := &commonpb.Timestamp{Data: 1234567890}

	// One entry with a blank embedded Name, one with an embedded Name that
	// mismatches its map key. Both should be preserved verbatim in the order,
	// while derived state uses the map key.
	order := &raftcmdpb.CreateLedgerOrder{
		AccountTypes: map[string]*commonpb.AccountType{
			"canonical-a": {Name: "", Pattern: "a:{id}"},
			"canonical-b": {Name: "wrong-embedded-name", Pattern: "b:{id}"},
		},
	}

	// Snapshot the deterministic bytes of the order BEFORE processing.
	fullOrder := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: "l",
				Payload: &raftcmdpb.LedgerScopedOrder_CreateLedger{
					CreateLedger: order,
				},
			},
		},
	}
	before := fullOrder.MarshalDeterministicVT(nil)

	expectGetLedger(mockStore, domain.LedgerKey{Name: "l"}, nil, domain.ErrNotFound)
	mockStore.EXPECT().IncrementNextLedgerID().Return(uint32(1))
	mockStore.EXPECT().GetDate().Return(now.AsReader())

	var storedInfo *commonpb.LedgerInfo
	expectPutLedger(t, mockStore, domain.LedgerKey{Name: "l"}, nil, func(_ string, info *commonpb.LedgerInfo) {
		storedInfo = info
	})
	expectPutBoundaries(t, mockStore, domain.LedgerKey{Name: "l"}, nil)

	log, derr := processCreateLedger("l", order, &Context{Scope: mockStore})
	require.Nil(t, derr)
	require.NotNil(t, log)

	// The order-owned messages must be untouched: both the raw embedded names
	// and the deterministic order bytes are identical to the pre-processing
	// snapshot.
	require.Equal(t, "", order.GetAccountTypes()["canonical-a"].GetName(),
		"blank embedded name must not be normalized into the order")
	require.Equal(t, "wrong-embedded-name", order.GetAccountTypes()["canonical-b"].GetName(),
		"mismatched embedded name must not be rewritten in the order")
	after := fullOrder.MarshalDeterministicVT(nil)
	require.Equal(t, before, after, "processing must not mutate the accepted order bytes")

	// Derived state (LedgerInfo) uses the map key as the canonical name.
	require.NotNil(t, storedInfo)
	require.Equal(t, "canonical-a", storedInfo.GetAccountTypes()["canonical-a"].GetName())
	require.Equal(t, "canonical-b", storedInfo.GetAccountTypes()["canonical-b"].GetName())

	// The created-ledger log uses the map key too, on its OWN messages that
	// share no pointer with the stored LedgerInfo.
	createLog := log.GetCreateLedger()
	require.NotNil(t, createLog)
	require.Equal(t, "canonical-a", createLog.GetAccountTypes()["canonical-a"].GetName())
	require.Equal(t, "canonical-b", createLog.GetAccountTypes()["canonical-b"].GetName())
	require.NotSame(t, storedInfo.GetAccountTypes()["canonical-a"], createLog.GetAccountTypes()["canonical-a"],
		"log must own an independent snapshot, not share the mutable store message")
	require.NotSame(t, order.GetAccountTypes()["canonical-a"], createLog.GetAccountTypes()["canonical-a"],
		"log must not share the order-owned message")
	require.NotSame(t, order.GetAccountTypes()["canonical-a"], storedInfo.GetAccountTypes()["canonical-a"],
		"LedgerInfo must not share the order-owned message")
}

func TestProcessCreateLedger_AlreadyExists(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0, false)
	require.NoError(t, err)

	existingLedger := &commonpb.LedgerInfo{Name: "test-ledger"}
	expectGetLedger(mockStore, domain.LedgerKey{Name: "test-ledger"}, existingLedger.AsReader(), nil)

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

func TestProcessDeleteLedger(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0, false)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	existingLedger := &commonpb.LedgerInfo{Name: "test-ledger"}

	expectGetLedger(mockStore, domain.LedgerKey{Name: "test-ledger"}, existingLedger.AsReader(), nil)
	mockStore.EXPECT().GetDate().Return(now.AsReader())
	expectPutLedger(t, mockStore, domain.LedgerKey{Name: "test-ledger"}, nil)

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
	require.Equal(t, "test-ledger", deleteLedgerLog.GetName())
	require.Equal(t, now, deleteLedgerLog.GetDeletedAt())
}

func TestProcessDeleteLedger_NotFound(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0, false)
	require.NoError(t, err)

	expectGetLedger(mockStore, domain.LedgerKey{Name: "test-ledger"}, nil, domain.ErrNotFound)

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
