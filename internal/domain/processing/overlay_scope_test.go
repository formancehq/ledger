package processing

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// overlayMockStubs bundles a kindStub per accessor kind the overlay
// proxies, plus the parent mock. wireOverlayParent attaches one stub per
// kind to the mock so newOrderOverlayScope can call parent.X() without
// blowing up; tests then read/write through the stubs to assert what the
// overlay flushed (or did not flush) onto the parent.
type overlayMockStubs struct {
	parent *MockScope

	ledgers           *kindStub[domain.LedgerKey, *commonpb.LedgerInfo, commonpb.LedgerInfoReader]
	boundaries        *kindStub[domain.LedgerKey, *raftcmdpb.LedgerBoundaries, raftcmdpb.LedgerBoundariesReader]
	volumes           *kindStub[domain.VolumeKey, *raftcmdpb.VolumePair, raftcmdpb.VolumePairReader]
	accountMetadata   *kindStub[domain.MetadataKey, *commonpb.MetadataValue, commonpb.MetadataValueReader]
	ledgerMetadata    *kindStub[domain.LedgerMetadataKey, *commonpb.MetadataValue, commonpb.MetadataValueReader]
	transactionStates *kindStub[domain.TransactionKey, *commonpb.TransactionState, commonpb.TransactionStateReader]
	transactionRefs   *kindStub[domain.TransactionReferenceKey, *commonpb.TransactionReferenceValue, commonpb.TransactionReferenceValueReader]
	preparedQueries   *kindStub[domain.PreparedQueryKey, *commonpb.PreparedQuery, commonpb.PreparedQueryReader]
	indexes           *kindStub[domain.IndexKey, *commonpb.Index, commonpb.IndexReader]
}

func wireOverlayParent(ctrl *gomock.Controller) *overlayMockStubs {
	s := &overlayMockStubs{
		parent:            NewMockScope(ctrl),
		ledgers:           &kindStub[domain.LedgerKey, *commonpb.LedgerInfo, commonpb.LedgerInfoReader]{},
		boundaries:        &kindStub[domain.LedgerKey, *raftcmdpb.LedgerBoundaries, raftcmdpb.LedgerBoundariesReader]{},
		volumes:           &kindStub[domain.VolumeKey, *raftcmdpb.VolumePair, raftcmdpb.VolumePairReader]{},
		accountMetadata:   &kindStub[domain.MetadataKey, *commonpb.MetadataValue, commonpb.MetadataValueReader]{},
		ledgerMetadata:    &kindStub[domain.LedgerMetadataKey, *commonpb.MetadataValue, commonpb.MetadataValueReader]{},
		transactionStates: &kindStub[domain.TransactionKey, *commonpb.TransactionState, commonpb.TransactionStateReader]{},
		transactionRefs:   &kindStub[domain.TransactionReferenceKey, *commonpb.TransactionReferenceValue, commonpb.TransactionReferenceValueReader]{},
		preparedQueries:   &kindStub[domain.PreparedQueryKey, *commonpb.PreparedQuery, commonpb.PreparedQueryReader]{},
		indexes:           &kindStub[domain.IndexKey, *commonpb.Index, commonpb.IndexReader]{},
	}
	s.parent.EXPECT().Ledgers().Return(s.ledgers).AnyTimes()
	s.parent.EXPECT().Boundaries().Return(s.boundaries).AnyTimes()
	s.parent.EXPECT().Volumes().Return(s.volumes).AnyTimes()
	s.parent.EXPECT().AccountMetadata().Return(s.accountMetadata).AnyTimes()
	s.parent.EXPECT().LedgerMetadata().Return(s.ledgerMetadata).AnyTimes()
	s.parent.EXPECT().TransactionStates().Return(s.transactionStates).AnyTimes()
	s.parent.EXPECT().TransactionReferences().Return(s.transactionRefs).AnyTimes()
	s.parent.EXPECT().PreparedQueries().Return(s.preparedQueries).AnyTimes()
	s.parent.EXPECT().Indexes().Return(s.indexes).AnyTimes()

	return s
}

// TestOrderOverlayScope_ReadYourWritesAcrossCategories pins the
// read-your-writes contract every per-order sub-processor relies on:
// every accessor kind that exposes a Get/Put pair must observe the staged
// value before falling back to the parent. Any new accessor added to the
// overlay needs an entry here so the contract stays exhaustive.
func TestOrderOverlayScope_ReadYourWritesAcrossCategories(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	s := wireOverlayParent(ctrl)
	overlay := newOrderOverlayScope(s.parent)

	// Ledger
	overlay.Ledgers().Put(domain.LedgerKey{Name: "L"}, &commonpb.LedgerInfo{Name: "L"})

	got, err := overlay.Ledgers().Get(domain.LedgerKey{Name: "L"})
	require.NoError(t, err)
	require.Equal(t, "L", got.GetName())

	// Boundaries
	overlay.Boundaries().Put(domain.LedgerKey{Name: "L"}, &raftcmdpb.LedgerBoundaries{NextTransactionId: 7})

	br, err := overlay.Boundaries().Get(domain.LedgerKey{Name: "L"})
	require.NoError(t, err)
	require.Equal(t, uint64(7), br.GetNextTransactionId())

	// Volume
	vk := domain.NewVolumeKey("L", "alice", "USD")
	overlay.Volumes().Put(vk, &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(50),
		Output: commonpb.NewUint256FromUint64(20),
	})

	vr, err := overlay.Volumes().Get(vk)
	require.NoError(t, err)
	require.Equal(t, uint64(50), vr.GetInput().GetV0())
	require.Equal(t, uint64(20), vr.GetOutput().GetV0())

	// Account metadata: Put then Get.
	mk := domain.MetadataKey{AccountKey: domain.AccountKey{LedgerName: "L", Account: "alice"}, Key: "k"}
	overlay.AccountMetadata().Put(mk, commonpb.NewStringValue("v1"))

	mgot, err := overlay.AccountMetadata().Get(mk)
	require.NoError(t, err)
	require.Equal(t, "v1", mgot.Mutate().GetStringValue())

	// Account metadata: Delete then Get -> ErrNotFound.
	require.NoError(t, overlay.AccountMetadata().Delete(mk))

	_, err = overlay.AccountMetadata().Get(mk)
	require.ErrorIs(t, err, domain.ErrNotFound)

	// Ledger metadata
	lmk := domain.LedgerMetadataKey{LedgerName: "L", Key: "k"}
	overlay.LedgerMetadata().Put(lmk, commonpb.NewStringValue("v1"))

	lmgot, err := overlay.LedgerMetadata().Get(lmk)
	require.NoError(t, err)
	require.Equal(t, "v1", lmgot.Mutate().GetStringValue())

	require.NoError(t, overlay.LedgerMetadata().Delete(lmk))

	_, err = overlay.LedgerMetadata().Get(lmk)
	require.ErrorIs(t, err, domain.ErrNotFound)

	// Reverted
	tk := domain.TransactionKey{LedgerName: "L", ID: 1}
	overlay.PutReverted(tk, true)

	r, err := overlay.GetReverted(tk)
	require.NoError(t, err)
	require.True(t, r)

	// Transaction reference
	trk := domain.TransactionReferenceKey{LedgerName: "L", Reference: "ref"}
	overlay.TransactionReferences().Put(trk, &commonpb.TransactionReferenceValue{TransactionId: 7})

	trGot, err := overlay.TransactionReferences().Get(trk)
	require.NoError(t, err)
	require.Equal(t, uint64(7), trGot.GetTransactionId())

	// Transaction state
	tsk := domain.TransactionKey{LedgerName: "L", ID: 7}
	overlay.TransactionStates().Put(tsk, &commonpb.TransactionState{CreatedByLog: 42})

	stGot, err := overlay.TransactionStates().Get(tsk)
	require.NoError(t, err)
	require.Equal(t, uint64(42), stGot.GetCreatedByLog())

	// Prepared queries
	pqk := domain.PreparedQueryKey{LedgerName: "L", Name: "q1"}
	overlay.PreparedQueries().Put(pqk, &commonpb.PreparedQuery{Name: "q1"})

	pqGot, err := overlay.PreparedQueries().Get(pqk)
	require.NoError(t, err)
	require.Equal(t, "q1", pqGot.GetName())

	// Indexes
	ik := domain.IndexKey{LedgerName: "L", Canonical: "canon"}
	overlay.Indexes().Put(ik, &commonpb.Index{})

	_, err = overlay.Indexes().Get(ik)
	require.NoError(t, err)

	// No Commit. The kindStubs receive no expectPut/expectDelete — any
	// flush leak would surface as an unexpected Put/Delete via the
	// stub's bookkeeping.
}

// TestOrderOverlayScope_RollbackOnNoCommit verifies the rollback semantics:
// if Commit is never called, the parent Scope sees zero writes.
func TestOrderOverlayScope_RollbackOnNoCommit(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	s := wireOverlayParent(ctrl)
	s.parent.EXPECT().GetNextSequenceID().Return(uint64(100)).AnyTimes()
	s.parent.EXPECT().GetNextLedgerID().Return(uint32(5)).AnyTimes()
	s.parent.EXPECT().GetNextChapterID().Return(uint64(2)).AnyTimes()
	s.parent.EXPECT().GetNextQueryCheckpointID().Return(uint64(0)).AnyTimes()

	overlay := newOrderOverlayScope(s.parent)
	overlay.Ledgers().Put(domain.LedgerKey{Name: "L"}, &commonpb.LedgerInfo{Name: "L"})
	overlay.Boundaries().Put(domain.LedgerKey{Name: "L"}, &raftcmdpb.LedgerBoundaries{})
	overlay.IncrementNextSequenceID()
	overlay.IncrementNextLedgerID()

	// No Commit. The kindStubs received no expectPut and the mock got no
	// Increment* expectation; any leak fails the test.
}

// TestOrderOverlayScope_CommitFlushesEveryCategory verifies the success
// path: every staged write is replayed onto the parent Scope.
func TestOrderOverlayScope_CommitFlushesEveryCategory(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	s := wireOverlayParent(ctrl)
	s.parent.EXPECT().GetNextSequenceID().Return(uint64(100)).AnyTimes()
	s.parent.EXPECT().GetNextLedgerID().Return(uint32(5)).AnyTimes()
	s.parent.EXPECT().GetNextChapterID().Return(uint64(2)).AnyTimes()
	s.parent.EXPECT().GetNextQueryCheckpointID().Return(uint64(0)).AnyTimes()
	s.parent.EXPECT().PutReverted(gomock.Any(), true)
	s.parent.EXPECT().IncrementNextSequenceID().Return(uint64(101)).Times(2)
	s.parent.EXPECT().IncrementNextLedgerID().Return(uint32(6))
	s.parent.EXPECT().IncrementNextChapterID().Return(uint64(3))
	s.parent.EXPECT().IncrementNextQueryCheckpointID().Return(uint64(1))

	lk := domain.LedgerKey{Name: "L"}
	vk := domain.NewVolumeKey("L", "alice", "USD")
	mk := domain.MetadataKey{AccountKey: domain.AccountKey{LedgerName: "L", Account: "alice"}, Key: "k"}
	lmk := domain.LedgerMetadataKey{LedgerName: "L", Key: "k"}
	tk := domain.TransactionKey{LedgerName: "L", ID: 1}
	trk := domain.TransactionReferenceKey{LedgerName: "L", Reference: "ref"}
	tsk := domain.TransactionKey{LedgerName: "L", ID: 7}
	pqk := domain.PreparedQueryKey{LedgerName: "L", Name: "q1"}
	ik := domain.IndexKey{LedgerName: "L", Canonical: "canon"}

	s.ledgers.expectPut(t, lk, nil)
	s.boundaries.expectPut(t, lk, nil)
	s.volumes.expectPut(t, vk, nil)
	s.accountMetadata.expectPut(t, mk, nil)
	s.ledgerMetadata.expectPut(t, lmk, nil)
	s.transactionRefs.expectPut(t, trk, nil)
	s.transactionStates.expectPut(t, tsk, nil)
	s.preparedQueries.expectPut(t, pqk, nil)
	s.indexes.expectPut(t, ik, nil)

	overlay := newOrderOverlayScope(s.parent)
	overlay.Ledgers().Put(lk, &commonpb.LedgerInfo{Name: "L"})
	overlay.Boundaries().Put(lk, &raftcmdpb.LedgerBoundaries{NextTransactionId: 7})
	overlay.Volumes().Put(vk, &raftcmdpb.VolumePair{})
	overlay.AccountMetadata().Put(mk, commonpb.NewStringValue("v1"))
	overlay.LedgerMetadata().Put(lmk, commonpb.NewStringValue("v1"))
	overlay.PutReverted(tk, true)
	overlay.TransactionReferences().Put(trk, &commonpb.TransactionReferenceValue{TransactionId: 7})
	overlay.TransactionStates().Put(tsk, &commonpb.TransactionState{})
	overlay.PreparedQueries().Put(pqk, &commonpb.PreparedQuery{Name: "q1"})
	overlay.Indexes().Put(ik, &commonpb.Index{})
	overlay.IncrementNextSequenceID()
	overlay.IncrementNextSequenceID()
	overlay.IncrementNextLedgerID()
	overlay.IncrementNextChapterID()
	overlay.IncrementNextQueryCheckpointID()

	require.NoError(t, overlay.Commit())
}

// TestOrderOverlayScope_CounterDeltasMonotonicWithinOrder verifies that a
// sub-processor incrementing a counter twice observes a monotonic sequence
// (without touching the parent before Commit). Captures the base lazily.
func TestOrderOverlayScope_CounterDeltasMonotonicWithinOrder(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	s := wireOverlayParent(ctrl)
	s.parent.EXPECT().GetNextSequenceID().Return(uint64(100)).Times(1)
	s.parent.EXPECT().GetNextLedgerID().Return(uint32(5)).Times(1)
	s.parent.EXPECT().GetNextChapterID().Return(uint64(2)).Times(1)
	s.parent.EXPECT().GetNextQueryCheckpointID().Return(uint64(0)).Times(1)

	overlay := newOrderOverlayScope(s.parent)

	require.Equal(t, uint64(100), overlay.IncrementNextSequenceID())
	require.Equal(t, uint64(101), overlay.IncrementNextSequenceID())
	require.Equal(t, uint64(102), overlay.GetNextSequenceID())
}

// TestOrderOverlayScope_DeleteOverridesPriorPut models the
// Put-then-Delete-in-same-order pattern: the delete must be staged with
// precedence so a subsequent Get returns ErrNotFound and Commit replays
// the delete (no Put on the parent).
func TestOrderOverlayScope_DeleteOverridesPriorPut(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	s := wireOverlayParent(ctrl)
	s.parent.EXPECT().GetNextSequenceID().Return(uint64(100)).AnyTimes()
	s.parent.EXPECT().GetNextLedgerID().Return(uint32(5)).AnyTimes()
	s.parent.EXPECT().GetNextChapterID().Return(uint64(2)).AnyTimes()
	s.parent.EXPECT().GetNextQueryCheckpointID().Return(uint64(0)).AnyTimes()

	mk := domain.MetadataKey{AccountKey: domain.AccountKey{LedgerName: "L", Account: "alice"}, Key: "k"}
	s.accountMetadata.expectDelete(t, mk)

	overlay := newOrderOverlayScope(s.parent)
	overlay.AccountMetadata().Put(mk, commonpb.NewStringValue("v1"))
	require.NoError(t, overlay.AccountMetadata().Delete(mk))

	_, err := overlay.AccountMetadata().Get(mk)
	require.ErrorIs(t, err, domain.ErrNotFound)

	require.NoError(t, overlay.Commit())
}
