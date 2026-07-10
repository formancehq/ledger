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

// failOnAnyParentWrite installs a catch-all Put/Delete hook on every parent
// kindStub that fails the test on ANY write. Without it, kindStub.Put /
// Delete silently no-op for keys that were never registered via
// expectPut/expectDelete, so a rollback regression where the overlay wrote
// straight through to the parent (instead of buffering) would go undetected
// (NumaryBot finding: rollback leak-detection is illusory without a global
// fail hook). The buffered path never touches the parent until Commit, so a
// rollback test that never commits must see zero parent writes.
func (s *overlayMockStubs) failOnAnyParentWrite(t *testing.T) {
	t.Helper()

	s.ledgers.onPut(func(k domain.LedgerKey, _ *commonpb.LedgerInfo) {
		t.Errorf("rollback leak: parent Ledgers().Put(%v) called", k)
	})
	s.ledgers.onDelete(func(k domain.LedgerKey) { t.Errorf("rollback leak: parent Ledgers().Delete(%v) called", k) })
	s.boundaries.onPut(func(k domain.LedgerKey, _ *raftcmdpb.LedgerBoundaries) {
		t.Errorf("rollback leak: parent Boundaries().Put(%v) called", k)
	})
	s.boundaries.onDelete(func(k domain.LedgerKey) { t.Errorf("rollback leak: parent Boundaries().Delete(%v) called", k) })
	s.volumes.onPut(func(k domain.VolumeKey, _ *raftcmdpb.VolumePair) {
		t.Errorf("rollback leak: parent Volumes().Put(%v) called", k)
	})
	s.volumes.onDelete(func(k domain.VolumeKey) { t.Errorf("rollback leak: parent Volumes().Delete(%v) called", k) })
	s.accountMetadata.onPut(func(k domain.MetadataKey, _ *commonpb.MetadataValue) {
		t.Errorf("rollback leak: parent AccountMetadata().Put(%v) called", k)
	})
	s.accountMetadata.onDelete(func(k domain.MetadataKey) {
		t.Errorf("rollback leak: parent AccountMetadata().Delete(%v) called", k)
	})
	s.ledgerMetadata.onPut(func(k domain.LedgerMetadataKey, _ *commonpb.MetadataValue) {
		t.Errorf("rollback leak: parent LedgerMetadata().Put(%v) called", k)
	})
	s.ledgerMetadata.onDelete(func(k domain.LedgerMetadataKey) {
		t.Errorf("rollback leak: parent LedgerMetadata().Delete(%v) called", k)
	})
	s.transactionStates.onPut(func(k domain.TransactionKey, _ *commonpb.TransactionState) {
		t.Errorf("rollback leak: parent TransactionStates().Put(%v) called", k)
	})
	s.transactionStates.onDelete(func(k domain.TransactionKey) {
		t.Errorf("rollback leak: parent TransactionStates().Delete(%v) called", k)
	})
	s.transactionRefs.onPut(func(k domain.TransactionReferenceKey, _ *commonpb.TransactionReferenceValue) {
		t.Errorf("rollback leak: parent TransactionReferences().Put(%v) called", k)
	})
	s.transactionRefs.onDelete(func(k domain.TransactionReferenceKey) {
		t.Errorf("rollback leak: parent TransactionReferences().Delete(%v) called", k)
	})
	s.preparedQueries.onPut(func(k domain.PreparedQueryKey, _ *commonpb.PreparedQuery) {
		t.Errorf("rollback leak: parent PreparedQueries().Put(%v) called", k)
	})
	s.preparedQueries.onDelete(func(k domain.PreparedQueryKey) {
		t.Errorf("rollback leak: parent PreparedQueries().Delete(%v) called", k)
	})
	s.indexes.onPut(func(k domain.IndexKey, _ *commonpb.Index) {
		t.Errorf("rollback leak: parent Indexes().Put(%v) called", k)
	})
	s.indexes.onDelete(func(k domain.IndexKey) { t.Errorf("rollback leak: parent Indexes().Delete(%v) called", k) })
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
	// This test never commits: reads must resolve against the staged
	// buffer, and no write may reach the parent.
	s.failOnAnyParentWrite(t)
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

	// No Commit. The failOnAnyParentWrite catch-all hook fails the test on
	// any Put/Delete that reaches the parent, so a write-through leak is
	// caught even for keys no expectPut/expectDelete registered.
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

	// Any write that reaches the parent before Commit is a rollback leak.
	s.failOnAnyParentWrite(t)

	overlay := newOrderOverlayScope(s.parent)
	overlay.Ledgers().Put(domain.LedgerKey{Name: "L"}, &commonpb.LedgerInfo{Name: "L"})
	overlay.Boundaries().Put(domain.LedgerKey{Name: "L"}, &raftcmdpb.LedgerBoundaries{})
	overlay.IncrementNextSequenceID()
	overlay.IncrementNextLedgerID()

	// No Commit. The catch-all parent write hook fails the test on any
	// leak; the mock got no Increment* expectation so a counter leak also
	// fails.
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
