package processing

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// TestOrderOverlayScope_ReadYourWritesAcrossCategories pins the
// read-your-writes contract every per-order sub-processor relies on:
// every category that exposes a Get/Put pair must observe the staged
// value before falling back to the parent. Any new category added to the
// overlay needs an entry here so the contract stays exhaustive.
func TestOrderOverlayScope_ReadYourWritesAcrossCategories(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	parent := NewMockScope(ctrl)
	overlay := newOrderOverlayScope(parent)

	// Ledger
	info := &commonpb.LedgerInfo{Name: "L"}
	overlay.PutLedger("L", info)

	got, err := overlay.GetLedger("L")
	require.NoError(t, err)
	require.Same(t, info, got)

	// Boundaries
	b := &raftcmdpb.LedgerBoundaries{NextTransactionId: 7}
	overlay.PutBoundaries("L", b)

	br, err := overlay.GetBoundaries("L")
	require.NoError(t, err)
	require.Equal(t, uint64(7), br.GetNextTransactionId())

	// Volume
	vk := domain.NewVolumeKey("L", "alice", "USD")
	vol := &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(50),
		Output: commonpb.NewUint256FromUint64(20),
	}
	overlay.PutVolume(vk, vol)

	vr, err := overlay.GetVolume(vk)
	require.NoError(t, err)
	require.Equal(t, uint64(50), vr.GetInput().GetV0())
	require.Equal(t, uint64(20), vr.GetOutput().GetV0())

	// Account metadata: Put then Get.
	mk := domain.MetadataKey{AccountKey: domain.AccountKey{LedgerName: "L", Account: "alice"}, Key: "k"}
	mv := commonpb.NewStringValue("v1")
	overlay.PutAccountMetadata(mk, mv)

	mgot, err := overlay.GetAccountMetadata(mk)
	require.NoError(t, err)
	require.Same(t, mv, mgot)

	// Account metadata: Delete then Get -> ErrNotFound.
	overlay.DeleteAccountMetadata(mk)

	_, err = overlay.GetAccountMetadata(mk)
	require.ErrorIs(t, err, domain.ErrNotFound)

	// Ledger metadata
	lmk := domain.LedgerMetadataKey{LedgerName: "L", Key: "k"}
	overlay.PutLedgerMetadata(lmk, mv)

	lmgot, err := overlay.GetLedgerMetadata(lmk)
	require.NoError(t, err)
	require.Same(t, mv, lmgot)

	overlay.DeleteLedgerMetadata(lmk)

	_, err = overlay.GetLedgerMetadata(lmk)
	require.ErrorIs(t, err, domain.ErrNotFound)

	// Reverted
	tk := domain.TransactionKey{LedgerName: "L", ID: 1}
	overlay.PutReverted(tk, true)

	r, err := overlay.GetReverted(tk)
	require.NoError(t, err)
	require.True(t, r)

	// Idempotency
	ik := domain.IdempotencyKey{Key: "idem"}
	idVal := &commonpb.IdempotencyKeyValue{}
	overlay.PutIdempotencyKey(ik, idVal)

	idGot, err := overlay.GetIdempotencyKey(ik)
	require.NoError(t, err)
	require.Same(t, idVal, idGot)

	// Transaction reference
	trk := domain.TransactionReferenceKey{LedgerName: "L", Reference: "ref"}
	trv := &commonpb.TransactionReferenceValue{TransactionId: 7}
	overlay.PutTransactionReference(trk, trv)

	trGot, err := overlay.GetTransactionReference(trk)
	require.NoError(t, err)
	require.Same(t, trv, trGot)

	// Transaction state
	tsk := domain.TransactionKey{LedgerName: "L", ID: 7}
	st := &commonpb.TransactionState{}
	overlay.PutTransactionState(tsk, st)

	stGot, err := overlay.GetTransactionState(tsk)
	require.NoError(t, err)
	require.Same(t, stGot, st)

	// Parent must NOT see any of these writes yet — no EXPECT() declared
	// against the mock means a call here would fail the test.
}

// TestOrderOverlayScope_RollbackOnNoCommit verifies the rollback semantics:
// if Commit is never called, the parent Scope sees zero writes. The mock
// records no EXPECT() against Put*/Increment*, so any leak would fail the
// gomock controller's Finish() call.
func TestOrderOverlayScope_RollbackOnNoCommit(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	parent := NewMockScope(ctrl)
	// Counter Gets are passed through; capture the base once.
	parent.EXPECT().GetNextSequenceID().Return(uint64(100)).AnyTimes()
	parent.EXPECT().GetNextLedgerID().Return(uint32(5)).AnyTimes()
	parent.EXPECT().GetNextChapterID().Return(uint64(2)).AnyTimes()
	parent.EXPECT().GetNextQueryCheckpointID().Return(uint64(0)).AnyTimes()

	overlay := newOrderOverlayScope(parent)
	overlay.PutLedger("L", &commonpb.LedgerInfo{Name: "L"})
	overlay.PutBoundaries("L", &raftcmdpb.LedgerBoundaries{})
	overlay.IncrementNextSequenceID()
	overlay.IncrementNextLedgerID()

	// No Commit. parent.EXPECT() declared no Put*/Increment*; the gomock
	// controller will fail if any was called.
}

// TestOrderOverlayScope_CommitFlushesEveryCategory verifies the success
// path: every staged write is replayed onto the parent Scope, in any order
// (the parent does not care, as long as every category lands).
func TestOrderOverlayScope_CommitFlushesEveryCategory(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	parent := NewMockScope(ctrl)
	parent.EXPECT().GetNextSequenceID().Return(uint64(100)).AnyTimes()
	parent.EXPECT().GetNextLedgerID().Return(uint32(5)).AnyTimes()
	parent.EXPECT().GetNextChapterID().Return(uint64(2)).AnyTimes()
	parent.EXPECT().GetNextQueryCheckpointID().Return(uint64(0)).AnyTimes()

	info := &commonpb.LedgerInfo{Name: "L"}
	b := &raftcmdpb.LedgerBoundaries{NextTransactionId: 7}
	vk := domain.NewVolumeKey("L", "alice", "USD")
	vol := &raftcmdpb.VolumePair{}
	mk := domain.MetadataKey{AccountKey: domain.AccountKey{LedgerName: "L", Account: "alice"}, Key: "k"}
	mv := commonpb.NewStringValue("v1")
	lmk := domain.LedgerMetadataKey{LedgerName: "L", Key: "k"}
	tk := domain.TransactionKey{LedgerName: "L", ID: 1}
	ik := domain.IdempotencyKey{Key: "idem"}
	idVal := &commonpb.IdempotencyKeyValue{}
	trk := domain.TransactionReferenceKey{LedgerName: "L", Reference: "ref"}
	trv := &commonpb.TransactionReferenceValue{TransactionId: 7}
	tsk := domain.TransactionKey{LedgerName: "L", ID: 7}
	st := &commonpb.TransactionState{}

	// Expectations on parent: every staged write must be replayed once.
	parent.EXPECT().PutLedger("L", info)
	parent.EXPECT().PutBoundaries("L", b)
	parent.EXPECT().PutVolume(vk, vol)
	parent.EXPECT().PutAccountMetadata(mk, mv)
	parent.EXPECT().PutLedgerMetadata(lmk, mv)
	parent.EXPECT().PutReverted(tk, true)
	parent.EXPECT().PutIdempotencyKey(ik, idVal)
	parent.EXPECT().PutTransactionReference(trk, trv)
	parent.EXPECT().PutTransactionState(tsk, st)
	parent.EXPECT().IncrementNextSequenceID().Return(uint64(101)).Times(2)
	parent.EXPECT().IncrementNextLedgerID().Return(uint32(6))
	parent.EXPECT().IncrementNextChapterID().Return(uint64(3))
	parent.EXPECT().IncrementNextQueryCheckpointID().Return(uint64(1))

	overlay := newOrderOverlayScope(parent)
	overlay.PutLedger("L", info)
	overlay.PutBoundaries("L", b)
	overlay.PutVolume(vk, vol)
	overlay.PutAccountMetadata(mk, mv)
	overlay.PutLedgerMetadata(lmk, mv)
	overlay.PutReverted(tk, true)
	overlay.PutIdempotencyKey(ik, idVal)
	overlay.PutTransactionReference(trk, trv)
	overlay.PutTransactionState(tsk, st)
	overlay.IncrementNextSequenceID()
	overlay.IncrementNextSequenceID()
	overlay.IncrementNextLedgerID()
	overlay.IncrementNextChapterID()
	overlay.IncrementNextQueryCheckpointID()

	overlay.Commit()
}

// TestOrderOverlayScope_CounterDeltasMonotonicWithinOrder verifies that a
// sub-processor incrementing a counter twice observes a monotonic sequence
// (without touching the parent before Commit). Captures the base lazily.
func TestOrderOverlayScope_CounterDeltasMonotonicWithinOrder(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	parent := NewMockScope(ctrl)
	parent.EXPECT().GetNextSequenceID().Return(uint64(100)).Times(1)
	parent.EXPECT().GetNextLedgerID().Return(uint32(5)).Times(1)
	parent.EXPECT().GetNextChapterID().Return(uint64(2)).Times(1)
	parent.EXPECT().GetNextQueryCheckpointID().Return(uint64(0)).Times(1)

	overlay := newOrderOverlayScope(parent)

	require.Equal(t, uint64(100), overlay.IncrementNextSequenceID())
	require.Equal(t, uint64(101), overlay.IncrementNextSequenceID())
	require.Equal(t, uint64(102), overlay.GetNextSequenceID())
}

// TestOrderOverlayScope_DeleteOverridesPriorPut models the
// Put-then-Delete-in-same-order pattern: the delete must be staged with
// precedence so a subsequent Get returns ErrNotFound and Commit replays
// the delete, not the prior put.
func TestOrderOverlayScope_DeleteOverridesPriorPut(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	parent := NewMockScope(ctrl)
	mk := domain.MetadataKey{AccountKey: domain.AccountKey{LedgerName: "L", Account: "alice"}, Key: "k"}
	mv := commonpb.NewStringValue("v1")

	parent.EXPECT().DeleteAccountMetadata(mk)

	overlay := newOrderOverlayScope(parent)
	overlay.PutAccountMetadata(mk, mv)
	overlay.DeleteAccountMetadata(mk)

	_, err := overlay.GetAccountMetadata(mk)
	require.ErrorIs(t, err, domain.ErrNotFound)

	overlay.Commit()
}
