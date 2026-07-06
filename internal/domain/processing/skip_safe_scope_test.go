package processing

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// TestSkipSafeScope_TrapsNonBufferedMutations pins the enforcement
// contract: every mutator whose effect the overlay does NOT buffer must
// panic AND, if delegation ever regressed, would trip gomock's
// unexpected-call failure. Two independent tripwires:
//
//   - require.PanicsWithValue on each call catches the loud-fail path
//     (trapUnbuffered's panic — the primary enforcement in non-Antithesis
//     builds where assert.Unreachable is a no-op).
//   - The MockScope has ZERO expectations for these methods, so a future
//     regression that turned the trap into a passthrough would surface as
//     a gomock "unexpected call" on top of the missing panic.
func TestSkipSafeScope_TrapsNonBufferedMutations(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Deliberately empty MockScope — any call reaching it fails the test.
	parent := NewMockScope(ctrl)
	trap := newSkipSafeScope(parent)

	cases := []struct {
		name string
		call func()
	}{
		{"AddSigningKey", func() { trap.AddSigningKey("k1", []byte("pub"), "parent") }},
		{"RemoveSigningKey", func() { trap.RemoveSigningKey("k1") }},
		{"SetRequireSignatures", func() { trap.SetRequireSignatures(true) }},
		{"SetMaintenanceMode", func() { trap.SetMaintenanceMode(true) }},
		{"SetCurrentOpenChapter", func() { trap.SetCurrentOpenChapter(&commonpb.Chapter{}) }},
		{"AddClosingChapter", func() { trap.AddClosingChapter(&commonpb.Chapter{}) }},
		{"RemoveClosingChapter", func() { trap.RemoveClosingChapter(7) }},
		{"UpdateChapter", func() { trap.UpdateChapter(&commonpb.Chapter{}) }},
		{"PutNumscript", func() { trap.PutNumscript("L", &commonpb.NumscriptInfo{}) }},
		{"DeleteNumscriptLatest", func() { trap.DeleteNumscriptLatest("L", "n") }},
		{"SaveQueryCheckpoint", func() { trap.SaveQueryCheckpoint(&raftcmdpb.QueryCheckpointState{}) }},
		{"DeleteQueryCheckpoint", func() { trap.DeleteQueryCheckpoint(7) }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			require.PanicsWithValue(
				t,
				"skip_safe_scope: skippable order attempted "+tc.name+" — the overlay does not buffer this mutation, and letting it through would leak past a rollback",
				tc.call,
				"trap for %s must panic loudly (assert.Unreachable is a no-op in non-Antithesis builds)",
				tc.name,
			)
		})
	}
}

// TestSkipSafeScope_ReadsAndBufferedWritesPassThrough verifies that
// safe methods (reads, and accessors buffered by the overlay) DO
// delegate to the inner scope — a bug that traps them by mistake would
// break every legitimate skip-tolerant order.
func TestSkipSafeScope_ReadsAndBufferedWritesPassThrough(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	s := wireOverlayParent(ctrl)
	s.parent.EXPECT().GetNextSequenceID().Return(uint64(100)).AnyTimes()
	s.parent.EXPECT().GetNextLedgerID().Return(uint32(5)).AnyTimes()
	s.parent.EXPECT().GetNextChapterID().Return(uint64(2)).AnyTimes()
	s.parent.EXPECT().GetNextQueryCheckpointID().Return(uint64(0)).AnyTimes()
	s.parent.EXPECT().GetNextAuditSequenceID().Return(uint64(50)).Times(1)

	overlay := newOrderOverlayScope(s.parent)
	trap := newSkipSafeScope(overlay)

	// Buffered accessor writes route through overlay's staged accessor.
	// Read-your-write across the trap layer.
	lk := domain.LedgerKey{Name: "L"}
	trap.Ledgers().Put(lk, &commonpb.LedgerInfo{Name: "L"})

	got, err := trap.Ledgers().Get(lk)
	require.NoError(t, err)
	require.Equal(t, "L", got.GetName())

	// Buffered counter reads/increments delegate through overlay.
	require.Equal(t, uint64(100), trap.GetNextSequenceID())
	require.Equal(t, uint64(100), trap.IncrementNextSequenceID())
	require.Equal(t, uint64(101), trap.GetNextSequenceID())

	// Non-buffered counter reads (audit seq is monotonic, read-only via
	// Scope) pass through.
	require.Equal(t, uint64(50), trap.GetNextAuditSequenceID())

	// PutReverted is buffered.
	tk := domain.TransactionKey{LedgerName: "L", ID: 1}
	trap.PutReverted(tk, true)

	reverted, err := trap.GetReverted(tk)
	require.NoError(t, err)
	require.True(t, reverted)
}

// TestSkipSafeScope_RollbackKeepsParentUntouched wires the full
// overlay+trap stack and verifies that a skip-tolerant order which
// mutates via the buffered surface leaves NO residue on the parent
// when Commit is skipped. Buffered mutations only — non-buffered ones
// are covered by TestSkipSafeScope_TrapsNonBufferedMutations (which
// asserts the panic contract in isolation).
func TestSkipSafeScope_RollbackKeepsParentUntouched(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	s := wireOverlayParent(ctrl)
	s.parent.EXPECT().GetNextSequenceID().Return(uint64(100)).AnyTimes()
	s.parent.EXPECT().GetNextLedgerID().Return(uint32(5)).AnyTimes()
	s.parent.EXPECT().GetNextChapterID().Return(uint64(2)).AnyTimes()
	s.parent.EXPECT().GetNextQueryCheckpointID().Return(uint64(0)).AnyTimes()

	// Any Increment* / Put on the parent would fail the test — no
	// expectation registered. The stubs likewise have no expectPut.
	overlay := newOrderOverlayScope(s.parent)
	trap := newSkipSafeScope(overlay)

	// Buffered mutations — end up in the overlay's staged buffer.
	trap.Ledgers().Put(domain.LedgerKey{Name: "L"}, &commonpb.LedgerInfo{Name: "L"})
	trap.Boundaries().Put(domain.LedgerKey{Name: "L"}, &raftcmdpb.LedgerBoundaries{})
	trap.PutReverted(domain.TransactionKey{LedgerName: "L", ID: 1}, true)
	trap.IncrementNextSequenceID()
	trap.IncrementNextLedgerID()
	trap.IncrementNextChapterID()
	trap.IncrementNextQueryCheckpointID()

	// No Commit — overlay is dropped and every buffered mutation stays in
	// the overlay maps. Parent Scope sees nothing.
}
