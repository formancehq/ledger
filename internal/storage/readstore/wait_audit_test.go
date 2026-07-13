package readstore

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// writeAuditProgress commits the audit cursor to seq through a batch.
func writeAuditProgress(t *testing.T, s *Store, seq uint64) {
	t.Helper()
	batch := s.NewBatch()
	require.NoError(t, s.WriteAuditProgress(batch, seq))
	require.NoError(t, batch.Commit())
}

// TestWaitForAuditSequenceFastPath returns immediately when the audit cursor is
// already at or beyond the requested sequence.
func TestWaitForAuditSequenceFastPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		progress uint64
		minSeq   uint64
	}{
		{name: "equal", progress: 5, minSeq: 5},
		{name: "ahead", progress: 10, minSeq: 5},
		{name: "zero bound with zero progress", progress: 0, minSeq: 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newTestStore(t)
			if tc.progress > 0 {
				writeAuditProgress(t, s, tc.progress)
			}

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			require.NoError(t, s.WaitForAuditSequence(ctx, tc.minSeq))
		})
	}
}

// TestWaitForAuditSequenceBlocksUntilProgress verifies a waiter blocks while the
// audit cursor lags and unblocks once the cursor reaches the target and
// NotifyProgress fires — the wakeup path the audit indexer drives.
func TestWaitForAuditSequenceBlocksUntilProgress(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	waitErr := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		waitErr <- s.WaitForAuditSequence(ctx, 7)
	}()

	// The waiter must not return yet — the cursor is still 0.
	select {
	case err := <-waitErr:
		t.Fatalf("WaitForAuditSequence returned before progress: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	// A commit below the target must not unblock the waiter.
	writeAuditProgress(t, s, 3)
	s.NotifyProgress()

	select {
	case err := <-waitErr:
		t.Fatalf("WaitForAuditSequence returned before reaching target: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	// Reaching the target unblocks the waiter, exactly as processBatch does
	// (WriteAuditProgress commit followed by NotifyProgress).
	writeAuditProgress(t, s, 7)
	s.NotifyProgress()

	select {
	case err := <-waitErr:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("WaitForAuditSequence did not return after progress reached target")
	}
}

// TestWaitForAuditSequenceContextCancel unblocks promptly on context
// cancellation and returns the context error. Exercises the missed-wakeup fix:
// the cancellation broadcast is delivered while holding progressMu.
func TestWaitForAuditSequenceContextCancel(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	ctx, cancel := context.WithCancel(context.Background())

	waitErr := make(chan error, 1)
	go func() {
		waitErr <- s.WaitForAuditSequence(ctx, 100)
	}()

	select {
	case err := <-waitErr:
		t.Fatalf("WaitForAuditSequence returned before cancel: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	cancel()

	select {
	case err := <-waitErr:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("WaitForAuditSequence did not return after context cancel")
	}
}

// TestLastIndexedAuditSequence reflects the committed audit cursor and is
// distinct from LastIndexedSequence (the log cursor).
func TestLastIndexedAuditSequence(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	got, err := s.LastIndexedAuditSequence()
	require.NoError(t, err)
	require.Zero(t, got)

	writeAuditProgress(t, s, 42)

	got, err = s.LastIndexedAuditSequence()
	require.NoError(t, err)
	require.Equal(t, uint64(42), got)

	// The log-index cursor is independent and must remain untouched.
	logSeq, err := s.LastIndexedSequence()
	require.NoError(t, err)
	require.Zero(t, logSeq)
}
