package readstore

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func TestProjectionRaftProgressRoundTripAndSnapshot(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)
	before := s.NewSnapshot()
	t.Cleanup(func() { _ = before.Close() })

	batch := s.NewBatch()
	require.NoError(t, s.WriteProgress(batch, 5))
	require.NoError(t, s.WriteRaftProgress(batch, 12))
	require.NoError(t, s.WriteAuditProgress(batch, 6))
	require.NoError(t, s.WriteAuditRaftProgress(batch, 12))
	require.NoError(t, batch.Commit())

	readProgress, err := s.ReadRaftProgress()
	require.NoError(t, err)
	require.Equal(t, uint64(12), readProgress)
	auditProgress, err := s.ReadAuditRaftProgress()
	require.NoError(t, err)
	require.Equal(t, uint64(12), auditProgress)

	readProgress, err = s.ReadRaftProgressFrom(before)
	require.NoError(t, err)
	require.Zero(t, readProgress, "a snapshot predating publication must not observe the certificate")
	auditProgress, err = s.ReadAuditRaftProgressFrom(before)
	require.NoError(t, err)
	require.Zero(t, auditProgress, "audit certification must obey the same snapshot boundary")

	after := s.NewSnapshot()
	t.Cleanup(func() { _ = after.Close() })
	nativeProgress, err := s.ReadProgressFrom(after)
	require.NoError(t, err)
	require.Equal(t, uint64(5), nativeProgress)
	readProgress, err = s.ReadRaftProgressFrom(after)
	require.NoError(t, err)
	require.Equal(t, uint64(12), readProgress)
	auditProgress, err = s.ReadAuditRaftProgressFrom(after)
	require.NoError(t, err)
	require.Equal(t, uint64(12), auditProgress)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, s.WaitForRaftProgress(ctx, 12))
	require.NoError(t, s.WaitForAuditRaftProgress(ctx, 12))
}

func TestProjectionRaftProgressWaitCancellation(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	require.ErrorIs(t, s.WaitForRaftProgress(ctx, 1), context.Canceled)
	require.ErrorIs(t, s.WaitForAuditRaftProgress(ctx, 1), context.Canceled)
}

func TestAuditRaftProgressRejectsUnavailableProjection(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)
	disabled, rebuilding := s.AuditProjectionState()
	require.False(t, disabled)
	require.False(t, rebuilding)

	s.SetAuditProjectionState(false, true)
	disabled, rebuilding, generation := s.AuditProjectionStateWithGeneration()
	require.False(t, disabled)
	require.True(t, rebuilding)
	require.Equal(t, uint64(1), generation)
	require.ErrorIs(t, s.WaitForAuditRaftProgress(context.Background(), 1), ErrAuditProjectionUnavailable)

	// Repeating the same state does not create a fictitious lifecycle change.
	s.SetAuditProjectionState(false, true)
	_, _, sameGeneration := s.AuditProjectionStateWithGeneration()
	require.Equal(t, generation, sameGeneration)

	s.SetAuditProjectionState(true, false)
	disabled, rebuilding, generation = s.AuditProjectionStateWithGeneration()
	require.True(t, disabled)
	require.False(t, rebuilding)
	require.Equal(t, uint64(2), generation)
	require.ErrorIs(t, s.WaitForAuditRaftProgress(context.Background(), 1), ErrAuditProjectionUnavailable)
}

func TestWaitForProgressWakesAfterNotification(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)
	var progress atomic.Uint64
	var calls atomic.Uint32
	secondRead := make(chan struct{})
	allowSecondRead := make(chan struct{})

	read := func() (uint64, error) {
		if calls.Add(1) == 2 {
			close(secondRead)
			<-allowSecondRead
		}

		return progress.Load(), nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	waitErr := make(chan error, 1)
	go func() { waitErr <- s.waitForProgress(ctx, 8, read, "test progress") }()

	select {
	case <-secondRead:
	case <-ctx.Done():
		t.Fatal("waiter did not enter the locked progress loop")
	}
	close(allowSecondRead)

	// Acquiring progressMu now waits until the waiter has entered Cond.Wait.
	// Broadcasting while holding the same lock makes the wake-up deterministic.
	s.progressMu.Lock()
	progress.Store(8)
	s.progressCond.Broadcast()
	s.progressMu.Unlock()

	select {
	case err := <-waitErr:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("waiter did not observe certified progress")
	}
}

func TestWaitForProgressReportsReadErrors(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)
	want := errors.New("read failed")
	require.ErrorIs(t, s.waitForProgress(context.Background(), 1, func() (uint64, error) {
		return 0, want
	}, "test progress"), want)

	var calls int
	err := s.waitForProgress(context.Background(), 1, func() (uint64, error) {
		calls++
		if calls == 1 {
			return 0, nil
		}

		return 0, want
	}, "test progress")
	require.ErrorIs(t, err, want)
}

func TestAuditIndexSnapshotQueriesPinnedRows(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)
	kb := dal.NewKeyBuilder()
	batch := s.NewBatch()
	require.NoError(t, batch.SetBytes(AuditIndexStringKey(kb, AuditFieldLedger, "main", 3), nil))
	require.NoError(t, batch.SetBytes(kb.Reset().
		PutByte(PrefixInternal).
		PutByte(SubInternalAuditIndex).
		PutByte(AuditFieldOutcome).
		PutByte(1).
		PutUint64(3).
		Build(), nil))
	require.NoError(t, batch.SetBytes(AuditIndexUint64Key(kb, AuditFieldProposalID, 9, 3), nil))
	require.NoError(t, batch.Commit())

	reader := s.NewSnapshot()
	t.Cleanup(func() { _ = reader.Close() })
	index := NewAuditIndexSnapshot(reader)

	seqs, err := index.AuditSeqsByString(AuditFieldLedger, "main")
	require.NoError(t, err)
	require.Equal(t, []uint64{3}, seqs)
	seqs, err = index.AuditSeqsByOutcome(true)
	require.NoError(t, err)
	require.Equal(t, []uint64{3}, seqs)
	seqs, err = index.AuditSeqsByUint64Range(AuditFieldProposalID, 9, 9)
	require.NoError(t, err)
	require.Equal(t, []uint64{3}, seqs)

	seqs, err = s.AuditSeqsByOutcome(false)
	require.NoError(t, err)
	require.Empty(t, seqs)
}
