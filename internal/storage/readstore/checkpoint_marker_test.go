package readstore

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

// TestCheckpointDirReadyRequiresMarker verifies readiness is keyed on the
// sentinel marker, not mere directory existence. A directory that exists but
// lacks the marker (a mid-checkpoint / half-linked state, EN-1460) must read
// as not-ready.
func TestCheckpointDirReadyRequiresMarker(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	// A directory that exists but has no marker is not ready.
	require.False(t, CheckpointDirReady(dir))

	require.NoError(t, MarkCheckpointReady(dir))
	require.True(t, CheckpointDirReady(dir))
}

// TestCreateCheckpointThenMarkIsOpenable mirrors what the index builder does
// (CreateCheckpoint + MarkCheckpointReady) and asserts the resulting directory
// is a valid, openable read-only Pebble read index — i.e. once the marker is
// present the checkpoint is genuinely readable, not just present on disk.
func TestCreateCheckpointThenMarkIsOpenable(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	destDir := filepath.Join(t.TempDir(), "readindex")
	require.NoError(t, s.CreateCheckpoint(destDir))
	require.NoError(t, MarkCheckpointReady(destDir))
	require.True(t, CheckpointDirReady(destDir))

	ro, err := OpenReadOnly(destDir, logging.NopZap())
	require.NoError(t, err)
	require.NoError(t, ro.Close())
}

// TestCreateCheckpointFailsIfDirExists documents the pebble contract the index
// builder relies on: CreateCheckpoint errors when the destination already
// exists, so the atomic materialization builds into a temp dir and renames.
// A recreate over a cleared path succeeds.
func TestCreateCheckpointFailsIfDirExists(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)
	destDir := filepath.Join(t.TempDir(), "readindex")

	require.NoError(t, s.CreateCheckpoint(destDir))

	// Second create over the existing dir fails (pebble ErrExist).
	require.Error(t, s.CreateCheckpoint(destDir))

	// After clearing, recreate succeeds — the idempotency the builder depends on.
	require.NoError(t, os.RemoveAll(destDir))
	require.NoError(t, s.CreateCheckpoint(destDir))
}

// TestWaitForCheckpointFastPath returns immediately when the marker already
// exists.
func TestWaitForCheckpointFastPath(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)
	dir := t.TempDir()
	require.NoError(t, MarkCheckpointReady(dir))

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	require.NoError(t, s.WaitForCheckpoint(ctx, dir))
}

// TestWaitForCheckpointBlocksUntilMarker reproduces the EN-1460 create-side
// race: CreateQueryCheckpoint waits on the creator node's marker; the wait must
// unblock only once the marker exists (after the atomic materialization + the
// builder's NotifyProgress), never on a bare directory.
func TestWaitForCheckpointBlocksUntilMarker(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)
	dir := t.TempDir()

	waitErr := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		waitErr <- s.WaitForCheckpoint(ctx, dir)
	}()

	// The waiter must not return yet — the marker is absent.
	select {
	case err := <-waitErr:
		t.Fatalf("WaitForCheckpoint returned before marker was written: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	// Simulate the builder finishing the checkpoint: write the marker, then
	// broadcast progress exactly as the inline path does after materialization.
	require.NoError(t, MarkCheckpointReady(dir))
	s.NotifyProgress()

	select {
	case err := <-waitErr:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("WaitForCheckpoint did not return after marker was written")
	}
}

// TestWaitForCheckpointContextCancel unblocks on context cancellation and
// returns the context error rather than hanging. Exercises the missed-wakeup
// fix: the cancellation broadcast is delivered while holding progressMu, so a
// cancel that lands in the narrow window around cond.Wait() is never lost.
func TestWaitForCheckpointContextCancel(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)
	dir := t.TempDir() // never marked ready

	ctx, cancel := context.WithCancel(context.Background())

	waitErr := make(chan error, 1)
	go func() {
		waitErr <- s.WaitForCheckpoint(ctx, dir)
	}()

	select {
	case err := <-waitErr:
		t.Fatalf("WaitForCheckpoint returned before cancel: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	cancel()

	select {
	case err := <-waitErr:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("WaitForCheckpoint did not return after context cancel")
	}
}
