package readstore

import (
	"context"
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

// TestWaitForCheckpointBlocksUntilMarker reproduces the EN-1460 race: a reader
// waits on a checkpoint whose directory is not yet materialized, then the
// index builder writes the marker and broadcasts. The wait must unblock only
// once the marker exists — never on the bare directory.
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

	// The waiter must not have returned yet — the marker is absent.
	select {
	case err := <-waitErr:
		t.Fatalf("WaitForCheckpoint returned before marker was written: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	// Simulate the index builder finishing the checkpoint: write the marker,
	// then broadcast progress exactly as createReadIndexCheckpoint does.
	require.NoError(t, MarkCheckpointReady(dir))
	s.NotifyProgress()

	select {
	case err := <-waitErr:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("WaitForCheckpoint did not return after marker was written")
	}
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

// TestWaitForCheckpointContextCancel unblocks on context cancellation and
// returns the context error rather than hanging.
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
