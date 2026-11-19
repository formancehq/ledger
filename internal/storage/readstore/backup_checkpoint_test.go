package readstore

import (
	"testing"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

func TestBackupCheckpointPreservesBatchesAndLivePromotionGate(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)
	before := IndexVersionState{CurrentVersion: 1, PendingVersion: 2, HighWater: 2, RewriteProgress: []byte("account:alice")}
	batch := s.NewBatch()
	require.NoError(t, s.WriteIndexVersionState(batch, servingLedger, servingCanonical, before))
	require.NoError(t, s.WriteProgress(batch, 41))
	require.NoError(t, batch.Commit())

	// The builder marked the new binding but has not committed its batch.
	// A concurrent full must not lift the live mark, even though it flushes.
	s.MarkPromotion(servingLedger, servingCanonical, 2)
	path, release, err := s.CaptureBackupCheckpoint()
	require.NoError(t, err)
	t.Cleanup(release)
	require.True(t, s.PromotionInFlight(servingLedger, servingCanonical, 2))

	batch = s.NewBatch()
	require.NoError(t, s.WriteIndexVersionState(batch, servingLedger, servingCanonical, IndexVersionState{CurrentVersion: 2, HighWater: 2}))
	require.NoError(t, s.WriteProgress(batch, 42))
	require.NoError(t, batch.Commit())
	require.NoError(t, s.FlushPromotions())

	frozen, err := OpenReadOnly(path, logging.NopZap())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, frozen.Close()) })
	progress, err := frozen.ReadProgress()
	require.NoError(t, err)
	require.Equal(t, uint64(41), progress)
	state, found, err := frozen.ReadIndexVersionState(servingLedger, servingCanonical)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, before, state)
}

func TestBackupCheckpointPinsAreIsolatedAndRecoveredOnRestart(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	s, err := New(dir, logging.NopZap(), DefaultConfig())
	require.NoError(t, err)
	first, release, err := s.CaptureBackupCheckpoint()
	require.NoError(t, err)
	second, _, err := s.CaptureBackupCheckpoint() // Simulate a lost release on crash.
	require.NoError(t, err)
	release()
	require.NoDirExists(t, first)
	require.DirExists(t, second)

	// An overlapping open that cannot own the DB must not release a live pin.
	_, err = New(dir, logging.NopZap(), DefaultConfig())
	require.Error(t, err)
	require.DirExists(t, second)
	require.NoError(t, s.Close())

	reopened, err := New(dir, logging.NopZap(), DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	require.NoDirExists(t, second)
}
