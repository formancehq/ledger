package dal_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// TestValidateFreshRestoreTarget pins the restore freshness guard: a restore
// stages a checkpoint whose RESTORED marker the next boot aligns the raft
// genesis snapshot with, but normal startup prefers an existing live/
// database over checkpoints — so any pre-existing store state in the target
// directory would boot under the wrong boundary and must be refused.
func TestValidateFreshRestoreTarget(t *testing.T) {
	t.Parallel()

	t.Run("fresh directory passes", func(t *testing.T) {
		t.Parallel()
		require.NoError(t, dal.ValidateFreshRestoreTarget(t.TempDir()))
	})

	t.Run("missing directory passes", func(t *testing.T) {
		t.Parallel()
		require.NoError(t, dal.ValidateFreshRestoreTarget(filepath.Join(t.TempDir(), "not-created-yet")))
	})

	t.Run("existing checkpoint is refused", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		require.NoError(t, os.MkdirAll(filepath.Join(dir, "checkpoints", "3"), 0o755))
		require.ErrorContains(t, dal.ValidateFreshRestoreTarget(dir), "checkpoints already exist")
	})

	t.Run("orphaned checkpoint 0 is reclaimed", func(t *testing.T) {
		// checkpoints/0 with no live database (and the caller having verified
		// no RESTORED marker) is a finalize that died between checkpoint
		// placement and the marker — the restore must be retryable, not
		// permanently refused.
		t.Parallel()

		dir := t.TempDir()
		require.NoError(t, os.MkdirAll(filepath.Join(dir, "checkpoints", "0"), 0o755))
		require.NoError(t, dal.ValidateFreshRestoreTarget(dir))

		_, err := os.Stat(filepath.Join(dir, "checkpoints"))
		require.True(t, os.IsNotExist(err), "the orphaned checkpoint must be reclaimed")
	})

	t.Run("checkpoint 0 alongside higher checkpoints is refused", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		require.NoError(t, os.MkdirAll(filepath.Join(dir, "checkpoints", "0"), 0o755))
		require.NoError(t, os.MkdirAll(filepath.Join(dir, "checkpoints", "5"), 0o755))
		require.ErrorContains(t, dal.ValidateFreshRestoreTarget(dir), "checkpoints already exist")
	})

	t.Run("checkpoint 0 next to live is refused", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		require.NoError(t, os.MkdirAll(filepath.Join(dir, "checkpoints", "0"), 0o755))
		require.NoError(t, os.MkdirAll(filepath.Join(dir, "live"), 0o755))
		require.ErrorContains(t, dal.ValidateFreshRestoreTarget(dir), "live/ already exists")
	})

	for _, stale := range []string{"live", "live.staging", "live.discard"} {
		t.Run("existing "+stale+" is refused", func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			require.NoError(t, os.MkdirAll(filepath.Join(dir, stale), 0o755))
			require.ErrorContains(t, dal.ValidateFreshRestoreTarget(dir), stale+"/ already exists")
		})
	}
}
