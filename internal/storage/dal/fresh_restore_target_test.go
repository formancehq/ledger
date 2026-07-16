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

	for _, stale := range []string{"live", "live.staging", "live.discard"} {
		t.Run("existing "+stale+" is refused", func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			require.NoError(t, os.MkdirAll(filepath.Join(dir, stale), 0o755))
			require.ErrorContains(t, dal.ValidateFreshRestoreTarget(dir), stale+"/ already exists")
		})
	}
}
