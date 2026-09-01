package dal

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

func newBaselineTestStore(t *testing.T) *Store {
	t.Helper()

	store, err := NewStore(t.TempDir(), logging.Testing(), noop.NewMeterProvider().Meter("t"), DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	return store
}

func stageBaseline(t *testing.T, store *Store, chapterID uint64) {
	t.Helper()

	path, err := store.StagedBaselineDir(chapterID)
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(path, 0755))
}

func baselineNames(t *testing.T, store *Store) []string {
	t.Helper()

	entries, err := os.ReadDir(filepath.Join(store.dataDir, baselineCheckpointsDir))
	if os.IsNotExist(err) {
		return nil
	}

	require.NoError(t, err)

	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}

	return names
}

// The checker's arithmetic is expected = baseline + replay(boundary..now), so
// the live baseline must be the staged snapshot of exactly the boundary close —
// promoted when the confirm advances the boundary, never when a chapter merely
// closes.
func TestPromoteStagedBaseline_PromotesTheBoundaryClose(t *testing.T) {
	t.Parallel()

	store := newBaselineTestStore(t)

	stageBaseline(t, store, 1)
	stageBaseline(t, store, 2)

	require.NoError(t, store.PromoteStagedBaseline(1))

	path, id, ok := store.BaselineCheckpointPath()
	require.True(t, ok)
	require.EqualValues(t, 1, id)
	require.DirExists(t, path)
	require.ElementsMatch(t, []string{"checker-00000000000000000001", "staged-00000000000000000002"}, baselineNames(t, store),
		"chapter 2 is closed but not archived: its staged snapshot must survive, unpromoted")
}

// Confirms can land several chapters ahead in one batch; only the boundary's
// snapshot becomes live, and everything staged at or below is consumed.
func TestPromoteStagedBaseline_ConsumesEverythingAtOrBelowTheBoundary(t *testing.T) {
	t.Parallel()

	store := newBaselineTestStore(t)

	stageBaseline(t, store, 1)
	stageBaseline(t, store, 2)
	stageBaseline(t, store, 3)

	require.NoError(t, store.PromoteStagedBaseline(3))

	_, id, ok := store.BaselineCheckpointPath()
	require.True(t, ok)
	require.EqualValues(t, 3, id)
	require.ElementsMatch(t, []string{"checker-00000000000000000003"}, baselineNames(t, store))
}

// Re-running with nothing new staged must keep the live baseline: this is every
// boot after a clean shutdown, and every confirm-free batch.
func TestPromoteStagedBaseline_IsIdempotent(t *testing.T) {
	t.Parallel()

	store := newBaselineTestStore(t)

	stageBaseline(t, store, 1)
	require.NoError(t, store.PromoteStagedBaseline(1))
	require.NoError(t, store.PromoteStagedBaseline(1))

	_, id, ok := store.BaselineCheckpointPath()
	require.True(t, ok)
	require.EqualValues(t, 1, id)
}

// A boundary whose own staging failed cannot be caught up by anything: an older
// staged snapshot is just as stale as the live one. Both go, and the checker
// degrades honestly on the missing baseline instead of comparing against the
// wrong state.
func TestPromoteStagedBaseline_RemovesWhatCannotReachTheBoundary(t *testing.T) {
	t.Parallel()

	store := newBaselineTestStore(t)

	stageBaseline(t, store, 1)
	require.NoError(t, store.PromoteStagedBaseline(1))

	// Chapter 2's close failed to stage; chapter 2 is then confirmed.
	require.NoError(t, store.PromoteStagedBaseline(2))

	_, _, ok := store.BaselineCheckpointPath()
	require.False(t, ok, "a baseline for chapter 1 must not serve a boundary at chapter 2")
}

// The crash window the boot reconcile exists for: the confirm committed, the
// rename did not happen, and the confirm is never re-applied.
func TestPromoteStagedBaseline_RecoversALostPromotion(t *testing.T) {
	t.Parallel()

	store := newBaselineTestStore(t)

	stageBaseline(t, store, 1)
	require.NoError(t, store.PromoteStagedBaseline(1))
	stageBaseline(t, store, 2)

	// Crash here: confirm of 2 was applied, promotion lost. Boot reconciles.
	require.NoError(t, store.PromoteStagedBaseline(2))

	_, id, ok := store.BaselineCheckpointPath()
	require.True(t, ok)
	require.EqualValues(t, 2, id)
}

func TestPromoteStagedBaseline_NothingArchivedIsANoOp(t *testing.T) {
	t.Parallel()

	store := newBaselineTestStore(t)

	stageBaseline(t, store, 1)
	require.NoError(t, store.PromoteStagedBaseline(0))

	_, _, ok := store.BaselineCheckpointPath()
	require.False(t, ok)
	require.ElementsMatch(t, []string{"staged-00000000000000000001"}, baselineNames(t, store))
}

// A hard crash between CreateBaselineSnapshot's temp write and its rename leaves
// a "staged-<id>.tmp-<pid>-<ns>" sibling. It must never pass for the snapshot it
// was meant to become — fmt.Sscanf's %d would accept it — and it is swept.
func TestPromoteStagedBaseline_IgnoresAndSweepsInterruptedWrites(t *testing.T) {
	t.Parallel()

	store := newBaselineTestStore(t)

	stageBaseline(t, store, 1)
	require.NoError(t, store.PromoteStagedBaseline(1))

	// Chapter 2's staging crashed mid-write: only the temp sibling exists.
	dir := filepath.Join(store.dataDir, baselineCheckpointsDir)
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "staged-00000000000000000002.tmp-1234-5678"), 0755))

	require.NoError(t, store.PromoteStagedBaseline(2))

	// The temp must not have been promoted as chapter 2's snapshot; with no real
	// snapshot for the boundary, the stale live baseline goes and the checker
	// degrades. The interrupted write is swept either way.
	_, _, ok := store.BaselineCheckpointPath()
	require.False(t, ok, "an interrupted write must not serve as the boundary snapshot")
	require.Empty(t, baselineNames(t, store))
}

// BaselineCheckpointPath applies the same anchored parse: an interrupted write
// next to the live baseline must not shadow or impersonate it.
func TestBaselineCheckpointPath_IgnoresInterruptedWrites(t *testing.T) {
	t.Parallel()

	store := newBaselineTestStore(t)

	stageBaseline(t, store, 3)
	require.NoError(t, store.PromoteStagedBaseline(3))

	dir := filepath.Join(store.dataDir, baselineCheckpointsDir)
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "checker-00000000000000000009.tmp-1-1"), 0755))

	_, id, ok := store.BaselineCheckpointPath()
	require.True(t, ok)
	require.EqualValues(t, 3, id)
}

// The clean snapshot must win over a leftover temp for the same chapter — the
// crash-then-retry case where staging eventually succeeded.
func TestPromoteStagedBaseline_PromotesTheCleanSnapshotPastItsTemp(t *testing.T) {
	t.Parallel()

	store := newBaselineTestStore(t)

	stageBaseline(t, store, 1)
	dir := filepath.Join(store.dataDir, baselineCheckpointsDir)
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "staged-00000000000000000001.tmp-9-9"), 0755))

	require.NoError(t, store.PromoteStagedBaseline(1))

	_, id, ok := store.BaselineCheckpointPath()
	require.True(t, ok)
	require.EqualValues(t, 1, id)
	require.ElementsMatch(t, []string{"checker-00000000000000000001"}, baselineNames(t, store))
}
