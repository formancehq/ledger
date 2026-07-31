package balancehistorystore

import (
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"
)

func TestNonZeroHistoryFloorIsReservedUntilBaseIsExternallyVerifiable(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	_, err := store.Publish(Publication{Coverage: Coverage{
		SourceComplete: true,
		EffectiveFloor: 1,
		InsertionFloor: 2,
	}})
	var gap *ErrSourceGap
	require.ErrorAs(t, err, &gap)
	require.ErrorContains(t, err, "chain-bound or signed base import")

	manifest, err := store.Manifest()
	require.NoError(t, err)
	require.Zero(t, manifest.Version)
	require.Zero(t, manifest.EffectiveFloor)
	require.Zero(t, manifest.InsertionFloor)
}

func TestTamperedNonZeroHistoryFloorIsCorrupt(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	tamperLatestHistoryFloor(t, store)

	var corrupt *ErrCorrupt
	require.ErrorAs(t, store.Verify(), &corrupt)
}

func TestTamperedNonZeroHistoryFloorIsRejectedByImmediateReads(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	tamperLatestHistoryFloor(t, store)

	_, err := store.Manifest()
	var corrupt *ErrCorrupt
	require.ErrorAs(t, err, &corrupt)
	require.ErrorContains(t, err, "non-zero history floor")

	_, err = store.OpenView(1)
	require.ErrorAs(t, err, &corrupt)
}

func TestTamperedNonZeroHistoryFloorIsQuarantinedOnRestart(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	store := openTestStoreAt(t, dir)
	tamperLatestHistoryFloor(t, store)
	require.NoError(t, store.Close())

	reopened := openTestStoreAt(t, dir)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })

	_, err := reopened.Manifest()
	var quarantined *ErrQuarantined
	require.ErrorAs(t, err, &quarantined)
	var corrupt *ErrCorrupt
	require.ErrorAs(t, err, &corrupt)
	require.ErrorContains(t, err, "non-zero history floor")
}

func tamperLatestHistoryFloor(t *testing.T, store *Store) {
	t.Helper()

	manifest := publishBalanced(t, store, 1, 1, 10, 20, 1)
	manifest.EffectiveFloor = 10
	encoded, err := encodeManifest(manifest)
	require.NoError(t, err)
	require.NoError(t, store.db.Set(manifestKey(manifest.Version), encoded, pebble.Sync))
}
