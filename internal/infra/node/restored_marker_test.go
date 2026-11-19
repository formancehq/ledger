package node

import (
	"errors"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRestoredMarkerSyncFailurePreservesPublication(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	expected := RestoredMarker{LastAppliedIndex: 45, ReadIndexGeneration: "test-generation"}
	fail := errors.New("parent sync failed")
	err := writeRestoredMarker(dir, expected, func(string) error { return fail })
	require.ErrorIs(t, err, fail)
	require.ErrorIs(t, err, ErrRestoredMarkerPublished)
	got, err := ReadRestoredMarker(dir)
	require.NoError(t, err)
	require.Equal(t, &expected, got)
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, 1)
}
