package backup

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

func TestBackupNamespacesRemainIsolatedThroughCleanup(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	storage, calls := newCrashSafetyStorage(t)
	storeA := newBackupTestStore(t)
	storeB := newBackupTestStore(t)

	_, err := RunBackup(ctx, logging.Testing(), storeA, storage, "id-a", "a-1")
	require.NoError(t, err)
	_, err = RunBackup(ctx, logging.Testing(), storeB, storage, "id-b", "b-1")
	require.NoError(t, err)
	manifestB, err := ReadManifest(ctx, storage, ManifestKey("id-b"))
	require.NoError(t, err)
	require.NotEmpty(t, manifestB.Checkpoint.Files)
	keysB, err := storage.ListFiles(ctx, "id-b/")
	require.NoError(t, err)
	require.NotEmpty(t, keysB)
	before := make(map[string][]byte, len(keysB))
	for _, key := range keysB {
		file, err := storage.GetFile(ctx, key)
		require.NoError(t, err)
		before[key], err = io.ReadAll(file)
		require.NoError(t, err)
		require.NoError(t, file.Close())
	}

	// Replacing A's checkpoint runs manifest publication and stale-object
	// cleanup while B's manifest and every referenced object remain intact.
	change := storeA.OpenWriteSession()
	require.NoError(t, change.SetBytes([]byte{0x7f, 0x01}, []byte("new checkpoint content")))
	require.NoError(t, change.Commit())
	require.NoError(t, storeA.Flush())
	_, err = RunBackup(ctx, logging.Testing(), storeA, storage, "id-a", "a-2")
	require.NoError(t, err)
	deletedStaleA := false
	for _, op := range calls.opsCopy() {
		if strings.HasPrefix(op, "del id-a/") {
			deletedStaleA = true
		}
		require.NotContains(t, op, "del id-b/", "A cleanup touched B's namespace")
	}
	require.True(t, deletedStaleA, "second A backup must exercise stale-object cleanup")
	for key, expected := range before {
		file, err := storage.GetFile(ctx, key)
		require.NoError(t, err, "B object %s was deleted by A cleanup", key)
		actual, err := io.ReadAll(file)
		require.NoError(t, err)
		require.NoError(t, file.Close())
		require.True(t, bytes.Equal(expected, actual), "B object %s was modified by A backup", key)
	}
	manifestAfter, err := ReadManifest(ctx, storage, ManifestKey("id-b"))
	require.NoError(t, err)
	require.Equal(t, manifestB, manifestAfter)
}
