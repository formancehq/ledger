package backup

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

func TestReadIndexRejectsSwappedFullsWithEqualHistoryHeads(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	logger := testLogger()
	// Use the SAME namespace on two independent storage backends: both
	// inventories have plausible object names, lengths and history sequences.
	const id = "tenant"
	full := func(ledger string) (*CheckpointManifest, Storage) {
		t.Helper()
		store := newBackupTestStore(t)
		batch := store.OpenWriteSession()
		require.NoError(t, batch.SetProto(coldLogKey(1), createLedgerLog(1, ledger, 1)))
		require.NoError(t, batch.Commit())
		index, err := readstore.New(t.TempDir(), logger, readstore.DefaultConfig())
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, index.Close()) })
		batch = index.NewBatch()
		require.NoError(t, index.WriteProgress(batch, 1))
		require.NoError(t, index.WriteBackfillCursor(batch, []byte("task"), []byte(ledger)))
		require.NoError(t, batch.Commit())
		dir := filepath.Join(t.TempDir(), "seed")
		require.NoError(t, index.CreateCheckpoint(dir))
		storage := newInMemoryBackupStorage()
		_, err = RunBackupWithReadIndex(ctx, logger, store, storage, id, "full", dir)
		require.NoError(t, err)
		manifest, err := ReadManifest(ctx, storage, ManifestKey(id))
		require.NoError(t, err)

		return manifest.Checkpoint, storage
	}

	first, firstStorage := full("one")
	second, storage := full("two")
	require.Equal(t, first.LastAppliedIndex, second.LastAppliedIndex)
	require.Equal(t, first.LastLogSequence, second.LastLogSequence)
	require.Equal(t, first.LastAuditSequence, second.LastAuditSequence)
	require.NotEqual(t, first.Files, second.Files)
	require.NotEqual(t, first.ReadIndex.Files, second.ReadIndex.Files)
	require.NotEqual(t, first.ReadIndex.InventorySHA256, second.ReadIndex.InventorySHA256)

	for _, pair := range []struct {
		checkpoint *CheckpointManifest
		storage    Storage
		want       string
	}{{first, firstStorage, "one"}, {second, storage, "two"}} {
		dir := t.TempDir()
		root, err := os.OpenRoot(dir)
		require.NoError(t, err)
		require.NoError(t, DownloadReadIndex(ctx, pair.storage, id, pair.checkpoint, root, ReadIndexDownloadOptions{Parallelism: 2}))
		require.NoError(t, root.Close())
		index, err := readstore.OpenReadOnly(dir, logger)
		require.NoError(t, err)
		value, ok := index.ReadBackfillCursor([]byte("task"))
		require.True(t, ok)
		require.Equal(t, pair.want, string(value))
		require.NoError(t, index.Close())
	}

	for _, filesOnly := range []bool{false, true} {
		mixed := *first
		index := *second.ReadIndex
		if filesOnly {
			// Splicing only the second inventory leaves the first digest.
			index.InventorySHA256 = first.ReadIndex.InventorySHA256
		}
		mixed.ReadIndex = &index
		// Even though all secondary objects are available and individually
		// hash-valid, neither restore nor incremental republication accepts
		// the mixed pair.
		root, err := os.OpenRoot(t.TempDir())
		require.NoError(t, err)
		err = DownloadReadIndex(ctx, storage, id, &mixed, root, ReadIndexDownloadOptions{Parallelism: 2})
		require.ErrorIs(t, err, ErrCheckpointFileCorrupted)
		require.ErrorContains(t, err, "inventory binding mismatch")
		entries, err := os.ReadDir(root.Name())
		require.NoError(t, err)
		require.Empty(t, entries)
		require.NoError(t, root.Close())
		err = WriteManifest(ctx, storage, ManifestKey(id), &Manifest{Checkpoint: &mixed})
		require.ErrorContains(t, err, "inventory binding mismatch")
	}
}

// Pin the v1 encoding independently of the current writer/reader pair. Adding
// a Go field or changing map iteration order must not invalidate prior fulls.
func TestReadIndexBindingV1(t *testing.T) {
	t.Parallel()
	primary := map[string]CheckpointFile{
		"b": {Size: 7, Key: "bucket/data/b.1"},
		"a": {Size: 0, Key: "bucket/data/a.0"},
	}
	secondary := map[string]CheckpointFile{"a": {Size: 2, Key: "bucket/data/a.2"}}
	require.Equal(t, "7fc76bbf5f13bd9a42b663685a742482577990c1689d9ab60eb18eb6f470b2ee", readIndexBinding(primary, secondary))
}

// The incremental writer rechecks this metadata on every publication. Each
// map models 4 TiB at 64 MiB per SST; the benchmark never reads an SST.
func BenchmarkReadIndexBinding(b *testing.B) {
	const count = 1 << 16
	primary, secondary := make(map[string]CheckpointFile, count), make(map[string]CheckpointFile, count)
	primaryHash, secondaryHash := strings.Repeat("a", 64), strings.Repeat("b", 64)
	for i := range count {
		name := fmt.Sprintf("%06d.sst", i)
		primary[name] = CheckpointFile{Size: 64 << 20, Key: CheckpointFileKey("test", name, primaryHash)}
		secondary[name] = CheckpointFile{Size: 64 << 20, Key: CheckpointFileKey("test", name, secondaryHash)}
	}
	b.ReportAllocs()
	for b.Loop() {
		readIndexBinding(primary, secondary)
	}
}
