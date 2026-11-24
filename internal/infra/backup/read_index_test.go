package backup

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

func TestReadIndexBackupChain(t *testing.T) {
	t.Parallel()
	const id = "tenant/finance"
	ctx := t.Context()
	logger := logging.Testing()
	primary := newBackupTestStore(t)
	index, err := readstore.New(t.TempDir(), logger, readstore.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, index.Close()) })

	setTask := func(value string) {
		batch := index.NewBatch()
		require.NoError(t, index.WriteBackfillCursor(batch, []byte("task"), []byte(value)))
		require.NoError(t, batch.Commit())
	}
	snapshot := func() string {
		dir := filepath.Join(t.TempDir(), "seed")
		require.NoError(t, index.CreateCheckpoint(dir))

		return dir
	}

	storage, state := newCrashSafetyStorage(t)
	setTask("full")
	_, err = RunBackupWithReadIndex(ctx, logger, primary, storage, id, "one", snapshot())
	require.NoError(t, err)
	full, err := ReadManifest(ctx, storage, ManifestKey(id))
	require.NoError(t, err)
	require.NotNil(t, full.Checkpoint.ReadIndex)
	require.NotEmpty(t, full.Checkpoint.ReadIndex.Files)
	require.EqualValues(t, ReadIndexVersion, full.Checkpoint.ReadIndex.Version)

	// A non-empty incremental must rewrite the manifest while preserving all
	// the read checkpoint's objects and their original inventory.
	batch := primary.OpenWriteSession()
	require.NoError(t, batch.SetProto(coldLogKey(1), createLedgerLog(1, "ledger", 1)))
	require.NoError(t, batch.Commit())
	result, err := RunIncrementalBackup(ctx, logger, primary, storage, id, 0)
	require.NoError(t, err)
	require.NotZero(t, result.SegmentsUploaded)
	incremental, err := ReadManifest(ctx, storage, ManifestKey(id))
	require.NoError(t, err)
	require.Equal(t, full.Checkpoint.ReadIndex, incremental.Checkpoint.ReadIndex)

	// Fail the next full at the manifest swap. The previous chain must still
	// restore its old read projection even though new objects exist remotely.
	setTask("replacement")
	replacement := snapshot()
	state.putErrKeys = []string{ManifestKey(id)}
	_, err = RunBackupWithReadIndex(ctx, logger, primary, storage, id, "two", replacement)
	require.ErrorContains(t, err, "writing manifest")
	afterFailure, err := ReadManifest(ctx, storage, ManifestKey(id))
	require.NoError(t, err)
	require.Equal(t, incremental, afterFailure)
	old := t.TempDir()
	root, err := os.OpenRoot(old)
	require.NoError(t, err)
	var verifiedFiles, verifiedBytes atomic.Int64
	require.NoError(t, DownloadReadIndex(ctx, storage, id, afterFailure.Checkpoint, root, ReadIndexDownloadOptions{
		Parallelism: 2, FileVerified: func(size int64) { verifiedBytes.Add(size); verifiedFiles.Add(1) },
	}))
	var total int64
	for _, cf := range full.Checkpoint.ReadIndex.Files {
		total += cf.Size
	}
	require.EqualValues(t, len(full.Checkpoint.ReadIndex.Files), verifiedFiles.Load())
	require.Equal(t, total, verifiedBytes.Load())
	require.NoError(t, root.Close())
	restored, err := readstore.OpenReadOnly(old, logger)
	require.NoError(t, err)
	value, found := restored.ReadBackfillCursor([]byte("task"))
	require.True(t, found)
	require.Equal(t, "full", string(value))
	require.NoError(t, restored.Close())

	// A successful replacement drops all objects referenced only by the old
	// full, retaining even files shared by the primary and read databases.
	state.putErrKeys = nil
	_, err = RunBackupWithReadIndex(ctx, logger, primary, storage, id, "three", replacement)
	require.NoError(t, err)
	current, err := ReadManifest(ctx, storage, ManifestKey(id))
	require.NoError(t, err)
	wantKeys := checkpointKeySet(current)
	gotKeys, err := storage.ListFiles(ctx, CheckpointPrefix(id))
	require.NoError(t, err)
	require.Len(t, gotKeys, len(wantKeys))
	for _, key := range gotKeys {
		require.Contains(t, wantKeys, key)
	}
	var retired bool
	for _, cf := range full.Checkpoint.ReadIndex.Files {
		if _, live := wantKeys[cf.Key]; !live {
			retired = true
			_, err := storage.GetFile(ctx, cf.Key)
			require.ErrorIs(t, err, ErrFileNotFound)
		}
	}
	require.True(t, retired, "replacement must actually retire an old read checkpoint file")
}

func TestDownloadReadIndexRejectsInvalidInventory(t *testing.T) {
	t.Parallel()
	const id = "one"
	hash := strings.Repeat("a", 64)
	for _, tc := range []struct {
		name, filename, key, want string
	}{
		{"traversal", "../outside", "irrelevant", "invalid read index checkpoint filename"},
		{"absolute", "/outside", "irrelevant", "invalid read index checkpoint filename"},
		{"subdirectory", "index/file", "irrelevant", "invalid read index checkpoint filename"},
		{"ready marker", ".ready", "irrelevant", "invalid read index checkpoint filename"},
		{"other chain", "0001.sst", CheckpointFileKey("two", "0001.sst", hash), "invalid read index object"},
		{"wrong filename", "0001.sst", CheckpointFileKey(id, "0002.sst", hash), "invalid read index object"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root, err := os.OpenRoot(t.TempDir())
			require.NoError(t, err)
			defer func() { require.NoError(t, root.Close()) }()
			cp := &CheckpointManifest{ReadIndex: &ReadIndexManifest{Version: ReadIndexVersion, Files: map[string]CheckpointFile{tc.filename: {Key: tc.key}}}}
			cp.ReadIndex.InventorySHA256 = readIndexBinding(cp.Files, cp.ReadIndex.Files)
			require.ErrorContains(t, DownloadReadIndex(t.Context(), newInMemoryBackupStorage(), id, cp, root, ReadIndexDownloadOptions{Parallelism: 2}), tc.want)
		})
	}
}

func TestDownloadReadIndexRejectsCorruptFilesAndSymlinks(t *testing.T) {
	t.Parallel()
	const name = "000002.log"
	contents := []byte("abcd")
	cf := checkpointFileFor(contents)
	cp := &CheckpointManifest{ReadIndex: &ReadIndexManifest{Version: ReadIndexVersion, Files: map[string]CheckpointFile{name: cf}}}
	cp.ReadIndex.InventorySHA256 = readIndexBinding(cp.Files, cp.ReadIndex.Files)
	for _, tc := range []struct {
		name, data, want string
		symlink          bool
	}{
		{"size", "abc", "size mismatch", false},
		{"digest", "efgh", "sha256 mismatch", false},
		{"symlink", "abcd", "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			storage := newInMemoryBackupStorage()
			require.NoError(t, storage.PutFile(ctx, cf.Key, strings.NewReader(tc.data), int64(len(tc.data))))
			dir := t.TempDir()
			outside := filepath.Join(t.TempDir(), "untouched")
			if tc.symlink {
				require.NoError(t, os.WriteFile(outside, []byte("safe"), 0o600))
				require.NoError(t, os.Symlink(outside, filepath.Join(dir, name)))
			}
			root, err := os.OpenRoot(dir)
			require.NoError(t, err)
			defer func() { require.NoError(t, root.Close()) }()
			var verified atomic.Int64
			err = DownloadReadIndex(ctx, storage, "bucket", cp, root, ReadIndexDownloadOptions{Parallelism: 1, FileVerified: func(int64) { verified.Add(1) }})
			require.Zero(t, verified.Load(), "a failed file must not advance job progress")
			if tc.symlink {
				require.ErrorIs(t, err, os.ErrExist)
				data, err := os.ReadFile(outside)
				require.NoError(t, err)
				require.Equal(t, "safe", string(data))
			} else {
				require.ErrorIs(t, err, ErrCheckpointFileCorrupted)
				require.ErrorContains(t, err, tc.want)
			}
		})
	}
}

// A nonempty incremental must not publish an inventory the restore worker
// would reject. Exercise small budgets rather than allocating a 32 MiB test
// fixture; each store independently fits, although their combined count does not.
func TestReadIndexManifestRestoreBudget(t *testing.T) {
	t.Parallel()
	manifest := &Manifest{Checkpoint: &CheckpointManifest{
		Files:     map[string]CheckpointFile{"one": {}},
		ReadIndex: &ReadIndexManifest{Version: ReadIndexVersion, Files: map[string]CheckpointFile{"two": {}}},
	}}
	manifest.Checkpoint.ReadIndex.InventorySHA256 = readIndexBinding(manifest.Checkpoint.Files, manifest.Checkpoint.ReadIndex.Files)
	full, err := marshalManifest(manifest, 1, MaxRestoreManifestBytes)
	require.NoError(t, err)
	_, err = marshalManifest(manifest, 1, len(full))
	require.NoError(t, err)
	manifest.Exports = []ExportSegment{{Type: "log", StartSeq: 1, EndSeq: 2}}
	_, err = marshalManifest(manifest, 1, len(full))
	require.ErrorContains(t, err, "encoded manifest")
	manifest.Checkpoint.Files["three"] = CheckpointFile{}
	_, err = marshalManifest(manifest, 1, MaxRestoreManifestBytes)
	require.ErrorContains(t, err, "2 primary and 1 read index")
	delete(manifest.Checkpoint.Files, "three")
	manifest.Checkpoint.ReadIndex.Files["four"] = CheckpointFile{}
	_, err = marshalManifest(manifest, 1, MaxRestoreManifestBytes)
	require.ErrorContains(t, err, "1 primary and 2 read index")
}

func TestFullRefusesReadIndexAheadOfItsPrimary(t *testing.T) {
	t.Parallel()
	const id = "finance"
	ctx := t.Context()
	primary := newBackupTestStore(t)
	storage, state := newCrashSafetyStorage(t)
	logger := logging.Testing()
	_, err := RunBackup(ctx, logger, primary, storage, id, "legacy")
	require.NoError(t, err)
	published, err := ReadManifest(ctx, storage, ManifestKey(id))
	require.NoError(t, err)
	require.Nil(t, published.Checkpoint.ReadIndex)
	index, err := readstore.New(t.TempDir(), logger, readstore.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, index.Close()) })
	batch := index.NewBatch()
	require.NoError(t, index.WriteProgress(batch, 2))
	require.NoError(t, batch.Commit())
	dir := filepath.Join(t.TempDir(), "seed")
	require.NoError(t, index.CreateCheckpoint(dir))

	state.resetOps()
	_, err = RunBackupWithReadIndex(ctx, logger, primary, storage, id, "ahead", dir)
	require.ErrorContains(t, err, "log cursor 2 exceeds full checkpoint head 0")
	require.Empty(t, state.opsCopy(), "invalid pairing must fail before uploading or pruning any objects")
	after, err := ReadManifest(ctx, storage, ManifestKey(id))
	require.NoError(t, err)
	require.Equal(t, published, after)
}

// An older binary can restore the primary even when it cannot decode the
// secondary. Its next incremental must carry the newer inventory byte values
// through to a binary that does know how to restore it.
func TestIncrementalPreservesUnknownReadIndexSchema(t *testing.T) {
	t.Parallel()
	const id = "future"
	const extension = `{"version":2,"files":[{"chunks":["a","b"],"shard":18446744073709551615}],"layout":"sharded"}`
	ctx := t.Context()
	storage := newInMemoryBackupStorage()
	primary := newBackupTestStore(t)
	legacy, err := RunBackup(ctx, testLogger(), primary, storage, id, "full")
	require.NoError(t, err)
	require.Zero(t, legacy.LastLogSequence)
	key := ManifestKey(id)
	manifest, err := ReadManifest(ctx, storage, key)
	require.NoError(t, err)
	var future ReadIndexManifest
	require.NoError(t, json.Unmarshal([]byte(extension), &future))
	require.EqualValues(t, 2, future.Version)
	require.Nil(t, future.Files)
	manifest.Checkpoint.ReadIndex = &future
	require.NoError(t, WriteManifest(ctx, storage, key, manifest))

	batch := primary.OpenWriteSession()
	require.NoError(t, batch.SetProto(coldLogKey(1), createLedgerLog(1, "ledger", 1)))
	require.NoError(t, batch.Commit())
	result, err := RunIncrementalBackup(ctx, testLogger(), primary, storage, id, 0)
	require.NoError(t, err)
	require.NotZero(t, result.SegmentsUploaded)
	blob, err := storage.GetFile(ctx, key)
	require.NoError(t, err)
	defer func() { require.NoError(t, blob.Close()) }()
	var raw struct {
		Checkpoint struct {
			ReadIndex json.RawMessage `json:"readIndex"`
		} `json:"checkpoint"`
	}
	require.NoError(t, json.NewDecoder(blob).Decode(&raw))
	// Both inputs are compact. Exact bytes also prove that the unknown uint64
	// was not rounded by a lossy map[string]any decode/remarshal.
	require.Equal(t, extension, string(raw.Checkpoint.ReadIndex))

	var invalid ReadIndexManifest
	require.Error(t, json.Unmarshal([]byte(`{"version":1,"files":["bad"]}`), &invalid), "malformed known schemas must still fail closed")
}

func TestCheckpointNamesRejectsOverBudgetBeforeHashing(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	for _, name := range []string{"MANIFEST-000001", "000002.sst"} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), nil, 0o600))
	}
	names, err := checkpointNames(t.Context(), dir, true, 2)
	require.NoError(t, err)
	require.Len(t, names, 2)
	_, err = checkpointNames(t.Context(), dir, true, 1)
	require.ErrorContains(t, err, "exceeds restore limit of 1 files per store")
}
