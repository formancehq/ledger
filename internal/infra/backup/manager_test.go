package backup

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/mock/gomock"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// recordingStorage serves a fixed manifest body (or not-found) and records every
// PutFile key, so a test can assert whether the manifest was (re)written.
type recordingStorage struct {
	manifestBody []byte // nil => GetFile returns ErrFileNotFound

	mu      sync.Mutex
	putKeys []string
}

func (r *recordingStorage) PutFile(_ context.Context, key string, data io.Reader, _ int64) error {
	_, _ = io.Copy(io.Discard, data)

	r.mu.Lock()
	r.putKeys = append(r.putKeys, key)
	r.mu.Unlock()

	return nil
}

func (r *recordingStorage) GetFile(_ context.Context, _ string) (io.ReadCloser, error) {
	if r.manifestBody == nil {
		return nil, ErrFileNotFound
	}

	return io.NopCloser(bytes.NewReader(r.manifestBody)), nil
}

func (r *recordingStorage) DeleteFile(_ context.Context, _ string) error { return nil }

func (r *recordingStorage) ListFiles(_ context.Context, _ string) ([]string, error) {
	return nil, nil
}

func (r *recordingStorage) wrote(key string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	return slices.Contains(r.putKeys, key)
}

func newBackupTestStore(t *testing.T) *dal.Store {
	t.Helper()

	ctx := logging.TestingContext()
	store, err := dal.NewStore(t.TempDir(), logging.FromContext(ctx), noop.NewMeterProvider().Meter("test"), dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	seedBackupTestAuditKey(t, store)

	return store
}

func seedBackupTestAuditKey(t *testing.T, store *dal.Store) {
	t.Helper()
	batch := store.OpenWriteSession()
	require.NoError(t, batch.SetBytes([]byte{dal.ZoneGlobal, dal.SubGlobAuditKey}, []byte("0123456789abcdef0123456789abcdef")))
	require.NoError(t, batch.Commit())
}

func TestRunBackupFlushesCommittedMemtable(t *testing.T) {
	t.Parallel()
	store := newBackupTestStore(t)
	batch := store.OpenWriteSession()
	require.NoError(t, batch.SetBytes([]byte("confirmed-transaction"), []byte("present")))
	require.NoError(t, batch.Commit())

	storage := newInMemoryBackupStorage()
	_, err := RunBackup(t.Context(), logging.Testing(), store, storage, "bucket", "flushed-backup")
	require.NoError(t, err)
	manifest, err := ReadManifest(t.Context(), storage, ManifestKey("bucket"))
	require.NoError(t, err)
	require.NotEmpty(t, manifest.Checkpoint.Files)

	// The idle fixture has no concurrent writer. Reopen the uploaded checkpoint
	// without its WAL files to prove the confirmed value reached an SST.
	dir := t.TempDir()
	for name, file := range manifest.Checkpoint.Files {
		if strings.HasSuffix(name, ".log") {
			continue
		}
		path := filepath.Join(dir, filepath.FromSlash(name))
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
		src, getErr := storage.GetFile(t.Context(), file.Key)
		require.NoError(t, getErr)
		data, readErr := io.ReadAll(src)
		require.NoError(t, readErr)
		require.NoError(t, src.Close())
		require.NoError(t, os.WriteFile(path, data, 0o600))
	}
	checkpoint, err := dal.OpenReadOnly(dir, logging.Testing())
	require.NoError(t, err)
	defer func() { require.NoError(t, checkpoint.Close()) }()
	got, closer, err := checkpoint.Get([]byte("confirmed-transaction"))
	require.NoError(t, err)
	require.Equal(t, []byte("present"), got)
	require.NoError(t, closer.Close())
}

// writeCorruptColdEntry writes an undecodable value under the given cold sub-zone
// so that the corresponding query.ReadLast* call fails with a decode error,
// simulating a truncated/corrupt store on the backup read path.
func writeCorruptColdEntry(t *testing.T, store *dal.Store, sub byte, seq uint64) {
	t.Helper()

	batch := store.OpenWriteSession()
	key := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneHistory, sub).PutUint64(seq).Build()
	require.NoError(t, batch.SetBytes(key, []byte{0xff, 0xff, 0xff, 0xff}))
	require.NoError(t, batch.Commit())
}

// TestRunBackup_AbortsBeforeWritingManifestOnSequenceReadFailure proves the
// headline risk is fixed: when a sequence read fails, RunBackup must NOT write
// a manifest (the pre-fix code ignored the error and wrote one with sequence 0,
// silently regressing backup history).
func TestRunBackup_AbortsBeforeWritingManifestOnSequenceReadFailure(t *testing.T) {
	t.Parallel()

	store := newBackupTestStore(t)
	// Corrupt the last log entry; the backup checkpoints the store, opens it,
	// and query.ReadLastLog then fails to decode.
	writeCorruptColdEntry(t, store, dal.SubHistoryLog, 10)

	storage := &recordingStorage{} // no prior manifest

	_, err := RunBackup(context.Background(), logging.Testing(), store, storage, "bucket", "test-backup")

	// The headline guarantee: no manifest is written when a sequence read fails.
	// The pre-fix code ignored the error and wrote a manifest with sequence 0.
	require.False(t, storage.wrote(ManifestKey("bucket")),
		"manifest must not be written when a sequence read fails")
	require.Error(t, err, "RunBackup must fail when a sequence read fails")
}

// A corrupt audit sequence must not publish a manifest.
func TestRunBackup_AbortsOnAuditSequenceReadFailure(t *testing.T) {
	t.Parallel()

	store := newBackupTestStore(t)
	writeCorruptColdEntry(t, store, dal.SubHistoryAudit, 10)

	storage := &recordingStorage{} // no prior manifest

	_, err := RunBackup(context.Background(), logging.Testing(), store, storage, "bucket", "test-backup")

	require.False(t, storage.wrote(ManifestKey("bucket")),
		"manifest must not be written when the audit sequence read fails")
	require.Error(t, err, "RunBackup must fail when the audit sequence read fails")
}

// A corrupt existing manifest must not be overwritten.
func TestRunBackup_AbortsOnCorruptManifest(t *testing.T) {
	t.Parallel()

	store := newBackupTestStore(t)
	storage := &recordingStorage{manifestBody: []byte("{ not valid json")}

	_, err := RunBackup(context.Background(), logging.Testing(), store, storage, "bucket", "test-backup")

	require.False(t, storage.wrote(ManifestKey("bucket")),
		"manifest must not be overwritten when the existing manifest is corrupt")
	require.Error(t, err, "RunBackup must fail on a corrupt existing manifest")
}

// An invalid checkpoint file entry must not allow a full backup to replace
// the existing manifest.
func TestRunBackup_AbortsOnInvalidFileEntry(t *testing.T) {
	t.Parallel()

	store := newBackupTestStore(t)
	invalid := []byte(`{"checkpoint":{"timestamp":"t","lastAppliedIndex":1,"lastLogSequence":1,"lastAuditSequence":1,"files":{"000001.sst":123}},"exports":null}`)
	storage := &recordingStorage{manifestBody: invalid}

	_, err := RunBackup(context.Background(), logging.Testing(), store, storage, "bucket", "test-backup")

	require.Error(t, err)
	require.False(t, storage.wrote(ManifestKey("bucket")),
		"RunBackup must not overwrite an unreadable manifest")
}

func anyKeyspace(root string) keyspace {
	return keyspace{root: root, owned: func(string) bool { return true }}
}

// TestPruneOrphans_DeletesUnexpectedKeysAndKeepsExpected verifies the unit's
// core contract: list everything under prefix, delete the keys not in
// expectedKeys, and return only the successfully deleted keys.
func TestPruneOrphans_DeletesUnexpectedKeysAndKeepsExpected(t *testing.T) {
	t.Parallel()

	storage := NewMockStorage(gomock.NewController(t))
	storage.EXPECT().ListFiles(gomock.Any(), "p/").
		Return([]string{"p/keep1", "p/orphan", "p/keep2"}, nil)
	storage.EXPECT().DeleteFile(gomock.Any(), "p/orphan").Return(nil)

	expected := map[string]struct{}{"p/keep1": {}, "p/keep2": {}}

	deleted := pruneOrphans(context.Background(), logging.Testing(), storage, anyKeyspace("p/"), expected)
	require.Equal(t, []string{"p/orphan"}, deleted)
}

// TestPruneOrphans_EmptyExpectedDeletesAll verifies the "wipe-everything"
// branch used by RunBackup for the exports/ prefix after a full backup
// (which always resets manifest.Exports to nil).
func TestPruneOrphans_EmptyExpectedDeletesAll(t *testing.T) {
	t.Parallel()

	storage := NewMockStorage(gomock.NewController(t))
	storage.EXPECT().ListFiles(gomock.Any(), "p/").
		Return([]string{"p/a", "p/b", "p/c"}, nil)
	storage.EXPECT().DeleteFile(gomock.Any(), "p/a").Return(nil)
	storage.EXPECT().DeleteFile(gomock.Any(), "p/b").Return(nil)
	storage.EXPECT().DeleteFile(gomock.Any(), "p/c").Return(nil)

	deleted := pruneOrphans(context.Background(), logging.Testing(), storage, anyKeyspace("p/"), nil)
	require.ElementsMatch(t, []string{"p/a", "p/b", "p/c"}, deleted)
}

// TestPruneOrphans_ToleratesListError verifies that a ListFiles failure is
// swallowed (logged elsewhere) and the function returns no keys. The caller can
// safely report the backup as Succeeded because the manifest is already
// committed at this point.
func TestPruneOrphans_ToleratesListError(t *testing.T) {
	t.Parallel()

	storage := NewMockStorage(gomock.NewController(t))
	storage.EXPECT().ListFiles(gomock.Any(), "p/").
		Return(nil, errors.New("s3 timeout"))

	deleted := pruneOrphans(context.Background(), logging.Testing(), storage, anyKeyspace("p/"), nil)
	require.Empty(t, deleted)
}

// TestPruneOrphans_ContinuesOnIndividualDeleteError verifies that a failure
// on one DeleteFile does not abort the prune for the remaining orphans. The
// failed key is excluded from the returned keys so the manifest's view of
// "we cleaned N files" stays accurate.
func TestPruneOrphans_ContinuesOnIndividualDeleteError(t *testing.T) {
	t.Parallel()

	storage := NewMockStorage(gomock.NewController(t))
	storage.EXPECT().ListFiles(gomock.Any(), "p/").
		Return([]string{"p/a", "p/b", "p/c"}, nil)
	storage.EXPECT().DeleteFile(gomock.Any(), "p/a").Return(nil)
	storage.EXPECT().DeleteFile(gomock.Any(), "p/b").Return(errors.New("s3 throttled"))
	storage.EXPECT().DeleteFile(gomock.Any(), "p/c").Return(nil)

	deleted := pruneOrphans(context.Background(), logging.Testing(), storage, anyKeyspace("p/"), nil)
	require.Equal(t, []string{"p/a", "p/c"}, deleted)
}

// TestRunBackup_InvokesPruneForBothPrefixes verifies the wiring: a full
// backup must run the prune step against both data/ and exports/ — the
// authority for "what is the right prefix" lives in RunBackup, not in the
// pruneOrphans unit.
func TestRunBackup_InvokesPruneForBothPrefixes(t *testing.T) {
	t.Parallel()

	const bucketID = "bucket"

	store := newBackupTestStore(t)
	storage := NewMockStorage(gomock.NewController(t))

	// Manifest reads return not-found (first run); puts and deletes are
	// absorbed; ListFiles returns nothing under both expected prefixes.
	storage.EXPECT().GetFile(gomock.Any(), gomock.Any()).Return(nil, ErrFileNotFound).AnyTimes()
	storage.EXPECT().PutFile(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, data io.Reader, _ int64) error {
			_, _ = io.Copy(io.Discard, data)

			return nil
		}).AnyTimes()
	storage.EXPECT().DeleteFile(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	// CheckpointPrefix is listed twice: once for the existence check (step 4)
	// and once by the post-manifest prune (step 8). ExportPrefix is listed once
	// by the prune.
	storage.EXPECT().ListFiles(gomock.Any(), CheckpointPrefix(bucketID)).Return(nil, nil).Times(2)
	storage.EXPECT().ListFiles(gomock.Any(), ExportPrefix(bucketID)).Return(nil, nil)

	result, err := RunBackup(context.Background(), logging.Testing(), store, storage, bucketID, "test-backup")
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Zero(t, result.OrphansDeleted)
	require.Zero(t, result.FilesDeleted)
}

// TestRunBackup_ClassifiesStaleVsOrphanDeletions verifies the FilesDeleted /
// OrphansDeleted split: a checkpoint object the PREVIOUS manifest referenced
// that the new backup supersedes is counted as FilesDeleted (ordinary churn),
// while a data/ object no manifest ever referenced (a leftover from a crashed
// run) is counted as OrphansDeleted.
func TestRunBackup_ClassifiesStaleVsOrphanDeletions(t *testing.T) {
	t.Parallel()

	const bucketID = "bucket"

	store := newBackupTestStore(t)

	seed := store.OpenWriteSession()
	require.NoError(t, seed.SetProto(coldLogKey(1), createLedgerLog(1, "ledger", 1)))
	require.NoError(t, seed.SetProto(coldAuditKey(1), auditSuccess(1, 1, 1)))
	require.NoError(t, seed.Commit())
	require.NoError(t, store.Flush())

	storage := newInMemoryBackupStorage()

	// First full backup establishes the baseline manifest and its checkpoint
	// objects under data/.
	first, err := RunBackup(context.Background(), logging.Testing(), store, storage, bucketID, "bk-1")
	require.NoError(t, err)
	require.Zero(t, first.FilesDeleted, "first backup supersedes nothing")

	prevManifest, err := ReadManifest(context.Background(), storage, ManifestKey(bucketID))
	require.NoError(t, err)
	prevKeys := checkpointKeySet(prevManifest)
	require.NotEmpty(t, prevKeys)

	// Inject a true orphan under data/ that no manifest references (as a run
	// crashing after upload but before manifest commit would leave behind).
	// Built through CheckpointFileKey: a crashed run leaves a real uploader's
	// key, and only such a key is this chain's to prune.
	orphanKey := CheckpointFileKey(bucketID, "leaked-by-crashed-run.sst", strings.Repeat("ab", sha256HexLen/2))
	require.NoError(t, storage.PutFile(context.Background(), orphanKey,
		bytes.NewReader([]byte("garbage")), int64(len("garbage"))))

	// Mutate + compact so the second checkpoint's SST set differs from the first,
	// leaving some of the previous manifest's checkpoint objects stale.
	mutate := store.OpenWriteSession()
	for seq := uint64(2); seq <= 20; seq++ {
		require.NoError(t, mutate.SetProto(coldLogKey(seq), createLedgerLog(seq, "ledger", uint32(seq))))
	}
	require.NoError(t, mutate.Commit())
	require.NoError(t, store.CompactAll())

	second, err := RunBackup(context.Background(), logging.Testing(), store, storage, bucketID, "bk-2")
	require.NoError(t, err)

	// The injected orphan (never in any manifest) must be classified as an
	// orphan, never as a stale file.
	require.GreaterOrEqual(t, second.OrphansDeleted, 1,
		"the leaked, never-referenced object must count as an orphan")

	// The stale checkpoint objects the previous manifest referenced and the new
	// one dropped must be counted as FilesDeleted.
	newManifest, err := ReadManifest(context.Background(), storage, ManifestKey(bucketID))
	require.NoError(t, err)
	newKeys := checkpointKeySet(newManifest)

	expectedStale := 0
	for k := range prevKeys {
		if _, stillReferenced := newKeys[k]; !stillReferenced {
			expectedStale++
		}
	}

	require.Positive(t, expectedStale, "test setup must produce at least one superseded checkpoint object")
	require.Equal(t, expectedStale, second.FilesDeleted,
		"every superseded previous-manifest object must be counted as FilesDeleted")

	// The injected orphan must be gone, and no orphan should have been
	// miscounted as a stale file.
	_, err = storage.GetFile(context.Background(), orphanKey)
	require.ErrorIs(t, err, ErrFileNotFound, "the orphan must have been pruned")
}

// TestRunBackup_ClassifiesSupersededExportsAsFiles verifies that legacy export
// segments referenced by the previous manifest are counted as
// FilesDeleted (ordinary supersede churn), while an export object no manifest
// ever referenced is counted as OrphansDeleted. Regression: previously every
// export deletion was miscounted as an orphan.
func TestRunBackup_ClassifiesSupersededExportsAsFiles(t *testing.T) {
	t.Parallel()

	const bucketID = "bucket"

	store := newBackupTestStore(t)

	seed := store.OpenWriteSession()
	require.NoError(t, seed.SetProto(coldLogKey(1), createLedgerLog(1, "ledger", 1)))
	require.NoError(t, seed.SetProto(coldAuditKey(1), auditSuccess(1, 1, 1)))
	require.NoError(t, seed.Commit())
	require.NoError(t, store.Flush())

	storage := newInMemoryBackupStorage()

	// 1. Full backup — baseline checkpoint, no exports.
	_, err := RunBackup(context.Background(), logging.Testing(), store, storage, bucketID, "bk-1")
	require.NoError(t, err)

	// 2. Simulate an old RC manifest and its one referenced export object.
	incManifest, err := ReadManifest(context.Background(), storage, ManifestKey(bucketID))
	require.NoError(t, err)
	legacyExport := bucketID + "/exports/logs-00000000000000000002-00000000000000000010-00000.bin"
	incManifest.Exports = []ExportSegment{{Type: "log", StartSeq: 2, EndSeq: 10, Key: legacyExport, Size: 6}}
	require.NoError(t, storage.PutFile(context.Background(), legacyExport, bytes.NewReader([]byte("legacy")), 6))
	require.NoError(t, WriteManifest(context.Background(), storage, ManifestKey(bucketID), incManifest))
	prevExportKeys := exportKeySet(incManifest)
	prevCheckpointKeys := checkpointKeySet(incManifest)

	// 3. Inject an old-format export object that no manifest references.
	orphanExport := bucketID + "/exports/logs-00000000000000900000-00000000000000900001-00000.bin"
	require.NoError(t, storage.PutFile(context.Background(), orphanExport,
		bytes.NewReader([]byte("garbage")), int64(len("garbage"))))

	// 4. Mutate + compact so the new full checkpoint's SST set differs, then run
	// a full backup that rolls the exports up and prunes everything under exports/.
	mutate := store.OpenWriteSession()
	for seq := uint64(11); seq <= 30; seq++ {
		require.NoError(t, mutate.SetProto(coldLogKey(seq), createLedgerLog(seq, "ledger", uint32(seq))))
	}
	require.NoError(t, mutate.Commit())
	require.NoError(t, store.CompactAll())

	full, err := RunBackup(context.Background(), logging.Testing(), store, storage, bucketID, "bk-3")
	require.NoError(t, err)

	newManifest, err := ReadManifest(context.Background(), storage, ManifestKey(bucketID))
	require.NoError(t, err)
	newCheckpointKeys := checkpointKeySet(newManifest)

	expectedStaleCheckpoints := 0
	for k := range prevCheckpointKeys {
		if _, stillReferenced := newCheckpointKeys[k]; !stillReferenced {
			expectedStaleCheckpoints++
		}
	}

	// Every superseded checkpoint file AND every rolled-up export segment counts
	// as FilesDeleted; the injected, never-referenced export is the only orphan.
	require.Equal(t, expectedStaleCheckpoints+len(prevExportKeys), full.FilesDeleted,
		"superseded checkpoint files and rolled-up export segments must all count as FilesDeleted")
	require.Equal(t, 1, full.OrphansDeleted,
		"only the never-referenced injected export must count as an orphan")

	_, err = storage.GetFile(context.Background(), orphanExport)
	require.ErrorIs(t, err, ErrFileNotFound, "the orphan export must have been pruned")
}

// inMemoryBackupStorage is a fully-functional in-memory Storage implementation
// (unlike recordingStorage which discards PutFile bodies). It lets tests
// exercise manifest and object cleanup without a real storage backend.
type inMemoryBackupStorage struct {
	mu    sync.Mutex
	files map[string][]byte
}

func newInMemoryBackupStorage() *inMemoryBackupStorage {
	return &inMemoryBackupStorage{files: make(map[string][]byte)}
}

func (s *inMemoryBackupStorage) PutFile(_ context.Context, key string, data io.Reader, _ int64) error {
	body, err := io.ReadAll(data)
	if err != nil {
		return err
	}

	s.mu.Lock()
	s.files[key] = body
	s.mu.Unlock()

	return nil
}

func (s *inMemoryBackupStorage) GetFile(_ context.Context, key string) (io.ReadCloser, error) {
	s.mu.Lock()
	body, ok := s.files[key]
	s.mu.Unlock()

	if !ok {
		return nil, ErrFileNotFound
	}

	return io.NopCloser(bytes.NewReader(body)), nil
}

func (s *inMemoryBackupStorage) DeleteFile(_ context.Context, key string) error {
	s.mu.Lock()
	delete(s.files, key)
	s.mu.Unlock()

	return nil
}

func (s *inMemoryBackupStorage) ListFiles(_ context.Context, prefix string) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var keys []string
	for k := range s.files {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}

	return keys, nil
}
