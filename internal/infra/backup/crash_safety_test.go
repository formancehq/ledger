package backup

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/query"
)

// orderedStorage is a fully functional in-memory Storage (like
// inMemoryBackupStorage) that additionally records the ordered sequence of
// Put/Delete operations so a test can assert crash-safety ordering:
// every referenced object is uploaded before the manifest, and no object is
// deleted before the manifest is written.
//
// putErrKeys lets a test inject a PutFile failure for keys matching any of the
// given substrings, to exercise the segment-upload failure path.
type orderedStorage struct {
	mu    sync.Mutex
	files map[string][]byte
	// ops records "put <key>" / "del <key>" in call order.
	ops []string
	// putErrKeys: if a Put key contains one of these substrings, PutFile fails.
	putErrKeys []string
}

func newOrderedStorage() *orderedStorage {
	return &orderedStorage{files: make(map[string][]byte)}
}

func (s *orderedStorage) PutFile(_ context.Context, key string, data io.Reader, _ int64) error {
	// Always drain the reader: the export path streams through an io.Pipe and
	// the writer goroutine blocks until the body is fully consumed.
	body, readErr := io.ReadAll(data)

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, frag := range s.putErrKeys {
		if strings.Contains(key, frag) {
			return errors.New("injected put failure for " + key)
		}
	}

	if readErr != nil {
		return readErr
	}

	s.files[key] = body
	s.ops = append(s.ops, "put "+key)

	return nil
}

func (s *orderedStorage) GetFile(_ context.Context, key string) (io.ReadCloser, error) {
	s.mu.Lock()
	body, ok := s.files[key]
	s.mu.Unlock()

	if !ok {
		return nil, ErrFileNotFound
	}

	return io.NopCloser(strings.NewReader(string(body))), nil
}

func (s *orderedStorage) DeleteFile(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.files, key)
	s.ops = append(s.ops, "del "+key)

	return nil
}

func (s *orderedStorage) ListFiles(_ context.Context, prefix string) ([]string, error) {
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

// opsCopy returns a snapshot of the recorded ops under the lock.
func (s *orderedStorage) opsCopy() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]string(nil), s.ops...)
}

// firstIndex returns the index of the first op satisfying pred, or -1.
func firstIndex(ops []string, pred func(string) bool) int {
	for i, op := range ops {
		if pred(op) {
			return i
		}
	}

	return -1
}

// TestRunBackup_ManifestWrittenAfterUploadsAndBeforeAnyDelete is the crash-safety
// ordering guarantee (EN-888 / EN-1055): a full backup must upload every object
// the new manifest references BEFORE writing the manifest, and must not delete
// any object before the manifest is committed. A crash at any point before the
// manifest write therefore leaves the previously published manifest fully
// restorable; a crash after it leaves the new manifest fully restorable.
func TestRunBackup_ManifestWrittenAfterUploadsAndBeforeAnyDelete(t *testing.T) {
	t.Parallel()

	const bucketID = "bucket"

	store := newBackupTestStore(t)

	// Seed a little data so the checkpoint has SST files to upload.
	seedBatch := store.OpenWriteSession()
	require.NoError(t, seedBatch.SetProto(coldLogKey(1), createLedgerLog(1, "ledger", 1)))
	require.NoError(t, seedBatch.SetProto(coldAuditKey(1), auditSuccess(1, 1, 1)))
	require.NoError(t, seedBatch.Commit())
	require.NoError(t, store.Flush())

	storage := newOrderedStorage()

	// First full backup — populates data/ and the manifest.
	_, err := RunBackup(context.Background(), logging.Testing(), store, storage, bucketID, "bk-1")
	require.NoError(t, err)

	// Write more data and compact so the second checkpoint's SST set differs
	// from the first. This gives the second backup stale checkpoint files to
	// clean up — the exact situation in which the pre-fix code issued deletes
	// BEFORE writing the manifest (opening the crash window this test guards).
	mutate := store.OpenWriteSession()
	for seq := uint64(2); seq <= 30; seq++ {
		require.NoError(t, mutate.SetProto(coldLogKey(seq), createLedgerLog(seq, "ledger", uint32(seq))))
	}
	require.NoError(t, mutate.Commit())
	require.NoError(t, store.CompactAll())

	// Reset the op log so we assert purely on the second backup's ordering.
	storage.mu.Lock()
	storage.ops = nil
	storage.mu.Unlock()

	_, err = RunBackup(context.Background(), logging.Testing(), store, storage, bucketID, "bk-2")
	require.NoError(t, err)

	ops := storage.opsCopy()
	manifestKey := ManifestKey(bucketID)

	manifestIdx := firstIndex(ops, func(op string) bool { return op == "put "+manifestKey })
	require.GreaterOrEqual(t, manifestIdx, 0, "manifest must be written")

	// Every data/ upload must precede the manifest write.
	for i, op := range ops {
		if strings.HasPrefix(op, "put "+CheckpointPrefix(bucketID)) {
			require.Less(t, i, manifestIdx,
				"checkpoint file upload %q must happen before the manifest write", op)
		}
	}

	// No delete may happen before the manifest write. On the pre-fix code the
	// second backup deleted stale checkpoint files (and old exports) at this
	// point, so firstDelIdx < manifestIdx and this assertion would fail.
	firstDelIdx := firstIndex(ops, func(op string) bool { return strings.HasPrefix(op, "del ") })
	if firstDelIdx >= 0 {
		require.Greater(t, firstDelIdx, manifestIdx,
			"no object may be deleted before the manifest is committed (crash-safety)")
	}
}

// TestRunBackup_SecondBackupSucceedsEvenWhenDeletesFail is the regression test
// for the crash-safety fix: the pre-fix code deleted stale checkpoint files and
// old export segments BEFORE writing the new manifest. This test drives a
// second full backup whose diff produces stale files to delete, using a storage
// whose DeleteFile always fails. The backup must still publish the new manifest
// — proving deletes are off the critical path and cannot block (or be observed
// before) the manifest commit.
func TestRunBackup_SecondBackupSucceedsEvenWhenDeletesFail(t *testing.T) {
	t.Parallel()

	const bucketID = "bucket"

	store := newBackupTestStore(t)

	seedBatch := store.OpenWriteSession()
	require.NoError(t, seedBatch.SetProto(coldLogKey(1), createLedgerLog(1, "ledger", 1)))
	require.NoError(t, seedBatch.SetProto(coldAuditKey(1), auditSuccess(1, 1, 1)))
	require.NoError(t, seedBatch.Commit())
	require.NoError(t, store.Flush())

	storage := &failingDeleteStorage{inner: newInMemoryBackupStorage()}

	// First full backup.
	_, err := RunBackup(context.Background(), logging.Testing(), store, storage, bucketID, "bk-1")
	require.NoError(t, err)
	require.True(t, storage.inner.has(ManifestKey(bucketID)), "first manifest must be written")

	// Mutate + compact so the second checkpoint's SST set differs, producing
	// stale files the old code would have tried to delete pre-manifest.
	mutate := store.OpenWriteSession()
	for seq := uint64(2); seq <= 20; seq++ {
		require.NoError(t, mutate.SetProto(coldLogKey(seq), createLedgerLog(seq, "ledger", uint32(seq))))
	}
	require.NoError(t, mutate.Commit())
	require.NoError(t, store.CompactAll())

	// Second full backup — DeleteFile fails for every key, but the new manifest
	// must still be committed.
	storage.failDeletes = true

	_, err = RunBackup(context.Background(), logging.Testing(), store, storage, bucketID, "bk-2")
	require.NoError(t, err, "a full backup must succeed even when every stale-file delete fails")

	manifest, err := ReadManifest(context.Background(), storage.inner, ManifestKey(bucketID))
	require.NoError(t, err)
	require.NotNil(t, manifest.Checkpoint)
	require.Empty(t, manifest.Exports)
}

// failingDeleteStorage wraps a working storage and can be flipped to fail every
// DeleteFile, to prove the backup's manifest commit does not depend on deletes.
type failingDeleteStorage struct {
	inner       *inMemoryBackupStorage
	failDeletes bool
}

func (s *failingDeleteStorage) PutFile(ctx context.Context, key string, data io.Reader, size int64) error {
	return s.inner.PutFile(ctx, key, data, size)
}

func (s *failingDeleteStorage) GetFile(ctx context.Context, key string) (io.ReadCloser, error) {
	return s.inner.GetFile(ctx, key)
}

func (s *failingDeleteStorage) DeleteFile(ctx context.Context, key string) error {
	if s.failDeletes {
		return errors.New("injected delete failure")
	}

	return s.inner.DeleteFile(ctx, key)
}

func (s *failingDeleteStorage) ListFiles(ctx context.Context, prefix string) ([]string, error) {
	return s.inner.ListFiles(ctx, prefix)
}

// has reports whether a key exists (test helper on the in-memory storage).
func (s *inMemoryBackupStorage) has(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.files[key]

	return ok
}

// TestRunIncrementalBackup_SegmentUploadFailureLeavesManifestUntouched proves a
// failed export segment upload never publishes a manifest referencing the
// missing object. The manifest on storage must remain byte-identical to the one
// present before the run, so a subsequent restore keeps working against the
// last good backup.
func TestRunIncrementalBackup_SegmentUploadFailureLeavesManifestUntouched(t *testing.T) {
	t.Parallel()

	const bucketID = "bucket"

	store := newBackupTestStore(t)

	batch := store.OpenWriteSession()
	for seq := uint64(1); seq <= 5; seq++ {
		require.NoError(t, batch.SetProto(coldLogKey(seq), createLedgerLog(seq, "ledger", uint32(seq))))
		require.NoError(t, batch.SetProto(coldAuditKey(seq), auditSuccess(seq, seq, seq)))
	}
	require.NoError(t, batch.Commit())

	storage := newOrderedStorage()
	// Fail every log-segment upload. The export writes the log segment first,
	// so RunIncrementalBackup must abort before touching the manifest.
	storage.putErrKeys = []string{"/exports/logs-"}

	// A prior full-backup manifest with a known body.
	base := &Manifest{Checkpoint: &CheckpointManifest{LastLogSequence: 0, LastAuditSequence: 0}}
	require.NoError(t, WriteManifest(context.Background(), storage, ManifestKey(bucketID), base))

	manifestBefore, err := storage.GetFile(context.Background(), ManifestKey(bucketID))
	require.NoError(t, err)
	beforeBytes, err := io.ReadAll(manifestBefore)
	require.NoError(t, err)
	_ = manifestBefore.Close()

	_, err = RunIncrementalBackup(context.Background(), logging.Testing(), store, storage, bucketID, 0)
	require.Error(t, err, "a failed segment upload must fail the incremental backup")

	// The manifest on storage is unchanged: the run never got to WriteManifest.
	manifestAfter, err := storage.GetFile(context.Background(), ManifestKey(bucketID))
	require.NoError(t, err)
	afterBytes, err := io.ReadAll(manifestAfter)
	require.NoError(t, err)
	_ = manifestAfter.Close()

	require.Equal(t, beforeBytes, afterBytes,
		"manifest must be byte-identical after a failed segment upload")
}

// TestBackup_MultipleIncrementalsChain_RoundTrips exercises the full →
// N-incrementals restore chain at the unit level: a full backup, then several
// rounds of (write new logs/audits → incremental backup), accumulating export
// segments in one manifest. Applying every accumulated segment onto a store
// that already holds the pre-checkpoint content (here reproduced by log replay,
// since raw SST checkpoint files cannot be ingested into a fresh Pebble store
// via a WriteSession) and rebuilding must reconstruct every post-checkpoint
// ledger across all incrementals. The end-to-end variant, including the opaque
// SST checkpoint restore, lives in tests/e2e/cluster/restore_test.go.
func TestBackup_MultipleIncrementalsChain_RoundTrips(t *testing.T) {
	t.Parallel()

	const bucketID = "bucket"

	ctx := context.Background()
	src := newBackupTestStore(t)
	storage := newInMemoryBackupStorage()

	seq := uint64(0)
	writeLedgerLog := func(name string) {
		seq++
		b := src.OpenWriteSession()
		require.NoError(t, b.SetProto(coldLogKey(seq), createLedgerLog(seq, name, uint32(seq))))
		require.NoError(t, b.SetProto(coldAuditKey(seq), auditSuccess(seq, seq, seq)))
		require.NoError(t, b.Commit())
	}

	// Pre-checkpoint data.
	writeLedgerLog("ledger-0")
	require.NoError(t, src.Flush())

	// Full backup captures ledger-0 in the checkpoint (LastLogSequence=1).
	fullResult, err := RunBackup(ctx, logging.Testing(), src, storage, bucketID, "bk-full")
	require.NoError(t, err)
	require.Positive(t, fullResult.TotalFiles)
	require.EqualValues(t, 1, fullResult.LastLogSequence)

	// Three incremental rounds, each adding one post-checkpoint ledger.
	var incrementalLedgers []string

	for round := range 3 {
		name := "ledger-inc-" + string(rune('a'+round))
		incrementalLedgers = append(incrementalLedgers, name)
		writeLedgerLog(name)

		incResult, err := RunIncrementalBackup(ctx, logging.Testing(), src, storage, bucketID, 0)
		require.NoError(t, err)
		require.Positive(t, incResult.LogEntriesExported, "round %d must export the new log", round)
	}

	// The manifest now holds the checkpoint plus every incremental's segments.
	manifest, err := ReadManifest(ctx, storage, ManifestKey(bucketID))
	require.NoError(t, err)
	require.NotNil(t, manifest.Checkpoint)
	require.NotEmpty(t, manifest.Exports)

	// Reproduce the checkpoint's logical content on a fresh store (pre-checkpoint
	// log at seq 1), then apply every accumulated incremental export and rebuild.
	dst := newBackupTestStore(t)

	seedBatch := dst.OpenWriteSession()
	require.NoError(t, seedBatch.SetProto(coldLogKey(1), createLedgerLog(1, "ledger-0", 1)))
	require.NoError(t, seedBatch.SetProto(coldAuditKey(1), auditSuccess(1, 1, 1)))
	require.NoError(t, seedBatch.Commit())

	require.NoError(t, ApplyExportsAndRebuild(ctx, logging.Testing(), storage, dst, manifest))

	// Verify: the last log sequence on the restored store equals the source's,
	// and every incremental ledger is present in the rebuilt global zone.
	handle, err := dst.NewDirectReadHandle()
	require.NoError(t, err)
	defer func() { _ = handle.Close() }()

	restoredLastLog, err := query.ReadLastLog(handle)
	require.NoError(t, err)
	require.NotNil(t, restoredLastLog)
	require.Equal(t, seq, restoredLastLog.GetSequence(),
		"restored store must hold every log across the full + all incrementals")

	for _, name := range incrementalLedgers {
		info, err := query.GetLedgerByName(ctx, handle, name)
		require.NoError(t, err, "ledger %q written post-checkpoint must be restored", name)
		require.NotNil(t, info, "ledger %q must exist after full + multi-incremental restore", name)
	}
}
