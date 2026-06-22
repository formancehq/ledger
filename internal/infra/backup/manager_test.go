package backup

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"slices"
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

	return store
}

// writeCorruptColdEntry writes an undecodable value under the given cold sub-zone
// so that the corresponding query.ReadLast* call fails with a decode error,
// simulating a truncated/corrupt store on the backup read path.
func writeCorruptColdEntry(t *testing.T, store *dal.Store, sub byte, seq uint64) {
	t.Helper()

	batch := store.OpenWriteSession()
	key := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneCold, sub).PutUint64(seq).Build()
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
	writeCorruptColdEntry(t, store, dal.SubColdLog, 10)

	storage := &recordingStorage{} // no prior manifest

	_, err := RunBackup(context.Background(), logging.Testing(), store, storage, "bucket", "test-backup")

	// The headline guarantee: no manifest is written when a sequence read fails.
	// The pre-fix code ignored the error and wrote a manifest with sequence 0.
	require.False(t, storage.wrote(ManifestKey("bucket")),
		"manifest must not be written when a sequence read fails")
	require.Error(t, err, "RunBackup must fail when a sequence read fails")
}

// TestRunIncrementalBackup_AbortsOnSequenceReadFailure proves the incremental
// path errors out (rather than silently proceeding) when a sequence read fails,
// and writes nothing.
func TestRunIncrementalBackup_AbortsOnSequenceReadFailure(t *testing.T) {
	t.Parallel()

	store := newBackupTestStore(t)
	writeCorruptColdEntry(t, store, dal.SubColdLog, 10)

	// A prior full backup exists, so the incremental has a base.
	manifest := &Manifest{
		Checkpoint: &CheckpointManifest{
			LastLogSequence:   5,
			LastAuditSequence: 5,
			Files:             map[string]int64{"000001.sst": 1},
		},
	}
	body, err := json.Marshal(manifest)
	require.NoError(t, err)

	storage := &recordingStorage{manifestBody: body}

	_, err = RunIncrementalBackup(context.Background(), logging.Testing(), store, storage, "bucket")
	require.Error(t, err, "RunIncrementalBackup must fail when a sequence read fails")

	require.False(t, storage.wrote(ManifestKey("bucket")),
		"manifest must not be rewritten when a sequence read fails")
}

// TestRunBackup_AbortsOnAuditSequenceReadFailure covers the audit-read error
// branch: with no log entry but a corrupt audit entry, ReadLastLog succeeds
// (empty) and ReadLastAuditSequence fails, so the backup must abort before
// writing a manifest.
func TestRunBackup_AbortsOnAuditSequenceReadFailure(t *testing.T) {
	t.Parallel()

	store := newBackupTestStore(t)
	writeCorruptColdEntry(t, store, dal.SubColdAudit, 10)

	storage := &recordingStorage{} // no prior manifest

	_, err := RunBackup(context.Background(), logging.Testing(), store, storage, "bucket", "test-backup")

	require.False(t, storage.wrote(ManifestKey("bucket")),
		"manifest must not be written when the audit sequence read fails")
	require.Error(t, err, "RunBackup must fail when the audit sequence read fails")
}

// TestRunIncrementalBackup_AbortsOnAuditSequenceReadFailure is the incremental
// counterpart for the audit-read error branch.
func TestRunIncrementalBackup_AbortsOnAuditSequenceReadFailure(t *testing.T) {
	t.Parallel()

	store := newBackupTestStore(t)
	writeCorruptColdEntry(t, store, dal.SubColdAudit, 10)

	storage := &recordingStorage{} // empty manifest is fine; we fail before the no-op check

	_, err := RunIncrementalBackup(context.Background(), logging.Testing(), store, storage, "bucket")

	require.Error(t, err, "RunIncrementalBackup must fail when the audit sequence read fails")
	require.False(t, storage.wrote(ManifestKey("bucket")),
		"manifest must not be rewritten when the audit sequence read fails")
}

// TestRunBackup_AbortsOnCorruptManifest covers the manager-level
// ReadManifestOrEmpty error branch: a corrupt existing manifest must abort the
// backup (rather than starting fresh and clobbering it).
func TestRunBackup_AbortsOnCorruptManifest(t *testing.T) {
	t.Parallel()

	store := newBackupTestStore(t)
	storage := &recordingStorage{manifestBody: []byte("{ not valid json")}

	_, err := RunBackup(context.Background(), logging.Testing(), store, storage, "bucket", "test-backup")

	require.False(t, storage.wrote(ManifestKey("bucket")),
		"manifest must not be overwritten when the existing manifest is corrupt")
	require.Error(t, err, "RunBackup must fail on a corrupt existing manifest")
}

// TestRunIncrementalBackup_AbortsOnCorruptManifest is the incremental
// counterpart for the manifest-error branch.
func TestRunIncrementalBackup_AbortsOnCorruptManifest(t *testing.T) {
	t.Parallel()

	store := newBackupTestStore(t)
	storage := &recordingStorage{manifestBody: []byte("{ not valid json")}

	_, err := RunIncrementalBackup(context.Background(), logging.Testing(), store, storage, "bucket")

	require.Error(t, err, "RunIncrementalBackup must fail on a corrupt existing manifest")
	require.False(t, storage.wrote(ManifestKey("bucket")),
		"manifest must not be overwritten when the existing manifest is corrupt")
}

// TestPruneOrphans_DeletesUnexpectedKeysAndKeepsExpected verifies the unit's
// core contract: list everything under prefix, delete the keys not in
// expectedKeys, count only successful deletes.
func TestPruneOrphans_DeletesUnexpectedKeysAndKeepsExpected(t *testing.T) {
	t.Parallel()

	storage := NewMockStorage(gomock.NewController(t))
	storage.EXPECT().ListFiles(gomock.Any(), "p/").
		Return([]string{"p/keep1", "p/orphan", "p/keep2"}, nil)
	storage.EXPECT().DeleteFile(gomock.Any(), "p/orphan").Return(nil)

	expected := map[string]struct{}{"p/keep1": {}, "p/keep2": {}}

	deleted := pruneOrphans(context.Background(), logging.Testing(), storage, "p/", expected)
	require.Equal(t, 1, deleted)
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

	deleted := pruneOrphans(context.Background(), logging.Testing(), storage, "p/", nil)
	require.Equal(t, 3, deleted)
}

// TestPruneOrphans_ToleratesListError verifies that a ListFiles failure is
// swallowed (logged elsewhere) and the function returns zero. The caller can
// safely report the backup as Succeeded because the manifest is already
// committed at this point.
func TestPruneOrphans_ToleratesListError(t *testing.T) {
	t.Parallel()

	storage := NewMockStorage(gomock.NewController(t))
	storage.EXPECT().ListFiles(gomock.Any(), "p/").
		Return(nil, errors.New("s3 timeout"))

	deleted := pruneOrphans(context.Background(), logging.Testing(), storage, "p/", nil)
	require.Zero(t, deleted)
}

// TestPruneOrphans_ContinuesOnIndividualDeleteError verifies that a failure
// on one DeleteFile does not abort the prune for the remaining orphans. The
// failed key is excluded from the returned count so the manifest's view of
// "we cleaned N files" stays accurate.
func TestPruneOrphans_ContinuesOnIndividualDeleteError(t *testing.T) {
	t.Parallel()

	storage := NewMockStorage(gomock.NewController(t))
	storage.EXPECT().ListFiles(gomock.Any(), "p/").
		Return([]string{"p/a", "p/b", "p/c"}, nil)
	storage.EXPECT().DeleteFile(gomock.Any(), "p/a").Return(nil)
	storage.EXPECT().DeleteFile(gomock.Any(), "p/b").Return(errors.New("s3 throttled"))
	storage.EXPECT().DeleteFile(gomock.Any(), "p/c").Return(nil)

	deleted := pruneOrphans(context.Background(), logging.Testing(), storage, "p/", nil)
	require.Equal(t, 2, deleted)
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

	storage.EXPECT().ListFiles(gomock.Any(), CheckpointPrefix(bucketID)).Return(nil, nil)
	storage.EXPECT().ListFiles(gomock.Any(), ExportPrefix(bucketID)).Return(nil, nil)

	result, err := RunBackup(context.Background(), logging.Testing(), store, storage, bucketID, "test-backup")
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Zero(t, result.OrphansDeleted)
}

// TestRunIncrementalBackup_InvokesPruneForExportsOnly verifies the wiring:
// the incremental backup must only touch exports/ (data/ is owned by the
// full backup and must remain untouched).
func TestRunIncrementalBackup_InvokesPruneForExportsOnly(t *testing.T) {
	t.Parallel()

	const bucketID = "bucket"

	store := newBackupTestStore(t)
	storage := NewMockStorage(gomock.NewController(t))

	// Pre-existing manifest with no entries to export, so RunIncrementalBackup
	// hits the no-op branch which still has to prune.
	manifest := &Manifest{Checkpoint: &CheckpointManifest{}}
	body, err := json.Marshal(manifest)
	require.NoError(t, err)

	storage.EXPECT().GetFile(gomock.Any(), ManifestKey(bucketID)).
		Return(io.NopCloser(bytes.NewReader(body)), nil)

	storage.EXPECT().ListFiles(gomock.Any(), ExportPrefix(bucketID)).Return(nil, nil)
	// data/ must NOT be listed by the incremental path. Any other ListFiles
	// call would fail the strict mock.

	result, err := RunIncrementalBackup(context.Background(), logging.Testing(), store, storage, bucketID)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Zero(t, result.OrphansDeleted)
}
