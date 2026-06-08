package backup

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"slices"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

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

	batch := store.NewBatch()
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

	_, err := RunBackup(context.Background(), logging.Testing(), store, storage, "bucket")

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

	_, err := RunBackup(context.Background(), logging.Testing(), store, storage, "bucket")

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

	_, err := RunBackup(context.Background(), logging.Testing(), store, storage, "bucket")

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
