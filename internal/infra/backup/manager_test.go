package backup

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
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

	_, err = RunIncrementalBackup(context.Background(), logging.Testing(), store, storage, "bucket", 0)
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

	_, err := RunIncrementalBackup(context.Background(), logging.Testing(), store, storage, "bucket", 0)

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

	_, err := RunIncrementalBackup(context.Background(), logging.Testing(), store, storage, "bucket", 0)

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

	result, err := RunIncrementalBackup(context.Background(), logging.Testing(), store, storage, bucketID, 0)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Zero(t, result.OrphansDeleted)
}

// memStorage is an in-memory Storage that keeps uploaded bytes, so a test can
// both inspect segments and feed them back through ApplyExports.
type memStorage struct {
	mu    sync.Mutex
	files map[string][]byte
}

func newMemStorage() *memStorage {
	return &memStorage{files: make(map[string][]byte)}
}

func (m *memStorage) PutFile(_ context.Context, key string, data io.Reader, _ int64) error {
	b, err := io.ReadAll(data)
	if err != nil {
		return err
	}

	m.mu.Lock()
	m.files[key] = b
	m.mu.Unlock()

	return nil
}

func (m *memStorage) GetFile(_ context.Context, key string) (io.ReadCloser, error) {
	m.mu.Lock()
	b, ok := m.files[key]
	m.mu.Unlock()

	if !ok {
		return nil, ErrFileNotFound
	}

	return io.NopCloser(bytes.NewReader(b)), nil
}

func (m *memStorage) DeleteFile(_ context.Context, key string) error {
	m.mu.Lock()
	delete(m.files, key)
	m.mu.Unlock()

	return nil
}

func (m *memStorage) ListFiles(_ context.Context, prefix string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var keys []string

	for k := range m.files {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}

	return keys, nil
}

func coldAuditItemKey(seq uint64, idx uint32) []byte {
	return dal.NewKeyBuilder().PutZonePrefix(dal.ZoneCold, dal.SubColdAuditItem).PutUint64(seq).PutUint32(idx).Build()
}

// TestExportEntries_SplitsLargeRangeAndRoundTrips is the headline coverage for
// the fix: a range whose serialized entries exceed the segment cap streams into
// several bounded segments (no full-segment buffer), those segments cover the
// range contiguously, and applying them reconstructs every entry.
func TestExportEntries_SplitsLargeRangeAndRoundTrips(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	src := newBackupTestStore(t)

	const (
		n         = 50
		valueSize = 1024
		capBytes  = 8 * 1024
	)

	want := make(map[uint64][]byte, n)

	batch := src.OpenWriteSession()
	for seq := uint64(1); seq <= n; seq++ {
		val := bytes.Repeat([]byte{byte(seq)}, valueSize)
		require.NoError(t, batch.SetBytes(coldLogKey(seq), val))
		want[seq] = val
	}

	require.NoError(t, batch.Commit())

	storage := newMemStorage()

	readHandle, err := src.NewReadHandle()
	require.NoError(t, err)
	t.Cleanup(func() { _ = readHandle.Close() })

	segs, count, err := exportEntries(ctx, storage, readHandle,
		dal.ZoneCold, dal.SubColdLog, 0, n, "log",
		func(part int) string { return ExportLogSegmentKey("bucket", 1, n, part) },
		capBytes,
	)
	require.NoError(t, err)
	require.Equal(t, uint64(n), count)
	require.Greater(t, len(segs), 1, "a range larger than the cap must split into multiple segments")

	require.Equal(t, uint64(1), segs[0].StartSeq)
	require.Equal(t, uint64(n), segs[len(segs)-1].EndSeq)

	for i, seg := range segs {
		require.Equal(t, "log", seg.Type)
		require.LessOrEqual(t, seg.StartSeq, seg.EndSeq)
		require.Positive(t, seg.Size)
		require.Equal(t, ExportLogSegmentKey("bucket", 1, n, i), seg.Key)
		require.Contains(t, storage.files, seg.Key)

		if i > 0 {
			require.Equal(t, segs[i-1].EndSeq+1, seg.StartSeq, "segments must be contiguous")
		}

		if i < len(segs)-1 {
			require.GreaterOrEqual(t, seg.Size, int64(capBytes), "non-final segments are bounded near the cap")
		}
	}

	// Restore round-trip: applying the split segments reconstructs every entry.
	dst := newBackupTestStore(t)
	require.NoError(t, ApplyExports(ctx, logging.Testing(), storage, dst, segs))

	dstHandle, err := dst.NewReadHandle()
	require.NoError(t, err)
	t.Cleanup(func() { _ = dstHandle.Close() })

	for seq := uint64(1); seq <= n; seq++ {
		got, closer, err := dstHandle.Get(coldLogKey(seq))
		require.NoError(t, err, "seq %d", seq)
		require.Equal(t, want[seq], got, "seq %d", seq)
		_ = closer.Close()
	}
}

// TestExportEntries_SplitsOnlyAtSequenceBoundaries proves a single sequence's
// keys (audit items share a sequence) never straddle two segments, even when
// the cap falls mid-sequence.
func TestExportEntries_SplitsOnlyAtSequenceBoundaries(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	src := newBackupTestStore(t)

	const (
		seqs        = 10
		itemsPerSeq = 5
		valueSize   = 1024
		capBytes    = 4 * 1024 // ~4 items → cap lands inside a sequence
	)

	batch := src.OpenWriteSession()
	for seq := uint64(1); seq <= seqs; seq++ {
		for idx := range uint32(itemsPerSeq) {
			val := bytes.Repeat([]byte{byte(seq), byte(idx)}, valueSize/2)
			require.NoError(t, batch.SetBytes(coldAuditItemKey(seq, idx), val))
		}
	}

	require.NoError(t, batch.Commit())

	storage := newMemStorage()

	readHandle, err := src.NewReadHandle()
	require.NoError(t, err)
	t.Cleanup(func() { _ = readHandle.Close() })

	segs, count, err := exportEntries(ctx, storage, readHandle,
		dal.ZoneCold, dal.SubColdAuditItem, 0, seqs, "auditItem",
		func(part int) string { return ExportAuditItemSegmentKey("bucket", 1, seqs, part) },
		capBytes,
	)
	require.NoError(t, err)
	require.Equal(t, uint64(seqs*itemsPerSeq), count)
	require.Greater(t, len(segs), 1)

	// Each sequence's items must all live in exactly one segment.
	seen := make(map[uint64]int)

	for _, seg := range segs {
		rc, err := storage.GetFile(ctx, seg.Key)
		require.NoError(t, err)

		reader := NewKVStreamReader(rc)
		require.NoError(t, reader.ReadHeader())

		segSeqs := make(map[uint64]int)

		for {
			k, _, err := reader.ReadEntry()
			if errors.Is(err, io.EOF) {
				break
			}

			require.NoError(t, err)
			segSeqs[seqFromKey(k)]++
		}

		_ = rc.Close()

		for seq, itemCount := range segSeqs {
			require.Equal(t, itemsPerSeq, itemCount, "seq %d must have all its items in one segment", seq)
			require.NotContains(t, seen, seq, "seq %d must not appear in more than one segment", seq)
			seen[seq]++
		}
	}

	require.Len(t, seen, seqs, "every sequence must be exported exactly once")
}

// TestExportEntries_EmptyRangeUploadsNothing verifies that a range with no
// entries produces no segments and no upload — the appliedProposal-only-failures
// case that must not leave a manifest entry pointing at a missing object.
func TestExportEntries_EmptyRangeUploadsNothing(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	src := newBackupTestStore(t)
	storage := newMemStorage()

	readHandle, err := src.NewReadHandle()
	require.NoError(t, err)
	t.Cleanup(func() { _ = readHandle.Close() })

	segs, count, err := exportEntries(ctx, storage, readHandle,
		dal.ZoneCold, dal.SubColdAppliedProposal, 0, 100, "appliedProposal",
		func(part int) string { return ExportAppliedProposalSegmentKey("bucket", 1, 100, part) },
		maxExportSegmentBytes,
	)
	require.NoError(t, err)
	require.Empty(t, segs)
	require.Zero(t, count)
	require.Empty(t, storage.files)
}
