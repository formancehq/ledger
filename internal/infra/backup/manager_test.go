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

	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
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
			Files:             map[string]CheckpointFile{"000001.sst": {Size: 1, Key: CheckpointFileKey("bucket", "000001.sst", "deadbeef")}},
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

// TestRunBackup_ProceedsOnLegacyManifest verifies that a full backup does NOT
// abort when the destination already holds a legacy pre-content-addressing
// manifest: the full backup overwrites the manifest wholesale and never diffs
// against it, so retaking a full backup is exactly the documented recovery path
// out of a legacy manifest. It must proceed and publish a new manifest
// (addresses the NumaryBot review on PR #1543). This is deliberately different
// from the corrupt-JSON case above, which stays fatal.
func TestRunBackup_ProceedsOnLegacyManifest(t *testing.T) {
	t.Parallel()

	store := newBackupTestStore(t)
	legacy := []byte(`{"checkpoint":{"timestamp":"t","lastAppliedIndex":1,"lastLogSequence":1,"lastAuditSequence":1,"files":{"000001.sst":123}},"exports":null}`)
	storage := &recordingStorage{manifestBody: legacy}

	_, err := RunBackup(context.Background(), logging.Testing(), store, storage, "bucket", "test-backup")

	require.NoError(t, err, "RunBackup must proceed past a legacy manifest and replace it")
	require.True(t, storage.wrote(ManifestKey("bucket")),
		"RunBackup must publish a fresh manifest, overwriting the legacy one")
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
// expectedKeys, and return only the successfully deleted keys.
func TestPruneOrphans_DeletesUnexpectedKeysAndKeepsExpected(t *testing.T) {
	t.Parallel()

	storage := NewMockStorage(gomock.NewController(t))
	storage.EXPECT().ListFiles(gomock.Any(), "p/").
		Return([]string{"p/keep1", "p/orphan", "p/keep2"}, nil)
	storage.EXPECT().DeleteFile(gomock.Any(), "p/orphan").Return(nil)

	expected := map[string]struct{}{"p/keep1": {}, "p/keep2": {}}

	deleted := pruneOrphans(context.Background(), logging.Testing(), storage, "p/", expected)
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

	deleted := pruneOrphans(context.Background(), logging.Testing(), storage, "p/", nil)
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

	deleted := pruneOrphans(context.Background(), logging.Testing(), storage, "p/", nil)
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

	deleted := pruneOrphans(context.Background(), logging.Testing(), storage, "p/", nil)
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
	orphanKey := CheckpointPrefix(bucketID) + "leaked-by-crashed-run.sst"
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

// TestRunBackup_ClassifiesSupersededExportsAsFiles verifies that when a full
// backup follows an incremental, the export segments the previous (incremental)
// manifest referenced — now rolled up into the new checkpoint — are counted as
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

	// 2. Add cold entries beyond the checkpoint and run an incremental so the
	// committed manifest references live export segments under exports/.
	more := store.OpenWriteSession()
	for seq := uint64(2); seq <= 10; seq++ {
		require.NoError(t, more.SetProto(coldLogKey(seq), createLedgerLog(seq, "ledger", uint32(seq))))
		require.NoError(t, more.SetProto(coldAuditKey(seq), auditSuccess(seq, seq, seq)))
	}
	require.NoError(t, more.Commit())
	require.NoError(t, store.Flush())

	_, err = RunIncrementalBackup(context.Background(), logging.Testing(), store, storage, bucketID, 0)
	require.NoError(t, err)

	incManifest, err := ReadManifest(context.Background(), storage, ManifestKey(bucketID))
	require.NoError(t, err)
	prevExportKeys := exportKeySet(incManifest)
	require.NotEmpty(t, prevExportKeys, "incremental must have produced export segments")
	prevCheckpointKeys := checkpointKeySet(incManifest)

	// 3. Inject a true orphan export object no manifest references.
	orphanExport := ExportPrefix(bucketID) + "leaked-by-crashed-run.seg"
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
// round-trip through RunIncrementalBackup → ApplyExports without a real
// storage backend.
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

// writeFailureAuditEntry writes a single AuditEntry proto with a Failure
// outcome under [ZoneCold][SubColdAudit][seq]. No AuditItem is written —
// this mirrors the FSM's failure path (state.machine.go calls
// writeAuditEntry(failureEntry, nil, "failure"), and appendAuditItems on an
// empty slice is a no-op), producing the "audit count > 0, auditItem
// count == 0" state that EN-1424 targets.
func writeFailureAuditEntry(t *testing.T, store *dal.Store, seq uint64) {
	t.Helper()

	entry := &auditpb.AuditEntry{
		Sequence: seq,
		Outcome: &auditpb.AuditEntry_Failure{
			Failure: &auditpb.AuditFailure{Reason: commonpb.ErrorReason_ERROR_REASON_INSUFFICIENT_FUNDS},
		},
	}

	batch := store.OpenWriteSession()
	key := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneCold, dal.SubColdAudit).PutUint64(seq).Build()
	require.NoError(t, batch.SetProto(key, entry))
	require.NoError(t, batch.Commit())
}

// writeSuccessAuditEntryWithItem writes a success AuditEntry AND its matching
// AuditItem under seq / order 0, mirroring what the FSM produces on the
// success path.
func writeSuccessAuditEntryWithItem(t *testing.T, store *dal.Store, seq uint64) {
	t.Helper()

	entry := &auditpb.AuditEntry{
		Sequence: seq,
		Outcome: &auditpb.AuditEntry_Success{
			Success: &auditpb.AuditSuccess{MinLogSequence: seq, MaxLogSequence: seq},
		},
	}
	item := &auditpb.AuditItem{OrderIndex: 0}

	batch := store.OpenWriteSession()
	entryKey := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneCold, dal.SubColdAudit).PutUint64(seq).Build()
	itemKey := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneCold, dal.SubColdAuditItem).PutUint64(seq).PutUint32(0).Build()
	require.NoError(t, batch.SetProto(entryKey, entry))
	require.NoError(t, batch.SetProto(itemKey, item))
	require.NoError(t, batch.Commit())
}

// findExport returns the ExportSegment of the given Type from a manifest, or
// nil if no such segment exists.
func findExport(manifest *Manifest, segType string) *ExportSegment {
	for i := range manifest.Exports {
		if manifest.Exports[i].Type == segType {
			return &manifest.Exports[i]
		}
	}

	return nil
}

// TestRunIncrementalBackup_FailureOnlyRange_SkipsAuditItemSegment is the
// EN-1424 regression: an incremental range whose entries are ALL failure
// AuditEntries produces zero AuditItems, so exportEntries uploads no
// auditItem object. The manifest must not reference the missing key, or a
// subsequent ApplyExports fails on GetFile and the backup is silently
// un-restorable. Mirrors the guard already applied to the appliedProposal
// branch.
func TestRunIncrementalBackup_FailureOnlyRange_SkipsAuditItemSegment(t *testing.T) {
	t.Parallel()

	const bucketID = "bucket"

	store := newBackupTestStore(t)
	// Three failure-only audit entries at seqs 1..3: no matching AuditItem,
	// no AppliedProposal.
	writeFailureAuditEntry(t, store, 1)
	writeFailureAuditEntry(t, store, 2)
	writeFailureAuditEntry(t, store, 3)

	// Prior manifest with LastAuditSequence=0 so the incremental range is
	// (0, 3] — entirely failures.
	storage := newInMemoryBackupStorage()
	require.NoError(t, WriteManifest(context.Background(), storage, ManifestKey(bucketID), &Manifest{
		Checkpoint: &CheckpointManifest{LastLogSequence: 0, LastAuditSequence: 0},
	}))

	result, err := RunIncrementalBackup(context.Background(), logging.Testing(), store, storage, bucketID, 0)
	require.NoError(t, err)
	require.NotNil(t, result)

	manifest, err := ReadManifest(context.Background(), storage, ManifestKey(bucketID))
	require.NoError(t, err)

	// The audit segment must be present (three failure entries were exported).
	auditSeg := findExport(manifest, "audit")
	require.NotNil(t, auditSeg, "audit segment must be exported for failure-only range")
	require.EqualValues(t, 1, auditSeg.StartSeq)
	require.EqualValues(t, 3, auditSeg.EndSeq)

	// The bug: manifest referenced an auditItem segment even when count==0
	// (no failure produced any item), leaving a dangling reference to a
	// non-existent object.
	require.Nil(t, findExport(manifest, "auditItem"),
		"auditItem segment must NOT be referenced when the range contains no items (regression: EN-1424)")

	// The appliedProposal branch already had this guard; assert it still holds.
	require.Nil(t, findExport(manifest, "appliedProposal"),
		"appliedProposal segment must NOT be referenced when the range contains no successes")

	// Round-trip: ApplyExports against a fresh store must succeed with the
	// generated manifest. Pre-fix, this failed at GetFile("...auditItem/...")
	// with ErrFileNotFound.
	restoreStore := newBackupTestStore(t)
	require.NoError(t, ApplyExports(context.Background(), logging.Testing(), storage, restoreStore, manifest.Exports),
		"ApplyExports must round-trip on a failure-only incremental backup")
}

// TestRunIncrementalBackup_MixedRange_ExportsAuditItem locks the positive
// side of the guard: a range that DOES contain audit items must still
// export the auditItem segment. Prevents an overly-eager future fix from
// dropping the segment unconditionally.
func TestRunIncrementalBackup_MixedRange_ExportsAuditItem(t *testing.T) {
	t.Parallel()

	const bucketID = "bucket"

	store := newBackupTestStore(t)
	// Two failures followed by one success — auditItem count > 0 because of
	// the one success at seq 3.
	writeFailureAuditEntry(t, store, 1)
	writeFailureAuditEntry(t, store, 2)
	writeSuccessAuditEntryWithItem(t, store, 3)

	storage := newInMemoryBackupStorage()
	require.NoError(t, WriteManifest(context.Background(), storage, ManifestKey(bucketID), &Manifest{
		Checkpoint: &CheckpointManifest{LastLogSequence: 0, LastAuditSequence: 0},
	}))

	_, err := RunIncrementalBackup(context.Background(), logging.Testing(), store, storage, bucketID, 0)
	require.NoError(t, err)

	manifest, err := ReadManifest(context.Background(), storage, ManifestKey(bucketID))
	require.NoError(t, err)

	require.NotNil(t, findExport(manifest, "audit"), "audit segment must be exported")
	require.NotNil(t, findExport(manifest, "auditItem"),
		"auditItem segment must be exported when the range contains at least one item")

	restoreStore := newBackupTestStore(t)
	require.NoError(t, ApplyExports(context.Background(), logging.Testing(), storage, restoreStore, manifest.Exports))
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

	storage := newInMemoryBackupStorage()

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

	storage := newInMemoryBackupStorage()

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
	storage := newInMemoryBackupStorage()

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
