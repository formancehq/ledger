package backup

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/query"
)

// crashSafetyState is the stateful backing behind the generated MockStorage used
// by the crash-safety tests. The MockStorage (see storage_generated_test.go)
// provides the interface-conformant surface and free call recording; this struct
// holds the extra observations those tests assert on and cannot be expressed
// with plain gomock expectations, because the number, keys and relative order of
// the Put/Delete calls are driven by Pebble's checkpoint internals and are not
// knowable at test-authoring time:
//
//   - ops records the ordered sequence of "put <key>" / "del <key>" so a test can
//     assert crash-safety ordering (every referenced object uploaded before the
//     manifest, no object deleted before the manifest is written);
//   - putErrKeys injects a PutFile failure for keys matching any substring, to
//     exercise the segment-upload failure path;
//   - mutatingOverwrites records every PutFile that replaced an already-present
//     object under data/ with DIFFERENT bytes — the precise immutability
//     violation: a re-put of byte-identical content under the same
//     content-addressed key is harmless and not recorded, but changing the bytes
//     behind an existing key corrupts any manifest that references it. With
//     content-addressing this must stay empty;
//   - failDeletes, once flipped, makes every DeleteFile fail, to prove the
//     manifest commit does not depend on deletes.
//
// Real bytes are stored in an inMemoryBackupStorage so GetFile/ListFiles behave
// like a real backend (round-tripping the manifest, hashing stored content).
type crashSafetyState struct {
	inner *inMemoryBackupStorage

	mu                 sync.Mutex
	ops                []string
	putErrKeys         []string
	mutatingOverwrites []string
	failDeletes        bool
}

// newCrashSafetyStorage wires a generated MockStorage whose behavior is backed
// by a crashSafetyState. It returns both so a test can drive RunBackup through
// the mock and later inspect the recorded observations.
func newCrashSafetyStorage(t *testing.T) (*MockStorage, *crashSafetyState) {
	t.Helper()

	st := &crashSafetyState{inner: newInMemoryBackupStorage()}
	storage := NewMockStorage(gomock.NewController(t))

	storage.EXPECT().PutFile(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, key string, data io.Reader, size int64) error {
			// Always drain the reader: the export path streams through an
			// io.Pipe and the writer goroutine blocks until the body is fully
			// consumed.
			body, readErr := io.ReadAll(data)

			st.mu.Lock()
			for _, frag := range st.putErrKeys {
				if strings.Contains(key, frag) {
					st.mu.Unlock()

					return errors.New("injected put failure for " + key)
				}
			}

			if readErr != nil {
				st.mu.Unlock()

				return readErr
			}

			if prev, err := st.inner.GetFile(ctx, key); err == nil {
				prevBody, _ := io.ReadAll(prev)
				_ = prev.Close()
				if strings.Contains(key, "/data/") && !bytes.Equal(prevBody, body) {
					st.mutatingOverwrites = append(st.mutatingOverwrites, key)
				}
			}

			st.ops = append(st.ops, "put "+key)
			st.mu.Unlock()

			return st.inner.PutFile(ctx, key, bytes.NewReader(body), size)
		}).AnyTimes()

	storage.EXPECT().GetFile(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, key string) (io.ReadCloser, error) {
			return st.inner.GetFile(ctx, key)
		}).AnyTimes()

	storage.EXPECT().DeleteFile(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, key string) error {
			st.mu.Lock()
			if st.failDeletes {
				st.mu.Unlock()

				return errors.New("injected delete failure")
			}
			st.ops = append(st.ops, "del "+key)
			st.mu.Unlock()

			return st.inner.DeleteFile(ctx, key)
		}).AnyTimes()

	storage.EXPECT().ListFiles(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, prefix string) ([]string, error) {
			return st.inner.ListFiles(ctx, prefix)
		}).AnyTimes()

	return storage, st
}

// resetOps clears the recorded op log so a later assertion sees only the ops of
// a subsequent backup run.
func (s *crashSafetyState) resetOps() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.ops = nil
}

// opsCopy returns a snapshot of the recorded ops under the lock.
func (s *crashSafetyState) opsCopy() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]string(nil), s.ops...)
}

// overwritesCopy returns a snapshot of content-mutating in-place data/
// overwrites under the lock.
func (s *crashSafetyState) overwritesCopy() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]string(nil), s.mutatingOverwrites...)
}

// setFailDeletes toggles the injected DeleteFile failure.
func (s *crashSafetyState) setFailDeletes(v bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.failDeletes = v
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

	storage, state := newCrashSafetyStorage(t)

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
	state.resetOps()

	_, err = RunBackup(context.Background(), logging.Testing(), store, storage, bucketID, "bk-2")
	require.NoError(t, err)

	ops := state.opsCopy()
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

// TestRunBackup_NeverOverwritesManifestReferencedObject is the immutability
// regression (MAJOR bug found in review of PR #1543): a Pebble checkpoint
// contains a MANIFEST-NNNNNN file that keeps the SAME local name but GROWS
// between checkpoints. With name-keyed storage keys, the second full backup
// re-uploaded data/MANIFEST-NNNNNN in place — overwriting an object the
// currently published backup manifest still referenced, BEFORE the manifest
// swap. A crash in that window left the previous backup pointing at corrupt
// Pebble metadata.
//
// With content-addressed keys, a file whose bytes change lands on a new key, so
// no object a published manifest references is ever overwritten in place. This
// test drives two full backups across a mutation that grows the MANIFEST and
// asserts zero in-place overwrites under data/. It FAILS on the pre-fix
// (name-keyed) code and passes after.
func TestRunBackup_NeverOverwritesManifestReferencedObject(t *testing.T) {
	t.Parallel()

	const bucketID = "bucket"

	store := newBackupTestStore(t)

	seedBatch := store.OpenWriteSession()
	require.NoError(t, seedBatch.SetProto(coldLogKey(1), createLedgerLog(1, "ledger", 1)))
	require.NoError(t, seedBatch.SetProto(coldAuditKey(1), auditSuccess(1, 1, 1)))
	require.NoError(t, seedBatch.Commit())
	require.NoError(t, store.Flush())

	storage, state := newCrashSafetyStorage(t)

	_, err := RunBackup(context.Background(), logging.Testing(), store, storage, bucketID, "bk-1")
	require.NoError(t, err)

	manifest1, err := ReadManifest(context.Background(), storage, ManifestKey(bucketID))
	require.NoError(t, err)
	keys1 := checkpointKeySet(manifest1)

	// Mutate + compact so the checkpoint's file set changes — most notably
	// Pebble's MANIFEST-NNNNNN, which keeps the same local name but grows in
	// place. This is the exact trigger that made the pre-fix (name-keyed) code
	// overwrite a manifest-referenced object.
	mutate := store.OpenWriteSession()
	for seq := uint64(2); seq <= 60; seq++ {
		require.NoError(t, mutate.SetProto(coldLogKey(seq), createLedgerLog(seq, "ledger", uint32(seq))))
	}
	require.NoError(t, mutate.Commit())
	require.NoError(t, store.CompactAll())
	require.NoError(t, store.Flush())

	_, err = RunBackup(context.Background(), logging.Testing(), store, storage, bucketID, "bk-2")
	require.NoError(t, err)

	manifest2, err := ReadManifest(context.Background(), storage, ManifestKey(bucketID))
	require.NoError(t, err)
	keys2 := checkpointKeySet(manifest2)

	// The precise immutability invariant: no object under data/ was ever
	// overwritten with different bytes.
	require.Empty(t, state.overwritesCopy(),
		"no object referenced by a published manifest may be overwritten with different bytes")

	// Sanity: the two checkpoints are genuinely different (the mutation+compaction
	// changed at least one file), so the test actually exercised a changed file
	// rather than a trivially identical checkpoint.
	require.NotEqual(t, keys1, keys2,
		"the second checkpoint must differ from the first for this test to be meaningful")

	// The stored object under every key the CURRENT manifest references must hash
	// to the content the key encodes. This is what makes objects immutable AND
	// catches the silent-skip failure mode: on the pre-fix name-keyed scheme, a
	// file whose content changed is either overwritten in place or silently
	// skipped (a same-name object already exists), so the stored bytes for a
	// grown MANIFEST-NNNNNN would NOT match the hash its key/name implies.
	//
	// (We check manifest2, the current backup — not manifest1, whose objects are
	// legitimately superseded and pruned once manifest2 is committed. The
	// guarantee that manifest1's objects were untouched *during* backup 2, before
	// its manifest swap, is covered by overwritesCopy() being empty above.)
	for name, cf := range manifest2.Checkpoint.Files {
		rc, err := storage.GetFile(context.Background(), cf.Key)
		require.NoError(t, err, "object for %s (%s) must exist", name, cf.Key)
		body, err := io.ReadAll(rc)
		require.NoError(t, err)
		_ = rc.Close()

		sum := sha256.Sum256(body)
		wantSuffix := "." + hex.EncodeToString(sum[:])
		require.True(t, strings.HasSuffix(cf.Key, wantSuffix),
			"stored bytes for %s must match the content hash its key encodes (key=%s)", name, cf.Key)
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

	storage, state := newCrashSafetyStorage(t)

	// First full backup.
	_, err := RunBackup(context.Background(), logging.Testing(), store, storage, bucketID, "bk-1")
	require.NoError(t, err)
	_, err = storage.GetFile(context.Background(), ManifestKey(bucketID))
	require.NoError(t, err, "first manifest must be written")

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
	state.setFailDeletes(true)

	_, err = RunBackup(context.Background(), logging.Testing(), store, storage, bucketID, "bk-2")
	require.NoError(t, err, "a full backup must succeed even when every stale-file delete fails")

	manifest, err := ReadManifest(context.Background(), storage, ManifestKey(bucketID))
	require.NoError(t, err)
	require.NotNil(t, manifest.Checkpoint)
	require.Empty(t, manifest.Exports)
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

	storage, state := newCrashSafetyStorage(t)
	// Fail every log-segment upload. The export writes the log segment first,
	// so RunIncrementalBackup must abort before touching the manifest.
	state.mu.Lock()
	state.putErrKeys = []string{"/exports/logs-"}
	state.mu.Unlock()

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
