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

	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func saveNextLedgerIDForBackupTest(t *testing.T, batch *dal.WriteSession, nextID uint32) {
	t.Helper()

	require.NoError(t, state.StoreNextLedgerID(batch, nextID))
}

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
			// Read each uploaded checkpoint object so the test can compare bytes.
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

func TestRunBackup_CheckpointUploadFailurePreservesPreviousManifest(t *testing.T) {
	t.Parallel()
	const bucketID = "upload-failure"
	store := newBackupTestStore(t)
	seedBackupTestAuditKey(t, store)
	seed := store.OpenWriteSession()
	require.NoError(t, seed.SetProto(coldLogKey(1), createLedgerLog(1, "first", 1)))
	require.NoError(t, seed.Commit())
	storage, state := newCrashSafetyStorage(t)
	_, err := RunBackup(t.Context(), logging.Testing(), store, storage, bucketID, "first")
	require.NoError(t, err)
	previous, err := ReadManifest(t.Context(), storage, ManifestKey(bucketID))
	require.NoError(t, err)

	change := store.OpenWriteSession()
	require.NoError(t, change.SetProto(coldLogKey(2), createLedgerLog(2, "second", 2)))
	require.NoError(t, change.Commit())
	state.mu.Lock()
	state.putErrKeys = []string{CheckpointPrefix(bucketID)}
	state.mu.Unlock()
	_, err = RunBackup(t.Context(), logging.Testing(), store, storage, bucketID, "failed")
	require.ErrorContains(t, err, "injected put failure")
	retained, err := ReadManifest(t.Context(), storage, ManifestKey(bucketID))
	require.NoError(t, err)
	require.Equal(t, previous, retained)
	oldStore := restoreAuditKeyCheckpoint(t, storage, retained)
	oldLog, err := dal.GetValue(oldStore, coldLogKey(1))
	require.NoError(t, err)
	require.NotEmpty(t, oldLog)

	state.mu.Lock()
	state.putErrKeys = nil
	state.mu.Unlock()
	result, err := RunBackup(t.Context(), logging.Testing(), store, storage, bucketID, "retry")
	require.NoError(t, err)
	require.Equal(t, uint64(2), result.LastLogSequence)
	latest, err := ReadManifest(t.Context(), storage, ManifestKey(bucketID))
	require.NoError(t, err)
	newStore := restoreAuditKeyCheckpoint(t, storage, latest)
	newLog, err := dal.GetValue(newStore, coldLogKey(2))
	require.NoError(t, err)
	require.NotEmpty(t, newLog)
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
