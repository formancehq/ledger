package attributes

import (
	"archive/tar"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/pkg/tarutil"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// readLastAppliedIndex reads the last applied Raft index directly from PebbleReader.
// Defined here to avoid importing state (which imports attributes, creating a cycle).
func readLastAppliedIndex(reader dal.PebbleGetter) (uint64, error) {
	get, closer, err := reader.Get([]byte{dal.ZoneGlobal, dal.SubGlobLastAppliedIndex})
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return 0, nil
		}

		return 0, err
	}

	defer func() { _ = closer.Close() }()

	if len(get) == 0 {
		return 0, nil
	}

	return binary.BigEndian.Uint64(get[:8]), nil
}

// setAppliedIndex writes the last applied Raft index via Batch.
// Defined here to avoid importing state (which imports attributes, creating a cycle).
func setAppliedIndex(b *dal.WriteSession, index uint64) error {
	value := make([]byte, 8)
	binary.BigEndian.PutUint64(value, index)

	return b.SetBytes([]byte{dal.ZoneGlobal, dal.SubGlobLastAppliedIndex}, value)
}

// createTarFromDir creates a tar archive of dirPath and writes it to the given writer.
func createTarFromDir(t *testing.T, dirPath string, w io.Writer) {
	t.Helper()

	tw := tar.NewWriter(w)

	defer func() { require.NoError(t, tw.Close()) }()

	err := filepath.Walk(dirPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		relPath, err := filepath.Rel(dirPath, path)
		if err != nil {
			return err
		}

		header, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return err
		}

		header.Name = relPath
		if err := tw.WriteHeader(header); err != nil {
			return err
		}

		if !info.IsDir() {
			f, err := os.Open(path)
			if err != nil {
				return err
			}

			defer func() { _ = f.Close() }()

			if _, err := io.Copy(tw, f); err != nil {
				return err
			}
		}

		return nil
	})
	require.NoError(t, err)
}

// snapshotAttributeZone reads every entry in the attribute zone [0xF1, 0xF2)
// into a key->value map for byte-for-byte comparison.
func snapshotAttributeZone(t *testing.T, s *dal.Store) map[string][]byte {
	t.Helper()

	handle, err := s.NewDirectReadHandle()
	require.NoError(t, err)

	defer func() { _ = handle.Close() }()

	iter, err := handle.NewIter(&pebble.IterOptions{
		LowerBound: []byte{dal.ZoneAttributes},
		UpperBound: []byte{dal.ZoneAttributes + 1},
	})
	require.NoError(t, err)

	defer func() { _ = iter.Close() }()

	out := make(map[string][]byte)
	for iter.First(); iter.Valid(); iter.Next() {
		v, err := iter.ValueAndErr()
		require.NoError(t, err)

		cp := make([]byte, len(v))
		copy(cp, v)
		out[string(iter.Key())] = cp
	}

	require.NoError(t, iter.Error())

	return out
}

// TestPrepareForBackupPreservesAttributesByteForByte asserts that the attribute
// zone is left untouched — no delete, no rewrite — across PrepareForBackup.
func TestPrepareForBackupPreservesAttributesByteForByte(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	logger := logging.FromContext(logging.TestingContext())
	s, err := dal.OpenDirect(tmpDir, logger)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	// Write one entry per registered attribute prefix so every type is covered.
	batch := s.OpenWriteSession()
	for _, a := range New().All() {
		require.NoError(t, batch.SetBytes([]byte{dal.ZoneAttributes, a.Prefix(), 'k'}, []byte("v")))
	}
	require.NoError(t, batch.Commit())
	require.NoError(t, s.Flush())

	before := snapshotAttributeZone(t, s)
	require.NotEmpty(t, before)

	require.NoError(t, PrepareForBackup(s))

	after := snapshotAttributeZone(t, s)
	require.Equal(t, before, after, "attribute zone must be byte-for-byte identical after PrepareForBackup")
}

// TestPrepareForBackupResetsGlobalZone asserts the three Global-zone resets:
// applied index -> 0, persisted config deleted, persisted bloom blocks dropped.
func TestPrepareForBackupResetsGlobalZone(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	logger := logging.FromContext(logging.TestingContext())
	s, err := dal.OpenDirect(tmpDir, logger)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	batch := s.OpenWriteSession()
	require.NoError(t, setAppliedIndex(batch, 200))
	require.NoError(t, batch.SetBytes([]byte{dal.ZoneGlobal, dal.SubGlobPersistedConfig}, []byte("node+cluster")))
	require.NoError(t, batch.SetBytes([]byte{dal.ZoneGlobal, dal.SubGlobBloom, 0x00}, []byte("stale-block")))
	// EN-1413: a peer entry left over from the source cluster — the
	// restore path must drop these so the booting node does not dial
	// the wrong pods.
	require.NoError(t, batch.SetBytes(
		append([]byte{dal.ZoneGlobal, dal.SubGlobPeers},
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07),
		[]byte("stale-peer-7"),
	))
	require.NoError(t, batch.Commit())

	require.NoError(t, PrepareForBackup(s))

	idx, err := readLastAppliedIndex(s)
	require.NoError(t, err)
	require.Equal(t, uint64(0), idx, "applied index must be reset to 0")

	_, _, err = s.Get([]byte{dal.ZoneGlobal, dal.SubGlobPersistedConfig})
	require.ErrorIs(t, err, pebble.ErrNotFound, "persisted config must be deleted")

	_, _, err = s.Get([]byte{dal.ZoneGlobal, dal.SubGlobBloom, 0x00})
	require.ErrorIs(t, err, pebble.ErrNotFound, "persisted bloom blocks must be dropped")

	_, _, err = s.Get(append([]byte{dal.ZoneGlobal, dal.SubGlobPeers},
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07))
	require.ErrorIs(t, err, pebble.ErrNotFound, "persisted Raft peers must be dropped (EN-1413)")
}

// TestPrepareForBackupRestorableOnFreshCluster runs the full backup->restore
// pipeline (write -> prepare -> tar -> extract -> reopen) and asserts the
// attribute values survive and the applied index is 0 on the fresh cluster.
func TestPrepareForBackupRestorableOnFreshCluster(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	logger := logging.FromContext(logging.TestingContext())
	canonicalKey := []byte("test-ledger")

	dbDir := filepath.Join(tmpDir, "db")
	require.NoError(t, os.MkdirAll(dbDir, 0755))

	// Phase 1: write data at index 5, prepare for backup, close.
	func() {
		s, err := dal.OpenDirect(dbDir, logger)
		require.NoError(t, err)

		defer func() { require.NoError(t, s.Close()) }()

		ledgerAttr := NewAttribute[*commonpb.LedgerInfo](dal.SubAttrLedger)
		batch := s.OpenWriteSession()
		_, err = ledgerAttr.Set(batch, canonicalKey, &commonpb.LedgerInfo{Name: "test-ledger"})
		require.NoError(t, err)

		boundaryAttr := NewAttribute[*raftcmdpb.LedgerBoundaries](dal.SubAttrBoundary)
		_, err = boundaryAttr.Set(batch, canonicalKey, &raftcmdpb.LedgerBoundaries{
			NextTransactionId: 1,
			NextLogId:         1,
		})
		require.NoError(t, err)

		require.NoError(t, setAppliedIndex(batch, 5))
		require.NoError(t, batch.Commit())

		require.NoError(t, PrepareForBackup(s))
	}()

	// Phase 2: tar the db dir.
	tarFile := filepath.Join(tmpDir, "backup.tar")
	func() {
		f, err := os.Create(tarFile)
		require.NoError(t, err)

		defer func() { require.NoError(t, f.Close()) }()

		createTarFromDir(t, dbDir, f)
	}()

	// Phase 3: extract to a fresh directory.
	extractDir := filepath.Join(tmpDir, "extracted")
	require.NoError(t, os.MkdirAll(extractDir, 0755))
	func() {
		f, err := os.Open(tarFile)
		require.NoError(t, err)

		defer func() { require.NoError(t, f.Close()) }()

		require.NoError(t, tarutil.ExtractTar(f, extractDir))
	}()

	// Phase 4: open the fresh cluster's data and verify.
	func() {
		s, err := dal.OpenDirect(extractDir, logger)
		require.NoError(t, err)

		defer func() { require.NoError(t, s.Close()) }()

		ledgerAttr := NewAttribute[*commonpb.LedgerInfo](dal.SubAttrLedger)
		val, err := ledgerAttr.Get(s, canonicalKey)
		require.NoError(t, err)
		require.NotNil(t, val, "ledger must survive the backup->restore pipeline")
		require.Equal(t, "test-ledger", val.GetName())

		boundaryAttr := NewAttribute[*raftcmdpb.LedgerBoundaries](dal.SubAttrBoundary)
		bval, err := boundaryAttr.Get(s, canonicalKey)
		require.NoError(t, err)
		require.NotNil(t, bval, "boundary must survive the backup->restore pipeline")
		require.Equal(t, uint64(1), bval.GetNextTransactionId())

		idx, err := readLastAppliedIndex(s)
		require.NoError(t, err)
		require.Equal(t, uint64(0), idx, "applied index must be 0 on the fresh cluster")
	}()
}

func TestAttributesAllCoversEveryRegisteredType(t *testing.T) {
	t.Parallel()

	attrs := New()
	all := attrs.All()

	got := make(map[byte]struct{}, len(all))
	for _, a := range all {
		got[a.Prefix()] = struct{}{}
	}

	want := []byte{
		dal.SubAttrVolume,
		dal.SubAttrMetadata,
		dal.SubAttrTransaction,
		dal.SubAttrLedger,
		dal.SubAttrBoundary,
		dal.SubAttrReference,
		dal.SubAttrLedgerMetadata,
		dal.SubAttrSinkConfig,
		dal.SubAttrNumscriptVersion,
		dal.SubAttrNumscriptContent,
		dal.SubAttrPreparedQuery,
		dal.SubAttrIndex,
	}

	require.Len(t, all, len(want), "All() must return every registered attribute")
	for _, w := range want {
		_, ok := got[w]
		require.Truef(t, ok, "All() missing attribute prefix 0x%02x", w)
	}
}
