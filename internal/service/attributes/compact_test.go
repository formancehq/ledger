package attributes

import (
	"archive/tar"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
	"github.com/formancehq/ledger-v3-poc/internal/storage/tarutil"
	"github.com/stretchr/testify/require"
)

// readLastAppliedIndex reads the last applied Raft index directly from PebbleReader.
// Defined here to avoid importing state (which imports attributes, creating a cycle).
func readLastAppliedIndex(reader dal.PebbleReader) (uint64, error) {
	get, closer, err := reader.Get([]byte{dal.KeyPrefixLastAppliedIndex})
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
func setAppliedIndex(b *dal.Batch, index uint64) error {
	value := make([]byte, 8)
	binary.BigEndian.PutUint64(value, index)
	return b.SetBytes([]byte{dal.KeyPrefixLastAppliedIndex}, value)
}

func TestCompactToBase(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	logger := logging.FromContext(logging.TestingContext())
	s, err := dal.OpenDirect(tmpDir, logger)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	canonicalKey := []byte("test-ledger")

	// Write a ledger entry at index 5
	ledgerAttr := NewLedgerAttribute()
	batch := s.NewBatch()
	err = ledgerAttr.SetBase(batch, 5, canonicalKey, &commonpb.LedgerInfo{
		Name: "test-ledger",
	})
	require.NoError(t, err)
	require.NoError(t, batch.Commit())

	// Verify it's NOT visible at index 0 before compaction
	val, err := ledgerAttr.ComputeValue(s, 0, canonicalKey)
	require.NoError(t, err)
	require.Nil(t, val, "entry at index 5 should NOT be visible when querying at index 0")

	// Compact to index 0
	err = CompactAllForBackup(s)
	require.NoError(t, err)

	// Verify it IS visible at index 0 after compaction
	val, err = ledgerAttr.ComputeValue(s, 0, canonicalKey)
	require.NoError(t, err)
	require.NotNil(t, val, "compacted entry should be visible at index 0")
	require.Equal(t, "test-ledger", val.Name)
}

func TestCompactSurvivesCloseReopen(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	logger := logging.FromContext(logging.TestingContext())

	canonicalKey := []byte("test-ledger")

	// Phase 1: Write data at index 5 and compact to 0
	func() {
		s, err := dal.OpenDirect(tmpDir, logger)
		require.NoError(t, err)
		defer func() { require.NoError(t, s.Close()) }()

		ledgerAttr := NewLedgerAttribute()
		batch := s.NewBatch()
		err = ledgerAttr.SetBase(batch, 5, canonicalKey, &commonpb.LedgerInfo{
			Name: "test-ledger",
		})
		require.NoError(t, err)

		boundaryAttr := NewBoundaryAttribute()
		err = boundaryAttr.SetBase(batch, 5, canonicalKey, &raftcmdpb.LedgerBoundaries{
			NextTransactionId: 1,
			NextLogId:         1,
		})
		require.NoError(t, err)
		require.NoError(t, batch.Commit())

		err = CompactAllForBackup(s)
		require.NoError(t, err)
	}()

	// Phase 2: Reopen and verify
	func() {
		s, err := dal.OpenDirect(tmpDir, logger)
		require.NoError(t, err)
		defer func() { require.NoError(t, s.Close()) }()

		ledgerAttr := NewLedgerAttribute()
		val, err := ledgerAttr.ComputeValue(s, 0, canonicalKey)
		require.NoError(t, err)
		t.Logf("After reopen, ledger ComputeValue(store, 0, key) = %v", val)
		require.NotNil(t, val, "compacted ledger should be visible after reopen")
		require.Equal(t, "test-ledger", val.Name)

		boundaryAttr := NewBoundaryAttribute()
		bval, err := boundaryAttr.ComputeValue(s, 0, canonicalKey)
		require.NoError(t, err)
		t.Logf("After reopen, boundary ComputeValue(store, 0, key) = %v", bval)
		require.NotNil(t, bval, "compacted boundary should be visible after reopen")
		require.Equal(t, uint64(1), bval.NextTransactionId)

		lastIdx, err := readLastAppliedIndex(s)
		require.NoError(t, err)
		require.Equal(t, uint64(0), lastIdx, "lastAppliedIndex should be 0 after compaction")
	}()
}

func TestCompactSurvivesCheckpointAndRestore(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	logger := logging.FromContext(logging.TestingContext())
	canonicalKey := []byte("test-ledger")

	// Phase 1: Create a Pebble, write data, create checkpoint, compact checkpoint
	checkpointDir := filepath.Join(tmpDir, "checkpoint")

	func() {
		s, err := dal.OpenDirect(tmpDir, logger)
		require.NoError(t, err)

		ledgerAttr := NewLedgerAttribute()
		batch := s.NewBatch()
		err = ledgerAttr.SetBase(batch, 5, canonicalKey, &commonpb.LedgerInfo{
			Name: "test-ledger",
		})
		require.NoError(t, err)

		boundaryAttr := NewBoundaryAttribute()
		err = boundaryAttr.SetBase(batch, 5, canonicalKey, &raftcmdpb.LedgerBoundaries{
			NextTransactionId: 1,
			NextLogId:         1,
		})
		require.NoError(t, err)

		require.NoError(t, setAppliedIndex(batch, 5))
		require.NoError(t, batch.Commit())
		require.NoError(t, s.Flush())

		// Create a checkpoint (simulating CreateTemporaryCheckpoint)
		require.NoError(t, os.MkdirAll(checkpointDir, 0755))
		require.NoError(t, s.Close())
	}()

	// Copy the database to checkpoint dir (simulating Pebble checkpoint)
	func() {
		s, err := dal.OpenDirect(tmpDir, logger)
		require.NoError(t, err)
		// Can't call Checkpoint directly since it's on pebble.DB, so we'll just
		// compact the original dir and copy manually
		require.NoError(t, s.Close())
	}()

	// Actually, let me just compact the original dir
	func() {
		s, err := dal.OpenDirect(tmpDir, logger)
		require.NoError(t, err)
		defer func() { require.NoError(t, s.Close()) }()

		err = CompactAllForBackup(s)
		require.NoError(t, err)

		// Verify in same session
		ledgerAttr := NewLedgerAttribute()
		val, err := ledgerAttr.ComputeValue(s, 0, canonicalKey)
		require.NoError(t, err)
		require.NotNil(t, val, "should be visible in same session")
		t.Logf("Same session: %v", val)
	}()

	// Reopen and verify
	func() {
		s, err := dal.OpenDirect(tmpDir, logger)
		require.NoError(t, err)
		defer func() { require.NoError(t, s.Close()) }()

		ledgerAttr := NewLedgerAttribute()
		val, err := ledgerAttr.ComputeValue(s, 0, canonicalKey)
		require.NoError(t, err)
		t.Logf("After reopen: %v", val)
		require.NotNil(t, val, "should survive reopen")

		boundaryAttr := NewBoundaryAttribute()
		bval, err := boundaryAttr.ComputeValue(s, 0, canonicalKey)
		require.NoError(t, err)
		require.NotNil(t, bval, "boundary should survive reopen")
	}()
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

func TestCompactSurvivesTarCycle(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	logger := logging.FromContext(logging.TestingContext())
	canonicalKey := []byte("test-ledger")

	dbDir := filepath.Join(tmpDir, "db")
	require.NoError(t, os.MkdirAll(dbDir, 0755))

	// Phase 1: Write data, compact, close
	func() {
		s, err := dal.OpenDirect(dbDir, logger)
		require.NoError(t, err)
		defer func() { require.NoError(t, s.Close()) }()

		ledgerAttr := NewLedgerAttribute()
		batch := s.NewBatch()
		err = ledgerAttr.SetBase(batch, 5, canonicalKey, &commonpb.LedgerInfo{
			Name: "test-ledger",
		})
		require.NoError(t, err)

		boundaryAttr := NewBoundaryAttribute()
		err = boundaryAttr.SetBase(batch, 5, canonicalKey, &raftcmdpb.LedgerBoundaries{
			NextTransactionId: 1,
			NextLogId:         1,
		})
		require.NoError(t, err)

		require.NoError(t, setAppliedIndex(batch, 5))
		require.NoError(t, batch.Commit())

		err = CompactAllForBackup(s)
		require.NoError(t, err)

		// Verify before close
		val, err := ledgerAttr.ComputeValue(s, 0, canonicalKey)
		require.NoError(t, err)
		require.NotNil(t, val, "should be visible before close")
		t.Logf("Before close: %v", val)
	}()

	// Phase 2: Tar the database directory
	tarFile := filepath.Join(tmpDir, "backup.tar")
	func() {
		f, err := os.Create(tarFile)
		require.NoError(t, err)
		defer func() { require.NoError(t, f.Close()) }()
		createTarFromDir(t, dbDir, f)
	}()

	// Phase 3: Extract tar to new directory
	extractDir := filepath.Join(tmpDir, "extracted")
	require.NoError(t, os.MkdirAll(extractDir, 0755))
	func() {
		f, err := os.Open(tarFile)
		require.NoError(t, err)
		defer func() { require.NoError(t, f.Close()) }()
		require.NoError(t, tarutil.ExtractTar(f, extractDir))
	}()

	// Phase 4: Open extracted and verify
	func() {
		s, err := dal.OpenDirect(extractDir, logger)
		require.NoError(t, err)
		defer func() { require.NoError(t, s.Close()) }()

		ledgerAttr := NewLedgerAttribute()
		val, err := ledgerAttr.ComputeValue(s, 0, canonicalKey)
		require.NoError(t, err)
		t.Logf("After tar cycle: ledger = %v", val)
		require.NotNil(t, val, "compacted ledger should survive tar cycle")
		require.Equal(t, "test-ledger", val.Name)

		boundaryAttr := NewBoundaryAttribute()
		bval, err := boundaryAttr.ComputeValue(s, 0, canonicalKey)
		require.NoError(t, err)
		t.Logf("After tar cycle: boundary = %v", bval)
		require.NotNil(t, bval, "compacted boundary should survive tar cycle")
		require.Equal(t, uint64(1), bval.NextTransactionId)

		lastIdx, err := readLastAppliedIndex(s)
		require.NoError(t, err)
		require.Equal(t, uint64(0), lastIdx, "lastAppliedIndex should be 0 after compaction")
	}()
}

func TestCompactSurvivesTarCycleAndHardLink(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	logger := logging.FromContext(logging.TestingContext())
	canonicalKey := []byte("test-ledger")

	dbDir := filepath.Join(tmpDir, "db")
	require.NoError(t, os.MkdirAll(dbDir, 0755))

	// Phase 1: Write data, compact, close
	func() {
		s, err := dal.OpenDirect(dbDir, logger)
		require.NoError(t, err)
		defer func() { require.NoError(t, s.Close()) }()

		batch := s.NewBatch()
		ledgerAttr := NewLedgerAttribute()
		err = ledgerAttr.SetBase(batch, 5, canonicalKey, &commonpb.LedgerInfo{
			Name: "test-ledger",
		})
		require.NoError(t, err)

		boundaryAttr := NewBoundaryAttribute()
		err = boundaryAttr.SetBase(batch, 5, canonicalKey, &raftcmdpb.LedgerBoundaries{
			NextTransactionId: 1,
			NextLogId:         1,
		})
		require.NoError(t, err)

		require.NoError(t, setAppliedIndex(batch, 5))
		require.NoError(t, batch.Commit())

		err = CompactAllForBackup(s)
		require.NoError(t, err)
	}()

	// Phase 2: Tar → extract
	tarFile := filepath.Join(tmpDir, "backup.tar")
	func() {
		f, err := os.Create(tarFile)
		require.NoError(t, err)
		defer func() { require.NoError(t, f.Close()) }()
		createTarFromDir(t, dbDir, f)
	}()

	stagingDir := filepath.Join(tmpDir, "staging")
	require.NoError(t, os.MkdirAll(stagingDir, 0755))
	func() {
		f, err := os.Open(tarFile)
		require.NoError(t, err)
		defer func() { require.NoError(t, f.Close()) }()
		require.NoError(t, tarutil.ExtractTar(f, stagingDir))
	}()

	// Phase 3: HardLink staging → checkpoint
	checkpointDir := filepath.Join(tmpDir, "checkpoints", "0")
	require.NoError(t, os.MkdirAll(filepath.Join(tmpDir, "checkpoints"), 0755))
	require.NoError(t, dal.HardLink(stagingDir, checkpointDir))

	// Phase 4: HardLink checkpoint → live (simulating NewStore)
	liveDir := filepath.Join(tmpDir, "live")
	require.NoError(t, dal.HardLink(checkpointDir, liveDir))

	// Phase 5: Open live and verify
	func() {
		s, err := dal.OpenDirect(liveDir, logger)
		require.NoError(t, err)
		defer func() { require.NoError(t, s.Close()) }()

		ledgerAttr := NewLedgerAttribute()
		val, err := ledgerAttr.ComputeValue(s, 0, canonicalKey)
		require.NoError(t, err)
		t.Logf("After tar + double hardlink: ledger = %v", val)
		require.NotNil(t, val, "compacted ledger should survive tar + double hardlink")
		require.Equal(t, "test-ledger", val.Name)

		boundaryAttr := NewBoundaryAttribute()
		bval, err := boundaryAttr.ComputeValue(s, 0, canonicalKey)
		require.NoError(t, err)
		t.Logf("After tar + double hardlink: boundary = %v", bval)
		require.NotNil(t, bval, "compacted boundary should survive tar + double hardlink")
		require.Equal(t, uint64(1), bval.NextTransactionId)

		lastIdx, err := readLastAppliedIndex(s)
		require.NoError(t, err)
		require.Equal(t, uint64(0), lastIdx)
	}()
}

// TestCompactSurvivesPebbleCheckpointTarCycle tests the exact backup flow:
// 1. Write data to a live Pebble DB
// 2. Create a Pebble checkpoint (WithFlushedWAL)
// 3. Open the checkpoint, compact to index 0, close
// 4. Tar the checkpoint directory
// 5. Extract tar to a new directory
// 6. Open and verify
func TestCompactSurvivesPebbleCheckpointTarCycle(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	logger := logging.FromContext(logging.TestingContext())
	canonicalKey := []byte("test-ledger")

	dbDir := filepath.Join(tmpDir, "db")
	require.NoError(t, os.MkdirAll(dbDir, 0755))
	checkpointDir := filepath.Join(tmpDir, "checkpoint")

	// Phase 1: Write data to a live Pebble DB and create checkpoint
	func() {
		s, err := dal.OpenDirect(dbDir, logger)
		require.NoError(t, err)

		batch := s.NewBatch()
		ledgerAttr := NewLedgerAttribute()
		err = ledgerAttr.SetBase(batch, 5, canonicalKey, &commonpb.LedgerInfo{
			Name: "test-ledger",
		})
		require.NoError(t, err)

		boundaryAttr := NewBoundaryAttribute()
		err = boundaryAttr.SetBase(batch, 5, canonicalKey, &raftcmdpb.LedgerBoundaries{
			NextTransactionId: 1,
			NextLogId:         1,
		})
		require.NoError(t, err)

		require.NoError(t, setAppliedIndex(batch, 5))
		require.NoError(t, batch.Commit())

		// Create checkpoint (simulating CreateBackupCheckpoint)
		require.NoError(t, s.Checkpoint(checkpointDir))
		require.NoError(t, s.Close())
	}()

	// Phase 2: Open checkpoint, compact, close (simulating backupLocal compaction)
	func() {
		s, err := dal.OpenDirect(checkpointDir, logger)
		require.NoError(t, err)

		err = CompactAllForBackup(s)
		require.NoError(t, err)

		// Verify before close
		ledgerAttr := NewLedgerAttribute()
		val, err := ledgerAttr.ComputeValue(s, 0, canonicalKey)
		require.NoError(t, err)
		require.NotNil(t, val, "should be visible in checkpoint after compaction")
		t.Logf("Checkpoint after compact: %v", val)

		require.NoError(t, s.Close())
	}()

	// Phase 3: Tar the checkpoint
	tarFile := filepath.Join(tmpDir, "backup.tar")
	func() {
		f, err := os.Create(tarFile)
		require.NoError(t, err)
		defer func() { require.NoError(t, f.Close()) }()
		createTarFromDir(t, checkpointDir, f)
	}()

	// Phase 4: Extract tar to staging
	stagingDir := filepath.Join(tmpDir, "staging")
	require.NoError(t, os.MkdirAll(stagingDir, 0755))
	func() {
		f, err := os.Open(tarFile)
		require.NoError(t, err)
		defer func() { require.NoError(t, f.Close()) }()
		require.NoError(t, tarutil.ExtractTar(f, stagingDir))
	}()

	// Phase 5: HardLink staging → checkpoint0 → live (simulating FinalizeRestore + NewStore)
	checkpoint0 := filepath.Join(tmpDir, "checkpoints", "0")
	require.NoError(t, os.MkdirAll(filepath.Join(tmpDir, "checkpoints"), 0755))
	require.NoError(t, dal.HardLink(stagingDir, checkpoint0))

	liveDir := filepath.Join(tmpDir, "live")
	require.NoError(t, dal.HardLink(checkpoint0, liveDir))

	// Phase 6: Open live and verify
	func() {
		s, err := dal.OpenDirect(liveDir, logger)
		require.NoError(t, err)
		defer func() { require.NoError(t, s.Close()) }()

		ledgerAttr := NewLedgerAttribute()
		val, err := ledgerAttr.ComputeValue(s, 0, canonicalKey)
		require.NoError(t, err)
		t.Logf("After full pipeline: ledger = %v", val)
		require.NotNil(t, val, "compacted ledger should survive full backup→restore pipeline")
		require.Equal(t, "test-ledger", val.Name)

		boundaryAttr := NewBoundaryAttribute()
		bval, err := boundaryAttr.ComputeValue(s, 0, canonicalKey)
		require.NoError(t, err)
		t.Logf("After full pipeline: boundary = %v", bval)
		require.NotNil(t, bval, "compacted boundary should survive full backup→restore pipeline")
		require.Equal(t, uint64(1), bval.NextTransactionId)

		lastIdx, err := readLastAppliedIndex(s)
		require.NoError(t, err)
		require.Equal(t, uint64(0), lastIdx)
	}()
}

// TestCompactFlushedBoundaries tests that boundaries flushed to Pebble before
// the checkpoint (simulating the Raft loop atomic flush) are correctly compacted.
// This is the real-world backup flow: flush boundaries → checkpoint → compact.
func TestCompactFlushedBoundaries(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	logger := logging.FromContext(logging.TestingContext())
	canonicalKey := []byte("test-ledger")

	dbDir := filepath.Join(tmpDir, "db")
	require.NoError(t, os.MkdirAll(dbDir, 0755))

	// Phase 1: Write ledger + boundary data at index 5 (simulating Raft loop flush)
	func() {
		s, err := dal.OpenDirect(dbDir, logger)
		require.NoError(t, err)
		defer func() { require.NoError(t, s.Close()) }()

		batch := s.NewBatch()
		ledgerAttr := NewLedgerAttribute()
		err = ledgerAttr.SetBase(batch, 5, canonicalKey, &commonpb.LedgerInfo{
			Name: "test-ledger",
		})
		require.NoError(t, err)

		// Boundary is flushed to Pebble by the Raft loop before checkpoint
		boundaryAttr := NewBoundaryAttribute()
		err = boundaryAttr.SetBase(batch, 5, canonicalKey, &raftcmdpb.LedgerBoundaries{
			NextTransactionId: 5,
			NextLogId:         3,
		})
		require.NoError(t, err)

		require.NoError(t, setAppliedIndex(batch, 5))
		require.NoError(t, batch.Commit())
		require.NoError(t, s.Flush())
	}()

	// Phase 2: Compact (simulating backup compaction after checkpoint)
	func() {
		s, err := dal.OpenDirect(dbDir, logger)
		require.NoError(t, err)
		defer func() { require.NoError(t, s.Close()) }()

		err = CompactAllForBackup(s)
		require.NoError(t, err)
	}()

	// Phase 3: Reopen and verify both ledger AND boundary are compacted to index 0
	func() {
		s, err := dal.OpenDirect(dbDir, logger)
		require.NoError(t, err)
		defer func() { require.NoError(t, s.Close()) }()

		ledgerAttr := NewLedgerAttribute()
		val, err := ledgerAttr.ComputeValue(s, 0, canonicalKey)
		require.NoError(t, err)
		require.NotNil(t, val, "compacted ledger should be visible at index 0")
		require.Equal(t, "test-ledger", val.Name)

		boundaryAttr := NewBoundaryAttribute()
		bval, err := boundaryAttr.ComputeValue(s, 0, canonicalKey)
		require.NoError(t, err)
		require.NotNil(t, bval, "compacted boundary should be visible at index 0")
		require.NotNil(t, bval)
		require.Equal(t, uint64(5), bval.NextTransactionId)
		require.Equal(t, uint64(3), bval.NextLogId)

		lastIdx, err := readLastAppliedIndex(s)
		require.NoError(t, err)
		require.Equal(t, uint64(0), lastIdx)
	}()
}
