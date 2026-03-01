package dal

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/formancehq/go-libs/v3/logging"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/protobuf/proto"
)

const (
	liveDir                 = "live"
	currentCheckpointFile   = "CURRENT_CHECKPOINT"
	checkpointsDir          = "checkpoints"
	temporaryCheckpointsDir = "tmp"
)

// Store is a Pebble implementation of dal.Store
// It stores balances and account metadata
type Store struct {
	db                atomic.Pointer[pebble.DB]
	opts              *pebble.Options
	logger            logging.Logger
	dataDir           string
	currentCheckPoint uint64
	oldestCheckpoint  uint64
	maxCheckpoints    int
}

// getDB returns the current pebble.DB via an atomic load.
// This is lock-free and never blocks, even during RestoreCheckpoint.
func (s *Store) getDB() *pebble.DB {
	return s.db.Load()
}

// Key prefixes for Pebble storage, organized into three zones:
//
//	Cold-storable zone [0x01, 0xF1) — archived to cold storage then purged per period.
//	Attributes zone    [0xF1, 0xF2) — derived data hashed during seal, stays in hot storage.
//	System zone        [0xF2, 0xFF] — metadata that lives forever.
var (
	// Cold-storable zone [0x01, 0xF1)
	//
	// Sequence-keyed prefixes: [prefix][sequence] -- support efficient range scan/delete.
	KeyPrefixLog   byte = 0x01 // [KeyPrefixLog][sequence] -> Log
	KeyPrefixAudit byte = 0x02 // [KeyPrefixAudit][sequence] -> AuditEntry

	// ColdSequencePrefixes lists cold-storable prefixes keyed by sequence number.
	// These support efficient range scan and range delete by [prefix][startSeq]..[prefix][endSeq].
	ColdSequencePrefixes = []byte{KeyPrefixLog, KeyPrefixAudit}

	// Composite-keyed cold prefix: [prefix][name]\x00[txID][byLog] -- requires filtered iteration.
	KeyPrefixTransactionUpdate byte = 0x03 // [KeyPrefixTransactionUpdate][name]\x00[txID(8)][byLog(8)] -> TransactionUpdate

	// Attributes zone [0xF1, 0xF2) -- seal hash domain
	KeyPrefixAttributes byte = 0xF1

	// System zone [0xF2, 0xFF]
	KeyPrefixLastAppliedIndex     byte = 0xF2 // [KeyPrefixLastAppliedIndex] -> uint64
	KeyPrefixLastAppliedTimestamp byte = 0xF3 // [KeyPrefixLastAppliedTimestamp] -> uint64 (HLC microseconds)
	KeyPrefixIdempotency          byte = 0xF4 // [KeyPrefixIdempotency][key] -> sequence
	KeyPrefixLedgerInfo           byte = 0xF5 // [KeyPrefixLedgerInfo][name] -> LedgerInfo
	KeyPrefixSigningKey           byte = 0xF6 // [KeyPrefixSigningKey][keyID] -> ed25519 public key (32 bytes)
	KeyPrefixPeriods              byte = 0xF7 // [KeyPrefixPeriods][periodID] -> Period
	KeyPrefixNextPeriodID         byte = 0xF8 // [KeyPrefixNextPeriodID] -> uint64 (next period ID)
	KeyPrefixSigningConfig        byte = 0xF9 // [KeyPrefixSigningConfig] -> signing config byte (0x00=false, 0x01=true)
	KeyPrefixSinkCursor           byte = 0xFA // [KeyPrefixSinkCursor][name] -> uint64 (per-sink last emitted log sequence)
	KeyPrefixEventsConfig         byte = 0xFB // [KeyPrefixEventsConfig][name] -> SinkConfig protobuf (per-sink)
	KeyPrefixSinkStatus           byte = 0xFC // [KeyPrefixSinkStatus][name] -> SinkStatus protobuf
	KeyPrefixMaintenanceMode      byte = 0xFD // [KeyPrefixMaintenanceMode] -> maintenance mode byte (0x00=false, 0x01=true)
	KeyPrefixPersistedConfig      byte = 0xFE // [KeyPrefixPersistedConfig] -> PersistedConfig JSON (startup safety checks)
	KeyPrefixPreparedQuery         byte = 0xE0 // [KeyPrefixPreparedQuery][name\x00][queryName] -> PreparedQuery protobuf
	KeyPrefixMirrorSourceHead     byte = 0xEB // [KeyPrefixMirrorSourceHead][ledger_name] -> uint64 (latest known v2 source log ID)
	KeyPrefixMirrorCursor         byte = 0xEC // [KeyPrefixMirrorCursor][ledger_name] -> uint64 (last ingested v2 log ID)
	KeyPrefixMirrorStatus         byte = 0xED // [KeyPrefixMirrorStatus][ledger_name] -> MirrorSyncError protobuf
	KeyPrefixAuditConfig          byte = 0xEE // [KeyPrefixAuditConfig] -> audit config byte (0x00=false, 0x01=true)
	KeyPrefixPeriodSchedule       byte = 0xEF // [KeyPrefixPeriodSchedule] -> cron expression string

	AttributePrefixVolume         = byte('V')
	AttributePrefixMetadata       = byte('M')
	AttributePrefixIdempotencyKey = byte('K')
	AttributePrefixReference      = byte('F')
	AttributePrefixLedger         = byte('G')
	AttributePrefixBoundary       = byte('B')
)

// NewStore creates a new Store instance
func NewStore(
	dataDir string,
	logger logging.Logger,
	meter metric.Meter,
	cfg Config,
) (*Store, error) {

	opts := &pebble.Options{
		EventListener: NewMetricsListener(meter),
		// 1) Absorb more writes before flush => fewer SST files, fewer compactions.
		MemTableSize:                cfg.MemTableSize,
		MemTableStopWritesThreshold: cfg.MemTableStopWritesThreshold,

		// 2) Control L0 pressure (main source of compactions/churn in write-heavy workloads).
		L0CompactionThreshold: cfg.L0CompactionThreshold,
		L0StopWritesThreshold: cfg.L0StopWritesThreshold,
		LBaseMaxBytes:         cfg.LBaseMaxBytes,
		Cache:                 pebble.NewCache(cfg.CacheSize),

		// 3) Table sizes: fewer small files => fewer compactions.
		Levels: []pebble.LevelOptions{
			{TargetFileSize: cfg.TargetFileSize},
		},

		// 4) Smooth IO during flush/compactions.
		BytesPerSync:    cfg.BytesPerSync,
		WALBytesPerSync: cfg.WALBytesPerSync,

		// 5) Compaction concurrency: OK but not too high (otherwise you saturate IO).
		MaxConcurrentCompactions: func() int { return cfg.MaxConcurrentCompactions },

		// 6) WAL configuration
		WALMinSyncInterval: func() time.Duration { return cfg.WALMinSyncInterval },
		DisableWAL:         cfg.DisableWAL,
	}

	var (
		db *pebble.DB
	)

	currentCheckpointRaw, err := os.ReadFile(filepath.Join(dataDir, currentCheckpointFile))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("reading current checkpoint: %w", err)
	}
	liveDir := filepath.Join(dataDir, liveDir)
	if errors.Is(err, os.ErrNotExist) {
		logger.Infof("No checkpoint found, creating new database in %s", liveDir)
		if err := os.RemoveAll(liveDir); err != nil {
			return nil, fmt.Errorf("removing old database: %w", err)
		}

		db, err = pebble.Open(liveDir, opts)
		if err != nil {
			return nil, fmt.Errorf("opening pebble database: %w", err)
		}

		if err := db.Checkpoint(filepath.Join(dataDir, checkpointsDir, "0")); err != nil {
			return nil, fmt.Errorf("creating initial checkpoint: %w", err)
		}

		f, err := os.Create(filepath.Join(dataDir, currentCheckpointFile))
		if err != nil {
			return nil, fmt.Errorf("creating current checkpoint file: %w", err)
		}

		if _, err := f.WriteString("0"); err != nil {
			return nil, fmt.Errorf("writing current checkpoint: %w", err)
		}

		if err := f.Sync(); err != nil {
			return nil, fmt.Errorf("syncing current checkpoint file: %w", err)
		}

		if err := f.Close(); err != nil {
			return nil, fmt.Errorf("closing current checkpoint file: %w", err)
		}
	} else {
		logger.Infof("Checkpoint found, restoring from checkpoint %s to directory %s", string(currentCheckpointRaw), liveDir)

		checkpointPath := filepath.Join(dataDir, checkpointsDir, string(currentCheckpointRaw))

		// Clean up incomplete post-compaction leftovers from a previous crash.
		_ = os.RemoveAll(checkpointPath + ".compacted")
		// If the main checkpoint dir is missing but .old exists, a crash happened
		// between rename(target→.old) and rename(.compacted→target). Restore it.
		if _, statErr := os.Stat(checkpointPath); os.IsNotExist(statErr) {
			oldPath := checkpointPath + ".old"
			if _, oldStatErr := os.Stat(oldPath); oldStatErr == nil {
				logger.Infof("Recovering checkpoint from .old (crash during post-compaction rename)")
				if renameErr := os.Rename(oldPath, checkpointPath); renameErr != nil {
					return nil, fmt.Errorf("recovering checkpoint from .old: %w", renameErr)
				}
			}
		}
		_ = os.RemoveAll(checkpointPath + ".old")

		removeStart := time.Now()
		if err := os.RemoveAll(liveDir); err != nil {
			return nil, fmt.Errorf("removing old database: %w", err)
		}
		logger.WithFields(map[string]any{
			"duration": time.Since(removeStart).String(),
		}).Infof("Removed old live directory")

		linkStart := time.Now()
		if err := HardLink(checkpointPath, liveDir); err != nil {
			return nil, fmt.Errorf("hard linking checkpoint: %w", err)
		}
		logger.WithFields(map[string]any{
			"duration": time.Since(linkStart).String(),
		}).Infof("Hard-linked checkpoint to live directory")

		openStart := time.Now()
		db, err = pebble.Open(liveDir, opts)
		if err != nil {
			return nil, fmt.Errorf("opening pebble database: %w", err)
		}
		m := db.Metrics()
		logger.WithFields(map[string]any{
			"duration":              time.Since(openStart).String(),
			"l0FileCount":           m.Levels[0].NumFiles,
			"l0Size":                m.Levels[0].Size,
			"l1FileCount":           m.Levels[1].NumFiles,
			"l1Size":                m.Levels[1].Size,
			"memTableCount":         m.MemTable.Count,
			"memTableSize":          m.MemTable.Size,
			"compactionCount":       m.Compact.Count,
			"compactionDebt":        m.Compact.InProgressBytes,
			"compactionEstDebt":     m.Compact.EstimatedDebt,
			"walFilesCount":         m.WAL.Files,
			"walSize":               m.WAL.Size,
			"totalLevelsSize":       m.DiskSpaceUsage(),
		}).Infof("Pebble database opened — LSM state")

		// Compact L0 at startup to avoid read amplification with a cold block cache.
		// The runtime L0CompactionThreshold is tuned for write throughput (e.g. 64),
		// but at startup with a cold cache, even a few L0 files force the merging
		// iterator to read each one from disk, stalling reads for tens of seconds.
		// We use a low startup-specific threshold (4) to ensure reads are fast
		// immediately after boot.
		//
		// After compaction, we overwrite the checkpoint with the compacted state so
		// that subsequent restarts don't repeat the (potentially multi-minute) compaction.
		const startupL0CompactThreshold = 4
		if m.Levels[0].NumFiles > startupL0CompactThreshold {
			logger.WithFields(map[string]any{
				"l0FileCount": m.Levels[0].NumFiles,
				"threshold":   startupL0CompactThreshold,
			}).Infof("L0 file count exceeds startup compaction threshold, compacting before serving reads")
			compactStart := time.Now()
			if err := db.Compact(nil, []byte{0xFF}, false); err != nil {
				logger.WithFields(map[string]any{"error": err}).Infof("Startup compaction failed (non-fatal)")
			} else {
				m2 := db.Metrics()
				logger.WithFields(map[string]any{
					"duration":    time.Since(compactStart).String(),
					"l0FileCount": m2.Levels[0].NumFiles,
					"l0Size":      m2.Levels[0].Size,
				}).Infof("Startup compaction complete")

				// Replace the checkpoint with the compacted LSM state.
				// 1. Write to a temp dir
				// 2. Rename old → .old (preserves it if rename in step 3 fails)
				// 3. Rename temp → target (atomic on same filesystem)
				// 4. Remove .old
				// If we crash at any point, recovery is safe:
				//   - .compacted leftover: cleaned up on next boot (see above)
				//   - .old exists + target missing: startup falls back to .old
				//     (but CURRENT_CHECKPOINT still points to the ID, and NewStore
				//     just needs the dir to exist — we handle this at the top)
				tmpPath := checkpointPath + ".compacted"
				oldPath := checkpointPath + ".old"
				_ = os.RemoveAll(tmpPath) // clean up leftover from a previous crash
				_ = os.RemoveAll(oldPath)
				if err := db.Checkpoint(tmpPath, pebble.WithFlushedWAL()); err != nil {
					logger.WithFields(map[string]any{"error": err}).Infof("Failed to create post-compaction checkpoint (non-fatal)")
					_ = os.RemoveAll(tmpPath)
				} else if err := os.Rename(checkpointPath, oldPath); err != nil {
					logger.WithFields(map[string]any{"error": err}).Infof("Failed to move old checkpoint aside (non-fatal)")
					_ = os.RemoveAll(tmpPath)
				} else if err := os.Rename(tmpPath, checkpointPath); err != nil {
					// Restore the old checkpoint
					logger.WithFields(map[string]any{"error": err}).Infof("Failed to rename post-compaction checkpoint (non-fatal)")
					_ = os.Rename(oldPath, checkpointPath)
					_ = os.RemoveAll(tmpPath)
				} else {
					_ = os.RemoveAll(oldPath)
					logger.Infof("Post-compaction checkpoint saved — next restart will skip compaction")
				}
			}
		}
	}

	var currentCheckpoint uint64
	if len(currentCheckpointRaw) > 0 {
		currentCheckpoint, err = strconv.ParseUint(string(currentCheckpointRaw), 10, 64)
		if err != nil {
			return nil, err
		}
	}

	// Calculate the oldest checkpoint that should exist
	// based on current checkpoint and max checkpoints configuration
	var oldestCheckpoint uint64
	if currentCheckpoint >= uint64(cfg.MaxCheckpoints) {
		oldestCheckpoint = currentCheckpoint - uint64(cfg.MaxCheckpoints) + 1
	}

	store := &Store{
		opts:              opts,
		logger:            logger.WithField("cmp", "pebble"),
		dataDir:           dataDir,
		currentCheckPoint: currentCheckpoint,
		oldestCheckpoint:  oldestCheckpoint,
		maxCheckpoints:    cfg.MaxCheckpoints,
	}
	store.db.Store(db)

	// Clean up any orphaned backup checkpoints from a previous crash
	store.cleanupTemporaryCheckpoints()

	return store, nil
}

// DataDir returns the base data directory path for this store.
func (s *Store) DataDir() string {
	return s.dataDir
}

// Flush forces a flush of Pebble's memtables to SSTs on disk.
func (s *Store) Flush() error {
	return s.getDB().Flush()
}

// WarmBlockCache iterates the attributes zone [0xF1, 0xF2) to preload
// Pebble's block cache. This turns the first query on a cold start from a
// full disk scan into cache hits, which dramatically improves latency for
// read-heavy commands such as "accounts analysis".
func (s *Store) WarmBlockCache() {
	start := time.Now()
	db := s.getDB()

	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: []byte{KeyPrefixAttributes},
		UpperBound: []byte{KeyPrefixAttributes + 1},
	})
	if err != nil {
		s.logger.WithFields(map[string]any{"error": err}).
			Errorf("Block cache warmup failed to create iterator")
		return
	}
	defer func() { _ = iter.Close() }()

	var keys int64
	for iter.First(); iter.Valid(); iter.Next() {
		// Reading the value forces Pebble to load the data block into the
		// block cache. Keys alone only load index blocks.
		if _, err := iter.ValueAndErr(); err != nil {
			s.logger.WithFields(map[string]any{"error": err}).
				Errorf("Block cache warmup aborted on value read error")
			return
		}
		keys++
	}
	if err := iter.Error(); err != nil {
		s.logger.WithFields(map[string]any{"error": err}).
			Errorf("Block cache warmup iterator error")
		return
	}

	m := db.Metrics()
	s.logger.WithFields(map[string]any{
		"duration":        time.Since(start).String(),
		"keys":            keys,
		"blockCacheSize":  m.BlockCache.Size,
		"blockCacheCount": m.BlockCache.Count,
	}).Infof("Block cache warmup complete")
}

// Close closes the Pebble database.
func (s *Store) Close() error {
	db := s.db.Load()
	if db != nil {
		s.db.Store(nil)
		if err := db.Close(); err != nil {
			return fmt.Errorf("closing store: %w", err)
		}
	}
	return nil
}

// CreateSnapshot creates a new checkpoint of the database and returns the checkpoint ID.
func (s *Store) CreateSnapshot() (uint64, error) {
	s.logger.Infof("Creating snapshot")

	newCheckpointID := s.currentCheckPoint + 1
	checkpointDir := filepath.Join(s.dataDir, "checkpoints", fmt.Sprintf("%d", newCheckpointID))
	if err := os.RemoveAll(checkpointDir); err != nil {
		return 0, fmt.Errorf("removing checkpoint directory: %w", err)
	}

	if err := s.getDB().Checkpoint(checkpointDir, pebble.WithFlushedWAL()); err != nil {
		return 0, fmt.Errorf("creating checkpoint: %w", err)
	}

	f, err := os.Create(filepath.Join(s.dataDir, currentCheckpointFile+".tmp"))
	if err != nil {
		return 0, fmt.Errorf("creating checkpoint file: %w", err)
	}
	defer func() {
		_ = f.Close()
	}()

	if _, err := fmt.Fprintf(f, "%d", newCheckpointID); err != nil {
		return 0, fmt.Errorf("writing checkpoint file: %w", err)
	}

	if err := f.Sync(); err != nil {
		return 0, fmt.Errorf("syncing checkpoint file: %w", err)
	}

	if err := f.Close(); err != nil {
		return 0, fmt.Errorf("closing checkpoint file: %w", err)
	}

	if err := os.Rename(filepath.Join(s.dataDir, currentCheckpointFile+".tmp"), filepath.Join(s.dataDir, currentCheckpointFile)); err != nil {
		return 0, fmt.Errorf("renaming checkpoint file: %w", err)
	}

	// Clean up old checkpoints beyond the configured maximum
	// Note: it can fail, leaving old checkpoints on disk
	// this is not critical, but we should fix it eventually
	if err := s.cleanupOldCheckpoints(); err != nil {
		s.logger.WithFields(map[string]any{"error": err}).Infof("Failed to cleanup old checkpoints")
	}

	s.logger.WithFields(map[string]any{
		"checkpoint": newCheckpointID,
	}).Infof("Snapshot created")
	s.currentCheckPoint = newCheckpointID

	return newCheckpointID, nil
}

// cleanupOldCheckpoints removes checkpoints older than the configured maximum.
// It keeps the most recent maxCheckpoints checkpoints.
func (s *Store) cleanupOldCheckpoints() error {
	newCheckpoint := s.currentCheckPoint + 1

	// Calculate the oldest checkpoint to keep
	// If newCheckpoint is 15 and maxCheckpoints is 10, we keep checkpoints 6-15
	// So we delete anything older than (newCheckpoint - maxCheckpoints + 1)
	if newCheckpoint < uint64(s.maxCheckpoints) {
		// Not enough checkpoints yet, nothing to delete
		return nil
	}

	oldestToKeep := newCheckpoint - uint64(s.maxCheckpoints) + 1

	// Delete checkpoints from oldestCheckpoint up to (but not including) oldestToKeep
	for i := s.oldestCheckpoint; i < oldestToKeep; i++ {
		checkpointPath := filepath.Join(s.dataDir, checkpointsDir, fmt.Sprintf("%d", i))
		if err := os.RemoveAll(checkpointPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("removing old checkpoint %d: %w", i, err)
		}
	}

	// Update the oldest checkpoint tracker
	s.oldestCheckpoint = oldestToKeep

	return nil
}

// GetCheckpointPath returns the filesystem path for a given checkpoint ID.
// Returns an error if the checkpoint does not exist.
func (s *Store) GetCheckpointPath(checkpointID uint64) (string, error) {
	path := filepath.Join(s.dataDir, checkpointsDir, fmt.Sprintf("%d", checkpointID))
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return "", fmt.Errorf("checkpoint %d not found", checkpointID)
		}
		return "", fmt.Errorf("checking checkpoint %d: %w", checkpointID, err)
	}
	return path, nil
}

// GetCurrentCheckpointID returns the current checkpoint ID.
func (s *Store) GetCurrentCheckpointID() uint64 {
	return s.currentCheckPoint
}

// CreateTemporaryCheckpoint creates a Pebble checkpoint in the tmp/<name> directory.
// Unlike CreateSnapshot, this does not modify currentCheckPoint, CURRENT_CHECKPOINT,
// or interfere with the Raft snapshot lifecycle in any way.
// The caller must call RemoveTemporaryCheckpoint when the checkpoint is no longer needed.
func (s *Store) CreateTemporaryCheckpoint(name string) (string, error) {
	path := filepath.Join(s.dataDir, temporaryCheckpointsDir, name)

	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return "", fmt.Errorf("creating temporary checkpoint directory: %w", err)
	}

	if err := os.RemoveAll(path); err != nil {
		return "", fmt.Errorf("removing existing temporary checkpoint: %w", err)
	}

	if err := s.getDB().Checkpoint(path, pebble.WithFlushedWAL()); err != nil {
		return "", fmt.Errorf("creating temporary checkpoint %q: %w", name, err)
	}

	return path, nil
}

// RemoveTemporaryCheckpoint removes a temporary checkpoint created by CreateTemporaryCheckpoint.
func (s *Store) RemoveTemporaryCheckpoint(name string) error {
	path := filepath.Join(s.dataDir, temporaryCheckpointsDir, name)
	if err := os.RemoveAll(path); err != nil {
		return fmt.Errorf("removing temporary checkpoint %q: %w", name, err)
	}
	return nil
}

// TemporaryCheckpointPath returns the path to a temporary checkpoint and whether it exists.
func (s *Store) TemporaryCheckpointPath(name string) (string, bool) {
	path := filepath.Join(s.dataDir, temporaryCheckpointsDir, name)
	if _, err := os.Stat(path); err != nil {
		return "", false
	}
	return path, true
}

// cleanupTemporaryCheckpoints removes the entire tmp/ directory on startup.
// Temporary checkpoints are ephemeral (backups, seal hashing);
// any leftovers are from a previous crash and can be safely deleted.
func (s *Store) cleanupTemporaryCheckpoints() {
	path := filepath.Join(s.dataDir, temporaryCheckpointsDir)
	if err := os.RemoveAll(path); err != nil {
		s.logger.WithFields(map[string]any{
			"error": err,
		}).Errorf("Failed to clean up temporary checkpoints")
	}
}

// PrepareCheckpointRestore prepares a directory for restoring a checkpoint from a remote peer.
// It returns the path to the directory where the checkpoint should be extracted.
func (s *Store) PrepareCheckpointRestore(checkpointID uint64) (string, error) {
	checkpointDir := filepath.Join(s.dataDir, checkpointsDir, fmt.Sprintf("%d", checkpointID))

	// Remove any existing directory at this path
	if err := os.RemoveAll(checkpointDir); err != nil {
		return "", fmt.Errorf("removing existing checkpoint directory: %w", err)
	}

	// Create the directory
	if err := os.MkdirAll(checkpointDir, 0755); err != nil {
		return "", fmt.Errorf("creating checkpoint directory: %w", err)
	}

	return checkpointDir, nil
}

// RestoreCheckpoint restores the database from a checkpoint.
// This closes the current database, replaces it with the checkpoint, and reopens.
// Uses atomic pointer swap so concurrent reads via getDB() are never blocked.
// There is a brief window (close → reopen) where getDB() returns a closed DB;
// callers will see a fast Pebble error instead of blocking on a mutex.
func (s *Store) RestoreCheckpoint(checkpointID uint64) error {
	checkpointDir := filepath.Join(s.dataDir, checkpointsDir, fmt.Sprintf("%d", checkpointID))

	// Verify the checkpoint exists
	if _, err := os.Stat(checkpointDir); err != nil {
		return fmt.Errorf("checkpoint %d not found: %w", checkpointID, err)
	}

	oldDB := s.db.Load()

	// Preserve this node's persisted config before replacing the database.
	// The checkpoint originates from the leader and contains the leader's
	// config (including its node-id). After restore we must re-write this
	// node's own identity so that startup config validation passes.
	var preservedConfig []byte
	if value, closer, err := oldDB.Get([]byte{KeyPrefixPersistedConfig}); err == nil {
		preservedConfig = append([]byte(nil), value...)
		_ = closer.Close()
	}

	// Close the current database
	if err := oldDB.Close(); err != nil {
		return fmt.Errorf("closing current database: %w", err)
	}

	// Remove the live directory
	liveDirectory := filepath.Join(s.dataDir, liveDir)
	if err := os.RemoveAll(liveDirectory); err != nil {
		return fmt.Errorf("removing live directory: %w", err)
	}

	// Hard link the checkpoint to the live directory
	if err := HardLink(checkpointDir, liveDirectory); err != nil {
		return fmt.Errorf("hard linking checkpoint to live directory: %w", err)
	}

	// Reopen the database with the same options
	newDB, err := pebble.Open(liveDirectory, s.opts)
	if err != nil {
		return fmt.Errorf("reopening database: %w", err)
	}

	// Atomic swap: new reads immediately see the fresh DB.
	s.db.Store(newDB)

	// Re-write this node's persisted config into the restored database.
	// After writing, flush and recreate the checkpoint so the config survives
	// the next startup (NewStore re-links live from the checkpoint directory).
	if preservedConfig != nil {
		if err := newDB.Set([]byte{KeyPrefixPersistedConfig}, preservedConfig, pebble.Sync); err != nil {
			return fmt.Errorf("re-writing persisted config after checkpoint restore: %w", err)
		}
		if err := newDB.Flush(); err != nil {
			return fmt.Errorf("flushing persisted config after checkpoint restore: %w", err)
		}
		if err := os.RemoveAll(checkpointDir); err != nil {
			return fmt.Errorf("removing old checkpoint dir for re-checkpoint: %w", err)
		}
		if err := newDB.Checkpoint(checkpointDir); err != nil {
			return fmt.Errorf("re-creating checkpoint with preserved config: %w", err)
		}
	}

	// Update the current checkpoint file
	f, err := os.Create(filepath.Join(s.dataDir, currentCheckpointFile))
	if err != nil {
		return fmt.Errorf("creating checkpoint file: %w", err)
	}
	defer func() {
		_ = f.Close()
	}()

	if _, err := fmt.Fprintf(f, "%d", checkpointID); err != nil {
		return fmt.Errorf("writing checkpoint file: %w", err)
	}

	if err := f.Sync(); err != nil {
		return fmt.Errorf("syncing checkpoint file: %w", err)
	}

	// Update internal state
	s.currentCheckPoint = checkpointID
	s.oldestCheckpoint = checkpointID

	s.logger.WithFields(map[string]any{
		"checkpointId": checkpointID,
	}).Infof("Database restored from checkpoint")

	return nil
}

// IterateColdKVPairs iterates all cold-storable KV pairs whose sequence (or byLog)
// falls in [startSeq, closeSeq] and calls fn for each raw key+value.
// IterateColdKVPairs iterates sequence-keyed prefixes (logs, audit) via efficient range scan.
// Transaction updates are not archived: they are redundant with log entries which already
// contain all creation, revert, and metadata information.
func (s *Store) IterateColdKVPairs(startSeq, closeSeq uint64, fn func(key, value []byte) error) error {
	db := s.getDB()

	for _, prefix := range ColdSequencePrefixes {
		kb := NewKeyBuilder()
		kb.PutByte(prefix).PutUInt64(startSeq)
		lowerBound := kb.Snapshot()
		kb.PutByte(prefix).PutUInt64(closeSeq + 1)
		upperBound := kb.Build()

		if err := iterateRawRange(db, lowerBound, upperBound, fn); err != nil {
			return fmt.Errorf("iterating prefix 0x%02x: %w", prefix, err)
		}
	}

	return nil
}

// iterateRawRange iterates all keys in [lowerBound, upperBound) and calls fn for each.
func iterateRawRange(db *pebble.DB, lowerBound, upperBound []byte, fn func(key, value []byte) error) error {
	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	if err != nil {
		return err
	}
	defer func() { _ = iter.Close() }()

	for iter.First(); iter.Valid(); iter.Next() {
		value, err := iter.ValueAndErr()
		if err != nil {
			return fmt.Errorf("reading value: %w", err)
		}
		if err := fn(iter.Key(), value); err != nil {
			return err
		}
	}
	return iter.Error()
}

// ProtoCursor implements Cursor[T] for Pebble where T is a proto.Message pointer.
type ProtoCursor[T proto.Message] struct {
	iter    *pebble.Iterator
	started bool
	elemTyp reflect.Type
}

func NewProtoCursor[T proto.Message](iter *pebble.Iterator) *ProtoCursor[T] {
	var zero T
	return &ProtoCursor[T]{
		iter:    iter,
		elemTyp: reflect.TypeOf(zero).Elem(),
	}
}

func (c *ProtoCursor[T]) Next() (T, error) {
	var zero T
	if !c.started {
		c.started = true
		if !c.iter.First() {
			if err := c.iter.Error(); err != nil {
				return zero, err
			}
			return zero, io.EOF
		}
	} else {
		if !c.iter.Next() {
			if err := c.iter.Error(); err != nil {
				return zero, err
			}
			return zero, io.EOF
		}
	}

	value, err := c.iter.ValueAndErr()
	if err != nil {
		return zero, fmt.Errorf("reading value: %w", err)
	}

	item := reflect.New(c.elemTyp).Interface().(T)
	if err := proto.Unmarshal(value, item); err != nil {
		return zero, fmt.Errorf("unmarshaling: %w", err)
	}
	return item, nil
}

func (c *ProtoCursor[T]) Close() error {
	if c.iter != nil {
		return c.iter.Close()
	}
	return nil
}

func (s *Store) NewIter(p *pebble.IterOptions) (*pebble.Iterator, error) {
	return s.getDB().NewIter(p)
}

func HardLink(srcDir, dstDir string) error {
	srcDir = filepath.Clean(srcDir)
	dstDir = filepath.Clean(dstDir)

	srcInfo, err := os.Stat(srcDir)
	if err != nil {
		return fmt.Errorf("stat srcDir: %w", err)
	}
	if !srcInfo.IsDir() {
		return fmt.Errorf("srcDir is not a directory: %s", srcDir)
	}
	if _, err := os.Stat(dstDir); err == nil {
		return fmt.Errorf("dstDir already exists: %s", dstDir)
	}

	parent := filepath.Dir(dstDir)
	base := filepath.Base(dstDir)
	tmpDir := filepath.Join(parent, base+fmt.Sprintf(".tmp-%d-%d", os.Getpid(), time.Now().UnixNano()))

	defer func() {
		_ = os.RemoveAll(tmpDir)
	}()

	// Create tmp root with same perms as source root (best effort).
	if err := os.MkdirAll(tmpDir, srcInfo.Mode().Perm()); err != nil {
		return fmt.Errorf("mkdir tmpDir: %w", err)
	}
	if err := fsyncDir(tmpDir); err != nil {
		return err
	}

	err = filepath.WalkDir(srcDir, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(srcDir, path)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}

		dstPath := filepath.Join(tmpDir, rel)
		info, err := d.Info()
		if err != nil {
			return err
		}

		switch {
		case info.IsDir():
			if err := os.MkdirAll(dstPath, info.Mode().Perm()); err != nil {
				return fmt.Errorf("mkdir %s: %w", dstPath, err)
			}
			_ = os.Chmod(dstPath, info.Mode().Perm())
			return fsyncDir(dstPath)

		case info.Mode().Type() == 0: // regular file
			if err := os.Link(path, dstPath); err != nil {
				return fmt.Errorf("hardlink %s -> %s: %w", path, dstPath, err)
			}
			_ = os.Chmod(dstPath, info.Mode().Perm())
			return nil

		case (info.Mode() & os.ModeSymlink) != 0:
			target, err := os.Readlink(path)
			if err != nil {
				return fmt.Errorf("readlink %s: %w", path, err)
			}
			if err := os.Symlink(target, dstPath); err != nil {
				return fmt.Errorf("symlink %s -> %s: %w", dstPath, target, err)
			}
			return nil

		default:
			return fmt.Errorf("unsupported file type %s mode=%s", path, info.Mode().String())
		}
	})
	if err != nil {
		return fmt.Errorf("walk: %w", err)
	}

	if err := fsyncDir(tmpDir); err != nil {
		return err
	}
	if err := fsyncDir(parent); err != nil {
		return err
	}

	if err := os.Rename(tmpDir, dstDir); err != nil {
		return fmt.Errorf("rename publish: %w", err)
	}

	return fsyncDir(parent)
}

// fsyncDir fsyncs a directory so its entries are durably recorded. On Windows, it's a no-op.
func fsyncDir(dir string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	f, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open dir for fsync %s: %w", dir, err)
	}
	defer func() {
		_ = f.Close()
	}()
	if err := f.Sync(); err != nil {
		return fmt.Errorf("fsync dir %s: %w", dir, err)
	}
	return nil
}

// Checkpoint creates a Pebble checkpoint at destDir with flushed WAL.
// This is a thin wrapper around pebble.DB.Checkpoint used for testing
// and backup operations that need a standalone copy of the database.
func (s *Store) Checkpoint(destDir string) error {
	return s.getDB().Checkpoint(destDir, pebble.WithFlushedWAL())
}
