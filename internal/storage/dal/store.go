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
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/protobuf/proto"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

// ErrStoreClosed is returned when a store operation is attempted after the
// Pebble database has been closed. This prevents panics during shutdown races.
var ErrStoreClosed = errors.New("store closed")

const (
	liveDir                 = "live"
	currentCheckpointFile   = "CURRENT_CHECKPOINT"
	checkpointsDir          = "checkpoints"
	temporaryCheckpointsDir = "tmp"
	baselineCheckpointsDir  = "baseline"
)

// Store is a Pebble implementation of dal.Store
// It stores balances and account metadata.
type Store struct {
	dbMu              sync.RWMutex // protects DB lifecycle (RestoreCheckpoint, Close)
	db                *pebble.DB
	opts              *pebble.Options
	logger            logging.Logger
	dataDir           string
	currentCheckPoint uint64
	oldestCheckpoint  uint64
	maxCheckpoints    int
	stallState        *WriteStallState
}

// getDB returns the current pebble.DB.
// Callers that create iterators (NewIter, IterateColdKVPairs) must hold
// dbMu.RLock to prevent RestoreCheckpoint/Close from closing the DB
// between the read and the iterator creation.
func (s *Store) getDB() *pebble.DB {
	return s.db
}

// WriteStallWaitCh returns a channel that blocks while Pebble is in a write stall.
// When not stalled, the channel is already closed (non-blocking).
// Safe to call on stores opened read-only (returns a pre-closed channel).
func (s *Store) WriteStallWaitCh() <-chan struct{} {
	if s.stallState == nil {
		ch := make(chan struct{})
		close(ch)

		return ch
	}

	return s.stallState.WaitCh()
}

// IsWriteStalled returns true if Pebble is currently in a write stall.
// Safe to call on stores opened read-only (always returns false).
func (s *Store) IsWriteStalled() bool {
	if s.stallState == nil {
		return false
	}

	return s.stallState.IsStalled()
}

// Key prefixes for Pebble storage, organized into five zones:
//
//	Cold zone            [0x01, 0x04) — archived to cold storage then purged per period.
//	Per-ledger system    [0xE0, 0xF1) — per-ledger data persisted forever (prepared queries, numscript, mirror, audit config, period schedule).
//	Attributes zone      [0xF1, 0xF2) — derived data hashed during seal, stays in hot storage.
//	Global system zone   [0xF2, 0xFF) — cluster-wide metadata persisted forever.
//	Cache snapshot zone  [0xFF, ...)  — cache snapshot data (written before Pebble checkpoints).

// Zone boundary constants define the contiguous key ranges.
const (
	ZoneColdStart         byte = 0x01
	ZoneColdEnd           byte = 0x04
	ZonePerLedgerSysStart byte = 0xE0
	ZonePerLedgerSysEnd   byte = 0xF1 // == KeyPrefixAttributes
	ZoneAttributesStart   byte = 0xF1
	ZoneAttributesEnd     byte = 0xF2
	ZoneGlobalSysStart    byte = 0xF2
	ZoneGlobalSysEnd      byte = 0xFF // exclusive: 0xFF is cache snapshot zone
)

// Canonical key separators used inside attribute canonical keys
// to delimit volume and metadata sub-keys.
const (
	CanonicalKeySepVolume          byte = 0x00
	CanonicalKeySepMetadata        byte = 0x01
	CanonicalKeySepTransaction     byte = 0x02
	CanonicalKeySepAssetPrecision  byte = 0x03
)

// MaxUint64Bytes is the big-endian representation of math.MaxUint64,
// used as an upper bound sentinel for sequence-keyed iterations.
var MaxUint64Bytes = []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}

var (
	// --- Cold zone [0x01, 0x04) ---.

	// Sequence-keyed prefixes: [prefix][sequence] — support efficient range scan/delete.
	KeyPrefixLog   byte = 0x01 // [KeyPrefixLog][sequence] -> Log
	KeyPrefixAudit byte = 0x02 // [KeyPrefixAudit][sequence] -> AuditEntry

	// ColdSequencePrefixes lists cold-storable prefixes keyed by sequence number.
	// These support efficient range scan and range delete by [prefix][startSeq]..[prefix][endSeq].
	ColdSequencePrefixes = []byte{KeyPrefixLog, KeyPrefixAudit}

	// --- Per-ledger system zone [0xE0, 0xF1) ---.

	KeyPrefixPreparedQuery        byte = 0xE0 // [KeyPrefixPreparedQuery][name\x00][queryName] -> PreparedQuery protobuf
	KeyPrefixPendingLedgerCleanup byte = 0xE1 // [KeyPrefixPendingLedgerCleanup][ledger_name] -> uint64 (delete log sequence)
	KeyPrefixNumscript            byte = 0xE9 // [KeyPrefixNumscript][name]\x00[version_BE] -> NumscriptInfo protobuf
	KeyPrefixNumscriptLatest      byte = 0xEA // [KeyPrefixNumscriptLatest][name] -> uint64 (latest version)
	KeyPrefixMirrorSourceHead     byte = 0xEB // [KeyPrefixMirrorSourceHead][ledger_name] -> uint64 (latest known v2 source log ID)
	KeyPrefixMirrorCursor         byte = 0xEC // [KeyPrefixMirrorCursor][ledger_name] -> uint64 (last ingested v2 log ID)
	KeyPrefixMirrorStatus         byte = 0xED // [KeyPrefixMirrorStatus][ledger_name] -> MirrorSyncError protobuf
	KeyPrefixAuditConfig          byte = 0xEE // [KeyPrefixAuditConfig] -> audit config byte (0x00=false, 0x01=true)
	KeyPrefixPeriodSchedule       byte = 0xEF // [KeyPrefixPeriodSchedule] -> cron expression string

	// --- Attributes zone [0xF1, 0xF2) — seal hash domain ---.

	KeyPrefixAttributes byte = 0xF1

	// Attribute type prefixes used within the attributes zone.
	AttributePrefixVolume      = byte('V') // Volume — unchanged
	AttributePrefixMetadata    = byte('M') // Metadata — unchanged
	AttributePrefixIdempotency = byte('I') // Idempotency
	AttributePrefixReference   = byte('R') // Reference
	AttributePrefixLedger      = byte('L') // Ledger
	AttributePrefixBoundary    = byte('B') // Boundary — unchanged
	AttributePrefixTransaction = byte('T') // Transaction state

	// --- Global system zone [0xF2, 0xFF] ---.

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
	KeyPrefixCacheSnapshot        byte = 0xFF // [KeyPrefixCacheSnapshot][sub...] -> cache snapshot data
)

// Cache snapshot sub-prefixes within the 0xFF zone.
// Key format:
//
//	[0xFF][gen: 0x00|0x01][type][16-byte U128 key] → proto value (attribute entry)
//	[0xFF][gen: 0x00|0x01][0x00]                   → CacheGenerationMeta (baseIndex)
//	[0xFF][0xFF]                                   → CacheSnapshotMeta (currentGeneration)
const (
	CacheGenMeta byte = 0x00 // Generation metadata sub-key (under [0xFF][gen])

	CacheTypeVolumes      byte = 0x01
	CacheTypeMetadata     byte = 0x02
	CacheTypeLedgers      byte = 0x03
	CacheTypeBoundaries   byte = 0x04
	CacheTypeReferences   byte = 0x05
	CacheTypeTransactions byte = 0x06
	CacheTypeNumscript    byte = 0x07
	CacheTypeIdempotency  byte = 0x08

	CacheMetaKey byte = 0xFF // [0xFF][0xFF] → CacheSnapshotMeta
)

// NewStore creates a new Store instance.
func NewStore(
	dataDir string,
	logger logging.Logger,
	meter metric.Meter,
	cfg Config,
) (*Store, error) {
	stallState := NewWriteStallState()

	opts := &pebble.Options{
		EventListener: NewMetricsListener(meter, stallState),
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
			{TargetFileSize: cfg.TargetFileSize, FilterPolicy: bloom.FilterPolicy(10)},
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

				renameErr := os.Rename(oldPath, checkpointPath)
				if renameErr != nil {
					return nil, fmt.Errorf("recovering checkpoint from .old: %w", renameErr)
				}
			}
		}

		_ = os.RemoveAll(checkpointPath + ".old")

		removeStart := time.Now()

		err := os.RemoveAll(liveDir)
		if err != nil {
			return nil, fmt.Errorf("removing old database: %w", err)
		}

		logger.WithFields(map[string]any{
			"duration": time.Since(removeStart).String(),
		}).Infof("Removed old live directory")

		linkStart := time.Now()

		err = HardLink(checkpointPath, liveDir)
		if err != nil {
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
			"duration":          time.Since(openStart).String(),
			"l0FileCount":       m.Levels[0].NumFiles,
			"l0Size":            m.Levels[0].Size,
			"l1FileCount":       m.Levels[1].NumFiles,
			"l1Size":            m.Levels[1].Size,
			"memTableCount":     m.MemTable.Count,
			"memTableSize":      m.MemTable.Size,
			"compactionCount":   m.Compact.Count,
			"compactionDebt":    m.Compact.InProgressBytes,
			"compactionEstDebt": m.Compact.EstimatedDebt,
			"walFilesCount":     m.WAL.Files,
			"walSize":           m.WAL.Size,
			"totalLevelsSize":   m.DiskSpaceUsage(),
		}).Infof("Pebble database opened — LSM state")
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
		stallState:        stallState,
	}
	store.db = db

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

// WarmSystemKeys preloads the system/config key zones into Pebble's block
// cache. This covers [0xE0, 0xF1) and [0xF2, 0xFF) — everything except the
// bulky attributes zone (0xF1) which contains volumes and metadata. This is
// fast (few keys) and should run synchronously before NewMachine so that FSM
// startup reads hit warm cache.
func (s *Store) WarmSystemKeys() {
	s.logger.Infof("Starting system key warmup...")

	start := time.Now()
	db := s.getDB()

	ranges := [][2]byte{
		{ZonePerLedgerSysStart, ZonePerLedgerSysEnd}, // 0xE0..0xF0 (numscripts, mirror, audit, period schedule, etc.)
		{ZoneGlobalSysStart, ZoneGlobalSysEnd},       // 0xF2..0xFE (applied index, ledger info, periods, signing, etc.)
	}

	var totalKeys int64

	for _, r := range ranges {
		rangeStart := time.Now()

		keys, err := s.warmRange(db, r[0], r[1])
		if err != nil {
			s.logger.WithFields(map[string]any{"error": err}).
				Errorf("System key warmup failed")

			return
		}

		totalKeys += keys
		s.logger.WithFields(map[string]any{
			"range":    fmt.Sprintf("[0x%02X, 0x%02X)", r[0], r[1]),
			"keys":     keys,
			"duration": time.Since(rangeStart).String(),
		}).Infof("System key warmup range done")
	}

	s.logger.WithFields(map[string]any{
		"duration": time.Since(start).String(),
		"keys":     totalKeys,
	}).Infof("System key warmup complete")
}

// WarmBlockCache iterates the attributes zone [0xF1, 0xF2) to preload
// Pebble's block cache. This is the heavyweight warmup (volumes, metadata,
// etc.) and should run in the background after servers are listening.
func (s *Store) WarmBlockCache() {
	start := time.Now()
	db := s.getDB()

	keys, err := s.warmRange(db, ZoneAttributesStart, ZoneAttributesEnd)
	if err != nil {
		s.logger.WithFields(map[string]any{"error": err}).
			Errorf("Block cache warmup failed")

		return
	}

	m := db.Metrics()
	s.logger.WithFields(map[string]any{
		"duration":        time.Since(start).String(),
		"keys":            keys,
		"blockCacheSize":  m.BlockCache.Size,
		"blockCacheCount": m.BlockCache.Count,
	}).Infof("Block cache warmup complete (attributes zone)")
}

// warmRange iterates [lower, upper) reading every value to populate the block cache.
func (s *Store) warmRange(db *pebble.DB, lower, upper byte) (int64, error) {
	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: []byte{lower},
		UpperBound: []byte{upper},
	})
	if err != nil {
		return 0, fmt.Errorf("creating warmup iterator [0x%02X, 0x%02X): %w", lower, upper, err)
	}

	defer func() { _ = iter.Close() }()

	var keys int64

	for iter.First(); iter.Valid(); iter.Next() {
		if _, err := iter.ValueAndErr(); err != nil {
			return keys, fmt.Errorf("warmup value read error at key %d: %w", keys, err)
		}

		keys++
	}

	if err := iter.Error(); err != nil {
		return keys, fmt.Errorf("warmup iterator error: %w", err)
	}

	return keys, nil
}

// Close closes the Pebble database.
func (s *Store) Close() error {
	s.dbMu.Lock()
	defer s.dbMu.Unlock()

	db := s.db
	if db != nil {
		s.db = nil

		err := db.Close()
		if err != nil {
			return fmt.Errorf("closing store: %w", err)
		}
	}

	return nil
}

// CreateSnapshot creates a new checkpoint of the database and returns the checkpoint ID.
// It holds dbMu exclusively to serialize against concurrent calls from triggerSnapshot
// (via FSM) and the CreateCheckpoint gRPC handler.
func (s *Store) CreateSnapshot() (uint64, error) {
	s.dbMu.Lock()
	defer s.dbMu.Unlock()

	s.logger.Infof("Creating snapshot")

	newCheckpointID := s.currentCheckPoint + 1

	checkpointDir := filepath.Join(s.dataDir, "checkpoints", strconv.FormatUint(newCheckpointID, 10))
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
		checkpointPath := filepath.Join(s.dataDir, checkpointsDir, strconv.FormatUint(i, 10))

		err := os.RemoveAll(checkpointPath)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
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
	path := filepath.Join(s.dataDir, checkpointsDir, strconv.FormatUint(checkpointID, 10))
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

	err := os.MkdirAll(filepath.Dir(path), 0755)
	if err != nil {
		return "", fmt.Errorf("creating temporary checkpoint directory: %w", err)
	}

	err = os.RemoveAll(path)
	if err != nil {
		return "", fmt.Errorf("removing existing temporary checkpoint: %w", err)
	}

	err = s.getDB().Checkpoint(path, pebble.WithFlushedWAL())
	if err != nil {
		return "", fmt.Errorf("creating temporary checkpoint %q: %w", name, err)
	}

	return path, nil
}

// RemoveTemporaryCheckpoint removes a temporary checkpoint created by CreateTemporaryCheckpoint.
func (s *Store) RemoveTemporaryCheckpoint(name string) error {
	path := filepath.Join(s.dataDir, temporaryCheckpointsDir, name)

	err := os.RemoveAll(path)
	if err != nil {
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

// BaselineSnapshotDir returns the path where the baseline attribute snapshot
// should be written and ensures its parent directory exists.
// Callers (e.g. the applier) use this path with attributes.CreateBaselineSnapshot.
func (s *Store) BaselineSnapshotDir() (string, error) {
	path := filepath.Join(s.dataDir, baselineCheckpointsDir, "checker")

	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return "", fmt.Errorf("creating baseline snapshot directory: %w", err)
	}

	return path, nil
}

// BaselineCheckpointPath returns the path to the baseline checker checkpoint and whether it exists.
func (s *Store) BaselineCheckpointPath() (string, bool) {
	path := filepath.Join(s.dataDir, baselineCheckpointsDir, "checker")
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

	err := os.RemoveAll(path)
	if err != nil {
		s.logger.WithFields(map[string]any{
			"error": err,
		}).Errorf("Failed to clean up temporary checkpoints")
	}
}

// PrepareCheckpointRestore prepares a directory for restoring a checkpoint from a remote peer.
// It returns the path to the directory where the checkpoint should be extracted.
func (s *Store) PrepareCheckpointRestore(checkpointID uint64) (string, error) {
	checkpointDir := filepath.Join(s.dataDir, checkpointsDir, strconv.FormatUint(checkpointID, 10))

	// Remove any existing directory at this path
	err := os.RemoveAll(checkpointDir)
	if err != nil {
		return "", fmt.Errorf("removing existing checkpoint directory: %w", err)
	}

	// Create the directory
	err = os.MkdirAll(checkpointDir, 0755)
	if err != nil {
		return "", fmt.Errorf("creating checkpoint directory: %w", err)
	}

	return checkpointDir, nil
}

// RestoreCheckpoint restores the database from a checkpoint.
// This closes the current database, replaces it with the checkpoint, and reopens.
// Holds dbMu exclusively so concurrent NewIter/IterateColdKVPairs calls block
// until the new DB is ready, preventing panics on the closed DB.
func (s *Store) RestoreCheckpoint(checkpointID uint64) error {
	s.dbMu.Lock()
	defer s.dbMu.Unlock()

	checkpointDir := filepath.Join(s.dataDir, checkpointsDir, strconv.FormatUint(checkpointID, 10))

	// Verify the checkpoint exists
	if _, err := os.Stat(checkpointDir); err != nil {
		return fmt.Errorf("checkpoint %d not found: %w", checkpointID, err)
	}

	oldDB := s.db

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

	s.db = newDB

	// Re-write this node's persisted config into the restored database.
	// After writing, flush and recreate the checkpoint so the config survives
	// the next startup (NewStore re-links live from the checkpoint directory).
	if preservedConfig != nil {
		err := newDB.Set([]byte{KeyPrefixPersistedConfig}, preservedConfig, pebble.Sync)
		if err != nil {
			return fmt.Errorf("re-writing persisted config after checkpoint restore: %w", err)
		}

		err = newDB.Flush()
		if err != nil {
			return fmt.Errorf("flushing persisted config after checkpoint restore: %w", err)
		}

		err = os.RemoveAll(checkpointDir)
		if err != nil {
			return fmt.Errorf("removing old checkpoint dir for re-checkpoint: %w", err)
		}

		err = newDB.Checkpoint(checkpointDir)
		if err != nil {
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
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()

	db := s.getDB()
	if db == nil {
		return ErrStoreClosed
	}

	for _, prefix := range ColdSequencePrefixes {
		kb := NewKeyBuilder()
		kb.PutByte(prefix).PutUint64(startSeq)
		lowerBound := kb.Snapshot()
		kb.PutByte(prefix).PutUint64(closeSeq + 1)
		upperBound := kb.Build()

		err := iterateRawRange(db, lowerBound, upperBound, fn)
		if err != nil {
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

// vtUnmarshaler is implemented by vtprotobuf-generated messages.
type vtUnmarshaler interface {
	UnmarshalVT([]byte) error
}

// ProtoCursorOption configures a ProtoCursor.
type ProtoCursorOption func(*protoCursorConfig)

type protoCursorConfig struct {
	reuse     bool
	resetFunc func(proto.Message)
}

// WithReuse enables object reuse: the same proto message is reset and
// unmarshaled into on each Next() call instead of allocating a new one.
// The caller must NOT retain the returned pointer across calls to Next().
func WithReuse() ProtoCursorOption {
	return func(c *protoCursorConfig) { c.reuse = true }
}

// WithResetFunc provides a custom reset function used when WithReuse is enabled.
// Instead of proto.Reset (which zeros all nested pointers), the custom function
// can preserve allocations for reuse by UnmarshalVT while clearing values to
// prevent stale data. The function must reset all fields that might not be
// overwritten by UnmarshalVT (e.g. optional fields absent from the wire).
func WithResetFunc(fn func(proto.Message)) ProtoCursorOption {
	return func(c *protoCursorConfig) { c.resetFunc = fn }
}

// ProtoCursor implements Cursor[T] for Pebble where T is a proto.Message pointer.
type ProtoCursor[T proto.Message] struct {
	iter      *pebble.Iterator
	started   bool
	elemTyp   reflect.Type
	reuse     bool
	resetFunc func(proto.Message)
	hasItem   bool
	item      T // reused when reuse=true
}

func NewProtoCursor[T proto.Message](iter *pebble.Iterator, opts ...ProtoCursorOption) *ProtoCursor[T] {
	var cfg protoCursorConfig
	for _, o := range opts {
		o(&cfg)
	}

	var zero T

	return &ProtoCursor[T]{
		iter:      iter,
		elemTyp:   reflect.TypeOf(zero).Elem(),
		reuse:     cfg.reuse,
		resetFunc: cfg.resetFunc,
	}
}

func (c *ProtoCursor[T]) Next() (T, error) {
	var zero T

	if !c.started {
		c.started = true
		if !c.iter.First() {
			err := c.iter.Error()
			if err != nil {
				return zero, err
			}

			return zero, io.EOF
		}
	} else if !c.iter.Next() {
		err := c.iter.Error()
		if err != nil {
			return zero, err
		}

		return zero, io.EOF
	}

	value, err := c.iter.ValueAndErr()
	if err != nil {
		return zero, fmt.Errorf("reading value: %w", err)
	}

	var item T
	if c.reuse && c.hasItem {
		if c.resetFunc != nil {
			c.resetFunc(c.item)
		} else {
			proto.Reset(c.item)
		}
		item = c.item
	} else {
		item = reflect.New(c.elemTyp).Interface().(T)
		if c.reuse {
			c.item = item
			c.hasItem = true
		}
	}

	// Fast path: vtprotobuf UnmarshalVT avoids reflection-based decoding.
	if vu, ok := any(item).(vtUnmarshaler); ok {
		if err := vu.UnmarshalVT(value); err != nil {
			return zero, fmt.Errorf("unmarshaling: %w", err)
		}
	} else if err := proto.Unmarshal(value, item); err != nil {
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
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()

	db := s.getDB()
	if db == nil {
		return nil, ErrStoreClosed
	}

	return db.NewIter(p)
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
			err := os.MkdirAll(dstPath, info.Mode().Perm())
			if err != nil {
				return fmt.Errorf("mkdir %s: %w", dstPath, err)
			}

			_ = os.Chmod(dstPath, info.Mode().Perm())

			return fsyncDir(dstPath)

		case info.Mode().Type() == 0: // regular file
			err := os.Link(path, dstPath)
			if err != nil {
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
