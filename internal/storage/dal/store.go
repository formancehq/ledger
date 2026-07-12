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
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/cockroachdb/pebble/v2/wal"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/protobuf/proto"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	"github.com/formancehq/invariants"

	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
)

// ErrStoreClosed is returned when a store operation is attempted after the
// Pebble database has been closed. This prevents panics during shutdown races.
var ErrStoreClosed = errors.New("store closed")

const (
	liveDir = "live"
	// liveStagingDir is where RestoreCheckpoint builds the new database
	// before publishing it. Hard-link, pebble.Open, persisted-config
	// rewrite, flush, and re-checkpoint all happen inside `live.staging/`.
	// Only once every post-open step succeeds do we `rename(live.staging,
	// live)` — that rename is the single atomic commit point of the
	// restore. Until it happens, `live/` does not exist, so a crash
	// anywhere in the build phase is unambiguous on the next boot: the
	// presence of `live/` means "restore committed", its absence (with
	// `live.discard/` present) means "restore aborted, revert to discard".
	liveStagingDir = "live.staging"
	// liveDiscardDir is the rename-aside name used by RestoreCheckpoint as
	// a rollback point. The restore renames `live/` -> `live.discard/`
	// before building the new database under `live.staging/`. If any step
	// fails before the staging->live publish, we drop staging and rename
	// `live.discard/` back to `live/`. After a successful publish the
	// `live.discard/` directory is removed.
	liveDiscardDir          = "live.discard"
	checkpointsDir          = "checkpoints"
	temporaryCheckpointsDir = "tmp"
	baselineCheckpointsDir  = "baseline"
	// incomingCheckpointDir sits at the dataDir top level (sibling of
	// checkpointsDir) so that checkpointsDir contains only numbered
	// checkpoint dirs. Same filesystem → ActivateIncomingRestore's
	// Rename(incoming, checkpoints/<id>) stays atomic.
	incomingCheckpointDir = "incoming-checkpoint"
)

// ScanLatestCheckpointID scans the checkpoints directory and returns the highest
// numeric checkpoint ID found and whether any checkpoint exists.
func ScanLatestCheckpointID(dataDir string) (latestID uint64, found bool, err error) {
	dir := filepath.Join(dataDir, checkpointsDir)

	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, false, nil
		}

		return 0, false, fmt.Errorf("reading checkpoints directory: %w", err)
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		id, err := strconv.ParseUint(entry.Name(), 10, 64)
		if err != nil {
			// Defensive: no producer writes non-numeric children to
			// checkpointsDir, but tolerate foreign artifacts (.DS_Store,
			// editor scratch files) instead of failing boot.
			continue
		}

		if !found || id > latestID {
			latestID = id
			found = true
		}
	}

	return latestID, found, nil
}

// Store is a Pebble implementation of dal.Store
// It stores balances and account metadata.
type Store struct {
	dbMu              sync.RWMutex // protects DB lifecycle (RestoreCheckpoint, Close)
	snapshotMu        sync.Mutex   // serializes checkpoint creation (currentCheckPoint counter)
	db                *pebble.DB
	opts              *pebble.Options
	logger            logging.Logger
	dataDir           string
	currentCheckPoint uint64
	oldestCheckpoint  uint64
	maxCheckpoints    int
	stallState        *WriteStallState
	iopsCounters      *IOPSCounters

	// restoreGeneration is a process-local monotonic counter bumped on every
	// successful RestoreCheckpoint. It lets audit-chain pollers (auditindexer,
	// usagebuilder) detect that the primary store was rolled back beneath their
	// cursor WITHOUT relying on the cursor>head position signal, which a
	// catch-up race can erase (restore below cursor, then the head re-grows past
	// the old cursor before the poller re-samples). A poller records the value
	// it observed and forces a reset+rebuild the moment it changes. It is
	// in-memory by design: RestoreCheckpoint only runs at runtime (follower sync
	// via SynchronizeWithLeader), where the pollers keep running across the
	// restore (StopBackgroundTasks does not stop them); a process restart resets
	// both the store and the poller to 0, falling back to the boot-time
	// cursor-ahead / gap heuristic that covers offline backup restores.
	restoreGeneration atomic.Uint64
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

// Key prefixes for Pebble storage. Every key starts with a 2-byte prefix:
// [zone_byte][sub_prefix_byte][...payload...].
//
// Zone bytes (first byte of every key):
//
//	0x01  Attributes   — derived data hashed during seal (volumes, metadata, transactions, ...).
//	0x02  Cache        — in-memory cache snapshot persisted before checkpoints.
//	0x03  Per-ledger   — per-ledger config/state (prepared queries, reversions, mirror).
//	0x04  Cold               — data archived to cold storage then purged per chapter (logs, audit).
//	0x05  Idempotency        — TTL-managed idempotency keys (evicted by Raft command, not chapter archival).
//	0x06  Global             — cluster-wide metadata persisted forever (applied index, signing, chapters, sinks, ...).
//	0x07  ClusterTransient   — FSM-tracked state that has no meaning after restore (backup jobs, future ephemeral state).
//	0x08..0xFF                — reserved for future zones.

// Zone bytes — first byte of every Pebble key.
const (
	ZoneAttributes  byte = 0x01
	ZoneCache       byte = 0x02
	ZonePerLedger   byte = 0x03
	ZoneCold        byte = 0x04
	ZoneIdempotency byte = 0x05
	ZoneGlobal      byte = 0x06
	// ZoneClusterTransient holds FSM state that is meaningful only inside
	// the current cluster's process lifetime. Survives snapshot/restore
	// WITHIN the same cluster (so a follower catching up via
	// InstallSnapshot rebuilds the right in-memory view), but is wiped at
	// FinalizeRestore on the receiving cluster — otherwise a backup taken
	// while a job was RUNNING would carry that entry into the restored
	// cluster, locking the destination forever.
	//
	// New writers in this zone must accept that their data does not
	// survive a cross-cluster restore. Everything operator-facing
	// (cluster config, signing keys, chapter state, …) stays in
	// ZoneGlobal.
	ZoneClusterTransient byte = 0x07
)

// Attribute sub-prefixes (zone 0x01), ordered by hot-path write frequency.
const (
	SubAttrVolume           byte = 0x01
	SubAttrMetadata         byte = 0x02
	SubAttrTransaction      byte = 0x03
	SubAttrLedger           byte = 0x04
	SubAttrBoundary         byte = 0x05
	SubAttrReference        byte = 0x06
	SubAttrLedgerMetadata   byte = 0x07
	SubAttrSinkConfig       byte = 0x08
	SubAttrNumscriptVersion byte = 0x09
	SubAttrNumscriptContent byte = 0x0A
	SubAttrPreparedQuery    byte = 0x0B
	SubAttrIndex            byte = 0x0C
)

// Cache sub-prefixes (zone 0x02).
// Attribute entries reuse SubAttr* codes: [ZoneCache][gen][SubAttr*][U128].
const (
	SubCacheGenMeta byte = 0x00 // [ZoneCache][gen][SubCacheGenMeta] → CacheGenerationMeta
	SubCacheMeta    byte = 0xFF // [ZoneCache][SubCacheMeta] → CacheSnapshotMeta (global)
)

// Per-ledger sub-prefixes (zone 0x03).
const (
	SubPLReversions       byte = 0x01
	SubPLPendingCleanup   byte = 0x02
	SubPLPreparedQuery    byte = 0x03
	SubPLMirrorSourceHead byte = 0x04
	SubPLMirrorCursor     byte = 0x05
	SubPLMirrorStatus     byte = 0x06
)

// Cold sub-prefixes (zone 0x04).
const (
	SubColdLog             byte = 0x01
	SubColdAudit           byte = 0x02
	SubColdAuditItem       byte = 0x03 // [ZoneCold][SubColdAuditItem][audit_seq BE 8][order_idx BE 4] → AuditItem
	SubColdAppliedProposal byte = 0x04 // [ZoneCold][SubColdAppliedProposal][applied_proposal_seq BE 8] → AppliedProposal
)

// Idempotency sub-prefixes (zone 0x05).
const (
	SubIdempKeys    byte = 0x01
	SubIdempTimeIdx byte = 0x02
)

// Global sub-prefixes (zone 0x06), ordered by hot-path write frequency.
const (
	SubGlobLastAppliedIndex        byte = 0x01
	SubGlobLastAppliedTimestamp    byte = 0x02
	SubGlobLedgerInfo              byte = 0x03
	SubGlobSigningKey              byte = 0x04
	SubGlobSigningConfig           byte = 0x05
	SubGlobChapters                byte = 0x06
	SubGlobNextChapterID           byte = 0x07
	SubGlobSinkCursor              byte = 0x08
	SubGlobEventsConfig            byte = 0x09
	SubGlobSinkStatus              byte = 0x0A
	SubGlobMaintenanceMode         byte = 0x0B
	SubGlobPersistedConfig         byte = 0x0C
	SubGlobChapterSchedule         byte = 0x0D
	SubGlobQueryCheckpoint         byte = 0x0E
	SubGlobNextQueryCheckpointID   byte = 0x0F
	SubGlobQueryCheckpointSchedule byte = 0x10
	SubGlobClusterConfig           byte = 0x11
	SubGlobBloom                   byte = 0x12
	SubGlobNextLedgerID            byte = 0x13
	// SubGlobPeers stores Raft cluster membership (one entry per voter or
	// learner): [ZoneGlobal][SubGlobPeers][node_id BE 8] → raftcmdpb.PeerAddress.
	// Mutations are driven by the Raft ConfChange apply path; the node
	// reloads from this prefix at boot so the bootstrap voter and every
	// other peer survive restarts without relying on the WAL snapshot
	// payload (EN-1413).
	SubGlobPeers byte = 0x14
	// SubGlobRemovedMembers stores the removed-member registry (EN-1045):
	// [ZoneGlobal][SubGlobRemovedMembers][node_id BE 8][instance_id 16] →
	// raftcmdpb.RemovedMemberEntry. Written atomically with the peer row
	// delete on ConfChangeRemoveNode (consensus and force paths), read by
	// JoinAsLearner admission and checkAndPromoteLearners to prevent a
	// still-alive removed pod from rejoining and being auto-promoted. See
	// docs/technical/architecture/subsystems/consensus/removed-member-registry.md.
	SubGlobRemovedMembers byte = 0x15
)

// ClusterTransient sub-prefixes (zone 0x07).
const (
	// SubTransientBackupJob holds active backup jobs keyed by
	// destination_key. The destination_key is canonicalised so two
	// requests targeting byte-equal destinations land on the same slot
	// (mutual exclusion).
	// [ZoneClusterTransient][SubTransientBackupJob][destination_key] → BackupJob proto.
	SubTransientBackupJob byte = 0x01
	// SubTransientBackupJobHistory keeps terminal backup records
	// (Complete / Fail). Sorted by completion applied-index so callers
	// can read the most recent N entries in O(1) seek; the destination
	// key suffix breaks ties when two jobs terminate at the same applied
	// index.
	// [ZoneClusterTransient][SubTransientBackupJobHistory][completed_at_index BE 8][destination_key] → BackupJob proto.
	SubTransientBackupJobHistory byte = 0x02
)

// ---------------------------------------------------------------------------
// Legacy constants — kept as aliases for backward compatibility in call sites.
// New code should use Zone* / SubAttr* / SubCache* directly.
// ---------------------------------------------------------------------------

// Canonical key separators used inside attribute canonical keys
// to delimit volume and metadata sub-keys.
// These must be BELOW all valid address-character bytes (lowest is '0' = 0x30)
// so that Pebble key order matches lexicographic address order.
const (
	CanonicalKeySepVolume      byte = 0x00
	CanonicalKeySepMetadata    byte = 0x01
	CanonicalKeySepTransaction byte = 0x02
)

// LedgerNameFixedSize is the fixed-width (zero-padded) block reserved for the
// ledger name in every ledger-scoped canonical key. Fixed width lets the
// Pebble Comparer split keys at a constant offset (bloom prefix, range
// bounds, ImmediateSuccessor) without parsing a length prefix on every
// comparison. Callers MUST validate the upstream name length against this
// limit to avoid silent truncation collisions.
//
// The value tracks invariants.LedgerNameMaxLength: the storage layout
// reserves exactly the maximum length imposed by the Formance-wide
// ledger-name invariant. Keep both numbers in lockstep — never let storage
// allow more bytes than admission accepts.
const LedgerNameFixedSize = invariants.LedgerNameMaxLength

// MaxUint64Bytes is the big-endian representation of math.MaxUint64,
// used as an upper bound sentinel for sequence-keyed iterations.
var MaxUint64Bytes = []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}

var (
	// ColdSequencePrefixes lists cold-storable zone+sub pairs keyed by sequence number.
	// These support efficient range scan and range delete by [zone][sub][startSeq]..[zone][sub][endSeq].
	ColdSequencePrefixes = [][2]byte{
		{ZoneCold, SubColdLog},
		{ZoneCold, SubColdAudit},
		{ZoneCold, SubColdAuditItem},
		{ZoneCold, SubColdAppliedProposal},
	}
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
		Logger:             NewPebbleLogger(logger),
		FormatMajorVersion: pebble.FormatNewest,
		EventListener:      NewMetricsListener(meter, stallState),
		// 1) Absorb more writes before flush => fewer SST files, fewer compactions.
		MemTableSize:                cfg.MemTableSize,
		MemTableStopWritesThreshold: cfg.MemTableStopWritesThreshold,

		// 2) Control L0 pressure (main source of compactions/churn in write-heavy workloads).
		L0CompactionThreshold: cfg.L0CompactionThreshold,
		L0StopWritesThreshold: cfg.L0StopWritesThreshold,
		LBaseMaxBytes:         cfg.LBaseMaxBytes,
		Cache:                 pebble.NewCache(cfg.CacheSize),

		// 3) Table sizes and compression (per-level, configurable).
		TargetFileSizes: cfg.BuildTargetFileSizes(),
		Levels:          cfg.BuildLevels(),

		// 4) Smooth IO during flush/compactions.
		BytesPerSync:    cfg.BytesPerSync,
		WALBytesPerSync: cfg.WALBytesPerSync,

		// 5) Compaction concurrency: OK but not too high (otherwise you saturate IO).
		CompactionConcurrencyRange: func() (int, int) {
			n := cfg.MaxConcurrentCompactions

			return n, n
		},

		// 6) WAL configuration
		WALMinSyncInterval: func() time.Duration { return cfg.WALMinSyncInterval },
		DisableWAL:         cfg.DisableWAL,
	}

	// 7) WAL failover: automatically switch WAL writes to a secondary directory
	// when the primary disk has high latency. Pebble monitors latency and
	// switches back when the primary is healthy again.
	if cfg.WALFailoverDir != "" {
		if err := os.MkdirAll(cfg.WALFailoverDir, 0o750); err != nil {
			return nil, fmt.Errorf("creating WAL failover directory: %w", err)
		}

		opts.WALFailover = &pebble.WALFailoverOptions{
			Secondary: wal.Dir{
				FS:      vfs.Default,
				Dirname: cfg.WALFailoverDir,
			},
			// Use Pebble defaults for all thresholds:
			// - UnhealthyOperationLatencyThreshold: 100ms
			// - PrimaryDirProbeInterval: 1s
			// - HealthyProbeLatencyThreshold: 25ms
			// - HealthyInterval: 15s
			// - ElevatedWriteStallThresholdLag: 60s
		}
	}

	// 8) VFS wrapper for IOPS counting.
	iopsCounters := &IOPSCounters{}
	opts.FS = NewMetricsFS(vfs.Default, iopsCounters)

	// 9) Enable columnar blocks (required for value separation, also improves scans).
	opts.Experimental.EnableColumnarBlocks = func() bool { return true }

	// 10) Value separation: store large values in blob files to reduce compaction IO.
	if cfg.ValueSeparation.Enabled {
		vs := cfg.ValueSeparation
		opts.Experimental.ValueSeparationPolicy = func() pebble.ValueSeparationPolicy {
			return pebble.ValueSeparationPolicy{
				Enabled:               true,
				MinimumSize:           vs.MinimumSize,
				MaxBlobReferenceDepth: vs.MaxBlobReferenceDepth,
				RewriteMinimumAge:     vs.RewriteMinimumAge,
				TargetGarbageRatio:    vs.TargetGarbageRatio,
			}
		}
	}

	var (
		db  *pebble.DB
		err error
	)

	liveDir := filepath.Join(dataDir, liveDir)

	// Reconcile any leftover live.discard/ from a crashed RestoreCheckpoint.
	// This runs before any other stat on live/ so we observe the recovered
	// state rather than the mid-restore state.
	if err := reconcileLiveAfterRestore(dataDir, logger); err != nil {
		return nil, fmt.Errorf("reconciling live directory after restore: %w", err)
	}

	// With incremental 0xFF cache persistence, the live/ directory is always
	// up-to-date after each Pebble batch commit. On restart we open it directly
	// — no checkpoint hard-linking needed. Checkpoints are only used for
	// follower sync (SynchronizeWithLeader) and as a safety fallback.
	//
	// Special case: after a restore/bootstrap, live/ does not exist but a
	// checkpoint does. In that case, hard-link the checkpoint to live/.
	_, liveDirErr := os.Stat(liveDir)

	// Scan the checkpoints directory to find the latest checkpoint ID.
	// The ID is derived from the highest-numbered directory in checkpoints/.
	latestCheckpointID, hasCheckpoint, err := ScanLatestCheckpointID(dataDir)
	if err != nil {
		return nil, fmt.Errorf("scanning checkpoints: %w", err)
	}

	if liveDirErr != nil {
		// live/ does not exist. Check if a checkpoint exists (restore/bootstrap path).
		// Gate on hasCheckpoint, not latestCheckpointID > 0: a restore writes the
		// base checkpoint as ID 0, which must still be adopted here.
		if hasCheckpoint {
			checkpointPath := filepath.Join(dataDir, checkpointsDir, strconv.FormatUint(latestCheckpointID, 10))

			logger.Infof("No live directory found, restoring from checkpoint %d", latestCheckpointID)

			if err = HardLink(checkpointPath, liveDir); err != nil {
				return nil, fmt.Errorf("hard linking checkpoint to live directory: %w", err)
			}
		} else {
			logger.Infof("No live directory found, creating new database in %s", liveDir)
		}
	} else {
		logger.Infof("Opening existing database from %s", liveDir)
	}

	openStart := time.Now()

	db, err = pebble.Open(liveDir, opts)
	if err != nil {
		return nil, fmt.Errorf("opening pebble database: %w", err)
	}

	m := db.Metrics()
	logger.WithFields(map[string]any{
		"duration":          time.Since(openStart).String(),
		"l0FileCount":       m.Levels[0].TablesCount,
		"l0Size":            m.Levels[0].TablesSize,
		"l1FileCount":       m.Levels[1].TablesCount,
		"l1Size":            m.Levels[1].TablesSize,
		"memTableCount":     m.MemTable.Count,
		"memTableSize":      m.MemTable.Size,
		"compactionCount":   m.Compact.Count,
		"compactionDebt":    m.Compact.InProgressBytes,
		"compactionEstDebt": m.Compact.EstimatedDebt,
		"walFilesCount":     m.WAL.Files,
		"walSize":           m.WAL.Size,
		"totalLevelsSize":   m.DiskSpaceUsage(),
	}).Infof("Pebble database opened — LSM state")

	// Calculate the oldest checkpoint that should exist
	// based on latest checkpoint and max checkpoints configuration
	var oldestCheckpoint uint64
	if latestCheckpointID >= uint64(cfg.MaxCheckpoints) {
		oldestCheckpoint = latestCheckpointID - uint64(cfg.MaxCheckpoints) + 1
	}

	store := &Store{
		opts:              opts,
		logger:            logger.WithField("cmp", "pebble"),
		dataDir:           dataDir,
		currentCheckPoint: latestCheckpointID,
		oldestCheckpoint:  oldestCheckpoint,
		maxCheckpoints:    cfg.MaxCheckpoints,
		stallState:        stallState,
		iopsCounters:      iopsCounters,
	}

	if _, err = iopsCounters.RegisterMetrics(meter); err != nil {
		_ = db.Close()

		return nil, fmt.Errorf("registering IOPS metrics: %w", err)
	}
	store.db = db

	// Clean up any orphaned backup checkpoints from a previous crash
	store.cleanupTemporaryCheckpoints()

	// Clean up any partial incoming checkpoint left by a crash during follower sync
	store.cleanupIncomingRestore()

	return store, nil
}

// DataDir returns the base data directory path for this store.
func (s *Store) DataDir() string {
	return s.dataDir
}

// Flush forces a flush of Pebble's memtables to SSTs on disk.
func (s *Store) Flush() error {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()

	db := s.getDB()
	if db == nil {
		return ErrStoreClosed
	}

	return db.Flush()
}

// SyncWAL forces an fsync of Pebble's WAL. After this call returns, every
// batch.Commit(NoSync) issued before SyncWAL was invoked is durable on disk.
// It does not flush memtables to SSTs — strictly a WAL fsync.
//
// Used by node.doMaintenance to establish the durability invariant
// "WAL snapshot index <= durable Pebble applied index" before creating a
// Raft WAL snapshot or compacting the Raft WAL. Without it, a power loss
// could leave the WAL snapshot referencing entries that were only in
// Pebble's unsynced memtable, and a subsequent Compact could erase those
// entries from the WAL too — making them unrecoverable from any source.
func (s *Store) SyncWAL() error {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()

	db := s.getDB()
	if db == nil {
		return ErrStoreClosed
	}

	return db.LogData(nil, &pebble.WriteOptions{Sync: true})
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
		{ZonePerLedger, ZonePerLedger + 1}, // zone 0x03 (per-ledger system keys)
		{ZoneGlobal, ZoneGlobal + 1},       // zone 0x06 (global system keys)
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

// WarmBlockCache iterates the attributes zone [ZoneAttributes, ZoneAttributes+1) to preload
// Pebble's block cache. This is the heavyweight warmup (volumes, metadata,
// etc.) and should run in the background after servers are listening.
func (s *Store) WarmBlockCache() {
	start := time.Now()

	s.dbMu.RLock()
	db := s.getDB()

	keys, err := s.warmRange(db, ZoneAttributes, ZoneAttributes+1)
	s.dbMu.RUnlock()
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

// closeDBSafe closes a Pebble DB, recovering from panics.
// In Pebble v2.1.x, DB.Close can panic with "element has outstanding
// references" in genericcache/shard.Close. The root cause is a race in
// Pebble's internal collectTableStats goroutine: it uses
// context.Background() (table_stats.go:96) and can hold FileCache refs
// after DB.Close releases d.mu and before it calls fileCache.Close().
// Fixed upstream (master only, not released in any v2.1.x tag as of v2.1.6):
//   - https://github.com/cockroachdb/pebble/pull/5813
//   - https://github.com/cockroachdb/pebble/pull/5854
//
// Recovering here is safe because the old data is being replaced.
func closeDBSafe(db *pebble.DB) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic during DB close (recovered): %v", r)
		}
	}()

	return db.Close()
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
// snapshotMu serializes concurrent calls (FSM triggerSnapshot vs gRPC CreateCheckpoint).
// dbMu is held as RLock only — checkpoint creation doesn't need to exclude readers.
func (s *Store) CreateSnapshot() (uint64, error) {
	s.snapshotMu.Lock()
	defer s.snapshotMu.Unlock()

	s.dbMu.RLock()
	defer s.dbMu.RUnlock()

	db := s.getDB()
	if db == nil {
		return 0, ErrStoreClosed
	}

	snapshotStart := time.Now()

	s.logger.Infof("Creating snapshot")

	newCheckpointID := s.currentCheckPoint + 1

	checkpointDir := filepath.Join(s.dataDir, "checkpoints", strconv.FormatUint(newCheckpointID, 10))
	if err := os.RemoveAll(checkpointDir); err != nil {
		return 0, fmt.Errorf("removing checkpoint directory: %w", err)
	}

	removeOldDone := time.Now()

	if err := db.Checkpoint(checkpointDir, pebble.WithFlushedWAL()); err != nil {
		return 0, fmt.Errorf("creating checkpoint: %w", err)
	}

	pebbleCheckpointDone := time.Now()

	// Clean up old checkpoints beyond the configured maximum
	// Note: it can fail, leaving old checkpoints on disk
	// this is not critical, but we should fix it eventually
	if err := s.cleanupOldCheckpoints(); err != nil {
		s.logger.WithFields(map[string]any{"error": err}).Infof("Failed to cleanup old checkpoints")
	}

	s.logger.WithFields(map[string]any{
		"checkpoint":       newCheckpointID,
		"total":            time.Since(snapshotStart).String(),
		"removeOld":        removeOldDone.Sub(snapshotStart).String(),
		"pebbleCheckpoint": pebbleCheckpointDone.Sub(removeOldDone).String(),
		"cleanup":          time.Since(pebbleCheckpointDone).String(),
	}).Infof("Snapshot created")
	s.currentCheckPoint = newCheckpointID

	return newCheckpointID, nil
}

// cleanupOldCheckpoints removes every checkpoint dir whose ID falls below
// (currentCheckPoint + 1) - maxCheckpoints + 1. It scans the checkpoints
// directory on disk rather than iterating from an in-memory tracker so it
// also reclaims fossil checkpoints left by a previous crash that fall
// outside the running tracker's window (see EN-1409). Each non-graceful
// exit otherwise preserves the live checkpoint set as untouchable orphans,
// permanently leaking `maxCheckpoints * checkpoint_size` per crash.
//
// Non-numeric children are skipped defensively (no producer writes them
// after the layout split that moved follower-sync staging out of
// checkpointsDir); only numeric dirs are considered checkpoints.
func (s *Store) cleanupOldCheckpoints() error {
	newCheckpoint := s.currentCheckPoint + 1

	// If we haven't reached maxCheckpoints yet there's nothing to keep
	// below 0; skip the scan.
	if newCheckpoint < uint64(s.maxCheckpoints) {
		return nil
	}

	oldestToKeep := newCheckpoint - uint64(s.maxCheckpoints) + 1
	checkpointsPath := filepath.Join(s.dataDir, checkpointsDir)

	// cleanupOldCheckpoints runs only after CreateSnapshot's db.Checkpoint()
	// has succeeded, which (re)creates the checkpoints/ parent — so ENOENT
	// here would signal filesystem tampering or a broken invariant, not a
	// benign empty state. Per CLAUDE.md invariant #7, surface it loudly.
	entries, err := os.ReadDir(checkpointsPath)
	if err != nil {
		return fmt.Errorf("listing checkpoints directory: %w", err)
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		id, err := strconv.ParseUint(entry.Name(), 10, 64)
		if err != nil {
			continue // defensive: see ScanLatestCheckpointID
		}

		if id >= oldestToKeep {
			continue
		}

		if err := os.RemoveAll(filepath.Join(checkpointsPath, entry.Name())); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("removing old checkpoint %d: %w", id, err)
		}
	}

	// Tracker is informational only after this change, but keep it
	// consistent with the new floor so external readers don't drift.
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
// Unlike CreateSnapshot, this does not modify currentCheckPoint,
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

	s.dbMu.RLock()
	defer s.dbMu.RUnlock()

	db := s.getDB()
	if db == nil {
		return "", ErrStoreClosed
	}

	if err := db.Checkpoint(path, pebble.WithFlushedWAL()); err != nil {
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

// queryCheckpointsDir is the directory where query checkpoint Pebble snapshots are stored.
const queryCheckpointsDir = "query-checkpoints"

// CreateQueryCheckpoint creates a Pebble checkpoint for query purposes at
// {dataDir}/query-checkpoints/{id}/main/. These are self-contained snapshots
// created by the FSM when a CreateQueryCheckpointOrder is applied via Raft.
func (s *Store) CreateQueryCheckpoint(id uint64) (string, error) {
	dir := filepath.Join(s.dataDir, queryCheckpointsDir, strconv.FormatUint(id, 10), "main")

	if err := os.MkdirAll(filepath.Dir(dir), 0755); err != nil {
		return "", fmt.Errorf("creating query checkpoint directory: %w", err)
	}

	s.dbMu.RLock()
	defer s.dbMu.RUnlock()

	db := s.getDB()
	if db == nil {
		return "", ErrStoreClosed
	}

	if err := db.Checkpoint(dir, pebble.WithFlushedWAL()); err != nil {
		return "", fmt.Errorf("creating query checkpoint: %w", err)
	}

	return dir, nil
}

// DeleteQueryCheckpointFiles removes the physical checkpoint files for a query checkpoint.
func (s *Store) DeleteQueryCheckpointFiles(id uint64) error {
	dir := filepath.Join(s.dataDir, queryCheckpointsDir, strconv.FormatUint(id, 10))

	return os.RemoveAll(dir)
}

// QueryCheckpointReadIndexDir returns the path for the read index within a query checkpoint.
func (s *Store) QueryCheckpointReadIndexDir(id uint64) string {
	return filepath.Join(s.dataDir, queryCheckpointsDir, strconv.FormatUint(id, 10), "readindex")
}

// QueryCheckpointMainDir returns the path for the main store within a query checkpoint.
func (s *Store) QueryCheckpointMainDir(id uint64) string {
	return filepath.Join(s.dataDir, queryCheckpointsDir, strconv.FormatUint(id, 10), "main")
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

// cleanupIncomingRestore removes the incoming restore directory on startup.
// A partial incoming checkpoint can be left if the node crashed during follower sync
// (between PrepareIncomingRestore and RestoreCheckpoint). It is safe to delete.
//
// Also drops the legacy path (dataDir/checkpoints/incoming) used by older
// builds that placed staging inside checkpointsDir; migration is one-way
// because the dir is always volatile staging.
func (s *Store) cleanupIncomingRestore() {
	paths := []string{
		filepath.Join(s.dataDir, incomingCheckpointDir),
		filepath.Join(s.dataDir, checkpointsDir, "incoming"),
	}

	for _, path := range paths {
		if err := os.RemoveAll(path); err != nil {
			s.logger.WithFields(map[string]any{
				"path":  path,
				"error": err,
			}).Errorf("Failed to clean up incoming restore directory")
		}
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

// PrepareIncomingRestore creates a staging directory for an incoming checkpoint
// from a leader. The directory lives outside checkpointsDir so that
// checkpointsDir holds only numbered checkpoints and the staging path cannot
// collide with the numbered checkpoints created by CreateSnapshot or the
// background checkpoint goroutine.
func (s *Store) PrepareIncomingRestore() (string, error) {
	dir := filepath.Join(s.dataDir, incomingCheckpointDir)

	if err := os.RemoveAll(dir); err != nil {
		return "", fmt.Errorf("removing existing incoming directory: %w", err)
	}

	if err := os.MkdirAll(dir, 0755); err != nil {
		return "", fmt.Errorf("creating incoming directory: %w", err)
	}

	return dir, nil
}

// ActivateIncomingRestore moves the incoming checkpoint to a numbered slot
// and returns the assigned checkpoint ID. It holds snapshotMu briefly to
// reserve the ID, preventing collision with the background checkpoint goroutine.
func (s *Store) ActivateIncomingRestore() (uint64, error) {
	s.snapshotMu.Lock()
	defer s.snapshotMu.Unlock()

	newID := s.currentCheckPoint + 1
	incomingDir := filepath.Join(s.dataDir, incomingCheckpointDir)
	targetParent := filepath.Join(s.dataDir, checkpointsDir)
	targetDir := filepath.Join(targetParent, strconv.FormatUint(newID, 10))

	// Ensure the checkpoints parent exists. Before the staging-dir split it
	// was created as a side effect of PrepareIncomingRestore; now staging
	// lives elsewhere and the parent may not exist on a freshly initialised
	// follower whose first checkpoint comes from the leader.
	if err := os.MkdirAll(targetParent, 0755); err != nil {
		return 0, fmt.Errorf("creating checkpoints directory: %w", err)
	}

	if err := os.RemoveAll(targetDir); err != nil {
		return 0, fmt.Errorf("removing target checkpoint directory: %w", err)
	}

	if err := os.Rename(incomingDir, targetDir); err != nil {
		return 0, fmt.Errorf("moving incoming checkpoint to %d: %w", newID, err)
	}

	// Reserve the ID so the background goroutine skips it.
	s.currentCheckPoint = newID

	return newID, nil
}

// reconcileLiveAfterRestore observes the live/, live.staging/ and
// live.discard/ directories at boot and finishes any half-completed
// RestoreCheckpoint swap.
//
// The restore protocol publishes the new database by atomically renaming
// `live.staging/` -> `live/` after every post-open step has succeeded.
// That rename is the single commit point — so the presence of `live/`
// after a crash is proof that the restore committed (or that no restore
// was ever in flight). `live.staging/` is always ephemeral: it represents
// in-progress build state and can be discarded without losing anything
// that wasn't already in the source checkpoint.
//
// Reconciliation rules:
//
//   - `live/` present → keep it. Drop any leftover staging/discard,
//     they are both stale at this point.
//   - `live/` missing + `live.discard/` present → restore was aborted
//     before the publish rename. Drop staging if present (incomplete
//     build) and rename `live.discard/` back to `live/`.
//   - `live/` missing + no discard → no rollback target. Drop staging
//     if present (incomplete build, no preserved original to recover);
//     NewStore will then take its normal "no live directory" path.
//
// The function is a no-op in the common cases. It is safe to call on
// every boot.
func reconcileLiveAfterRestore(dataDir string, logger logging.Logger) error {
	liveDirectory := filepath.Join(dataDir, liveDir)
	stagingDirectory := filepath.Join(dataDir, liveStagingDir)
	discardDirectory := filepath.Join(dataDir, liveDiscardDir)

	_, liveErr := os.Stat(liveDirectory)
	_, stagingErr := os.Stat(stagingDirectory)
	_, discardErr := os.Stat(discardDirectory)

	liveExists := liveErr == nil
	stagingExists := stagingErr == nil
	discardExists := discardErr == nil

	// Nothing to reconcile: no in-flight or aborted restore left a trace.
	if !stagingExists && !discardExists {
		return nil
	}

	if liveExists {
		// `live/` is durable proof the restore committed (or never ran).
		// Any staging or discard left next to it is stale.
		if stagingExists {
			logger.WithFields(map[string]any{
				"liveDir":    liveDirectory,
				"stagingDir": stagingDirectory,
			}).Infof("Dropping stale live.staging next to live/ (previous RestoreCheckpoint either crashed pre-publish or left this around)")

			if err := os.RemoveAll(stagingDirectory); err != nil {
				return fmt.Errorf("removing stale live.staging directory: %w", err)
			}
		}

		if discardExists {
			logger.WithFields(map[string]any{
				"liveDir":    liveDirectory,
				"discardDir": discardDirectory,
			}).Infof("Cleaning up live.discard from a previously successful RestoreCheckpoint")

			if err := os.RemoveAll(discardDirectory); err != nil {
				return fmt.Errorf("removing leftover live.discard directory: %w", err)
			}
		}

		return nil
	}

	// live/ is missing. Drop any in-progress staging — it is incomplete
	// by definition (the publish rename would have replaced it with live/).
	if stagingExists {
		logger.WithFields(map[string]any{
			"stagingDir": stagingDirectory,
		}).Infof("Dropping incomplete live.staging from an aborted RestoreCheckpoint (live/ missing means the publish never happened)")

		if err := os.RemoveAll(stagingDirectory); err != nil {
			return fmt.Errorf("removing incomplete live.staging directory: %w", err)
		}
	}

	if discardExists {
		// Revert: rename(discard, live) restores the pre-restore state.
		logger.WithFields(map[string]any{
			"liveDir":    liveDirectory,
			"discardDir": discardDirectory,
		}).Infof("WARNING: detected aborted RestoreCheckpoint (live missing, live.discard present); reverting to pre-restore live")

		if err := os.Rename(discardDirectory, liveDirectory); err != nil {
			return fmt.Errorf("reverting live.discard to live: %w", err)
		}
	}

	return nil
}

// RestoreCheckpoint restores the database from a checkpoint using a
// staging directory + atomic publish so that any failure leaves the
// original live directory recoverable AND so that a crash anywhere
// during the build is unambiguous on the next boot.
//
// Sequence on the happy path:
//
//  1. Verify the checkpoint exists.
//  2. Read the current node's persisted-config from the live DB.
//  3. Close the live DB and rename live/ -> live.discard/ (atomic, O(1)).
//  4. Hard-link the checkpoint into a fresh live.staging/.
//  5. Open live.staging/ as a Pebble DB.
//  6. Re-write the preserved config, flush, and re-checkpoint.
//  7. rename live.staging/ -> live/. THIS is the atomic commit point.
//  8. Remove live.discard/.
//
// The split between steps 3-6 (build under staging) and step 7 (publish)
// is what makes crash recovery unambiguous: until step 7 succeeds,
// `live/` does not exist, so seeing `live/` on the next boot is proof
// the restore committed.
//
// If any of steps 4-6 fails the rollback path runs:
//
//   - close the staging DB if it was opened,
//   - remove live.staging/,
//   - rename live.discard/ back to live/ (atomic),
//   - reopen the original DB.
//
// Holds dbMu exclusively so concurrent NewIter/IterateColdKVPairs calls
// block until the new DB is ready.
//
// Crash recovery: if the process dies anywhere between steps 3 and 7,
// `live/` is missing and `live.discard/` (plus maybe an incomplete
// `live.staging/`) is on disk. NewStore detects it at boot (see
// reconcileLiveAfterRestore) and reverts to the pre-restore live. If
// the process dies between steps 7 and 8, `live/` is present and stale
// `live.discard/` is on disk; boot drops the discard.
func (s *Store) RestoreCheckpoint(checkpointID uint64) error {
	s.dbMu.Lock()
	defer s.dbMu.Unlock()

	checkpointDir := filepath.Join(s.dataDir, checkpointsDir, strconv.FormatUint(checkpointID, 10))

	// Verify the checkpoint exists
	if _, err := os.Stat(checkpointDir); err != nil {
		return fmt.Errorf("checkpoint %d not found: %w", checkpointID, err)
	}

	liveDirectory := filepath.Join(s.dataDir, liveDir)
	stagingDirectory := filepath.Join(s.dataDir, liveStagingDir)
	discardDirectory := filepath.Join(s.dataDir, liveDiscardDir)

	// Stale live.staging/ or live.discard/ from an earlier aborted attempt
	// must not be inherited. If they represented recovery state, boot
	// would have already consumed them via reconcileLiveAfterRestore.
	if err := os.RemoveAll(stagingDirectory); err != nil {
		return fmt.Errorf("clearing stale live.staging directory: %w", err)
	}

	if err := os.RemoveAll(discardDirectory); err != nil {
		return fmt.Errorf("clearing stale live.discard directory: %w", err)
	}

	// Step 2: preserve this node's persisted-config from the live DB.
	// The checkpoint originates from the leader and contains the leader's
	// node-id; we must re-write our own identity into the new live so
	// startup config validation passes on the next boot.
	//
	// The DB may already be closed from a previous failed RestoreCheckpoint
	// (e.g. the rollback path below failed to reopen). In that case skip
	// config preservation — the next caller (or the operator) can sort it
	// out from outside.
	oldDB := s.db

	var preservedConfig []byte

	if oldDB != nil {
		if value, closer, getErr := oldDB.Get([]byte{ZoneGlobal, SubGlobPersistedConfig}); getErr == nil {
			preservedConfig = append([]byte(nil), value...)
			_ = closer.Close()
		}

		if closeErr := closeDBSafe(oldDB); closeErr != nil {
			// Pebble v2.1.x can panic here due to an internal race in
			// collectTableStats (see closeDBSafe). Log and continue — the
			// old data is stale and being replaced. The rename in step 3
			// still works regardless of close cleanliness.
			s.logger.WithFields(map[string]any{
				"error": closeErr,
			}).Errorf("Error closing old database during checkpoint restore (continuing)")
		}

		s.db = nil
	}

	// Step 3: rename live/ -> live.discard/. This is atomic on POSIX
	// (single inode-table update). If we crash here, boot recovery sees
	// live missing + live.discard present and reverts.
	if _, err := os.Stat(liveDirectory); err == nil {
		if err := os.Rename(liveDirectory, discardDirectory); err != nil {
			return fmt.Errorf("renaming live directory aside: %w", err)
		}
	}
	// If liveDirectory didn't exist (e.g. a previous failed restore left
	// it absent), we have nothing to roll back to. Continue: the worst
	// case is no rollback target, same as the old destructive path.

	// From this point on, any failure must drop the staging tree and
	// roll back to live.discard/ before propagating the error.
	rollback := func(reason error) error {
		// Close the staging DB if it managed to open before failing.
		if s.db != nil {
			if closeErr := closeDBSafe(s.db); closeErr != nil {
				s.logger.WithFields(map[string]any{
					"error": closeErr,
				}).Errorf("Error closing partial staging DB during rollback (continuing)")
			}

			s.db = nil
		}

		// Drop the incomplete staging tree. The hard-linked SSTs remain
		// in the checkpoint directory; removing the staging tree only
		// drops the link.
		if err := os.RemoveAll(stagingDirectory); err != nil {
			s.logger.WithFields(map[string]any{
				"error": err,
			}).Errorf("Removing partial live.staging directory during rollback (continuing)")
		}

		// Revert live.discard/ -> live/. Atomic.
		if _, statErr := os.Stat(discardDirectory); statErr == nil {
			if err := os.Rename(discardDirectory, liveDirectory); err != nil {
				return fmt.Errorf("rollback failed (could not restore live from live.discard): %w; original error: %w", err, reason)
			}
		}

		// Reopen the original DB so the Store stays usable.
		// FileCache must be cleared for the same reason as below.
		s.opts.FileCache = nil

		revivedDB, openErr := pebble.Open(liveDirectory, s.opts)
		if openErr != nil {
			return fmt.Errorf("rollback failed (could not reopen original live): %w; original error: %w", openErr, reason)
		}

		s.db = revivedDB

		return reason
	}

	// Step 4: hard-link the checkpoint into a fresh live.staging/.
	if err := HardLink(checkpointDir, stagingDirectory); err != nil {
		return rollback(fmt.Errorf("hard linking checkpoint to live.staging directory: %w", err))
	}

	// Step 5: open the staged database. Clear FileCache so pebble.Open
	// creates a fresh one. The first Open mutates opts.FileCache in-place;
	// reusing the stale pointer causes a panic in shard.Close ("element
	// has outstanding references") because the old FileCache was already
	// closed when the old DB was closed.
	s.opts.FileCache = nil

	newDB, err := pebble.Open(stagingDirectory, s.opts)
	if err != nil {
		return rollback(fmt.Errorf("opening staged database: %w", err))
	}

	s.db = newDB

	// Step 6: re-write this node's persisted config and re-checkpoint
	// (still inside the staging directory).
	if preservedConfig != nil {
		if err := newDB.Set([]byte{ZoneGlobal, SubGlobPersistedConfig}, preservedConfig, pebble.Sync); err != nil {
			return rollback(fmt.Errorf("re-writing persisted config after checkpoint restore: %w", err))
		}

		if err := newDB.Flush(); err != nil {
			return rollback(fmt.Errorf("flushing persisted config after checkpoint restore: %w", err))
		}

		if err := os.RemoveAll(checkpointDir); err != nil {
			return rollback(fmt.Errorf("removing old checkpoint dir for re-checkpoint: %w", err))
		}

		if err := newDB.Checkpoint(checkpointDir); err != nil {
			return rollback(fmt.Errorf("re-creating checkpoint with preserved config: %w", err))
		}
	}

	// Step 7: PUBLISH. The atomic rename is the single commit point of
	// the restore. The DB has been opened on stagingDirectory; once we
	// rename the directory out from under it, subsequent file operations
	// inside pebble (compactions, flushes) will follow the inode they
	// already hold open, but new opens must use the new path. So we
	// close the staging DB, do the rename, then reopen on liveDirectory.
	//
	// This double-open is the price of using rename as the commit
	// point — it is cheap (warm OS cache, no compactions) and only
	// happens once per restore.
	if closeErr := closeDBSafe(newDB); closeErr != nil {
		// Same Pebble v2.1.x caveat as above: log and continue. The on-
		// disk state is durable (Flush + Checkpoint above already synced).
		s.logger.WithFields(map[string]any{
			"error": closeErr,
		}).Errorf("Error closing staging DB before publish rename (continuing)")
	}

	s.db = nil

	if err := os.Rename(stagingDirectory, liveDirectory); err != nil {
		return rollback(fmt.Errorf("publishing live.staging to live: %w", err))
	}

	// Restore committed — past this point any failure is non-fatal for
	// correctness; the worst case is a stale live.discard/ that boot
	// will sweep up.

	s.opts.FileCache = nil

	publishedDB, err := pebble.Open(liveDirectory, s.opts)
	if err != nil {
		// We renamed staging to live so the new state is durable, but
		// we cannot reopen it in-process. Surface the error; the next
		// boot will open live/ normally.
		return fmt.Errorf("reopening published live directory after restore: %w", err)
	}

	s.db = publishedDB

	// Step 8: drop the rollback target.
	if err := os.RemoveAll(discardDirectory); err != nil {
		// Cleanup failure post-success is non-fatal — the next call (or
		// boot reconciliation) will sweep it up. Log and continue.
		s.logger.WithFields(map[string]any{
			"error": err,
		}).Errorf("Removing live.discard directory after successful restore (will be cleaned up on next boot)")
	}

	// Update internal state
	s.currentCheckPoint = checkpointID
	s.oldestCheckpoint = checkpointID

	// Bump the restore generation so audit-chain pollers detect this rollback
	// regardless of where the audit head lands relative to their cursor. Done
	// under dbMu.Lock (still held via the deferred unlock) so it is ordered
	// after the publish; the atomic lets lock-free pollers read it.
	s.restoreGeneration.Add(1)

	s.logger.WithFields(map[string]any{
		"checkpointId": checkpointID,
	}).Infof("Database restored from checkpoint")

	return nil
}

// RestoreGeneration returns the process-local count of successful
// RestoreCheckpoint calls. Audit-chain pollers (auditindexer, usagebuilder)
// snapshot this at boot and compare on every tick: a change means the primary
// store was rolled back beneath their cursor, so they must reset+rebuild even
// when the cursor>head position signal was erased by a catch-up race. See the
// restoreGeneration field comment.
func (s *Store) RestoreGeneration() uint64 {
	return s.restoreGeneration.Load()
}

// IterateColdKVPairs iterates every cold-storable KV pair belonging to a
// chapter via efficient prefixed range scans.
//
// Logs and audit entries advance on independent sequence counters, so the
// caller must supply both ranges. SubColdLog is scanned with the log range;
// SubColdAudit, SubColdAuditItem and SubColdAppliedProposal are scanned with
// the audit range (AppliedProposal sequences are 1:1 with AuditEntry on the
// success path). Mixing them (#312) drops every audit entry that happens to
// fall outside the log window — and the matching purge still removes it from
// Pebble, permanently losing the audit trail.
//
// Transaction updates are not archived: they are redundant with log entries
// which already contain all creation, revert, and metadata information.
func (s *Store) IterateColdKVPairs(logStart, logClose, auditStart, auditClose uint64, fn func(key, value []byte) error) error {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()

	db := s.getDB()
	if db == nil {
		return ErrStoreClosed
	}

	rangeFor := func(sub byte) (low, high uint64) {
		if sub == SubColdLog {
			return logStart, logClose
		}

		return auditStart, auditClose
	}

	for _, zp := range ColdSequencePrefixes {
		low, high := rangeFor(zp[1])

		// Audit range may legitimately be empty (high < low) when no audit
		// entries were produced in the chapter. Skip the scan in that case.
		if high < low {
			continue
		}

		lowerBound := NewKeyBuilder().PutZonePrefix(zp[0], zp[1]).PutUint64(low).Build()
		upperBound := NewKeyBuilder().PutZonePrefix(zp[0], zp[1]).PutUint64(high + 1).Build()

		err := iterateRawRange(db, lowerBound, upperBound, fn)
		if err != nil {
			return fmt.Errorf("iterating zone 0x%02x sub 0x%02x: %w", zp[0], zp[1], err)
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

// ReadProto reads a protobuf message from Pebble. Returns the zero value of T if not found.
func ReadProto[T proto.Message](reader PebbleGetter, key []byte) (T, error) {
	var zero T

	val, err := GetValue(reader, key)
	if err != nil {
		return zero, err
	}

	if val == nil {
		return zero, nil
	}

	msg := reflect.New(reflect.TypeOf(zero).Elem()).Interface().(T)
	if vu, ok := any(msg).(vtUnmarshaler); ok {
		if err := vu.UnmarshalVT(val); err != nil {
			return zero, err
		}
	} else if err := proto.Unmarshal(val, msg); err != nil {
		return zero, err
	}

	return msg, nil
}

// ScanZone returns a ProtoCursor over all entries in a [zone][sub] prefix range.
func ScanZone[T proto.Message](reader PebbleReader, zone, sub byte, opts ...ProtoCursorOption) (*ProtoCursor[T], error) {
	lowerBound := []byte{zone, sub}
	upperBound := []byte{zone, sub + 1}

	iter, err := NewBoundedIter(reader, lowerBound, upperBound)
	if err != nil {
		return nil, err
	}

	return NewProtoCursor[T](iter, opts...), nil
}

// CollectZone scans a [zone][sub] range and returns all proto entries as a slice.
func CollectZone[T proto.Message](reader PebbleReader, zone, sub byte) ([]T, error) {
	c, err := ScanZone[T](reader, zone, sub)
	if err != nil {
		return nil, err
	}

	return cursor.Collect[T](c)
}

// ReadLastEntry reads the last entry in a [zone][sub] prefix range using iter.Last().
// Returns the zero value of T if no entries exist.
func ReadLastEntry[T proto.Message](reader PebbleReader, zone, sub byte) (T, error) {
	var zero T

	kb := NewKeyBuilder()
	kb.PutZonePrefix(zone, sub)
	lowerBound := kb.Snapshot()
	kb.Reset()

	kb.PutZonePrefix(zone, sub).PutBytes(MaxUint64Bytes)
	upperBound := kb.Build()

	iter, err := NewBoundedIter(reader, lowerBound, upperBound)
	if err != nil {
		return zero, err
	}

	defer func() { _ = iter.Close() }()

	if !iter.Last() {
		return zero, nil
	}

	value, err := iter.ValueAndErr()
	if err != nil {
		return zero, err
	}

	msg := reflect.New(reflect.TypeOf(zero).Elem()).Interface().(T)
	if vu, ok := any(msg).(vtUnmarshaler); ok {
		if err := vu.UnmarshalVT(value); err != nil {
			return zero, err
		}
	} else if err := proto.Unmarshal(value, msg); err != nil {
		return zero, err
	}

	return msg, nil
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
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()

	db := s.getDB()
	if db == nil {
		return ErrStoreClosed
	}

	return db.Checkpoint(destDir, pebble.WithFlushedWAL())
}
