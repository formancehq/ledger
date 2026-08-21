package balancehistorystore

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/pebble/v2"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/pebblecfg"
)

// DefaultConfig returns a conservative configuration for the history peer
// store. The cache and compaction concurrency are intentionally isolated from
// the primary FSM store so history backfill cannot consume its Pebble budget.
func DefaultConfig() pebblecfg.Config {
	return pebblecfg.Config{
		MemTableSize:                32 << 20,
		MemTableStopWritesThreshold: 4,
		L0CompactionThreshold:       8,
		L0StopWritesThreshold:       24,
		LBaseMaxBytes:               256 << 20,
		CacheSize:                   64 << 20,
		TargetFileSize:              32 << 20,
		BytesPerSync:                1 << 20,
		MaxConcurrentCompactions:    1,
		Compression:                 pebblecfg.DefaultLevelCompression(),
	}
}

// Store owns one local, rebuildable balance-history database.
type Store struct {
	storeCore
	publisher
	compactor
	verifier
	viewManager
	garbageCollector
	metricsRegistrar
}

// storeCore is the single shared state behind every Store role. Role types
// embed this pointer so all mutations keep using the same locks, atomics, DB,
// generation, and failure state.
type storeCore struct {
	db     *pebble.DB
	dir    string
	logger logging.Logger

	mutationMu   sync.Mutex
	compactionMu sync.Mutex
	waitMu       sync.Mutex
	changed      chan struct{}

	leaseMu        sync.Mutex
	manifestLeases map[uint64]uint64
	runLeases      map[uint64]uint64
	preparedRuns   map[uint64]uint64
	generation     atomic.Uint64
	failure        atomic.Pointer[storeFailure]
}

type failureKind byte

const (
	failureSourceMissing failureKind = 1
	failureQuarantined   failureKind = 2
	failureRebuilding    failureKind = 3
)

type storeFailure struct {
	kind   failureKind
	detail string
}

// New opens or creates a history store under dir.
func New(dir string, logger logging.Logger, cfg pebblecfg.Config) (*Store, error) {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("creating balance history directory: %w", err)
	}

	dbPath := filepath.Join(dir, "balancehistorydb")
	cache := pebble.NewCache(cfg.CacheSize)
	defer cache.Unref()

	db, err := pebble.Open(dbPath, &pebble.Options{
		Logger:                      dal.NewPebbleLogger(logger),
		FormatMajorVersion:          pebble.FormatNewest,
		MemTableSize:                cfg.MemTableSize,
		MemTableStopWritesThreshold: cfg.MemTableStopWritesThreshold,
		L0CompactionThreshold:       cfg.L0CompactionThreshold,
		L0StopWritesThreshold:       cfg.L0StopWritesThreshold,
		LBaseMaxBytes:               cfg.LBaseMaxBytes,
		BytesPerSync:                cfg.BytesPerSync,
		CompactionConcurrencyRange: func() (int, int) {
			n := cfg.MaxConcurrentCompactions

			return n, n
		},
		Cache:           cache,
		TargetFileSizes: cfg.BuildTargetFileSizes(),
		Levels:          cfg.BuildLevels(),
	})
	if err != nil {
		return nil, fmt.Errorf("opening balance history store: %w", err)
	}

	store := &Store{storeCore: storeCore{
		db:             db,
		dir:            dir,
		logger:         logger.WithFields(map[string]any{"cmp": "balance-history-store"}),
		changed:        make(chan struct{}),
		manifestLeases: make(map[uint64]uint64),
		runLeases:      make(map[uint64]uint64),
		preparedRuns:   make(map[uint64]uint64),
	}}
	store.publisher = publisher{storeCore: &store.storeCore}
	store.garbageCollector = garbageCollector{storeCore: &store.storeCore}
	store.viewManager = viewManager{storeCore: &store.storeCore}
	store.compactor = compactor{
		compactionStreamer: &compactionStreamer{viewManager: &store.viewManager},
		garbageCollector:   &store.garbageCollector,
	}
	store.verifier = verifier{storeCore: &store.storeCore}
	store.metricsRegistrar = metricsRegistrar{storeCore: &store.storeCore}
	store.generation.Store(1)
	if err := store.loadFailure(); err != nil {
		_ = db.Close()

		return nil, err
	}
	if failure := store.failure.Load(); failure == nil || failure.kind != failureQuarantined {
		if err := store.verifyLatest(); err != nil {
			if isIntegrityError(err) {
				if quarantineErr := store.setFailureLocked(failureQuarantined, err.Error()); quarantineErr != nil {
					_ = db.Close()

					return nil, errors.Join(err, quarantineErr)
				}
			} else {
				_ = db.Close()

				return nil, err
			}
		}
	}

	return store, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) Path() string {
	return s.dir
}

// SyncWAL makes every preceding asynchronous publication, compaction, and GC
// mutation durable. Pebble documents LogData(nil, Sync) as the WAL barrier to
// use before checkpoints; mutationMu makes the barrier's prefix unambiguous to
// the builder that schedules it.
func (s *Store) SyncWAL() error {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()

	if err := s.db.LogData(nil, pebble.Sync); err != nil {
		return fmt.Errorf("syncing balance history WAL: %w", err)
	}

	return nil
}

type pebbleValueGetter interface {
	Get(key []byte) ([]byte, io.Closer, error)
}

func readManifest(reader pebbleValueGetter) (Manifest, error) {
	encodedVersion, closer, err := reader.Get(latestManifestKey())
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return initialManifest(), nil
		}

		return Manifest{}, fmt.Errorf("reading latest balance history manifest pointer: %w", err)
	}
	if len(encodedVersion) != 8 {
		_ = closer.Close()

		return Manifest{}, &ErrCorrupt{Detail: fmt.Sprintf("latest manifest pointer has %d bytes, want 8", len(encodedVersion))}
	}
	version := binary.BigEndian.Uint64(encodedVersion)
	if err := closer.Close(); err != nil {
		return Manifest{}, fmt.Errorf("closing latest manifest pointer: %w", err)
	}

	encoded, closer, err := reader.Get(manifestKey(version))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return Manifest{}, &ErrCorrupt{Detail: fmt.Sprintf("manifest version %d is missing", version)}
		}

		return Manifest{}, fmt.Errorf("reading balance history manifest %d: %w", version, err)
	}
	copyEncoded := append([]byte(nil), encoded...)
	if err := closer.Close(); err != nil {
		return Manifest{}, fmt.Errorf("closing balance history manifest %d: %w", version, err)
	}

	manifest, err := decodeManifest(copyEncoded)
	if err != nil {
		return Manifest{}, err
	}
	if manifest.Version != version {
		return Manifest{}, &ErrCorrupt{Detail: fmt.Sprintf("manifest pointer is %d but payload is %d", version, manifest.Version)}
	}
	if err := verifyManifestStructure(manifest); err != nil {
		return Manifest{}, err
	}

	return manifest, nil
}

// Manifest returns the latest published immutable descriptor.
func (s *storeCore) Manifest() (Manifest, error) {
	if err := s.ensureNotQuarantined(); err != nil {
		return Manifest{}, err
	}
	snapshot := s.db.NewSnapshot()
	manifest, err := readManifest(snapshot)
	if closeErr := snapshot.Close(); closeErr != nil {
		err = errors.Join(err, fmt.Errorf("closing balance history manifest snapshot: %w", closeErr))
	}

	return manifest, err
}

func (s *storeCore) loadFailure() error {
	encoded, closer, err := s.db.Get(quarantineKey())
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil
		}

		return fmt.Errorf("reading balance history failure state: %w", err)
	}
	copyEncoded := append([]byte(nil), encoded...)
	if err := closer.Close(); err != nil {
		return fmt.Errorf("closing balance history failure state: %w", err)
	}
	if len(copyEncoded) < 1 {
		s.failure.Store(&storeFailure{kind: failureQuarantined, detail: "failure marker is empty"})

		return nil
	}
	kind := failureKind(copyEncoded[0])
	detail := string(copyEncoded[1:])
	if kind != failureSourceMissing && kind != failureQuarantined && kind != failureRebuilding {
		kind = failureQuarantined
		detail = "failure marker has an unsupported kind"
	}
	if detail == "" {
		detail = "unspecified failure"
	}
	s.failure.Store(&storeFailure{kind: kind, detail: detail})

	return nil
}

func (s *storeCore) ensureNotQuarantined() error {
	failure := s.failure.Load()
	if failure == nil || failure.kind != failureQuarantined {
		return nil
	}

	return &ErrQuarantined{Detail: failure.detail}
}

func (s *storeCore) readFailure() error {
	failure := s.failure.Load()
	if failure == nil {
		return nil
	}
	if failure.kind == failureSourceMissing {
		return &ErrSourceMissing{Detail: failure.detail}
	}

	return &ErrQuarantined{Detail: failure.detail}
}

// ReadinessError exposes the persisted fail-closed state without opening a
// view. Rebuild markers are BUILDING for status reporting, while ordinary
// OpenView calls remain closed as quarantined until CompleteRebuild verifies
// and clears the marker.
func (s *Store) ReadinessError() error {
	failure := s.failure.Load()
	if failure == nil {
		return nil
	}
	switch failure.kind {
	case failureSourceMissing:
		return &ErrSourceMissing{Detail: failure.detail}
	case failureRebuilding:
		return &ErrBuilding{}
	default:
		return &ErrQuarantined{Detail: failure.detail}
	}
}

// MarkSourceMissing persists a verified source-coverage failure. It stops
// historical reads without destroying otherwise valid local segments, allowing
// an operator to restore the missing source before clearing the marker.
func (s *storeCore) MarkSourceMissing(detail string) error {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()

	if failure := s.failure.Load(); failure != nil &&
		(failure.kind == failureQuarantined || failure.kind == failureRebuilding) {
		return &ErrQuarantined{Detail: failure.detail}
	}

	return s.setFailureLocked(failureSourceMissing, detail)
}

// ClearFailure clears SOURCE_MISSING after the source has been repaired and
// published through the caller's pinned source head. A corruption quarantine
// is reset-only and cannot be cleared here.
func (s *Store) ClearFailure(requiredAuditSequence, requiredLogSequence uint64) error {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()

	failure := s.failure.Load()
	if failure == nil {
		return nil
	}
	if failure.kind == failureQuarantined || failure.kind == failureRebuilding {
		return &ErrQuarantined{Detail: failure.detail}
	}
	manifest, err := readManifest(s.db)
	if err != nil {
		return fmt.Errorf("reading repaired balance history manifest: %w", err)
	}
	if err := requireManifestHead(manifest, requiredAuditSequence, requiredLogSequence); err != nil {
		return err
	}
	if err := s.db.Delete(quarantineKey(), pebble.Sync); err != nil {
		return fmt.Errorf("clearing balance history failure state: %w", err)
	}
	s.failure.Store(nil)
	s.generation.Add(1)
	s.signalChanged()

	return nil
}

// Quarantine persists an integrity failure and immediately makes every view
// fail closed. Reset is the only operation that clears this state.
func (s *storeCore) Quarantine(detail string) error {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()

	return s.setFailureLocked(failureQuarantined, detail)
}

func (s *storeCore) setFailureLocked(kind failureKind, detail string) error {
	current := s.failure.Load()
	if current != nil && current.kind == failureQuarantined && kind != failureRebuilding {
		return nil
	}
	detail = boundedFailureDetail(detail)
	if current != nil && current.kind == kind && current.detail == detail {
		return nil
	}
	encoded := append([]byte{byte(kind)}, detail...)
	if err := s.db.Set(quarantineKey(), encoded, pebble.Sync); err != nil {
		return fmt.Errorf("persisting balance history failure state: %w", err)
	}
	s.failure.Store(&storeFailure{kind: kind, detail: detail})
	s.generation.Add(1)
	s.signalChanged()

	return nil
}

// ResetForRebuild drops every derived segment, manifest, and cursor while keeping
// the store persistently fail-closed in REBUILDING state. Manifest, Publish,
// Compact, and Verify remain available to the builder; OpenView does not.
func (s *Store) ResetForRebuild() error {
	return s.ResetForConfiguration(nil)
}

// ResetForConfiguration atomically drops the local projection, persists the
// client-selected ledger set, and keeps reads fail-closed until a complete
// audit replay has reached the pinned source head.
func (s *Store) ResetForConfiguration(ledgers []string) error {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()

	ledgers = append([]string(nil), ledgers...)
	slices.Sort(ledgers)
	if hasDuplicateStrings(ledgers) {
		return errors.New("historical-balance ledger configuration contains duplicates")
	}
	if slices.Contains(ledgers, "") {
		return errors.New("historical-balance ledger configuration contains an empty name")
	}

	detail := "balance history rebuild is in progress"
	if failure := s.failure.Load(); failure != nil && failure.detail != "" {
		detail = "balance history rebuild is in progress after: " + failure.detail
	}
	detail = boundedFailureDetail(detail)
	encoded := append([]byte{byte(failureRebuilding)}, detail...)

	batch := s.db.NewBatch()
	defer func() { _ = batch.Close() }()
	if err := batch.DeleteRange([]byte{prefixLatestManifest}, []byte{prefixRunCatalog + 1}, nil); err != nil {
		return fmt.Errorf("staging balance history rebuild reset: %w", err)
	}
	if err := stageInitialManifest(batch, ledgers); err != nil {
		return err
	}
	if err := batch.Set(quarantineKey(), encoded, nil); err != nil {
		return fmt.Errorf("staging balance history rebuilding state: %w", err)
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("committing balance history rebuild reset: %w", err)
	}
	s.failure.Store(&storeFailure{kind: failureRebuilding, detail: detail})
	s.generation.Add(1)
	s.signalChanged()

	return nil
}

// ResetForSourceRepair drops every derived segment, manifest, and cursor while
// preserving the persistent SOURCE_MISSING marker. This keeps reads
// fail-closed throughout a full-prefix repair; ClearFailure is the only step
// which reopens them once the builder has reached and durably synced its
// pinned source head.
func (s *Store) ResetForSourceRepair() error {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()

	failure := s.failure.Load()
	if failure == nil || failure.kind != failureSourceMissing {
		return errors.New("balance history store is not repairing a missing source")
	}
	manifest, err := readManifest(s.db)
	if err != nil {
		return fmt.Errorf("reading configured ledgers before balance history source repair: %w", err)
	}
	encoded := append([]byte{byte(failureSourceMissing)}, failure.detail...)

	batch := s.db.NewBatch()
	defer func() { _ = batch.Close() }()
	if err := batch.DeleteRange([]byte{prefixLatestManifest}, []byte{prefixRunCatalog + 1}, nil); err != nil {
		return fmt.Errorf("staging balance history source-repair reset: %w", err)
	}
	if err := stageInitialManifest(batch, manifest.Ledgers); err != nil {
		return err
	}
	if err := batch.Set(quarantineKey(), encoded, nil); err != nil {
		return fmt.Errorf("preserving balance history source-missing state: %w", err)
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("committing balance history source-repair reset: %w", err)
	}
	s.failure.Store(&storeFailure{kind: failureSourceMissing, detail: failure.detail})
	s.generation.Add(1)
	s.signalChanged()

	return nil
}

func stageInitialManifest(batch *pebble.Batch, ledgers []string) error {
	manifest := initialManifest()
	manifest.Version = 1
	manifest.Ledgers = append([]string(nil), ledgers...)
	encodedManifest, err := encodeManifest(manifest)
	if err != nil {
		return err
	}
	if err := batch.Set(manifestKey(manifest.Version), encodedManifest, nil); err != nil {
		return fmt.Errorf("staging initial balance history manifest: %w", err)
	}
	var version [8]byte
	binary.BigEndian.PutUint64(version[:], manifest.Version)
	if err := batch.Set(latestManifestKey(), version[:], nil); err != nil {
		return fmt.Errorf("staging initial balance history manifest pointer: %w", err)
	}

	return nil
}

// CompleteRebuild verifies the rebuilt projection structure and its source
// coverage before reopening reads.
func (s *Store) CompleteRebuild(requiredAuditSequence, requiredLogSequence uint64) error {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()

	failure := s.failure.Load()
	if failure == nil || failure.kind != failureRebuilding {
		return errors.New("balance history store is not rebuilding")
	}
	manifest, err := readManifest(s.db)
	if err != nil {
		return s.failRebuildLocked(err)
	}
	if err := requireManifestHead(manifest, requiredAuditSequence, requiredLogSequence); err != nil {
		return err
	}
	if err := s.verifyLatestContext(context.Background(), true); err != nil {
		return s.failRebuildLocked(err)
	}
	if err := s.db.Delete(quarantineKey(), pebble.Sync); err != nil {
		return fmt.Errorf("clearing verified balance history rebuild state: %w", err)
	}
	s.failure.Store(nil)
	s.generation.Add(1)
	s.signalChanged()

	return nil
}

func requireManifestHead(manifest Manifest, requiredAuditSequence, requiredLogSequence uint64) error {
	if !manifest.SourceComplete {
		return &ErrBuilding{Current: manifest.LogWatermark, Target: requiredLogSequence}
	}
	if manifest.AuditWatermark < requiredAuditSequence || manifest.LogWatermark < requiredLogSequence {
		return fmt.Errorf(
			"balance history manifest audit/log (%d,%d) is behind required head (%d,%d)",
			manifest.AuditWatermark,
			manifest.LogWatermark,
			requiredAuditSequence,
			requiredLogSequence,
		)
	}

	return nil
}

func (s *Store) failRebuildLocked(err error) error {
	if !isIntegrityError(err) {
		return err
	}
	if quarantineErr := s.setFailureLocked(failureQuarantined, err.Error()); quarantineErr != nil {
		return errors.Join(err, quarantineErr)
	}

	return err
}

func boundedFailureDetail(detail string) string {
	detail = strings.TrimSpace(detail)
	if detail == "" {
		detail = "unspecified failure"
	}
	const maxFailureDetail = 4096
	if len(detail) > maxFailureDetail {
		detail = detail[:maxFailureDetail]
	}

	return detail
}

func (s *storeCore) signalChanged() {
	s.waitMu.Lock()
	close(s.changed)
	s.changed = make(chan struct{})
	s.waitMu.Unlock()
}

// Changes returns a subscription which closes on the next manifest or store
// state mutation. Callers must subscribe again after every notification.
func (s *Store) Changes() <-chan struct{} {
	s.waitMu.Lock()
	defer s.waitMu.Unlock()

	return s.changed
}

// WaitForLogWatermark blocks until a manifest covers required or ctx is done.
// Cancellation and deadlines retain their context identity so transport code
// does not misclassify an abandoned request as projection lag.
func (s *Store) WaitForLogWatermark(ctx context.Context, required uint64) error {
	for {
		s.waitMu.Lock()
		changed := s.changed
		s.waitMu.Unlock()

		if err := s.readFailure(); err != nil {
			return err
		}
		manifest, err := s.Manifest()
		if err != nil {
			return err
		}
		if manifest.SourceComplete && manifest.LogWatermark >= required {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}

// Reset atomically removes every segment, manifest, and cursor. The next builder
// pass must replay the authoritative audit prefix from genesis.
func (s *Store) Reset() error {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()

	batch := s.db.NewBatch()
	defer func() { _ = batch.Close() }()

	if err := batch.DeleteRange([]byte{prefixStoreState}, []byte{prefixRunCatalog + 1}, nil); err != nil {
		return fmt.Errorf("staging balance history reset: %w", err)
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("committing balance history reset: %w", err)
	}

	s.failure.Store(nil)
	s.generation.Add(1)
	s.signalChanged()

	return nil
}
