package dal

import (
	"fmt"
	"io"
)

// This file declares the narrow capability interfaces that callers should
// receive instead of the full *Store. Each interface scopes one specific use
// of the main Pebble store; together they make the structural invariants
// "no Pebble reads on the hot path" (I1) and "no Pebble writes outside the
// hot path or declared lifecycle paths" (I2) compiler-enforced for any code
// that uses these capabilities instead of a raw *Store.
//
// *Store implements every capability interface declared below; consumers are
// wired with the narrow capability they actually need, never the full *Store.
//
// Two complementary styles:
//   - Pure segregation interfaces (RecoveryReader, CheckpointStaging,
//     QueryCheckpoints, ColdStorageScanner): expose only the methods a single
//     consumer uses.
//   - Scoped callback factories (SentinelFactory, IncomingRestoreFactory):
//     encapsulate multi-step coordination behind a Run(fn) boundary so the
//     primitive ops are not callable individually.

// WriteSessionFactory opens write-only sessions on the main store.
//
// Intended consumer: the FSM apply hot path. The factory should be passed as
// a parameter to ApplyEntries / PrepareEntries, not stored as a field — this
// makes it impossible for any other method on the holder to open a write
// session.
type WriteSessionFactory interface {
	OpenWriteSession() *WriteSession
}

// SentinelFactory exposes a scoped reader for post-commit sentinel checks in
// debug mode. The Reader is only ever materialised inside the callback; it
// does not survive the call. Implementations are free to make Run a no-op
// when the sentinel mode is disabled — callers do not need to test the flag
// themselves.
//
// IsEnabled is exposed so callers that gate auxiliary tracing /
// verification work on sentinel mode (sentinelTracer, cache-coherence
// dump) do not need a separate boolean parameter.
//
// Intended consumer: the FSM in CommitPreparedBatch.
type SentinelFactory interface {
	Run(fn func(r PebbleReader) error) error
	IsEnabled() bool
}

// BackgroundScanner exposes the "open a direct (non-snapshot) read handle"
// primitive used by long-running background scans of the main store.
//
// Intended consumer: the idempotency-eviction scheduler, which scans the time
// index periodically. The direct handle is cheap to open / close and does not
// pin SSTs.
type BackgroundScanner interface {
	NewDirectReadHandle() (*ReadHandle, error)
}

// SnapshotReader exposes the "open a snapshot read handle" primitive. The
// snapshot pins a point-in-time view, suitable for callers that need
// consistency across multiple reads.
type SnapshotReader interface {
	NewReadHandle() (*ReadHandle, error)
}

// BackgroundReader is the read surface used by background workers that need
// snapshot consistency plus point lookups (e.g. the metadata converter
// resolving a ledger by name then iterating its rows under a snapshot).
type BackgroundReader interface {
	SnapshotReader
	Get(key []byte) ([]byte, io.Closer, error)
}

// RecoveryReader exposes the full read surface used by boot / recovery
// phases (FSM Machine.RecoverState, CacheSnapshotter). It composes the
// direct + snapshot openers and adds point lookups.
//
// Intended consumers: FSM Machine.RecoverState (boot + post-sync), the
// CacheSnapshotter (cache restore).
type RecoveryReader interface {
	BackgroundScanner
	SnapshotReader
	Get(key []byte) ([]byte, io.Closer, error)
}

// CheckpointStaging exposes the temporary-checkpoint file-system primitives
// the Sealer uses to compute the seal state hash off the hot path.
//
// Intended consumer: the Sealer background worker.
type CheckpointStaging interface {
	CreateTemporaryCheckpoint(name string) (string, error)
	TemporaryCheckpointPath(name string) (string, bool)
	RemoveTemporaryCheckpoint(name string) error
}

// QueryCheckpoints exposes the query-checkpoint lifecycle: create on demand,
// delete a known one. Both ops are file-system level (Pebble checkpoint dirs).
//
// Intended consumers: the FSM (delete-on-apply) and the bootstrap-side
// query-checkpoint creator.
type QueryCheckpoints interface {
	CreateQueryCheckpoint(id uint64) (string, error)
	DeleteQueryCheckpointFiles(id uint64) error
}

// ColdStorageScanner exposes the cold-storage iteration primitive used to
// export an archived period's data.
//
// Intended consumer: the Archiver background worker.
type ColdStorageScanner interface {
	IterateColdKVPairs(logStart, logClose, auditStart, auditClose uint64, fn func(key, value []byte) error) error
}

// IncomingRestoreFactory encapsulates the three-step incoming-restore
// sequence (PrepareIncomingRestore → caller writes the snapshot payload into
// stagingDir → ActivateIncomingRestore → RestoreCheckpoint) behind a single
// scoped callback. The Prepare/Activate/Restore primitives are not exposed
// individually, so the contract of the sequence becomes the type itself: a
// caller cannot leave the store half-prepared or skip the Restore step.
//
// If fn returns an error, the staging dir is left for offline inspection and
// no checkpoint is restored.
//
// Intended consumer: FSM Machine.SynchronizeWithLeader.
type IncomingRestoreFactory interface {
	Run(fn func(stagingDir string) error) (checkpointID uint64, err error)
}

// NewSentinelFactory builds a SentinelFactory bound to store. When enabled is
// false, the returned factory is a no-op: Run never calls fn and always
// returns nil. This lets callers always invoke Run regardless of mode.
func NewSentinelFactory(store *Store, enabled bool) SentinelFactory {
	if !enabled {
		return disabledSentinelFactory{}
	}

	return enabledSentinelFactory{store: store}
}

type disabledSentinelFactory struct{}

func (disabledSentinelFactory) Run(_ func(PebbleReader) error) error { return nil }
func (disabledSentinelFactory) IsEnabled() bool                      { return false }

type enabledSentinelFactory struct {
	store *Store
}

func (enabledSentinelFactory) IsEnabled() bool { return true }

func (e enabledSentinelFactory) Run(fn func(PebbleReader) error) error {
	// Use a snapshot handle (not a direct one) so the sentinel view is pinned
	// to the DB state right after the just-completed batch commit. Even though
	// runCommitter serialises commits today, the snapshot is cheap and keeps
	// the invariant if a future change reintroduces a synchronous commit path.
	handle, err := e.store.NewReadHandle()
	if err != nil {
		return fmt.Errorf("opening sentinel read handle: %w", err)
	}

	defer func() { _ = handle.Close() }()

	return fn(handle)
}

// NewIncomingRestoreFactory builds an IncomingRestoreFactory bound to store.
// The factory owns the Prepare/Activate/Restore primitives; callers only see
// Run.
func NewIncomingRestoreFactory(store *Store) IncomingRestoreFactory {
	return incomingRestoreFactory{store: store}
}

type incomingRestoreFactory struct {
	store *Store
}

func (f incomingRestoreFactory) Run(fn func(stagingDir string) error) (uint64, error) {
	stagingDir, err := f.store.PrepareIncomingRestore()
	if err != nil {
		return 0, fmt.Errorf("preparing incoming restore: %w", err)
	}

	if err := fn(stagingDir); err != nil {
		// Staging dir is preserved for offline inspection.
		return 0, err
	}

	checkpointID, err := f.store.ActivateIncomingRestore()
	if err != nil {
		return 0, fmt.Errorf("activating incoming restore: %w", err)
	}

	if err := f.store.RestoreCheckpoint(checkpointID); err != nil {
		return 0, fmt.Errorf("restoring checkpoint %d: %w", checkpointID, err)
	}

	return checkpointID, nil
}
