package balancehistorystore

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble/v2"
)

type garbageCollector struct {
	*storeCore
}

// CollectGarbage removes immutable manifests and runs that are referenced by
// neither the latest manifest nor an active View lease. Publication visibility
// never depends on this cleanup: a crash before or during GC can only leave
// unreachable bytes, never expose a partial run.
func (s *garbageCollector) CollectGarbage() (bool, error) {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()

	if err := s.ensureNotQuarantined(); err != nil {
		return false, err
	}

	return s.collectGarbageLocked()
}

func (s *garbageCollector) collectGarbageLocked() (bool, error) {
	snapshot := s.db.NewSnapshot()
	defer func() { _ = snapshot.Close() }()

	manifest, err := readManifest(snapshot)
	if err != nil {
		return false, err
	}
	keepRuns := make(map[uint64]struct{}, len(manifest.Runs))
	for _, run := range manifest.Runs {
		if !run.LocalRemoved {
			keepRuns[run.ID] = struct{}{}
		}
	}
	keepManifests := map[uint64]struct{}{manifest.Version: {}}

	s.leaseMu.Lock()
	for version := range s.manifestLeases {
		keepManifests[version] = struct{}{}
	}
	for runID := range s.runLeases {
		keepRuns[runID] = struct{}{}
	}
	for runID := range s.preparedRuns {
		keepRuns[runID] = struct{}{}
	}
	s.leaseMu.Unlock()

	physicalRuns := make(map[uint64]struct{})
	for _, kind := range []byte{prefixRunData, prefixRunMeta, prefixRunCatalog} {
		iter, err := snapshot.NewIter(&pebble.IterOptions{
			LowerBound: []byte{kind},
			UpperBound: []byte{kind + 1},
		})
		if err != nil {
			return false, fmt.Errorf("opening balance history GC run iterator: %w", err)
		}
		for valid := iter.First(); valid; valid = iter.Next() {
			if len(iter.Key()) < 9 {
				_ = iter.Close()

				return s.quarantineGCError(fmt.Sprintf("truncated run key under prefix 0x%x", kind))
			}
			physicalRuns[binary.BigEndian.Uint64(iter.Key()[1:9])] = struct{}{}
		}
		iterErr := iter.Error()
		closeErr := iter.Close()
		if iterErr != nil {
			return false, fmt.Errorf("iterating balance history GC runs: %w", iterErr)
		}
		if closeErr != nil {
			return false, fmt.Errorf("closing balance history GC run iterator: %w", closeErr)
		}
	}

	physicalManifests := make(map[uint64]struct{})
	iter, err := snapshot.NewIter(&pebble.IterOptions{
		LowerBound: []byte{prefixManifest},
		UpperBound: []byte{prefixManifest + 1},
	})
	if err != nil {
		return false, fmt.Errorf("opening balance history GC manifest iterator: %w", err)
	}
	for valid := iter.First(); valid; valid = iter.Next() {
		if len(iter.Key()) != 9 {
			_ = iter.Close()

			return s.quarantineGCError("invalid immutable manifest key")
		}
		physicalManifests[binary.BigEndian.Uint64(iter.Key()[1:])] = struct{}{}
	}
	iterErr := iter.Error()
	closeErr := iter.Close()
	if iterErr != nil {
		return false, fmt.Errorf("iterating balance history GC manifests: %w", iterErr)
	}
	if closeErr != nil {
		return false, fmt.Errorf("closing balance history GC manifest iterator: %w", closeErr)
	}

	batch := s.db.NewBatch()
	defer func() { _ = batch.Close() }()
	changed := false
	for runID := range physicalRuns {
		if _, keep := keepRuns[runID]; keep {
			continue
		}
		for _, kind := range []byte{prefixRunData, prefixRunCatalog} {
			prefix := runPrefix(kind, runID)
			if err := batch.DeleteRange(prefix, prefixEnd(prefix), nil); err != nil {
				return false, fmt.Errorf("staging orphan run %d deletion: %w", runID, err)
			}
		}
		if err := batch.Delete(runMetaKey(runID), nil); err != nil {
			return false, fmt.Errorf("staging orphan run %d metadata deletion: %w", runID, err)
		}
		changed = true
	}
	for version := range physicalManifests {
		if _, keep := keepManifests[version]; keep {
			continue
		}
		if err := batch.Delete(manifestKey(version), nil); err != nil {
			return false, fmt.Errorf("staging unleased manifest %d deletion: %w", version, err)
		}
		changed = true
	}
	if !changed {
		return false, nil
	}
	// GC only removes unreachable rebuildable bytes. Losing these tombstones
	// can leave harmless orphans after a crash, which a later GC reclaims.
	if err := batch.Commit(pebble.NoSync); err != nil {
		return false, fmt.Errorf("committing balance history garbage collection: %w", err)
	}

	return true, nil
}

func (s *garbageCollector) quarantineGCError(detail string) (bool, error) {
	err := &ErrCorrupt{Detail: detail}
	if quarantineErr := s.setFailureLocked(failureQuarantined, err.Error()); quarantineErr != nil {
		return false, errors.Join(err, fmt.Errorf("persisting balance history quarantine: %w", quarantineErr))
	}

	return false, err
}
