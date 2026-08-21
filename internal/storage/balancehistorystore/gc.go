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
// unreachable bytes, never expose a partial segment.
func (s *garbageCollector) CollectGarbage() (bool, error) {
	generation := s.generation.Load()
	snapshot := s.db.NewSnapshot()
	physicalRuns, physicalManifests, scanErr := scanGarbageCandidates(snapshot)
	closeErr := snapshot.Close()
	if scanErr == nil && closeErr != nil {
		scanErr = fmt.Errorf("closing balance history GC snapshot: %w", closeErr)
	}

	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()
	if generation != s.generation.Load() {
		// A reset may reuse manifest and segment identifiers. Discard candidates
		// observed before that generation change rather than risk deleting the
		// newly rebuilt rows which now carry the same identifiers.
		return false, nil
	}
	if err := s.ensureNotQuarantined(); err != nil {
		return false, err
	}
	if scanErr != nil {
		var corrupt *ErrCorrupt
		if errors.As(scanErr, &corrupt) {
			return s.quarantineGCError(corrupt.Detail)
		}

		return false, scanErr
	}

	manifest, err := readManifest(s.db)
	if err != nil {
		return false, err
	}
	keepRuns := make(map[uint64]struct{}, len(manifest.Segments))
	for _, segment := range manifest.Segments {
		keepRuns[segment.ID] = struct{}{}
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
				return false, fmt.Errorf("staging orphan segment %d deletion: %w", runID, err)
			}
		}
		if err := batch.Delete(runMetaKey(runID), nil); err != nil {
			return false, fmt.Errorf("staging orphan segment %d metadata deletion: %w", runID, err)
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

// scanGarbageCandidates performs the expensive physical key walk without
// holding mutationMu. CollectGarbage revalidates the current manifest, leases,
// prepared runs, and store generation under the lock before deleting anything.
func scanGarbageCandidates(snapshot *pebble.Snapshot) (map[uint64]struct{}, map[uint64]struct{}, error) {
	physicalRuns := make(map[uint64]struct{})
	for _, kind := range []byte{prefixRunData, prefixRunMeta, prefixRunCatalog} {
		iter, err := snapshot.NewIter(&pebble.IterOptions{
			LowerBound: []byte{kind},
			UpperBound: []byte{kind + 1},
		})
		if err != nil {
			return nil, nil, fmt.Errorf("opening balance history GC segment iterator: %w", err)
		}
		for valid := iter.First(); valid; valid = iter.Next() {
			if len(iter.Key()) < 9 {
				_ = iter.Close()

				return nil, nil, &ErrCorrupt{Detail: fmt.Sprintf("truncated segment key under prefix 0x%x", kind)}
			}
			physicalRuns[binary.BigEndian.Uint64(iter.Key()[1:9])] = struct{}{}
		}
		iterErr := iter.Error()
		closeErr := iter.Close()
		if iterErr != nil {
			return nil, nil, fmt.Errorf("iterating balance history GC runs: %w", iterErr)
		}
		if closeErr != nil {
			return nil, nil, fmt.Errorf("closing balance history GC run iterator: %w", closeErr)
		}
	}

	physicalManifests := make(map[uint64]struct{})
	iter, err := snapshot.NewIter(&pebble.IterOptions{
		LowerBound: []byte{prefixManifest},
		UpperBound: []byte{prefixManifest + 1},
	})
	if err != nil {
		return nil, nil, fmt.Errorf("opening balance history GC manifest iterator: %w", err)
	}
	for valid := iter.First(); valid; valid = iter.Next() {
		if len(iter.Key()) != 9 {
			_ = iter.Close()

			return nil, nil, &ErrCorrupt{Detail: "invalid immutable manifest key"}
		}
		physicalManifests[binary.BigEndian.Uint64(iter.Key()[1:])] = struct{}{}
	}
	iterErr := iter.Error()
	closeErr := iter.Close()
	if iterErr != nil {
		return nil, nil, fmt.Errorf("iterating balance history GC manifests: %w", iterErr)
	}
	if closeErr != nil {
		return nil, nil, fmt.Errorf("closing balance history GC manifest iterator: %w", closeErr)
	}

	return physicalRuns, physicalManifests, nil
}

func (s *garbageCollector) quarantineGCError(detail string) (bool, error) {
	err := &ErrCorrupt{Detail: detail}
	if quarantineErr := s.setFailureLocked(failureQuarantined, err.Error()); quarantineErr != nil {
		return false, errors.Join(err, fmt.Errorf("persisting balance history quarantine: %w", quarantineErr))
	}

	return false, err
}
