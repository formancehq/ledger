package balancehistorystore

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
)

type compactor struct {
	*compactionStreamer

	garbageCollector *garbageCollector
}

const DefaultRunCompactionThreshold = 4

// Compact merges the oldest threshold runs of the lowest eligible logical
// level. It publishes the replacement manifest in one atomic WAL batch;
// SyncWAL provides periodic durability. Existing Views retain old runs through
// explicit leases and their Pebble snapshots until lease-aware GC.
func (s *compactor) Compact(threshold int) (bool, error) {
	return s.CompactContext(context.Background(), threshold)
}

// CompactContext prepares a merge outside mutationMu so rehydrating cold runs
// never stalls publication. The final manifest update is a small CAS-like
// critical section: if another compaction or reset superseded an input, the
// prepared replacement is discarded and a later pass retries.
func (s *compactor) CompactContext(ctx context.Context, threshold int) (bool, error) {
	if threshold <= 1 {
		threshold = DefaultRunCompactionThreshold
	}

	s.compactionMu.Lock()
	defer s.compactionMu.Unlock()

	view, err := s.openCompactionView(ctx)
	if err != nil {
		return false, err
	}
	selected := selectCompactionRuns(view.manifest.Runs, threshold)
	if len(selected) == 0 {
		return false, view.Close()
	}

	for _, run := range selected {
		if err := ctx.Err(); err != nil {
			return false, errors.Join(err, view.Close())
		}
		if cold := view.coldRuns[run.ID]; cold != nil {
			err = verifyArchivedRun(ctx, cold.archive, run)
		} else {
			err = verifyStoredRunContext(ctx, view.snapshot, run)
		}
		if err != nil {
			return false, errors.Join(s.compactionInputError(err), view.Close())
		}
	}

	runID, reserved, err := s.reserveCompactionRun(selected, view.generation)
	if err != nil || !reserved {
		return false, errors.Join(err, view.Close())
	}
	merged, err := s.streamCompactedRun(ctx, view, selected, runID)
	if err != nil {
		return false, errors.Join(s.compactionInputError(err), view.Close(), s.discardPreparedRun(runID))
	}
	if err := view.Close(); err != nil {
		return false, errors.Join(err, s.discardPreparedRun(runID))
	}

	published, err := s.publishPreparedCompaction(selected, merged, view.generation)
	if err != nil || !published {
		return false, errors.Join(err, s.discardPreparedRun(runID))
	}
	if _, err := s.garbageCollector.CollectGarbage(); err != nil {
		return true, fmt.Errorf("collecting superseded balance history runs: %w", err)
	}

	return true, nil
}

func (s *compactor) compactionInputError(err error) error {
	err = mapArchiveError(err)
	if !isIntegrityError(err) {
		return fmt.Errorf("reading selected run before compaction: %w", err)
	}
	if quarantineErr := s.Quarantine(err.Error()); quarantineErr != nil {
		err = errors.Join(err, quarantineErr)
	}

	return fmt.Errorf("verifying selected run before compaction: %w", err)
}

func compactionInputsPresent(current, selected []RunRef) bool {
	byID := make(map[uint64]RunRef, len(current))
	for _, run := range current {
		byID[run.ID] = run
	}
	for _, expected := range selected {
		run, ok := byID[expected.ID]
		if !ok || !sameCompactionInput(run, expected) {
			return false
		}
	}

	return true
}

func sameCompactionInput(left, right RunRef) bool {
	return left.ID == right.ID &&
		left.Level == right.Level &&
		left.FirstAuditSequence == right.FirstAuditSequence &&
		left.LastAuditSequence == right.LastAuditSequence &&
		left.MaxLogSequence == right.MaxLogSequence &&
		left.EntryCount == right.EntryCount &&
		left.IdentityCount == right.IdentityCount &&
		left.Checksum == right.Checksum
}

func selectCompactionRuns(runs []RunRef, threshold int) []RunRef {
	byLevel := make(map[uint32][]RunRef)
	levels := make([]uint32, 0)
	for _, run := range runs {
		if _, ok := byLevel[run.Level]; !ok {
			levels = append(levels, run.Level)
		}
		byLevel[run.Level] = append(byLevel[run.Level], run)
	}
	slices.Sort(levels)
	for _, level := range levels {
		candidates := byLevel[level]
		if len(candidates) < threshold {
			continue
		}
		sort.Slice(candidates, func(i, j int) bool { return candidates[i].ID < candidates[j].ID })

		return candidates[:threshold]
	}

	return nil
}
