package attributes

import (
	"fmt"

	"github.com/formancehq/ledger-v3-poc/internal/storage/data"
)

// CompactAllForBackup compacts all attribute types in the store to index 0 and resets
// the lastAppliedIndex to 0. This prepares the database for use as a backup that can
// be restored on a fresh cluster without raft index conflicts.
//
// The caller must ensure that all in-memory state (dirty boundaries, etc.) has been
// flushed to Pebble before the checkpoint was taken. The backup flow achieves this
// by running the flush and checkpoint atomically on the Raft loop.
func CompactAllForBackup(s *data.Store) error {
	attrs := New()
	batch := s.NewBatch()

	if err := attrs.Volume.CompactToBase(s, batch, 0); err != nil {
		_ = batch.Cancel()
		return fmt.Errorf("compacting volumes: %w", err)
	}
	if err := attrs.Metadata.CompactToBase(s, batch, 0); err != nil {
		_ = batch.Cancel()
		return fmt.Errorf("compacting metadata: %w", err)
	}
	if err := attrs.LedgerMetadata.CompactToBase(s, batch, 0); err != nil {
		_ = batch.Cancel()
		return fmt.Errorf("compacting ledger metadata: %w", err)
	}
	if err := attrs.Reverted.CompactToBase(s, batch, 0); err != nil {
		_ = batch.Cancel()
		return fmt.Errorf("compacting reverted: %w", err)
	}
	if err := attrs.IdempotencyKeys.CompactToBase(s, batch, 0); err != nil {
		_ = batch.Cancel()
		return fmt.Errorf("compacting idempotency keys: %w", err)
	}
	if err := attrs.References.CompactToBase(s, batch, 0); err != nil {
		_ = batch.Cancel()
		return fmt.Errorf("compacting references: %w", err)
	}
	if err := attrs.Ledger.CompactToBase(s, batch, 0); err != nil {
		_ = batch.Cancel()
		return fmt.Errorf("compacting ledgers: %w", err)
	}
	if err := attrs.Boundary.CompactToBase(s, batch, 0); err != nil {
		_ = batch.Cancel()
		return fmt.Errorf("compacting boundaries: %w", err)
	}

	// Reset lastAppliedIndex to 0 so the restored cluster starts fresh
	if err := batch.SetAppliedIndex(0); err != nil {
		_ = batch.Cancel()
		return fmt.Errorf("resetting applied index: %w", err)
	}

	if err := batch.Commit(); err != nil {
		return fmt.Errorf("committing compacted attributes: %w", err)
	}

	// Force a Pebble flush to ensure all compacted data is written to SSTs
	if err := s.Flush(); err != nil {
		return fmt.Errorf("flushing compacted data: %w", err)
	}

	return nil
}
