package attributes

import (
	"fmt"

	"github.com/cockroachdb/pebble"
	"github.com/formancehq/ledger-v3-poc/internal/storage/data"
)

// keyCompactor compacts a single canonical key to a base entry at the given index.
// This interface lets CompactAllForBackup dispatch to the correct typed Attribute[V]
// without knowing the concrete protobuf type.
type keyCompactor interface {
	compactKey(s *data.Store, batch *data.Batch, targetIndex uint64, canonicalKey []byte) error
}

// CompactAllForBackup compacts all attribute types in the store to index 0 and resets
// the lastAppliedIndex to 0. This prepares the database for use as a backup that can
// be restored on a fresh cluster without raft index conflicts.
//
// It performs a single scan over the entire attribute range [0xF1, 0xF2) and dispatches
// each unique (canonicalKey, attrType) pair to the correct typed attribute handler.
//
// The caller must ensure that all in-memory state (dirty boundaries, etc.) has been
// flushed to Pebble before the checkpoint was taken. The backup flow achieves this
// by running the flush and checkpoint atomically on the Raft loop.
func CompactAllForBackup(s *data.Store) error {
	attrs := New()
	batch := s.NewBatch()

	// Build dispatch table: attrType byte → keyCompactor
	dispatch := map[byte]keyCompactor{
		data.AttributePrefixVolume:         attrs.Volume,
		data.AttributePrefixMetadata:       attrs.Metadata,
		data.AttributePrefixLedgerMetadata: attrs.LedgerMetadata,
		data.AttributePrefixReverted:       attrs.Reverted,
		data.AttributePrefixIdempotencyKey: attrs.IdempotencyKeys,
		data.AttributePrefixReference:      attrs.References,
		data.AttributePrefixLedger:         attrs.Ledger,
		data.AttributePrefixBoundary:       attrs.Boundary,
	}

	// Single scan over the entire attribute range
	type entryKey struct {
		canonicalKey string
		attrType     byte
	}

	buf := make([]byte, 2)
	buf[0] = data.KeyPrefixAttributes
	buf[1] = data.KeyPrefixAttributes + 1

	iter, err := s.NewIter(&pebble.IterOptions{
		LowerBound: buf[:1],
		UpperBound: buf[1:2],
	})
	if err != nil {
		_ = batch.Cancel()
		return fmt.Errorf("creating iterator for attributes: %w", err)
	}

	seen := make(map[entryKey]struct{})
	minKeyLen := 1 + suffixLen // prefix(1) + suffix(10)

	for iter.First(); iter.Valid(); iter.Next() {
		iterKey := iter.Key()
		if len(iterKey) <= minKeyLen {
			continue
		}

		attrType := iterKey[len(iterKey)-suffixLen]
		canonicalKey := string(iterKey[1 : len(iterKey)-suffixLen])

		ek := entryKey{canonicalKey: canonicalKey, attrType: attrType}
		if _, ok := seen[ek]; ok {
			continue
		}
		seen[ek] = struct{}{}

		handler, ok := dispatch[attrType]
		if !ok {
			continue
		}

		canonicalBytes := make([]byte, len(canonicalKey))
		copy(canonicalBytes, canonicalKey)

		if err := handler.compactKey(s, batch, 0, canonicalBytes); err != nil {
			_ = iter.Close()
			_ = batch.Cancel()
			return fmt.Errorf("compacting key (type=%c): %w", attrType, err)
		}
	}

	if err := iter.Error(); err != nil {
		_ = iter.Close()
		_ = batch.Cancel()
		return fmt.Errorf("iterating attributes: %w", err)
	}
	_ = iter.Close()

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
