package attributes

import (
	"fmt"

	"github.com/cockroachdb/pebble/v2"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// compactor accumulates attribute entries of a single type during a forward scan
// and writes compacted base values at the target index. This is the type-erased
// interface used by CompactAllForBackup to dispatch entries by attribute type.
type compactor interface {
	Feed(pebbleKey, pebbleValue []byte) error
	Flush() error
}

// typedCompactor wraps accumulatorBase to compact entries for a single attribute type.
// When a canonical key boundary is crossed, it writes the computed value into the batch.
type typedCompactor[V proto.Message] struct {
	accumulatorBase[V]

	batch *dal.Batch
}

func newCompactor[V proto.Message](attr *Attribute[V], batch *dal.Batch) *typedCompactor[V] {
	return &typedCompactor[V]{
		accumulatorBase: accumulatorBase[V]{attr: attr},
		batch:           batch,
	}
}

func (c *typedCompactor[V]) Feed(pebbleKey, pebbleValue []byte) error {
	_, prev, err := c.feed(pebbleKey, pebbleValue)
	if err != nil {
		return err
	}

	if prev != nil {
		return c.writeCompacted(prev)
	}

	return nil
}

func (c *typedCompactor[V]) writeCompacted(entry *ComputedEntry[V]) error {
	_, err := c.attr.Set(c.batch, entry.CanonicalKey, entry.Value)

	return err
}

func (c *typedCompactor[V]) Flush() error {
	entry := c.flush()
	if entry != nil {
		return c.writeCompacted(entry)
	}

	return nil
}

// CompactAllForBackup compacts all attribute types in the store to index 0 and resets
// the lastAppliedIndex to 0. This prepares the database for use as a backup that can
// be restored on a fresh cluster without raft index conflicts.
//
// It performs a single forward scan over the entire attribute range [0xF1, 0xF2),
// dispatching each entry to a type-specific compactor that uses accumulatorBase
// to compute the final value per canonical key. Old entries are bulk-deleted with
// a single DeleteRange, and compacted values are written into the same batch.
//
// The caller must ensure that all in-memory state (dirty boundaries, etc.) has been
// flushed to Pebble before the checkpoint was taken. The backup flow achieves this
// by running the flush and checkpoint atomically on the Raft loop.
func CompactAllForBackup(s *dal.Store) error {
	attrs := New()
	batch := s.NewBatch()

	// Bulk-delete the entire attribute range — compacted values are written back below.
	if err := batch.DeleteRange(
		[]byte{dal.ZoneAttributesStart},
		[]byte{dal.ZoneAttributesEnd},
		pebble.NoSync,
	); err != nil {
		_ = batch.Cancel()

		return fmt.Errorf("deleting attribute range: %w", err)
	}

	// Build dispatch table: attrType byte → compactor
	dispatch := map[byte]compactor{
		dal.AttributePrefixVolume:           newCompactor(attrs.Volume, batch),
		dal.AttributePrefixMetadata:         newCompactor(attrs.Metadata, batch),
		dal.AttributePrefixIdempotency:      newCompactor(attrs.IdempotencyKeys, batch),
		dal.AttributePrefixReference:        newCompactor(attrs.References, batch),
		dal.AttributePrefixLedger:           newCompactor(attrs.Ledger, batch),
		dal.AttributePrefixBoundary:         newCompactor(attrs.Boundary, batch),
		dal.AttributePrefixTransaction:      newCompactor(attrs.Transaction, batch),
		dal.AttributePrefixSinkConfig:       newCompactor(attrs.SinkConfig, batch),
		dal.AttributePrefixNumscriptVersion: newCompactor(attrs.NumscriptVersion, batch),
		dal.AttributePrefixNumscriptContent: newCompactor(attrs.NumscriptContent, batch),
	}

	// Single scan over the entire attribute range
	buf := make([]byte, 2)
	buf[0] = dal.ZoneAttributesStart
	buf[1] = dal.ZoneAttributesEnd

	iter, err := s.NewIter(&pebble.IterOptions{
		LowerBound: buf[:1],
		UpperBound: buf[1:2],
	})
	if err != nil {
		_ = batch.Cancel()

		return fmt.Errorf("creating iterator for attributes: %w", err)
	}

	minKeyLen := 1 + SuffixLen

	for iter.First(); iter.Valid(); iter.Next() {
		iterKey := iter.Key()
		if len(iterKey) <= minKeyLen {
			continue
		}

		attrType := iterKey[len(iterKey)-SuffixLen]

		handler, ok := dispatch[attrType]
		if !ok {
			continue
		}

		valueBytes, err := iter.ValueAndErr()
		if err != nil {
			_ = iter.Close()
			_ = batch.Cancel()

			return fmt.Errorf("reading value: %w", err)
		}

		if err := handler.Feed(iterKey, valueBytes); err != nil {
			_ = iter.Close()
			_ = batch.Cancel()

			return fmt.Errorf("feeding compactor (type=%c): %w", attrType, err)
		}
	}

	if err := iter.Error(); err != nil {
		_ = iter.Close()
		_ = batch.Cancel()

		return fmt.Errorf("iterating attributes: %w", err)
	}

	_ = iter.Close()

	// Flush all compactors to write the last pending entry for each type
	for attrType, handler := range dispatch {
		err := handler.Flush()
		if err != nil {
			_ = batch.Cancel()

			return fmt.Errorf("flushing compactor (type=%c): %w", attrType, err)
		}
	}

	// Reset lastAppliedIndex to 0 so the restored cluster starts fresh
	if err := batch.SetBytes([]byte{dal.KeyPrefixLastAppliedIndex}, make([]byte, 8)); err != nil {
		_ = batch.Cancel()

		return fmt.Errorf("resetting applied index: %w", err)
	}

	// Remove persisted config (nodeId, clusterId) so the backup is portable to any cluster
	if err := batch.DeleteKey([]byte{dal.KeyPrefixPersistedConfig}); err != nil {
		_ = batch.Cancel()

		return fmt.Errorf("deleting persisted config: %w", err)
	}

	if err := batch.Commit(); err != nil {
		return fmt.Errorf("committing compacted attributes: %w", err)
	}

	// Force a Pebble flush to ensure all compacted data is written to SSTs
	// todo: directly commit with NoSync
	if err := s.Flush(); err != nil {
		return fmt.Errorf("flushing compacted data: %w", err)
	}

	return nil
}
