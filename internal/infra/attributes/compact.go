package attributes

import (
	"fmt"

	"github.com/cockroachdb/pebble/v2"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
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

	batch *dal.WriteSession
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
	batch := s.OpenWriteSession()

	// Bulk-delete the entire attribute range — compacted values are written back below.
	if err := batch.DeleteRange(
		[]byte{dal.ZoneAttributes},
		[]byte{dal.ZoneAttributes + 1},
		pebble.NoSync,
	); err != nil {
		_ = batch.Cancel()

		return fmt.Errorf("deleting attribute range: %w", err)
	}

	// Build dispatch table from the attribute registry so every registered
	// attribute type is covered automatically; a newly added attribute needs no
	// edit here. See attributes.All().
	all := attrs.All()
	dispatch := make(map[byte]compactor, len(all))
	for _, attr := range all {
		dispatch[attr.Prefix()] = attr.newCompactor(batch)
	}

	// Single scan over the entire attribute range
	handle, err := s.NewDirectReadHandle()
	if err != nil {
		_ = batch.Cancel()

		return fmt.Errorf("creating read handle: %w", err)
	}
	defer func() { _ = handle.Close() }()

	buf := make([]byte, 2)
	buf[0] = dal.ZoneAttributes
	buf[1] = dal.ZoneAttributes + 1

	iter, err := handle.NewIter(&pebble.IterOptions{
		LowerBound: buf[:1],
		UpperBound: buf[1:2],
	})
	if err != nil {
		_ = batch.Cancel()

		return fmt.Errorf("creating iterator for attributes: %w", err)
	}

	minKeyLen := 1 + AttrTypeLen

	for iter.First(); iter.Valid(); iter.Next() {
		iterKey := iter.Key()
		if len(iterKey) <= minKeyLen {
			continue
		}

		attrType := iterKey[1]

		handler, ok := dispatch[attrType]
		if !ok {
			_ = iter.Close()
			_ = batch.Cancel()

			return fmt.Errorf("invariant: no compactor registered for attribute type 0x%02x in backup compaction", attrType)
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
	if err := batch.SetBytes([]byte{dal.ZoneGlobal, dal.SubGlobLastAppliedIndex}, make([]byte, 8)); err != nil {
		_ = batch.Cancel()

		return fmt.Errorf("resetting applied index: %w", err)
	}

	// Remove persisted config (nodeId, clusterId) so the backup is portable to any cluster
	if err := batch.DeleteKey([]byte{dal.ZoneGlobal, dal.SubGlobPersistedConfig}); err != nil {
		_ = batch.Cancel()

		return fmt.Errorf("deleting persisted config: %w", err)
	}

	// Drop persisted bloom blocks. After an incremental restore they are stale
	// (they predate the logs RebuildDelta replayed into the attribute zone, so
	// they lack any post-checkpoint account), and their block layout is tied to
	// the source's bloom config — not necessarily the config of the cluster that
	// boots this data. Clearing them forces the booting node to rebuild the
	// bloom from a full attribute scan using its own config; otherwise
	// RestoreFromStore loads the stale blocks and post-checkpoint accounts get
	// bloom-false-negatived (read as {0,0}) on the apply path.
	if err := batch.DeleteRange(
		[]byte{dal.ZoneGlobal, dal.SubGlobBloom},
		[]byte{dal.ZoneGlobal, dal.SubGlobBloom + 1},
		pebble.NoSync,
	); err != nil {
		_ = batch.Cancel()

		return fmt.Errorf("deleting persisted bloom blocks: %w", err)
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
