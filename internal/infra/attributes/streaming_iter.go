package attributes

import (
	"fmt"

	"github.com/cockroachdb/pebble/v2"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// StreamingIter is a pull-based iterator over computed attribute entries.
// It wraps accumulatorBase with a pull API so callers can drive iteration
// externally — essential for the 3-way merge in the checker.
//
// Usage:
//
//	iter, err := attr.NewStreamingIter(reader, prefix)
//	if err != nil { return err }
//	defer iter.Close()
//	for iter.Next() {
//	    entry := iter.Entry()
//	    // use entry.CanonicalKey and entry.Value
//	}
//	if err := iter.Err(); err != nil { return err }
type StreamingIter[V proto.Message] struct {
	ab         accumulatorBase[V]
	iter       *pebble.Iterator
	started    bool
	flushed    bool
	current    *ComputedEntry[V]
	err        error
	minKeyLen  int
	attrPrefix byte
}

// NewStreamingIter creates a pull-based iterator over computed entries for all
// canonical keys sharing the given prefix. Pass nil for the full attribute space.
// Thread-safe: allocates its own iterator and buffer for concurrent access.
func (a *Attribute[V]) NewStreamingIter(reader dal.PebbleReader, canonicalPrefix []byte) (*StreamingIter[V], error) {
	lowerBound := make([]byte, 1+len(canonicalPrefix))
	lowerBound[0] = dal.KeyPrefixAttributes
	copy(lowerBound[1:], canonicalPrefix)

	var upperBound []byte
	if incPrefix := IncrementBytes(canonicalPrefix); incPrefix != nil {
		upperBound = make([]byte, 1+len(incPrefix))
		upperBound[0] = dal.KeyPrefixAttributes
		copy(upperBound[1:], incPrefix)
	} else {
		upperBound = []byte{dal.KeyPrefixAttributes + 1}
	}

	iter, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, fmt.Errorf("creating iterator for streaming scan: %w", err)
	}

	return &StreamingIter[V]{
		ab:   accumulatorBase[V]{attr: a},
		iter: iter,
		// Minimum key length: [0xF1 prefix][at least 1 byte canonical][attrType] = 3
		minKeyLen:  1 + SuffixLen,
		attrPrefix: a.prefix,
	}, nil
}

// Next advances the iterator to the next computed entry.
// Returns true if an entry is available via Entry(), false when exhausted or on error.
func (si *StreamingIter[V]) Next() bool {
	if si.err != nil || si.flushed {
		return false
	}

	for {
		var valid bool
		if !si.started {
			si.started = true
			valid = si.iter.First()
		} else {
			valid = si.iter.Next()
		}

		if !valid {
			if err := si.iter.Error(); err != nil {
				si.err = err

				return false
			}

			// Flush the last canonical key
			si.flushed = true
			if entry := si.ab.flush(); entry != nil {
				si.current = entry

				return true
			}

			return false
		}

		key := si.iter.Key()
		if len(key) <= si.minKeyLen {
			continue
		}

		// Check attribute type from the key suffix before reading the value.
		// This avoids fetching (and decompressing) value data for entries
		// belonging to other attribute types — critical when the attributes
		// zone contains millions of keys across many types.
		if key[len(key)-SuffixLen] != si.attrPrefix {
			continue
		}

		valueBytes, err := si.iter.ValueAndErr()
		if err != nil {
			si.err = fmt.Errorf("reading value: %w", err)

			return false
		}

		_, prev, err := si.ab.feed(key, valueBytes)
		if err != nil {
			si.err = err

			return false
		}

		if prev != nil {
			si.current = prev

			return true
		}
	}
}

// Entry returns the current computed entry. Only valid after Next() returns true.
func (si *StreamingIter[V]) Entry() ComputedEntry[V] {
	return *si.current
}

// Err returns the first error encountered during iteration.
func (si *StreamingIter[V]) Err() error {
	return si.err
}

// Close releases the underlying Pebble iterator.
func (si *StreamingIter[V]) Close() error {
	return si.iter.Close()
}
