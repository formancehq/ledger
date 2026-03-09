package query

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble"

	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// ReadLastAppliedIndex returns the last applied Raft index from the given reader.
// Returns 0 if not found.
func ReadLastAppliedIndex(reader dal.PebbleReader) (uint64, error) {
	get, closer, err := reader.Get([]byte{dal.KeyPrefixLastAppliedIndex})
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return 0, nil
		}

		return 0, err
	}

	defer func() {
		_ = closer.Close()
	}()

	if len(get) == 0 {
		return 0, nil
	}

	return binary.BigEndian.Uint64(get[:8]), nil
}

// ReadLastAppliedTimestamp returns the last applied HLC timestamp (microseconds since epoch) from the given reader.
// Returns 0 if not found.
func ReadLastAppliedTimestamp(reader dal.PebbleReader) (uint64, error) {
	get, closer, err := reader.Get([]byte{dal.KeyPrefixLastAppliedTimestamp})
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return 0, nil
		}

		return 0, err
	}

	defer func() {
		_ = closer.Close()
	}()

	if len(get) == 0 {
		return 0, nil
	}

	return binary.BigEndian.Uint64(get[:8]), nil
}

// ReadRaftIndexForSequence returns the raft index that produced (or contains)
// the given log sequence by performing a SeekLE on the seq→raftIndex mapping.
// Returns 0 if no mapping exists (fresh cluster or no logs yet).
func ReadRaftIndexForSequence(reader dal.PebbleReader, sequence uint64) (uint64, error) {
	// Build bounds: lower = [prefix][0x00..], upper = [prefix][sequence+1]
	// Then SeekToLast to find the largest firstSeq <= sequence.
	var lower [9]byte
	lower[0] = dal.KeyPrefixSeqToRaftIndex

	var upper [9]byte
	upper[0] = dal.KeyPrefixSeqToRaftIndex
	binary.BigEndian.PutUint64(upper[1:], sequence+1)

	iter, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: lower[:],
		UpperBound: upper[:],
	})
	if err != nil {
		return 0, fmt.Errorf("creating seq-to-raft-index iterator: %w", err)
	}

	defer func() { _ = iter.Close() }()

	if !iter.Last() {
		if err := iter.Error(); err != nil {
			return 0, fmt.Errorf("seeking seq-to-raft-index: %w", err)
		}

		return 0, nil // no mapping found
	}

	value, err := iter.ValueAndErr()
	if err != nil {
		return 0, fmt.Errorf("reading seq-to-raft-index value: %w", err)
	}

	if len(value) != 8 {
		return 0, fmt.Errorf("corrupt seq-to-raft-index value: expected 8 bytes, got %d", len(value))
	}

	return binary.BigEndian.Uint64(value), nil
}

// ReadMaintenanceMode loads the maintenance mode flag from the given reader.
// Returns false if the config key does not exist.
func ReadMaintenanceMode(reader dal.PebbleReader) (bool, error) {
	value, closer, err := reader.Get([]byte{dal.KeyPrefixMaintenanceMode})
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return false, nil
		}

		return false, fmt.Errorf("loading maintenance mode: %w", err)
	}

	defer func() { _ = closer.Close() }()

	if len(value) == 0 {
		return false, nil
	}

	return value[0] == 0x01, nil
}
