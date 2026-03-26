package query

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble"

	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// ReadQueryCheckpoint reads a single query checkpoint by ID from Pebble.
// Returns nil if the checkpoint does not exist.
func ReadQueryCheckpoint(reader dal.PebbleReader, checkpointID uint64) (*raftcmdpb.QueryCheckpointState, error) {
	kb := dal.NewKeyBuilder()
	kb.PutByte(dal.KeyPrefixQueryCheckpoint)
	kb.PutUint64(checkpointID)
	key := kb.Build()

	val, closer, err := reader.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, nil
		}

		return nil, fmt.Errorf("reading query checkpoint %d: %w", checkpointID, err)
	}

	defer func() { _ = closer.Close() }()

	cp := &raftcmdpb.QueryCheckpointState{}
	if err := cp.UnmarshalVT(val); err != nil {
		return nil, fmt.Errorf("unmarshaling query checkpoint %d: %w", checkpointID, err)
	}

	return cp, nil
}

// ReadNextQueryCheckpointID reads the next checkpoint ID counter from Pebble.
// Returns 1 if no counter has been stored yet.
func ReadNextQueryCheckpointID(reader dal.PebbleReader) (uint64, error) {
	value, closer, err := reader.Get([]byte{dal.KeyPrefixNextQueryCheckpointID})
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return 1, nil
		}

		return 0, fmt.Errorf("getting next query checkpoint ID: %w", err)
	}

	defer func() { _ = closer.Close() }()

	return binary.BigEndian.Uint64(value[:8]), nil
}

// ListQueryCheckpoints reads all query checkpoints from Pebble, sorted by checkpoint ID ascending.
func ListQueryCheckpoints(reader dal.PebbleReader) ([]*raftcmdpb.QueryCheckpointState, error) {
	lowerBound := []byte{dal.KeyPrefixQueryCheckpoint}
	upperBound := []byte{dal.KeyPrefixQueryCheckpoint + 1}

	iter, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, fmt.Errorf("creating query checkpoint iterator: %w", err)
	}

	defer func() { _ = iter.Close() }()

	var checkpoints []*raftcmdpb.QueryCheckpointState

	for iter.First(); iter.Valid(); iter.Next() {
		val, err := iter.ValueAndErr()
		if err != nil {
			return nil, fmt.Errorf("reading query checkpoint value: %w", err)
		}

		cp := &raftcmdpb.QueryCheckpointState{}
		if err := cp.UnmarshalVT(val); err != nil {
			return nil, fmt.Errorf("unmarshaling query checkpoint: %w", err)
		}

		checkpoints = append(checkpoints, cp)
	}

	return checkpoints, nil
}
