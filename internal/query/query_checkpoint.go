package query

import (
	"encoding/binary"
	"fmt"

	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// ReadQueryCheckpointRows returns every live query-checkpoint row keyed by its
// Pebble key id (the authoritative identity), for the checker to compare against
// the audit-derived set.
func ReadQueryCheckpointRows(reader dal.PebbleReader) (map[uint64]*raftcmdpb.QueryCheckpointState, error) {
	ids, err := ReadLiveQueryCheckpointIDs(reader)
	if err != nil {
		return nil, err
	}

	rows := make(map[uint64]*raftcmdpb.QueryCheckpointState, len(ids))

	for id := range ids {
		cp, err := ReadQueryCheckpoint(reader, id)
		if err != nil {
			return nil, err
		}

		rows[id] = cp
	}

	return rows, nil
}

// ReadLiveQueryCheckpointIDs returns the set of query-checkpoint IDs currently
// live in the store. Used at recovery to rehydrate FSMState so the FSM can
// enforce the checkpoint cap and reject deletes of non-live IDs without
// scanning Pebble on the apply path, by the checker to compare the stored rows
// against the audit chain, and by orphan reclamation.
//
// The ID is read from the Pebble KEY, never the payload's checkpoint_id: the key
// is what CreateQueryCheckpoint / DeleteQueryCheckpoint address, so it is the
// authoritative identity of the row.
func ReadLiveQueryCheckpointIDs(reader dal.PebbleReader) (map[uint64]struct{}, error) {
	iter, err := dal.NewBoundedIter(reader,
		[]byte{dal.ZoneGlobal, dal.SubGlobQueryCheckpoint},
		[]byte{dal.ZoneGlobal, dal.SubGlobQueryCheckpoint + 1})
	if err != nil {
		return nil, fmt.Errorf("iterating query checkpoints: %w", err)
	}

	defer func() { _ = iter.Close() }()

	ids := make(map[uint64]struct{})

	for iter.First(); iter.Valid(); iter.Next() {
		// Key layout: [ZoneGlobal][SubGlobQueryCheckpoint][checkpoint_id BE 8].
		key := iter.Key()
		if len(key) != 10 {
			return nil, fmt.Errorf("malformed query checkpoint key (len %d): % x", len(key), key)
		}

		ids[binary.BigEndian.Uint64(key[2:])] = struct{}{}
	}

	return ids, iter.Error()
}

// ReadQueryCheckpoint reads a single query checkpoint by ID from Pebble.
// Returns nil if the checkpoint does not exist.
func ReadQueryCheckpoint(reader dal.PebbleGetter, checkpointID uint64) (*raftcmdpb.QueryCheckpointState, error) {
	kb := dal.NewKeyBuilder()
	kb.PutZonePrefix(dal.ZoneGlobal, dal.SubGlobQueryCheckpoint)
	kb.PutUint64(checkpointID)

	cp, err := dal.ReadProto[*raftcmdpb.QueryCheckpointState](reader, kb.Build())
	if err != nil {
		return nil, fmt.Errorf("reading query checkpoint %d: %w", checkpointID, err)
	}

	return cp, nil
}

// ReadNextQueryCheckpointID reads the next checkpoint ID counter from Pebble.
// Returns 1 if no counter has been stored yet.
func ReadNextQueryCheckpointID(reader dal.PebbleGetter) (uint64, error) {
	v, err := dal.ReadUint64(reader, []byte{dal.ZoneGlobal, dal.SubGlobNextQueryCheckpointID}, 1)
	if err != nil {
		return 0, fmt.Errorf("getting next query checkpoint ID: %w", err)
	}

	return v, nil
}

// ReadQueryCheckpointSchedule loads the query checkpoint schedule cron expression from the given reader.
// Returns an empty string if no schedule is configured.
func ReadQueryCheckpointSchedule(reader dal.PebbleGetter) (string, error) {
	v, err := dal.ReadString(reader, []byte{dal.ZoneGlobal, dal.SubGlobQueryCheckpointSchedule})
	if err != nil {
		return "", fmt.Errorf("loading query checkpoint schedule: %w", err)
	}

	return v, nil
}

// ListQueryCheckpoints reads all query checkpoints from Pebble, sorted by checkpoint ID ascending.
func ListQueryCheckpoints(reader dal.PebbleReader) ([]*raftcmdpb.QueryCheckpointState, error) {
	checkpoints, err := dal.CollectZone[*raftcmdpb.QueryCheckpointState](reader, dal.ZoneGlobal, dal.SubGlobQueryCheckpoint)
	if err != nil {
		return nil, fmt.Errorf("listing query checkpoints: %w", err)
	}

	return checkpoints, nil
}
