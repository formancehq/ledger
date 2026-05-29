package query

import (
	"fmt"

	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

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
