package query

import (
	"fmt"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// ReadLastAppliedIndex returns the last applied Raft index from the given reader.
// Returns 0 if not found.
func ReadLastAppliedIndex(reader dal.PebbleReader) (uint64, error) {
	return dal.ReadUint64(reader, []byte{dal.ZoneGlobal, dal.SubGlobLastAppliedIndex}, 0)
}

// ReadLastAppliedTimestamp returns the last applied HLC timestamp (microseconds since epoch) from the given reader.
// Returns 0 if not found.
func ReadLastAppliedTimestamp(reader dal.PebbleReader) (uint64, error) {
	return dal.ReadUint64(reader, []byte{dal.ZoneGlobal, dal.SubGlobLastAppliedTimestamp}, 0)
}

// ReadMaintenanceMode loads the maintenance mode flag from the given reader.
// Returns false if the config key does not exist.
func ReadMaintenanceMode(reader dal.PebbleReader) (bool, error) {
	v, err := dal.ReadBool(reader, []byte{dal.ZoneGlobal, dal.SubGlobMaintenanceMode})
	if err != nil {
		return false, fmt.Errorf("loading maintenance mode: %w", err)
	}

	return v, nil
}

// ReadClusterState loads the persisted cluster state from the given reader.
// Returns nil if the key does not exist (first boot).
func ReadClusterState(reader dal.PebbleReader) (*commonpb.PersistedClusterState, error) {
	state, err := dal.ReadProto[*commonpb.PersistedClusterState](reader, []byte{dal.ZoneGlobal, dal.SubGlobClusterConfig})
	if err != nil {
		return nil, fmt.Errorf("loading cluster state: %w", err)
	}

	return state, nil
}
