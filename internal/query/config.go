package query

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble/v2"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// ReadLastAppliedIndex returns the last applied Raft index from the given reader.
// Returns 0 if not found.
func ReadLastAppliedIndex(reader dal.PebbleGetter) (uint64, error) {
	return dal.ReadUint64(reader, []byte{dal.ZoneGlobal, dal.SubGlobLastAppliedIndex}, 0)
}

// ReadLastAppliedTimestamp returns the last applied HLC timestamp (microseconds since epoch) from the given reader.
// Returns 0 if not found.
func ReadLastAppliedTimestamp(reader dal.PebbleGetter) (uint64, error) {
	return dal.ReadUint64(reader, []byte{dal.ZoneGlobal, dal.SubGlobLastAppliedTimestamp}, 0)
}

// ReadMaintenanceMode loads the maintenance mode flag from the given reader.
// Returns false if the config key does not exist.
func ReadMaintenanceMode(reader dal.PebbleGetter) (bool, error) {
	v, err := dal.ReadBool(reader, []byte{dal.ZoneGlobal, dal.SubGlobMaintenanceMode})
	if err != nil {
		return false, fmt.Errorf("loading maintenance mode: %w", err)
	}

	return v, nil
}

// ReadClusterState loads the persisted cluster state from the given reader.
// Returns nil if the key does not exist (first boot).
func ReadClusterState(reader dal.PebbleGetter) (*commonpb.PersistedClusterState, error) {
	state, err := dal.ReadProto[*commonpb.PersistedClusterState](reader, []byte{dal.ZoneGlobal, dal.SubGlobClusterConfig})
	if err != nil {
		return nil, fmt.Errorf("loading cluster state: %w", err)
	}

	return state, nil
}

// ReadPersistedConfig loads the persisted node/cluster configuration block
// from the given reader. Returns nil if the key does not exist (first boot).
//
// Lives in this leaf package — rather than internal/bootstrap — so that
// adapter and CLI code can read ClusterID from an opened store without
// pulling in the composition root (which would create an import cycle).
func ReadPersistedConfig(reader dal.PebbleGetter) (*commonpb.PersistedConfig, error) {
	cfg, err := dal.ReadProto[*commonpb.PersistedConfig](reader, []byte{dal.ZoneGlobal, dal.SubGlobPersistedConfig})
	if err != nil {
		return nil, fmt.Errorf("loading persisted config: %w", err)
	}

	return cfg, nil
}

// ReadFSMDigest loads the rolling cross-node FSM digest from the given
// reader. The persisted value layout is
// [appliedIndex BE 8][snapshotIndex BE 8][digest N]. Returns (0, 0, nil, nil)
// when the key does not exist (cluster has the deterministic-encoding flag
// OFF, or the first apply has not yet completed). Used by Recovery /
// Synchronizer to hydrate FSMState.LastFSMDigest at boot or after a snapshot
// install.
func ReadFSMDigest(reader dal.PebbleGetter) (appliedIndex, snapshotIndex uint64, digest []byte, err error) {
	value, closer, getErr := reader.Get([]byte{dal.ZoneGlobal, dal.SubGlobFSMDigest})
	if getErr != nil {
		if errors.Is(getErr, pebble.ErrNotFound) {
			return 0, 0, nil, nil
		}

		return 0, 0, nil, fmt.Errorf("loading fsm digest: %w", getErr)
	}

	defer func() { _ = closer.Close() }()

	// 8 (appliedIndex) + 8 (snapshotIndex) + at least 1 byte of digest.
	const minLen = 17

	if len(value) < minLen {
		return 0, 0, nil, fmt.Errorf("fsm digest value too short: %d bytes", len(value))
	}

	appliedIndex = binary.BigEndian.Uint64(value[0:8])
	snapshotIndex = binary.BigEndian.Uint64(value[8:16])
	digest = append([]byte(nil), value[16:]...)

	return appliedIndex, snapshotIndex, digest, nil
}
