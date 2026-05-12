package query

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble/v2"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
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

// ReadClusterState loads the persisted cluster state from the given reader.
// Returns nil if the key does not exist (first boot).
func ReadClusterState(reader dal.PebbleReader) (*commonpb.PersistedClusterState, error) {
	value, closer, err := reader.Get([]byte{dal.KeyPrefixClusterConfig})
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, nil
		}

		return nil, fmt.Errorf("loading cluster state: %w", err)
	}

	defer func() { _ = closer.Close() }()

	state := &commonpb.PersistedClusterState{}
	if err := state.UnmarshalVT(value); err != nil {
		return nil, fmt.Errorf("unmarshaling cluster state: %w", err)
	}

	return state, nil
}
