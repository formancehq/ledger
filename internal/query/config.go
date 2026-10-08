package query

import (
	"fmt"

	commonpb "github.com/formancehq/ledger/pkg/client/v3/grpc"

	internalstatepb "github.com/formancehq/ledger/v3/internal/proto/internalstatepb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// ReadLastAppliedIndex returns the last applied Raft index from the given reader.
// Returns 0 if not found.
func ReadLastAppliedIndex(reader dal.PebbleGetter) (uint64, error) {
	return dal.ReadUint64(reader, []byte{dal.ZoneClusterPersistent, dal.SubGlobLastAppliedIndex}, 0)
}

// ReadLastAppliedTimestamp returns the last applied HLC timestamp (microseconds since epoch) from the given reader.
// Returns 0 if not found.
func ReadLastAppliedTimestamp(reader dal.PebbleGetter) (uint64, error) {
	return dal.ReadUint64(reader, []byte{dal.ZoneGlobal, dal.SubGlobLastAppliedTimestamp}, 0)
}

// ReadLastIdempotencyEvictionCutoff returns the high-water cutoff (wall-clock
// microseconds — the leader's eviction cutoff, not an HLC timestamp) of every
// applied idempotency eviction. Returns 0 (no eviction applied yet) if not found.
func ReadLastIdempotencyEvictionCutoff(reader dal.PebbleGetter) (uint64, error) {
	return dal.ReadUint64(reader, []byte{dal.ZoneGlobal, dal.SubGlobLastIdempotencyEvictionCutoff}, 0)
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
func ReadClusterState(reader dal.PebbleGetter) (*internalstatepb.PersistedClusterState, error) {
	state, err := dal.ReadProto[*internalstatepb.PersistedClusterState](reader, []byte{dal.ZoneGlobal, dal.SubGlobClusterConfig})
	if err != nil {
		return nil, fmt.Errorf("loading cluster state: %w", err)
	}

	return state, nil
}

// ReadAuditKey returns the replicated audit secret. Absence is valid only
// before the first audit entry; malformed rows always fail closed.
func ReadAuditKey(reader dal.PebbleGetter) ([]byte, error) {
	key, err := dal.GetValue(reader, []byte{dal.ZoneGlobal, dal.SubGlobAuditKey})
	if err != nil {
		return nil, fmt.Errorf("loading audit key: %w", err)
	}
	if key != nil && len(key) != 32 {
		return nil, fmt.Errorf("invalid audit key length %d", len(key))
	}

	return key, nil
}

// ReadClusterPolicy loads the replicated cluster policy from the given reader.
// Returns nil if the key does not exist (no policy committed yet).
func ReadClusterPolicy(reader dal.PebbleGetter) (*commonpb.ClusterPolicy, error) {
	policy, err := dal.ReadProto[*commonpb.ClusterPolicy](reader, []byte{dal.ZoneGlobal, dal.SubGlobClusterPolicy})
	if err != nil {
		return nil, fmt.Errorf("loading cluster policy: %w", err)
	}

	return policy, nil
}

// ReadPersistedConfig loads the persisted node/cluster configuration block
// from the given reader. Returns nil if the key does not exist (first boot).
//
// Lives in this leaf package — rather than internal/bootstrap — so that
// adapter and CLI code can read ClusterID from an opened store without
// pulling in the composition root (which would create an import cycle).
func ReadPersistedConfig(reader dal.PebbleGetter) (*internalstatepb.PersistedConfig, error) {
	cfg, err := dal.ReadProto[*internalstatepb.PersistedConfig](reader, []byte{dal.ZoneClusterPersistent, dal.SubGlobPersistedConfig})
	if err != nil {
		return nil, fmt.Errorf("loading persisted config: %w", err)
	}

	return cfg, nil
}
