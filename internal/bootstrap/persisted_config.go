package bootstrap

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble/v2"

	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// PersistedConfig stores critical configuration parameters that must not change
// between restarts with existing data. It is JSON-encoded at Pebble key 0xFE.
type PersistedConfig struct {
	NodeID                uint64 `json:"nodeId"`
	ClusterID             string `json:"clusterId"`
	IdempotencyTTLSeconds uint64 `json:"idempotencyTTLSeconds,omitempty"`
}

// ConfigMismatchError is returned when a persisted configuration value differs
// from the current startup configuration.
type ConfigMismatchError struct {
	Field     string
	Persisted string
	Current   string
}

func (e *ConfigMismatchError) Error() string {
	return fmt.Sprintf(
		"configuration mismatch for %s: persisted=%s, current=%s (use --unsafe-skip-config-validation to override)",
		e.Field, e.Persisted, e.Current,
	)
}

// LoadPersistedConfig reads the persisted configuration from Pebble.
// Returns nil if no configuration has been persisted yet (first boot).
func LoadPersistedConfig(reader dal.PebbleReader) (*PersistedConfig, error) {
	value, closer, err := reader.Get([]byte{dal.KeyPrefixPersistedConfig})
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, nil
		}

		return nil, fmt.Errorf("loading persisted config: %w", err)
	}

	defer func() { _ = closer.Close() }()

	var cfg PersistedConfig
	if err := json.Unmarshal(value, &cfg); err != nil {
		return nil, fmt.Errorf("unmarshaling persisted config: %w", err)
	}

	return &cfg, nil
}

// SavePersistedConfig writes the persisted configuration to the batch.
func SavePersistedConfig(b *dal.Batch, cfg *PersistedConfig) error {
	value, err := json.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("marshaling persisted config: %w", err)
	}

	return b.SetBytes([]byte{dal.KeyPrefixPersistedConfig}, value)
}
