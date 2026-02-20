package data

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble"
)

// PersistedConfig stores critical configuration parameters that must not change
// between restarts with existing data. It is JSON-encoded at Pebble key 0xFE.
type PersistedConfig struct {
	NodeID       uint64 `json:"nodeId"`
	ClusterID    string `json:"clusterId"`
	AuditEnabled bool   `json:"auditEnabled"`
}

// ConfigMismatchError is returned when a persisted configuration value differs
// from the current startup configuration.
type ConfigMismatchError struct {
	Field      string
	Persisted  string
	Current    string
}

func (e *ConfigMismatchError) Error() string {
	return fmt.Sprintf(
		"configuration mismatch for %s: persisted=%s, current=%s (use --unsafe-skip-config-validation to override)",
		e.Field, e.Persisted, e.Current,
	)
}

// LoadPersistedConfig reads the persisted configuration from Pebble.
// Returns nil if no configuration has been persisted yet (first boot).
func (s *Store) LoadPersistedConfig() (*PersistedConfig, error) {
	value, closer, err := s.getDB().Get([]byte{keyPrefixPersistedConfig})
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

// DeletePersistedConfig removes the persisted configuration from the batch.
// This is used during backup compaction so the backup is portable to any cluster.
func (b *Batch) DeletePersistedConfig() error {
	if b.committed {
		return fmt.Errorf("batch already committed")
	}

	if err := b.batch.Delete([]byte{keyPrefixPersistedConfig}, pebble.NoSync); err != nil {
		return fmt.Errorf("deleting persisted config: %w", err)
	}
	return nil
}

// SavePersistedConfig writes the persisted configuration to the batch.
func (b *Batch) SavePersistedConfig(cfg *PersistedConfig) error {
	if b.committed {
		return fmt.Errorf("batch already committed")
	}

	value, err := json.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("marshaling persisted config: %w", err)
	}

	if err := b.batch.Set([]byte{keyPrefixPersistedConfig}, value, pebble.NoSync); err != nil {
		return fmt.Errorf("saving persisted config: %w", err)
	}
	return nil
}
