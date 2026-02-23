package application

import (
	"fmt"
	"strconv"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// ValidateOrPersistConfig checks that critical configuration parameters have not
// changed since the last boot. On first boot (no persisted config), it persists
// the current values. On subsequent boots, it compares node-id and cluster-id
// and returns an error on mismatch unless force is true.
func ValidateOrPersistConfig(store *dal.Store, cfg Config, logger logging.Logger, force bool) error {
	persisted, err := LoadPersistedConfig(store)
	if err != nil {
		return fmt.Errorf("loading persisted config: %w", err)
	}

	current := &PersistedConfig{
		NodeID:    cfg.RaftConfig.NodeID,
		ClusterID: cfg.ClusterID,
	}

	if persisted == nil {
		// First boot: persist current config
		logger.Infof("First boot detected, persisting configuration for future safety checks")
		return persistConfig(store, current)
	}

	// Subsequent boot: validate critical parameters
	var mismatches []*ConfigMismatchError

	if persisted.NodeID != current.NodeID {
		mismatches = append(mismatches, &ConfigMismatchError{
			Field:     "node-id",
			Persisted: strconv.FormatUint(persisted.NodeID, 10),
			Current:   strconv.FormatUint(current.NodeID, 10),
		})
	}

	if persisted.ClusterID != current.ClusterID {
		mismatches = append(mismatches, &ConfigMismatchError{
			Field:     "cluster-id",
			Persisted: persisted.ClusterID,
			Current:   current.ClusterID,
		})
	}

	if len(mismatches) > 0 {
		if force {
			for _, m := range mismatches {
				logger.WithFields(map[string]any{
					"field":     m.Field,
					"persisted": m.Persisted,
					"current":   m.Current,
				}).Infof("WARNING: configuration mismatch overridden by --unsafe-skip-config-validation")
			}
			// Force-overwrite persisted config
			return persistConfig(store, current)
		}
		// Return the first mismatch as a fatal error
		return mismatches[0]
	}

	return nil
}

// persistConfig writes the given configuration to Pebble.
func persistConfig(store *dal.Store, cfg *PersistedConfig) error {
	batch := store.NewBatch()
	if err := SavePersistedConfig(batch, cfg); err != nil {
		_ = batch.Cancel()
		return fmt.Errorf("saving persisted config: %w", err)
	}
	if err := batch.Commit(); err != nil {
		return fmt.Errorf("committing persisted config: %w", err)
	}
	return nil
}
