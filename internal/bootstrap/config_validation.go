package bootstrap

import (
	"fmt"
	"strconv"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
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

	current := &commonpb.PersistedConfig{
		NodeId:                cfg.RaftConfig.NodeID,
		ClusterId:             cfg.ClusterID,
		IdempotencyTtlSeconds: uint64(cfg.IdempotencyTTL.Seconds()),
	}

	if persisted == nil {
		// First boot: persist current config
		logger.Infof("First boot detected, persisting configuration for future safety checks")

		return persistConfig(store, current)
	}

	// Backfill IdempotencyTtlSeconds for configs persisted before this field existed.
	if persisted.GetIdempotencyTtlSeconds() == 0 && current.GetIdempotencyTtlSeconds() != 0 {
		persisted.IdempotencyTtlSeconds = current.GetIdempotencyTtlSeconds()

		logger.Infof("Backfilling idempotency-ttl-seconds=%d into persisted config", current.GetIdempotencyTtlSeconds())

		if err := persistConfig(store, persisted); err != nil {
			return fmt.Errorf("backfilling persisted config: %w", err)
		}
	}

	// Subsequent boot: validate critical parameters
	var mismatches []*ConfigMismatchError

	if persisted.GetNodeId() != current.GetNodeId() {
		mismatches = append(mismatches, &ConfigMismatchError{
			Field:     "node-id",
			Persisted: strconv.FormatUint(persisted.GetNodeId(), 10),
			Current:   strconv.FormatUint(current.GetNodeId(), 10),
		})
	}

	if persisted.GetClusterId() != current.GetClusterId() {
		mismatches = append(mismatches, &ConfigMismatchError{
			Field:     "cluster-id",
			Persisted: persisted.GetClusterId(),
			Current:   current.GetClusterId(),
		})
	}

	// IdempotencyTtlSeconds: only validate if persisted value is non-zero (backward compat).
	if persisted.GetIdempotencyTtlSeconds() != 0 && persisted.GetIdempotencyTtlSeconds() != current.GetIdempotencyTtlSeconds() {
		mismatches = append(mismatches, &ConfigMismatchError{
			Field:     "idempotency-ttl",
			Persisted: strconv.FormatUint(persisted.GetIdempotencyTtlSeconds(), 10) + "s",
			Current:   strconv.FormatUint(current.GetIdempotencyTtlSeconds(), 10) + "s",
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
func persistConfig(store *dal.Store, cfg *commonpb.PersistedConfig) error {
	batch := store.NewBatch()

	err := SavePersistedConfig(batch, cfg)
	if err != nil {
		_ = batch.Cancel()

		return fmt.Errorf("saving persisted config: %w", err)
	}

	err = batch.Commit()
	if err != nil {
		return fmt.Errorf("committing persisted config: %w", err)
	}

	return nil
}
