package bootstrap

import (
	"fmt"
	"strconv"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// CurrentStorageSchemaVersion is the storage schema version that this binary
// expects. Increment this when the Pebble key layout or value encoding changes
// in a way that is not backward-compatible.
const CurrentStorageSchemaVersion uint32 = 1

// SchemaVersionError is returned when the persisted storage schema version is
// incompatible with the running binary. This is NOT bypassable with
// --unsafe-skip-config-validation because data corruption is certain.
type SchemaVersionError struct {
	Persisted uint32
	Current   uint32
	Downgrade bool // true when persisted > current
}

func (e *SchemaVersionError) Error() string {
	if e.Downgrade {
		return fmt.Sprintf(
			"cannot open storage at schema version %d: this binary supports up to version %d (downgrade not supported)",
			e.Persisted, e.Current,
		)
	}

	return fmt.Sprintf(
		"storage schema version %d is too old: this binary requires version %d (run the migration tool or use the matching binary version)",
		e.Persisted, e.Current,
	)
}

// ValidateOrPersistConfig checks that critical configuration parameters have not
// changed since the last boot. On first boot (no persisted config), it persists
// the current values. On subsequent boots, it compares node-id, cluster-id, and
// storage schema version and returns an error on mismatch unless force is true.
//
// Schema version mismatches are never bypassed by force — they indicate data
// incompatibility that would lead to corruption.
func ValidateOrPersistConfig(store *dal.Store, cfg Config, logger logging.Logger, force bool) error {
	persisted, err := LoadPersistedConfig(store)
	if err != nil {
		return fmt.Errorf("loading persisted config: %w", err)
	}

	current := &commonpb.PersistedConfig{
		NodeId:                cfg.RaftConfig.NodeID,
		ClusterId:             cfg.ClusterID,
		IdempotencyTtlSeconds: uint64(cfg.IdempotencyTTL.Seconds()),
		StorageSchemaVersion:  CurrentStorageSchemaVersion,
	}

	if persisted == nil {
		// First boot: persist current config
		logger.Infof("First boot detected, persisting configuration for future safety checks (schema version %d)", CurrentStorageSchemaVersion)

		return persistConfig(store, current)
	}

	// Backfill StorageSchemaVersion for configs persisted before this field existed.
	// Treat version 0 as version 1 (the schema that existed before versioning was added).
	needsBackfill := false

	if persisted.GetStorageSchemaVersion() == 0 {
		persisted.StorageSchemaVersion = 1
		needsBackfill = true

		logger.Infof("Backfilling storage-schema-version=1 into persisted config")
	}

	// Backfill IdempotencyTtlSeconds for configs persisted before this field existed.
	if persisted.GetIdempotencyTtlSeconds() == 0 && current.GetIdempotencyTtlSeconds() != 0 {
		persisted.IdempotencyTtlSeconds = current.GetIdempotencyTtlSeconds()
		needsBackfill = true

		logger.Infof("Backfilling idempotency-ttl-seconds=%d into persisted config", current.GetIdempotencyTtlSeconds())
	}

	if needsBackfill {
		if err := persistConfig(store, persisted); err != nil {
			return fmt.Errorf("backfilling persisted config: %w", err)
		}
	}

	// Schema version check — never bypassable, even with --unsafe-skip-config-validation.
	if persisted.GetStorageSchemaVersion() > current.GetStorageSchemaVersion() {
		return &SchemaVersionError{
			Persisted: persisted.GetStorageSchemaVersion(),
			Current:   current.GetStorageSchemaVersion(),
			Downgrade: true,
		}
	}

	if persisted.GetStorageSchemaVersion() < current.GetStorageSchemaVersion() {
		return &SchemaVersionError{
			Persisted: persisted.GetStorageSchemaVersion(),
			Current:   current.GetStorageSchemaVersion(),
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
