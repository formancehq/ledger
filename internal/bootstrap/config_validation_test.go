package bootstrap

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func newTestStore(t *testing.T) *dal.Store {
	t.Helper()
	dir := t.TempDir()
	logger := logging.Testing()
	meter := noop.Meter{}
	store, err := dal.NewStore(dir, logger, meter, dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})

	return store
}

func TestValidateOrPersistConfig_FirstBoot(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)
	logger := logging.Testing()

	cfg := Config{
		RaftConfig: node.NodeConfig{NodeID: 1},
		ClusterID:  "test-cluster",
	}

	err := ValidateOrPersistConfig(store, cfg, logger, false)
	require.NoError(t, err)

	// Verify config was persisted with schema version
	persisted, err := LoadPersistedConfig(store)
	require.NoError(t, err)
	require.NotNil(t, persisted)
	require.Equal(t, uint64(1), persisted.GetNodeId())
	require.Equal(t, "test-cluster", persisted.GetClusterId())
	require.Equal(t, CurrentStorageSchemaVersion, persisted.GetStorageSchemaVersion())
}

func TestValidateOrPersistConfig_MatchingConfig(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)
	logger := logging.Testing()

	cfg := Config{
		RaftConfig: node.NodeConfig{NodeID: 42},
		ClusterID:  "my-cluster",
	}

	// First boot
	err := ValidateOrPersistConfig(store, cfg, logger, false)
	require.NoError(t, err)

	// Second boot with same config
	err = ValidateOrPersistConfig(store, cfg, logger, false)
	require.NoError(t, err)
}

func TestValidateOrPersistConfig_NodeIDMismatch(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)
	logger := logging.Testing()

	cfg := Config{
		RaftConfig: node.NodeConfig{NodeID: 1},
		ClusterID:  "test-cluster",
	}

	// First boot
	err := ValidateOrPersistConfig(store, cfg, logger, false)
	require.NoError(t, err)

	// Second boot with different node-id
	cfg.RaftConfig.NodeID = 2
	err = ValidateOrPersistConfig(store, cfg, logger, false)
	require.Error(t, err)

	var mismatchErr *ConfigMismatchError
	require.ErrorAs(t, err, &mismatchErr)
	require.Equal(t, "node-id", mismatchErr.Field)
	require.Equal(t, "1", mismatchErr.Persisted)
	require.Equal(t, "2", mismatchErr.Current)
}

func TestValidateOrPersistConfig_ClusterIDMismatch(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)
	logger := logging.Testing()

	cfg := Config{
		RaftConfig: node.NodeConfig{NodeID: 1},
		ClusterID:  "cluster-a",
	}

	// First boot
	err := ValidateOrPersistConfig(store, cfg, logger, false)
	require.NoError(t, err)

	// Second boot with different cluster-id
	cfg.ClusterID = "cluster-b"
	err = ValidateOrPersistConfig(store, cfg, logger, false)
	require.Error(t, err)

	var mismatchErr *ConfigMismatchError
	require.ErrorAs(t, err, &mismatchErr)
	require.Equal(t, "cluster-id", mismatchErr.Field)
	require.Equal(t, "cluster-a", mismatchErr.Persisted)
	require.Equal(t, "cluster-b", mismatchErr.Current)
}

func TestValidateOrPersistConfig_ForceOverride(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)
	logger := logging.Testing()

	cfg := Config{
		RaftConfig: node.NodeConfig{NodeID: 1},
		ClusterID:  "cluster-a",
	}

	// First boot
	err := ValidateOrPersistConfig(store, cfg, logger, false)
	require.NoError(t, err)

	// Second boot with different values but force=true
	cfg.RaftConfig.NodeID = 2
	cfg.ClusterID = "cluster-b"
	err = ValidateOrPersistConfig(store, cfg, logger, true)
	require.NoError(t, err)

	// Verify config was overwritten
	persisted, err := LoadPersistedConfig(store)
	require.NoError(t, err)
	require.Equal(t, uint64(2), persisted.GetNodeId())
	require.Equal(t, "cluster-b", persisted.GetClusterId())
}

func TestValidateOrPersistConfig_SchemaVersionBackfill(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)
	logger := logging.Testing()

	// Simulate a pre-versioning persisted config (schema_version == 0).
	batch := store.OpenWriteSession()
	require.NoError(t, SavePersistedConfig(batch, &commonpb.PersistedConfig{
		NodeId:    1,
		ClusterId: "test",
	}))
	require.NoError(t, batch.Commit())

	// Boot with current code should backfill to version 1 and succeed.
	cfg := Config{
		RaftConfig: node.NodeConfig{NodeID: 1},
		ClusterID:  "test",
	}
	err := ValidateOrPersistConfig(store, cfg, logger, false)
	require.NoError(t, err)

	persisted, err := LoadPersistedConfig(store)
	require.NoError(t, err)
	require.Equal(t, uint32(1), persisted.GetStorageSchemaVersion())
}

func TestValidateOrPersistConfig_SchemaVersionTooNew(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)
	logger := logging.Testing()

	// Persist a schema version higher than what the code supports (simulate downgrade).
	batch := store.OpenWriteSession()
	require.NoError(t, SavePersistedConfig(batch, &commonpb.PersistedConfig{
		NodeId:               1,
		ClusterId:            "test",
		StorageSchemaVersion: CurrentStorageSchemaVersion + 1,
	}))
	require.NoError(t, batch.Commit())

	cfg := Config{
		RaftConfig: node.NodeConfig{NodeID: 1},
		ClusterID:  "test",
	}
	err := ValidateOrPersistConfig(store, cfg, logger, false)
	require.Error(t, err)

	var schemaErr *SchemaVersionError
	require.ErrorAs(t, err, &schemaErr)
	require.True(t, schemaErr.Downgrade)
	require.Equal(t, CurrentStorageSchemaVersion+1, schemaErr.Persisted)
	require.Equal(t, CurrentStorageSchemaVersion, schemaErr.Current)

	// Force flag must NOT bypass schema version errors.
	err = ValidateOrPersistConfig(store, cfg, logger, true)
	require.Error(t, err)
	require.ErrorAs(t, err, &schemaErr)
}

func TestHealthThresholdsHysteresisValidation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		block, resume float64
		wantErr       bool
	}{
		{"valid", 0.8, 0.75, false},
		{"valid low block", 0.7, 0.65, false},
		{"resume equals block", 0.8, 0.8, true},
		{"resume above block", 0.8, 0.85, true},
		{"resume negative", 0.8, -0.1, true},
		{"resume zero", 0.8, 0, true},
		{"block above one", 1.5, 0.75, true},
		{"block zero", 0, 0, true},
		{"block NaN", math.NaN(), 0.75, true},
		{"resume NaN", 0.8, math.NaN(), true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := validateHealthThresholds(tc.block, tc.resume)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateOrPersistConfig_SchemaVersionTooOld(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)
	logger := logging.Testing()

	// Persist a schema version lower than current (simulate upgrade without migration).
	batch := store.OpenWriteSession()
	require.NoError(t, SavePersistedConfig(batch, &commonpb.PersistedConfig{
		NodeId:               1,
		ClusterId:            "test",
		StorageSchemaVersion: 1,
	}))
	require.NoError(t, batch.Commit())

	// This test only fails when CurrentStorageSchemaVersion > 1.
	// With version 1, persisted == current so it passes. We still keep
	// this test to catch regressions when the version is bumped.
	cfg := Config{
		RaftConfig: node.NodeConfig{NodeID: 1},
		ClusterID:  "test",
	}

	if CurrentStorageSchemaVersion > 1 {
		err := ValidateOrPersistConfig(store, cfg, logger, false)
		require.Error(t, err)

		var schemaErr *SchemaVersionError
		require.ErrorAs(t, err, &schemaErr)
		require.False(t, schemaErr.Downgrade)
		require.Equal(t, uint32(1), schemaErr.Persisted)
		require.Equal(t, CurrentStorageSchemaVersion, schemaErr.Current)

		// Force flag must NOT bypass schema version errors.
		err = ValidateOrPersistConfig(store, cfg, logger, true)
		require.Error(t, err)
		require.ErrorAs(t, err, &schemaErr)
	} else {
		// Version 1 == current: should pass.
		err := ValidateOrPersistConfig(store, cfg, logger, false)
		require.NoError(t, err)
	}
}

func TestValidateOrPersistConfig_FSMDeterminismMismatch(t *testing.T) {
	t.Parallel()

	type tc struct {
		name          string
		firstBootFlag bool
		secondFlag    bool
		wantMismatch  bool
	}

	cases := []tc{
		{"both-off", false, false, false},
		{"both-on", true, true, false},
		{"off-to-on", false, true, true},
		{"on-to-off", true, false, true},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			store := newTestStore(t)
			logger := logging.Testing()

			cfg := Config{
				RaftConfig:            node.NodeConfig{NodeID: 1},
				ClusterID:             "test-cluster",
				FSMDeterminismEnabled: c.firstBootFlag,
			}
			require.NoError(t, ValidateOrPersistConfig(store, cfg, logger, false))

			cfg.FSMDeterminismEnabled = c.secondFlag
			err := ValidateOrPersistConfig(store, cfg, logger, false)

			if !c.wantMismatch {
				require.NoError(t, err)

				return
			}

			require.Error(t, err)
			var mismatchErr *FSMDeterminismMismatchError
			require.ErrorAs(t, err, &mismatchErr)
			require.Equal(t, c.firstBootFlag, mismatchErr.Persisted)
			require.Equal(t, c.secondFlag, mismatchErr.Current)

			// Force flag must NOT bypass FSM determinism errors.
			err = ValidateOrPersistConfig(store, cfg, logger, true)
			require.Error(t, err)
			require.ErrorAs(t, err, &mismatchErr)
		})
	}
}
