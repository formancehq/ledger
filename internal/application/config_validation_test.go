package application

import (
	"testing"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/service/node"
	"github.com/formancehq/ledger-v3-poc/internal/storage/data"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
)

func newTestStore(t *testing.T) *data.Store {
	t.Helper()
	dir := t.TempDir()
	logger := logging.Testing()
	meter := noop.Meter{}
	store, err := data.NewStore(dir, logger, meter, data.DefaultConfig())
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
		AuditEnabled: true,
	}

	err := ValidateOrPersistConfig(store, cfg, logger, false)
	require.NoError(t, err)

	// Verify config was persisted
	persisted, err := store.LoadPersistedConfig()
	require.NoError(t, err)
	require.NotNil(t, persisted)
	require.Equal(t, uint64(1), persisted.NodeID)
	require.Equal(t, "test-cluster", persisted.ClusterID)
	require.True(t, persisted.AuditEnabled)
}

func TestValidateOrPersistConfig_MatchingConfig(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)
	logger := logging.Testing()

	cfg := Config{
		RaftConfig: node.NodeConfig{NodeID: 42},
		ClusterID:  "my-cluster",
		AuditEnabled: true,
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

	var mismatchErr *data.ConfigMismatchError
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

	var mismatchErr *data.ConfigMismatchError
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
	persisted, err := store.LoadPersistedConfig()
	require.NoError(t, err)
	require.Equal(t, uint64(2), persisted.NodeID)
	require.Equal(t, "cluster-b", persisted.ClusterID)
}

func TestValidateOrPersistConfig_AuditDisabledWarning(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)
	logger := logging.Testing()

	cfg := Config{
		RaftConfig:   node.NodeConfig{NodeID: 1},
		ClusterID:    "test-cluster",
		AuditEnabled: true,
	}

	// First boot with audit enabled
	err := ValidateOrPersistConfig(store, cfg, logger, false)
	require.NoError(t, err)

	// Second boot with audit disabled — should warn but not fail
	cfg.AuditEnabled = false
	err = ValidateOrPersistConfig(store, cfg, logger, false)
	require.NoError(t, err)

	// Verify the config was updated
	persisted, err := store.LoadPersistedConfig()
	require.NoError(t, err)
	require.False(t, persisted.AuditEnabled)
}
