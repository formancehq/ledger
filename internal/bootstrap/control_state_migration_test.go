package bootstrap

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func TestControlStateSchemaTwoMigration(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	store, err := dal.NewStore(directory, logging.Testing(), noop.Meter{}, dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	cfg := validBaseConfig()
	ordinary := &fakeAdmission{}
	reconcileClusterPolicy(t.Context(), ordinary, store, cfg, func() bool { return true }, logging.Testing())
	legacy := ordinary.lastPolicy
	legacy.CapabilityActivations = []*commonpb.CapabilityActivation{{Id: 1}, {Id: 2}}
	batch := store.OpenWriteSession()
	require.NoError(t, SavePersistedConfig(batch, &commonpb.PersistedConfig{NodeId: cfg.RaftConfig.NodeID, ClusterId: cfg.ClusterID, StorageSchemaVersion: 2}))
	require.NoError(t, batch.SetProto([]byte{dal.ZoneGlobal, dal.SubGlobClusterPolicy}, legacy))
	require.NoError(t, batch.Commit())
	wrong := cfg
	wrong.ClusterID += "-wrong"
	require.Error(t, ValidateOrPersistConfig(store, wrong, logging.Testing(), false))
	anchor, err := LoadPersistedConfig(store)
	require.NoError(t, err)
	require.Equal(t, uint32(2), anchor.GetStorageSchemaVersion())
	batch = store.OpenWriteSession()
	require.NoError(t, state.SaveClusterPolicy(batch, legacy))
	anchor.StorageSchemaVersion = 3
	require.NoError(t, SavePersistedConfig(batch, anchor))
	require.NoError(t, batch.Cancel())
	require.NoError(t, store.Close())
	store, err = dal.NewStore(directory, logging.Testing(), noop.Meter{}, dal.DefaultConfig())
	require.NoError(t, err)
	require.NoError(t, ValidateOrPersistConfig(store, cfg, logging.Testing(), false))
	stored, err := query.ReadClusterPolicy(store)
	require.NoError(t, err)
	require.Equal(t, legacy.GetRevision(), stored.GetRevision())
	require.Equal(t, legacy.GetQueryCheckpointLimit(), stored.GetQueryCheckpointLimit())
	require.True(t, legacy.EqualVT(stored), "migration preserves the logical state at the applied index")
	require.Len(t, stored.GetCapabilityActivations(), 2)
	require.False(t, stored.GetCapabilityActivations()[0].GetActive())
	require.False(t, stored.GetCapabilityActivations()[1].GetRequested())
	old, err := dal.ReadProto[*commonpb.ClusterPolicy](store, []byte{dal.ZoneGlobal, dal.SubGlobClusterPolicy})
	require.NoError(t, err)
	require.Nil(t, old)
	anchor, err = LoadPersistedConfig(store)
	require.NoError(t, err)
	require.Equal(t, uint32(3), anchor.GetStorageSchemaVersion())
	require.NoError(t, store.Close())
	store, err = dal.NewStore(directory, logging.Testing(), noop.Meter{}, dal.DefaultConfig())
	require.NoError(t, err)
	require.NoError(t, ValidateOrPersistConfig(store, cfg, logging.Testing(), false))
	batch = store.OpenWriteSession()
	require.NoError(t, batch.DeleteKey([]byte{dal.ZoneGlobal, dal.SubGlobCapabilityState}))
	require.NoError(t, batch.Commit())
	require.ErrorContains(t, ValidateOrPersistConfig(store, cfg, logging.Testing(), true), "incomplete")
}
