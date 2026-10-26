package state

import (
	"bytes"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/bloom"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func TestBloomProjectionMatchesAfterLocalConfigDrift(t *testing.T) {
	t.Parallel()
	committed := &commonpb.ClusterConfig{
		RotationThreshold: 1000,
		BloomVolumes:      &commonpb.BloomTypeConfig{ExpectedKeys: 1000, FpRate: 0.01},
	}
	local := []*commonpb.ClusterConfig{
		nil, // one node started with Bloom disabled
		{BloomVolumes: &commonpb.BloomTypeConfig{ExpectedKeys: 10, FpRate: 0.2}},
	}
	var projections [][]byte
	for _, localCfg := range local {
		machine, store, _ := newTestMachine(t)
		filters := bloom.NewFilterSet(localCfg, noop.NewMeterProvider().Meter("test"))
		machine.BloomFilters = filters
		machine.cacheSnapshotter.bloomFilters = filters
		batch := store.OpenWriteSession()
		require.NoError(t, machine.applyClusterConfig(batch, 1, committed))
		require.NoError(t, batch.Commit())

		// Recreate the local shape just before checkpoint recovery. Recovery
		// must discard it before the next committed entry mutates Bloom.
		filters.Rebuild(localCfg)
		require.NoError(t, NewRecovery(machine, store).RecoverState())
		require.True(t, filters.Enabled())
		filters.AddCanonicalKeys(&bloom.BloomUpdates{
			Volumes: []attributes.U128{attributes.HashU128([]byte("same-committed-key"))},
		})
		batch = store.OpenWriteSession()
		require.NoError(t, filters.PersistDirtyBlocks(batch))
		require.NoError(t, batch.Commit())
		projections = append(projections, bloomNamespaceBytes(t, store))
	}
	require.NotEmpty(t, projections[0])
	require.Equal(t, projections[0], projections[1])
}

func bloomNamespaceBytes(t *testing.T, store *dal.Store) []byte {
	t.Helper()
	handle, err := store.NewDirectReadHandle()
	require.NoError(t, err)
	defer func() { _ = handle.Close() }()
	iter, err := handle.NewIter(&pebble.IterOptions{
		LowerBound: []byte{dal.ZoneClusterPersistent, dal.SubGlobBloom},
		UpperBound: []byte{dal.ZoneClusterPersistent, dal.SubGlobBloom + 1},
	})
	require.NoError(t, err)
	defer func() { _ = iter.Close() }()
	var rows bytes.Buffer
	for iter.First(); iter.Valid(); iter.Next() {
		rows.Write(iter.Key())
		rows.Write(iter.Value())
	}
	require.NoError(t, iter.Error())

	return rows.Bytes()
}

func TestBloomCommittedEnableDisableAndRecovery(t *testing.T) {
	t.Parallel()
	machine, store, _ := newTestMachine(t)
	filters := bloom.NewFilterSet(nil, noop.NewMeterProvider().Meter("test"))
	machine.BloomFilters = filters
	machine.cacheSnapshotter.bloomFilters = filters
	require.False(t, filters.Enabled())

	enabled := &commonpb.ClusterConfig{
		RotationThreshold: machine.Registry.Cache.GenerationThreshold(),
		BloomVolumes:      &commonpb.BloomTypeConfig{ExpectedKeys: 1000, FpRate: 0.01},
	}
	batch := store.OpenWriteSession()
	require.NoError(t, machine.applyClusterConfig(batch, 1, enabled))
	require.NoError(t, batch.Commit())
	require.True(t, filters.Enabled(), "a committed update must enable a node started disabled")
	require.NotNil(t, filters.FilterForAttrType(dal.SubAttrVolume))

	// Simulate a follower whose in-memory shape came from different local
	// dimensions. Recovery must replace it with the committed shape.
	filters.Rebuild(&commonpb.ClusterConfig{
		BloomMetadata: &commonpb.BloomTypeConfig{ExpectedKeys: 10, FpRate: 0.1},
	})
	require.NoError(t, NewRecovery(machine, store).RecoverState())
	require.NotNil(t, filters.FilterForAttrType(dal.SubAttrVolume))
	require.Nil(t, filters.FilterForAttrType(dal.SubAttrMetadata))

	disabled := &commonpb.ClusterConfig{RotationThreshold: enabled.GetRotationThreshold()}
	batch = store.OpenWriteSession()
	require.NoError(t, machine.applyClusterConfig(batch, 2, disabled))
	require.NoError(t, batch.Commit())
	require.False(t, filters.Enabled(), "a committed update must disable without restart")
	require.NoError(t, NewRecovery(machine, store).RecoverState())
	require.False(t, filters.Enabled())
}
