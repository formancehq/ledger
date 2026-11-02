package bootstrap

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func TestBloomBootUsesCommittedConfig(t *testing.T) {
	t.Parallel()
	meter := noop.NewMeterProvider()
	store, err := dal.NewStore(t.TempDir(), logging.Testing(), meter.Meter("test"), dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	filters, err := newCommittedBloomFilterSet(store, meter)
	require.NoError(t, err)
	require.False(t, filters.Enabled(), "a fresh node has no committed Bloom configuration")

	committed := &commonpb.ClusterConfig{
		BloomVolumes: &commonpb.BloomTypeConfig{ExpectedKeys: 1000, FpRate: 0.01},
	}
	batch := store.OpenWriteSession()
	require.NoError(t, batch.SetProto([]byte{dal.ZoneGlobal, dal.SubGlobClusterConfig}, &commonpb.PersistedClusterState{Config: committed}))
	require.NoError(t, batch.Commit())

	// The provider has no node-local configuration input: a restart adopts C1.
	filters, err = newCommittedBloomFilterSet(store, meter)
	require.NoError(t, err)
	require.True(t, filters.Enabled())
	require.NotNil(t, filters.FilterForAttrType(dal.SubAttrVolume))
	require.Nil(t, filters.FilterForAttrType(dal.SubAttrMetadata))
}
