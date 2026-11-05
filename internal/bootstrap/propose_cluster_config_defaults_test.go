package bootstrap

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric/noop"
	"google.golang.org/protobuf/proto"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/cache"
	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/infra/plan"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/futures"
	"github.com/formancehq/ledger/v3/internal/pkg/signal"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// Capture at the Raft boundary after the real builder has serialized the command.
type clusterConfigCapture struct {
	data []byte
}

func (p *clusterConfigCapture) Propose(_ context.Context, proposal *node.Proposal) (*futures.Future[state.ApplyResult], error) {
	p.data = append([]byte(nil), proposal.Data()...)
	proposal.Resolve(nil, nil)
	result := futures.New[state.ApplyResult]()
	result.Resolve(state.ApplyResult{}, nil)

	return result, nil
}

func TestProposeClusterConfig_DefaultThreshold(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		raw  uint64
		want uint64
	}{
		{name: "zero sentinel", raw: 0, want: 1000},
		{name: "omitted CLI default", raw: 1000, want: 1000},
		{name: "custom", raw: 42, want: 42},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := validBaseConfig()
			cfg.RaftConfig.RotationThreshold = tc.raw
			cfg.RaftConfig.WalDir = t.TempDir()
			cfg.BloomConfig = &commonpb.ClusterConfig{}
			require.NoError(t, cfg.Validate())
			nodeCfg, err := provideNodeConfig(cfg)
			require.NoError(t, err)
			require.Equal(t, tc.want, nodeCfg.RotationThreshold)
			meters := noop.NewMeterProvider()
			c, err := cache.New(nodeCfg.RotationThreshold, meters.Meter("test"))
			require.NoError(t, err)
			require.Equal(t, tc.want, c.GenerationThreshold())
			store := newTestStore(t)
			logger := logging.Testing()
			attrs := attributes.New()
			builder := plan.NewBuilder(node.NewIndexTracker(1), c, attrs, store, nil, logger, 0)
			proposer := &clusterConfigCapture{}
			proposeClusterConfigIfNeeded(t.Context(), proposer, builder, store, cfg, logger)
			require.NotEmpty(t, proposer.data)
			captured := &raftcmdpb.Proposal{}
			require.NoError(t, captured.UnmarshalVT(proposer.data))
			require.Len(t, captured.GetBatch().GetTechnicalUpdates(), 1)
			require.Equal(t, tc.want, captured.GetBatch().GetTechnicalUpdates()[0].GetClusterConfig().GetRotationThreshold())
			require.Equal(t, tc.raw, cfg.RaftConfig.RotationThreshold, "defaulting must stay local to the desired proposal")
			require.Zero(t, cfg.BloomConfig.GetRotationThreshold(), "shared Bloom config must remain untouched")

			// Apply the actual serialized bytes, without patching the payload or
			// bypassing attribution, coverage, or stale-proposal validation.
			registry := state.NewStateRegistry(c, attrs)
			machine, err := state.NewMachine(logger, registry, state.NewCacheSnapshotter(logger, registry, nil), store,
				dal.NewSentinelFactory(store, false), meters, nil, state.NewSharedState(), signal.NewNotifications(), nil,
				"threshold-test", 0, func(*raftpb.Entry, *dal.WriteSession) error { return nil })
			require.NoError(t, err)
			t.Cleanup(machine.Close)
			result, err := machine.ApplyEntries(t.Context(), store, &raftpb.Entry{
				Type: raftpb.EntryNormal.Enum(), Index: proto.Uint64(1), Term: proto.Uint64(1), Data: proposer.data,
			})
			require.NoError(t, err)
			require.Len(t, result.Results, 1)
			require.NoError(t, result.Results[0].Error)
			require.Equal(t, tc.want, c.GenerationThreshold())
			persisted, err := query.ReadClusterState(store)
			require.NoError(t, err)
			require.Equal(t, tc.want, persisted.GetConfig().GetRotationThreshold())
		})
	}
}

func TestProposeClusterConfig_ZeroMatchesCommittedDefault(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)
	seedClusterState(t, store, &commonpb.ClusterConfig{RotationThreshold: 1000})
	proposer := &clusterConfigCapture{}
	proposeClusterConfigIfNeeded(t.Context(), proposer, nil, store,
		Config{BloomConfig: &commonpb.ClusterConfig{}}, logging.Testing())
	require.Empty(t, proposer.data, "zero must compare as 1000 without building a redundant proposal")
}
