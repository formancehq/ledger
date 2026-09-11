package mirror

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/mock/gomock"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	v2 "github.com/formancehq/ledger/v3/internal/adapter/v2"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/crypto/keystore"
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

// applyingMirrorProposer replaces only Raft transport: the bytes produced by
// Builder.Run are decoded and applied by the real FSM, including rotation,
// preload, coverage gates and durable writes.
type applyingMirrorProposer struct {
	t           *testing.T
	machine     *state.Machine
	store       *dal.Store
	tracker     *node.IndexTracker
	beforeApply func(*raftcmdpb.Proposal)
}

func (p *applyingMirrorProposer) Propose(ctx context.Context, proposal *node.Proposal) (*futures.Future[state.ApplyResult], error) {
	cmd := &raftcmdpb.Proposal{}
	require.NoError(p.t, cmd.UnmarshalVT(proposal.Data()))
	require.Equal(p.t, p.tracker.Next(), cmd.GetPredictedIndex())
	if p.beforeApply != nil {
		p.beforeApply(cmd)
	}
	result, err := p.machine.ApplyEntries(ctx, p.store, &raftpb.Entry{Index: new(p.tracker.Next()), Term: new(uint64(1)), Type: new(raftpb.EntryNormal), Data: proposal.Data()})
	require.NoError(p.t, err)
	require.Len(p.t, result.Results, 1)
	require.NoError(p.t, result.Results[0].Error)
	p.tracker.Increment(1)
	proposal.Resolve(nil, nil)
	future := futures.New[state.ApplyResult]()
	future.Resolve(result.Results[0], nil)

	return future, nil
}

func TestWorker_RotationPreloadsReadOnlyLedger(t *testing.T) {
	t.Parallel()
	for _, idle := range []bool{false, true} {
		name := "ingest"
		if idle {
			name = "standalone_sync"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctx := logging.TestingContext()
			logger := logging.FromContext(ctx)
			meters := noop.NewMeterProvider()
			store, err := dal.NewStore(t.TempDir(), logger, meters.Meter("test"), dal.DefaultConfig())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, store.Close()) })
			attrs := attributes.New()
			c, err := cache.New(1, meters.Meter("test"))
			require.NoError(t, err)
			registry := state.NewStateRegistry(c, attrs)
			notifier := signal.NewNotifications()
			machine, err := state.NewMachine(logger, registry, state.NewCacheSnapshotter(logger, registry, nil), store, dal.NewSentinelFactory(store, false), meters, keystore.NewKeyStore(), state.NewSharedState(), notifier, nil, "test-cluster", 0, func(*raftpb.Entry, *dal.WriteSession) error { return nil })
			require.NoError(t, err)
			require.NoError(t, state.NewRecovery(machine, store).RecoverState())
			key := domain.LedgerKey{Name: "mirrored"}
			info := &commonpb.LedgerInfo{Name: key.Name, Mode: commonpb.LedgerMode_LEDGER_MODE_MIRROR}
			batch := store.OpenWriteSession()
			_, identity, err := registry.Ledgers.PutWithCache(batch, 0, key.Bytes(), info)
			require.NoError(t, err)
			_, _, err = registry.Boundaries.PutWithCache(batch, 0, key.Bytes(), &raftcmdpb.LedgerBoundaries{NextTransactionId: 1})
			require.NoError(t, err)
			require.NoError(t, batch.Commit())
			// Begin at the last index before the first rotation. Both rotations
			// below are driven by real worker proposals.
			_, err = machine.ApplyEntries(ctx, store, &raftpb.Entry{Index: new(uint64(1)), Term: new(uint64(1)), Type: new(raftpb.EntryNormal)})
			require.NoError(t, err)
			tracker := node.NewIndexTracker(2)
			builder := plan.NewBuilder(tracker, c, attrs, store, nil, logger, 0)
			proposed := 0
			proposer := &applyingMirrorProposer{t: t, machine: machine, store: store, tracker: tracker, beforeApply: func(cmd *raftcmdpb.Proposal) {
				proposed++
				require.Equal(t, uint64(proposed+1), cmd.GetPredictedIndex())
				found := false
				for _, entry := range cmd.GetExecutionPlan().GetAttributes() {
					if entry.GetAttrCode() == uint32(dal.SubAttrLedger) {
						require.Equal(t, identity.ID[:], entry.GetId().GetId())
						if proposed == 1 {
							require.Nil(t, entry.GetValue(), "Gen0 survives the first rotation without a seed")
							found = true

							continue
						}
						require.NotNil(t, entry.GetValue(), "Gen1-only ledger must be loaded from durable storage before rotation")
						seeded := &commonpb.LedgerInfo{}
						require.NoError(t, seeded.UnmarshalVT(entry.GetValue().GetRawValue()))
						require.Equal(t, info.GetName(), seeded.GetName())
						require.Equal(t, info.GetMode(), seeded.GetMode())
						found = true
					}
				}
				require.True(t, found)
				require.NotEmpty(t, cmd.GetTechnicalUpdates()[0].GetCoverageBits())
				if idle {
					require.Empty(t, cmd.GetOrders())
				} else {
					require.Len(t, cmd.GetOrders(), 1)
					require.NotEmpty(t, cmd.GetOrders()[0].GetTechnical().GetCoverageBits())
				}
			}}
			source := v2.NewMockSource(gomock.NewController(t))
			var logs []v2.V2Log
			if !idle {
				logs = []v2.V2Log{{ID: 1, Type: "SET_METADATA", Date: "2023-11-14T22:13:20Z", Data: []byte(`{"targetType":"ACCOUNT","targetId":"users:001","metadata":{"role":"admin"}}`)}}
			}
			source.EXPECT().FetchLogs(gomock.Any(), uint64(0), gomock.Any()).Return(logs, false, nil)
			var nextLogs []v2.V2Log
			afterID := uint64(0)
			if !idle {
				nextLogs = append([]v2.V2Log(nil), logs...)
				nextLogs[0].ID = 2
				afterID = 1
			}
			source.EXPECT().FetchLogs(gomock.Any(), afterID, gomock.Any()).Return(nextLogs, false, nil)
			worker := newWorkerWithProposer(t, key.Name, source, store, builder, proposer)
			worker.sourceHeadObserved = true
			worker.sourceLogCount = 1
			more, err := worker.processBatch(ctx)
			require.NoError(t, err)
			require.False(t, more)
			require.Equal(t, 1, proposed)
			_, present := c.Ledgers.Gen0().Get(identity.ID)
			require.False(t, present, "read-only ingest and sync must leave the ledger Gen1-only after the first rotation")
			_, present = c.Ledgers.Gen1().Get(identity.ID)
			require.True(t, present)
			require.Equal(t, cache.CacheMiss, c.Ledgers.CheckCache(3, identity.ID), "the predicted second rotation requires a seed")
			worker.sourceLogCount = 2
			more, err = worker.processBatch(ctx)
			require.NoError(t, err)
			require.False(t, more)
			require.Equal(t, 2, proposed)
			require.Equal(t, uint64(2), c.CurrentGeneration())
			require.True(t, worker.statusClearConfirmed)
			require.Equal(t, uint64(2), worker.lastPublishedSourceHead)
			progress, err := query.ReadMirrorSyncProgress(ctx, store, attrs.Boundary, key.Name)
			require.NoError(t, err)
			require.Equal(t, uint64(2), progress.GetSourceLogCount())
			require.Nil(t, progress.GetError())
			if !idle {
				boundaries, err := builder.ReadBoundaries(key.Name)
				require.NoError(t, err)
				require.Equal(t, uint64(2), boundaries.GetLastMirrorV2LogId())
			}
			// A raw read is deliberately not a touch: without another producer's
			// preload, two more rotations must evict the configuration completely.
			for index := uint64(4); index <= 5; index++ {
				_, err = machine.ApplyEntries(ctx, store, &raftpb.Entry{Index: new(index), Term: new(uint64(1)), Type: new(raftpb.EntryNormal)})
				require.NoError(t, err)
				if index == 4 {
					_, present = c.Ledgers.Get(identity.ID)
					require.True(t, present)
				}
			}
			_, present = c.Ledgers.Get(identity.ID)
			require.False(t, present, "read-only ledger access must not prevent generation eviction")
		})
	}
}
