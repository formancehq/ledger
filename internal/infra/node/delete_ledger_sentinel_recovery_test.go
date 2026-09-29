package node

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric/noop"
	"google.golang.org/protobuf/proto"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/cache"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/futures"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/spool"
	"github.com/formancehq/ledger/v3/internal/storage/wal"
)

// Antithesis observed a lagged follower applying a transaction and DeleteLedger
// in one commit batch, then failing the sentinel again during startup WAL replay.
// Both paths must accept the intentional volume deletion while checking the
// surviving ledger's volumes with the real FSM, Pebble, and enabled sentinel.
func TestDeleteLedgerSentinelRecovery(t *testing.T) {
	t.Parallel()

	for _, path := range []string{"wal_restart", "follower_catch_up"} {
		t.Run(path, func(t *testing.T) {
			t.Parallel()
			ctx := logging.TestingContext()
			dir := t.TempDir()
			setup, closeSetup := newDeleteSentinelApplier(t, dir)
			const deleted = "deleted-ledger"
			const survivor = "surviving-ledger"
			createDeleted, _ := makeCreateLedgerEntry(t, 1, deleted)
			// Commit the normal cluster policy through the FSM before business
			// transactions, so recovery restores the same metadata limits.
			var initial raftcmdpb.Proposal
			require.NoError(t, initial.UnmarshalVT(createDeleted.GetData()))
			initial.Orders = append([]*raftcmdpb.Order{{Type: &raftcmdpb.Order_SystemScoped{
				SystemScoped: &raftcmdpb.SystemScopedOrder{Payload: &raftcmdpb.SystemScopedOrder_SetClusterPolicy{
					SetClusterPolicy: &raftcmdpb.SetClusterPolicyOrder{Policy: &commonpb.ClusterPolicy{
						Revision: 1, QueryCheckpointLimit: 10,
						MetadataMaxEntriesPerEntity: domain.DefaultMetadataMaxEntriesPerEntity,
						MetadataMaxKeyBytes:         domain.DefaultMetadataMaxKeyBytes,
						MetadataMaxValueBytes:       domain.DefaultMetadataMaxValueBytes,
						MetadataMaxEntityBytes:      domain.DefaultMetadataMaxEntityBytes,
						MetadataMaxCommandBytes:     domain.DefaultMetadataMaxCommandBytes,
					}},
				}},
			}}}, initial.GetOrders()...)
			var err error
			createDeleted.Data, err = initial.MarshalVT()
			require.NoError(t, err)
			createSurvivor, _ := makeCreateLedgerEntry(t, 2, survivor)
			entries := []*raftpb.Entry{
				createDeleted,
				createSurvivor,
				makeDeleteSentinelEntry(t, 3, deleted, false),
				makeDeleteSentinelEntry(t, 4, survivor, false),
				makeDeleteSentinelEntry(t, 5, deleted, true),
			}
			require.NoError(t, setup.wal.Append(&raftpb.HardState{
				Term: proto.Uint64(1), Vote: proto.Uint64(1), Commit: proto.Uint64(5),
			}, entries))

			// Only ledger creation has reached Pebble; the committed transactions
			// and deletion await application by the follower or after restart.
			result, err := setup.fsm.ApplyEntries(ctx, setup.store, entries[:2]...)
			require.NoError(t, err)
			for _, applied := range result.Results {
				require.NoError(t, applied.Error)
			}
			if path == "wal_restart" {
				closeSetup()
				setup, _ = newDeleteSentinelApplier(t, dir)
			}
			require.Equal(t, uint64(2), setup.fsm.State.LastAppliedIndex)

			// Futures also verify that all three business orders succeeded;
			// a rejected transaction must not make the sentinel regression pass.
			pending := make([]*futures.Future[state.ApplyResult], 0, 3)
			for _, entry := range entries[2:] {
				future := futures.New[state.ApplyResult]()
				setup.applier.StoreFuture(entry.GetIndex(), entry.GetTerm(), future)
				pending = append(pending, future)
			}
			if path == "wal_restart" {
				upToDate, err := setup.applier.RecoverAndReplay(ctx)
				require.NoError(t, err, "WAL recovery must not reject an intentional ledger deletion")
				require.True(t, upToDate)
			} else {
				runDeleteSentinelCatchUp(t, ctx, setup, entries[2:])
			}
			waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			for _, future := range pending {
				applied, err := future.Wait(waitCtx)
				require.NoError(t, err)
				require.NoError(t, applied.Error)
			}

			appliedIndex, err := query.ReadLastAppliedIndex(setup.store)
			require.NoError(t, err)
			require.Equal(t, uint64(5), appliedIndex)
			_, err = query.GetLedgerByName(ctx, setup.store, deleted)
			require.ErrorIs(t, err, domain.ErrNotFound)
			_, err = query.GetLedgerByName(ctx, setup.store, survivor)
			require.NoError(t, err)
			for _, account := range []string{"world", "treasury"} {
				key := domain.VolumeKey{AccountKey: domain.AccountKey{LedgerName: deleted, Account: account}, Asset: "EUR"}
				volume, err := setup.fsm.Registry.Attrs.Volume.Get(setup.store, key.Bytes())
				require.NoError(t, err)
				require.Nil(t, volume, "deleted ledger volumes must be absent")
				key.LedgerName = survivor
				volume, err = setup.fsm.Registry.Attrs.Volume.Get(setup.store, key.Bytes())
				require.NoError(t, err)
				require.NotNil(t, volume, "other ledgers' volumes must survive")
				input, output := uint64(100), uint64(0)
				if account == "world" {
					input, output = output, input
				}
				require.True(t, proto.Equal(commonpb.NewUint256FromUint64(input), volume.GetInput()))
				require.True(t, proto.Equal(commonpb.NewUint256FromUint64(output), volume.GetOutput()))
			}
		})
	}
}

func newDeleteSentinelApplier(t *testing.T, dir string) (*testApplierSetup, func()) {
	t.Helper()
	logger := logging.Testing()
	provider := noop.NewMeterProvider()
	meter := provider.Meter("delete-sentinel")
	w, err := wal.New(filepath.Join(dir, "wal"), logger, meter)
	require.NoError(t, err)
	s, err := dal.NewStore(filepath.Join(dir, "data"), logger, meter, dal.DefaultConfig())
	require.NoError(t, err)
	sp, err := spool.NewDefault(spool.DefaultSpoolConfig{Dir: filepath.Join(dir, "spool")})
	require.NoError(t, err)
	closeSetup := sync.OnceFunc(func() {
		require.NoError(t, sp.Close())
		require.NoError(t, s.Close())
		require.NoError(t, w.Close())
	})
	t.Cleanup(closeSetup)
	c, err := cache.New(1000, nil)
	require.NoError(t, err)
	registry := state.NewStateRegistry(c, attributes.New())
	fsm, err := state.NewMachine(logger, registry, state.NewCacheSnapshotter(logger, registry, nil),
		s, dal.NewSentinelFactory(s, true), provider, nil, state.NewSharedState(), newNoopNotifier(t), nil,
		"test-cluster", 0, func(*raftpb.Entry, *dal.WriteSession) error { return nil })
	require.NoError(t, err)
	recovery := state.NewRecovery(fsm, s)
	require.NoError(t, recovery.RecoverState())
	confState := &raftpb.ConfState{Voters: []uint64{1}}
	lastIndex, err := w.LastIndex()
	require.NoError(t, err)
	if lastIndex == 0 {
		require.NoError(t, w.CreateSnapshot(0, confState, nil))
	}
	sink := make(LocalResponses, 10)
	applier, err := NewApplier(fsm, recovery, state.NewSynchronizer(fsm, recovery, dal.NewIncomingRestoreFactory(s)),
		sp, s, w, logger, meter, 0, 1000, nil, func() {}, sink)
	require.NoError(t, err)

	return &testApplierSetup{applier: applier, store: s, wal: w, spool: sp, fsm: fsm,
		stop: make(chan struct{}), confState: confState, responseSink: sink}, closeSetup
}

func makeDeleteSentinelEntry(t *testing.T, index uint64, ledger string, deleting bool) *raftpb.Entry {
	t.Helper()
	ls := &raftcmdpb.LedgerScopedOrder{Ledger: ledger}
	ledgerID, tag := attributes.MakeKey(domain.LedgerKey{Name: ledger}.Bytes())
	plans := []*raftcmdpb.AttributeCoverage{
		{Id: &raftcmdpb.AttributeID{Id: ledgerID[:], Tag: tag}, AttrCode: uint32(dal.SubAttrLedger)},
		{Id: &raftcmdpb.AttributeID{Id: ledgerID[:], Tag: tag}, AttrCode: uint32(dal.SubAttrBoundary)},
	}
	if deleting {
		ls.Payload = &raftcmdpb.LedgerScopedOrder_DeleteLedger{DeleteLedger: &raftcmdpb.DeleteLedgerOrder{}}
	} else {
		ls.Payload = &raftcmdpb.LedgerScopedOrder_Apply{Apply: &raftcmdpb.LedgerApplyOrder{
			Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{CreateTransaction: &raftcmdpb.CreateTransactionOrder{
				Force: true, Postings: []*commonpb.Posting{{Source: "world", Destination: "treasury", Asset: "EUR", Amount: commonpb.NewUint256FromUint64(100)}},
			}},
		}}
		zero, err := (&raftcmdpb.VolumePair{Input: commonpb.NewUint256FromUint64(0), Output: commonpb.NewUint256FromUint64(0)}).MarshalVT()
		require.NoError(t, err)
		for _, account := range []string{"world", "treasury"} {
			key := domain.VolumeKey{AccountKey: domain.AccountKey{LedgerName: ledger, Account: account}, Asset: "EUR"}
			id, tag := attributes.MakeKey(key.Bytes())
			plans = append(plans, &raftcmdpb.AttributeCoverage{
				Id: &raftcmdpb.AttributeID{Id: id[:], Tag: tag}, AttrCode: uint32(dal.SubAttrVolume),
				Value: &raftcmdpb.AttributeValue{RawValue: zero},
			})
		}
	}
	proposal := &raftcmdpb.Proposal{
		Id: index, Date: &commonpb.Timestamp{Data: 1700000000 + index},
		ExecutionPlan: &raftcmdpb.ExecutionPlan{Attributes: plans},
		Orders: []*raftcmdpb.Order{{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: ls},
			Technical: &raftcmdpb.OrderTechnical{CoverageBits: []byte{byte(1<<len(plans)) - 1}}}},
	}
	data, err := proposal.MarshalVT()
	require.NoError(t, err)

	return &raftpb.Entry{Term: proto.Uint64(1), Index: new(index), Type: new(raftpb.EntryNormal), Data: data}
}

func runDeleteSentinelCatchUp(t *testing.T, ctx context.Context, setup *testApplierSetup, entries []*raftpb.Entry) {
	t.Helper()
	stop := sync.OnceFunc(func() { close(setup.stop) })
	done := make(chan struct{})
	var runErr error
	go func() {
		defer close(done)
		defer stop()
		runErr = setup.applier.Run(ctx, setup.stop)
	}()
	t.Cleanup(func() {
		stop()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("catch-up applier did not stop before fixture cleanup")
		}
	})
	setup.applier.Submit(entries, setup.confState, nil, setup.stop)
	setup.applier.Drain(setup.stop)
	stop()
	select {
	case <-done:
		require.NoError(t, runErr, "follower catch-up must not reject an intentional ledger deletion")
	case <-time.After(5 * time.Second):
		t.Fatal("catch-up applier did not stop")
	}
}
