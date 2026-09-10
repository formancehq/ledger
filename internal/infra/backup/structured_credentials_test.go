package backup

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric/noop"
	"google.golang.org/protobuf/proto"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/application/check"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/crypto/keystore"
	"github.com/formancehq/ledger/v3/internal/domain/processing"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/cache"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
	"github.com/formancehq/ledger/v3/internal/pkg/signal"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// This uses committed orders and an actual Pebble checkpoint, rather than
// seeding operational logs that could conceal a mismatch with the live FSM.
func TestStructuredCredentials_IncrementalRestoreParity(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	logger := logging.Testing()
	src := newBackupTestStore(t)
	machine := newConnectionTestMachine(t, src)
	var index uint64
	apply := func(order *raftcmdpb.Order) {
		t.Helper()
		index++
		applyConnectionTestOrder(t, machine, src, index, order)
	}
	addSink := func(name, password string) *raftcmdpb.Order {
		return &raftcmdpb.Order{Type: &raftcmdpb.Order_SystemScoped{SystemScoped: &raftcmdpb.SystemScopedOrder{
			Payload: &raftcmdpb.SystemScopedOrder_AddEventsSink{AddEventsSink: &raftcmdpb.AddEventsSinkOrder{Config: &commonpb.SinkConfigInput{
				Name: name, Format: "json", BatchSize: 12,
				Type: &commonpb.SinkConfigInput_Nats{Nats: &commonpb.NatsSinkConfigInput{Url: "nats://publisher:" + password + "@nats.example:4222", Topic: "ledger.events"}},
			}}},
		}}}
	}
	removeSink := func(name string) *raftcmdpb.Order {
		return &raftcmdpb.Order{Type: &raftcmdpb.Order_SystemScoped{SystemScoped: &raftcmdpb.SystemScopedOrder{
			Payload: &raftcmdpb.SystemScopedOrder_RemoveEventsSink{RemoveEventsSink: &raftcmdpb.RemoveEventsSinkOrder{Name: name}},
		}}}
	}
	createMirror := func(name string) *raftcmdpb.Order {
		return &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{
			Ledger: name, Payload: &raftcmdpb.LedgerScopedOrder_CreateLedger{CreateLedger: &raftcmdpb.CreateLedgerOrder{
				Mode: commonpb.LedgerMode_LEDGER_MODE_MIRROR,
				MirrorSource: &commonpb.MirrorSourceConfigInput{LedgerName: "upstream", BatchSize: 17,
					Type: &commonpb.MirrorSourceConfigInput_Postgres{Postgres: &commonpb.PostgresMirrorSourceConfigInput{Dsn: "postgres://reader:mirror-password@postgres.example:5432/source?sslmode=require"}},
				},
			}},
		}}}
	}
	apply(addSink("rotated", "before"))
	apply(addSink("removed", "removed-password"))
	apply(createMirror("promoted"))
	apply(createMirror("deleted"))

	prefixIndex := index
	storage := newInMemoryBackupStorage()
	full, err := RunBackup(ctx, logger, src, storage, "connections", "prefix")
	require.NoError(t, err)
	require.Positive(t, full.TotalFiles)
	// Open the same prefix as a separate database. The production export/rebuild
	// pipeline below starts from this real checkpoint's exact sequence boundary.
	restoredRoot := t.TempDir()
	require.NoError(t, src.Checkpoint(filepath.Join(restoredRoot, "live")))
	dst, err := dal.NewStore(restoredRoot, logger, noop.NewMeterProvider().Meter("test"), dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dst.Close()) })

	apply(removeSink("removed"))
	apply(removeSink("rotated"))
	apply(addSink("rotated", "after"))
	apply(addSink("new", "new-password"))
	apply(createMirror("new-mirror"))
	apply(&raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{
		Ledger: "promoted", Payload: &raftcmdpb.LedgerScopedOrder_PromoteLedger{PromoteLedger: &raftcmdpb.PromoteLedgerOrder{}},
	}}})
	apply(&raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{
		Ledger: "deleted", Payload: &raftcmdpb.LedgerScopedOrder_DeleteLedger{DeleteLedger: &raftcmdpb.DeleteLedgerOrder{}},
	}}})
	delta, err := RunIncrementalBackup(ctx, logger, src, storage, "connections", 0)
	require.NoError(t, err)
	require.Positive(t, delta.LogEntriesExported)
	manifest, err := ReadManifest(ctx, storage, ManifestKey("connections"))
	require.NoError(t, err)
	require.Equal(t, full.LastLogSequence, manifest.Checkpoint.LastLogSequence)
	require.NotEmpty(t, manifest.Exports)
	require.NoError(t, ApplyExportsAndRebuild(ctx, logger, storage, dst, manifest))

	readConfigs := func(store *dal.Store) (map[string]*commonpb.SinkConfig, map[string]*commonpb.LedgerInfo) {
		t.Helper()
		h, err := store.NewDirectReadHandle()
		require.NoError(t, err)
		defer func() { require.NoError(t, h.Close()) }()
		sinks, err := query.ReadAllSinkConfigs(attributes.New().SinkConfig, h)
		require.NoError(t, err)
		byName := map[string]*commonpb.SinkConfig{}
		for _, cfg := range sinks {
			byName[cfg.GetName()] = cfg
		}
		ledgers, err := query.ReadLedgers(ctx, h)
		require.NoError(t, err)
		infos, err := cursor.Collect(ledgers)
		require.NoError(t, err)
		byLedger := map[string]*commonpb.LedgerInfo{}
		for _, info := range infos {
			byLedger[info.GetName()] = info
		}

		return byName, byLedger
	}
	wantSinks, wantLedgers := readConfigs(src)
	gotSinks, gotLedgers := readConfigs(dst)
	require.Len(t, gotSinks, 2)
	require.NotContains(t, gotSinks, "removed")
	for name, want := range wantSinks {
		require.True(t, proto.Equal(want, gotSinks[name]), "sink %s", name)
	}
	require.Equal(t, "after", gotSinks["rotated"].GetNats().GetServers()[0].GetPassword())
	require.Equal(t, "nats.example", gotSinks["new"].GetNats().GetServers()[0].GetAddress().GetHost())
	require.Len(t, gotLedgers, len(wantLedgers))
	for name, want := range wantLedgers {
		require.True(t, proto.Equal(want, gotLedgers[name]), "ledger %s", name)
	}
	require.Nil(t, gotLedgers["promoted"].GetMirrorSource())
	require.Equal(t, commonpb.LedgerMode_LEDGER_MODE_NORMAL, gotLedgers["promoted"].GetMode())
	require.NotNil(t, gotLedgers["deleted"].GetDeletedAt())
	require.Equal(t, "mirror-password", gotLedgers["new-mirror"].GetMirrorSource().GetPostgres().GetConnection().GetPassword())

	// Existing checker validates the restored chain and covered projections;
	// connection projection equality itself is asserted above (checker PR1912).
	for _, store := range []*dal.Store{src, dst} {
		var findings []*servicepb.CheckStoreError
		checker := check.NewChecker(store, attributes.New(), "connection-test-cluster", nil, logger)
		require.NoError(t, checker.Check(ctx, func(event *servicepb.CheckStoreEvent) {
			if failure := event.GetError(); failure != nil {
				findings = append(findings, failure)
			}
		}))
		require.Empty(t, findings)
	}
	// Recovery must make the restored sink available to the next gated FSM read.
	recovered := newConnectionTestMachine(t, dst)
	// The restored store resumes its checkpoint Raft index, independently
	// of the source-cluster indexes carried by the exported history.
	applyConnectionTestOrder(t, recovered, dst, prefixIndex+1, removeSink("rotated"))
	remaining, _ := readConfigs(dst)
	require.NotContains(t, remaining, "rotated")
	require.Contains(t, remaining, "new")
}

func newConnectionTestMachine(t *testing.T, store *dal.Store) *state.Machine {
	t.Helper()
	logger := logging.Testing()
	meter := noop.NewMeterProvider()
	c, err := cache.New(1000, meter.Meter("test"))
	require.NoError(t, err)
	registry := state.NewStateRegistry(c, attributes.New(), 0)
	snapshotter := state.NewCacheSnapshotter(logger, registry, nil)
	machine, err := state.NewMachine(logger, registry, snapshotter, store, dal.NewSentinelFactory(store, false), meter,
		keystore.NewKeyStore(), state.NewSharedState(), signal.NewNotifications(), nil, "connection-test-cluster", 0,
		func(*raftpb.Entry, *dal.WriteSession) error {
			return errors.New("unexpected raft configuration change in connection fixture")
		})
	require.NoError(t, err)
	require.NoError(t, state.NewRecovery(machine, store).RecoverState())

	return machine
}

func applyConnectionTestOrder(t *testing.T, machine *state.Machine, store *dal.Store, index uint64, order *raftcmdpb.Order) {
	t.Helper()
	var canonical []byte
	var codes []byte
	if ls := order.GetLedgerScoped(); ls != nil {
		canonical = domain.LedgerKey{Name: ls.GetLedger()}.Bytes()
		codes = []byte{dal.SubAttrLedger, dal.SubAttrBoundary}
	} else {
		ss := order.GetSystemScoped()
		name := ss.GetAddEventsSink().GetConfig().GetName()
		if ss.GetRemoveEventsSink() != nil {
			name = ss.GetRemoveEventsSink().GetName()
		}
		canonical = domain.SinkConfigKey{Name: name}.Bytes()
		codes = []byte{dal.SubAttrSinkConfig}
	}
	id, tag := attributes.MakeKey(canonical)
	plan := &raftcmdpb.ExecutionPlan{}
	var bits byte
	for i, code := range codes {
		plan.Attributes = append(plan.Attributes, &raftcmdpb.AttributeCoverage{
			Id: &raftcmdpb.AttributeID{Id: id[:], Tag: tag}, AttrCode: uint32(code),
		})
		// Admission seeds cache misses from the operational attribute before
		// committing the proposal. This also permits the post-restore read on
		// a fresh cache, without granting the FSM a Pebble read capability.
		if code == dal.SubAttrSinkConfig {
			handle, err := store.NewDirectReadHandle()
			require.NoError(t, err)
			cfg, err := attributes.New().SinkConfig.Get(handle, canonical)
			require.NoError(t, err)
			require.NoError(t, handle.Close())
			if cfg != nil {
				raw, err := cfg.MarshalVT()
				require.NoError(t, err)
				plan.Attributes[i].Value = &raftcmdpb.AttributeValue{RawValue: raw}
			}
		}
		bits |= 1 << i
	}
	order.Technical = &raftcmdpb.OrderTechnical{CoverageBits: []byte{bits}}
	before := order.MarshalDeterministicVT(nil)
	intent := processing.MarshalOrderBusinessIntent(order, nil)
	proposal := &raftcmdpb.Proposal{Id: index, Date: &commonpb.Timestamp{Data: 1700000000 + index}, Orders: []*raftcmdpb.Order{order}, ExecutionPlan: plan}
	raw, err := proto.Marshal(proposal)
	require.NoError(t, err)
	result, err := machine.ApplyEntries(context.Background(), store, &raftpb.Entry{Index: new(index), Term: proto.Uint64(1), Type: new(raftpb.EntryNormal), Data: raw})
	require.NoError(t, err)
	require.Len(t, result.Results, 1)
	require.NoError(t, result.Results[0].Error)
	require.True(t, result.Results[0].AuditEntryWritten)
	require.Equal(t, before, order.MarshalDeterministicVT(nil))
	handle, err := store.NewDirectReadHandle()
	require.NoError(t, err)
	defer func() { require.NoError(t, handle.Close()) }()
	last, err := query.ReadLastAuditEntry(handle)
	require.NoError(t, err)
	items, err := query.ReadAuditItems(context.Background(), handle, last.GetSequence())
	require.NoError(t, err)
	require.Len(t, items, 1)
	require.Equal(t, intent, items[0].GetSerializedOrder(), "persisted audit must preserve raw accepted connection input")
}
