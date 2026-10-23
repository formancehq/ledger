package check

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric/noop"
	"google.golang.org/protobuf/proto"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	"github.com/formancehq/ledger/pkg/proto/v3/ledgerpb"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/crypto/keystore"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/cache"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

const tombstoneFSMAuditKey = "0123456789abcdef0123456789abcdef"

type tombstoneFSMNotifier struct{}

func (tombstoneFSMNotifier) NotifyLogsCommitted(uint64) {}
func (tombstoneFSMNotifier) NotifyConfigChanged()       {}

// newTombstoneFSMMachine builds a real FSM over its own store: the testEngine
// in checker_test.go does not run WriteSet.Merge's purge phase.
func newTombstoneFSMMachine(t *testing.T) (*state.Machine, *dal.Store, *attributes.Attributes) {
	t.Helper()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meterProvider := noop.NewMeterProvider()

	store, err := dal.NewStore(t.TempDir(), logger, meterProvider.Meter("test"), dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	batch := store.OpenWriteSession()
	require.NoError(t, batch.SetBytes([]byte{dal.ZoneGlobal, dal.SubGlobAuditKey}, []byte(tombstoneFSMAuditKey)))
	require.NoError(t, batch.Commit())

	attrs := attributes.New()

	c, err := cache.New(1000, meterProvider.Meter("test"))
	require.NoError(t, err)

	registry := state.NewStateRegistry(c, attrs)

	machine, err := state.NewMachine(
		logger,
		registry,
		state.NewCacheSnapshotter(logger, registry, nil),
		store,
		dal.NewSentinelFactory(store, false),
		meterProvider,
		keystore.NewKeyStore(),
		state.NewSharedState(),
		tombstoneFSMNotifier{},
		nil,
		tombstoneFSMAuditKey,
		0,
		func(*raftpb.Entry, *dal.WriteSession) error { return nil },
	)
	require.NoError(t, err)

	machine.State.UpdateClusterPolicy(&ledgerpb.ClusterPolicy{
		Revision: 1, QueryCheckpointLimit: 10,
		MetadataMaxEntriesPerEntity: domain.DefaultMetadataMaxEntriesPerEntity,
		MetadataMaxKeyBytes:         domain.DefaultMetadataMaxKeyBytes,
		MetadataMaxValueBytes:       domain.DefaultMetadataMaxValueBytes,
		MetadataMaxEntityBytes:      domain.DefaultMetadataMaxEntityBytes,
		MetadataMaxCommandBytes:     domain.DefaultMetadataMaxCommandBytes,
	})

	return machine, store, attrs
}

type tombstoneFSMCoverageItem struct {
	code byte
	key  []byte
}

// applyTombstoneFSMProposal drives one proposal through the real apply path.
// Like admission, it seeds keys present in Pebble and declares absent keys
// coverage-only.
func applyTombstoneFSMProposal(
	t *testing.T,
	machine *state.Machine,
	store *dal.Store,
	attrs *attributes.Attributes,
	index uint64,
	orders ...*raftcmdpb.Order,
) {
	t.Helper()

	var (
		items []tombstoneFSMCoverageItem
		seen  = map[string]struct{}{}
	)
	add := func(code byte, key []byte) {
		dedupe := string(append([]byte{code}, key...))
		if _, ok := seen[dedupe]; ok {
			return
		}
		seen[dedupe] = struct{}{}
		items = append(items, tombstoneFSMCoverageItem{code: code, key: key})
	}

	for _, order := range orders {
		ledger := order.GetLedgerScoped().GetLedger()
		add(dal.SubAttrLedger, domain.LedgerKey{Name: ledger}.Bytes())
		add(dal.SubAttrBoundary, domain.LedgerKey{Name: ledger}.Bytes())

		for _, posting := range order.GetLedgerScoped().GetApply().GetCreateTransaction().GetPostings() {
			for _, account := range []string{posting.GetSource(), posting.GetDestination()} {
				add(dal.SubAttrVolume, domain.NewVolumeKey(ledger, account, posting.GetAsset(), "").Bytes())
			}
		}
	}

	handle, err := store.NewDirectReadHandle()
	require.NoError(t, err)

	plans := make([]*raftcmdpb.AttributeCoverage, 0, len(items))

	for _, item := range items {
		id, tag := attributes.MakeKey(item.key)
		plan := &raftcmdpb.AttributeCoverage{
			Id:       &raftcmdpb.AttributeID{Id: id[:], Tag: tag},
			AttrCode: uint32(item.code),
		}

		var raw []byte

		switch item.code {
		case dal.SubAttrLedger:
			value, getErr := attrs.Ledger.Get(handle, item.key)
			require.NoError(t, getErr)
			if value != nil {
				raw, err = value.MarshalVT()
			}
		case dal.SubAttrBoundary:
			value, getErr := attrs.Boundary.Get(handle, item.key)
			require.NoError(t, getErr)
			if value != nil {
				raw, err = value.MarshalVT()
			}
		case dal.SubAttrVolume:
			value, getErr := attrs.Volume.Get(handle, item.key)
			require.NoError(t, getErr)
			if value != nil {
				raw, err = value.MarshalVT()
			}
		}

		require.NoError(t, err)

		if raw != nil {
			plan.Value = &raftcmdpb.AttributeValue{RawValue: raw}
		}

		plans = append(plans, plan)
	}

	require.NoError(t, handle.Close())

	bits := make([]byte, (len(plans)+7)/8)
	for i := range plans {
		bits[i/8] |= 1 << (i % 8)
	}

	for _, order := range orders {
		order.Technical = &raftcmdpb.OrderTechnical{CoverageBits: bits}
	}

	proposal := &raftcmdpb.Proposal{
		Id:     index,
		Orders: orders,
		Date:   &ledgerpb.Timestamp{Data: 1700000000 + index},
		CallerSnapshot: &ledgerpb.CallerSnapshot{
			Principal: &ledgerpb.CallerSnapshot_AuthDisabled{AuthDisabled: &ledgerpb.AuthDisabledCaller{}},
		},
		ExecutionPlan: &raftcmdpb.ExecutionPlan{Attributes: plans},
	}

	data, err := proto.Marshal(proposal)
	require.NoError(t, err)

	result, err := machine.ApplyEntries(context.Background(), store, &raftpb.Entry{
		Index: new(index),
		Term:  proto.Uint64(1),
		Type:  new(raftpb.EntryNormal),
		Data:  data,
	})
	require.NoError(t, err)

	for _, r := range result.Results {
		require.NoError(t, r.Error)
	}
}

func tombstoneFSMApplyOrder(apply *raftcmdpb.LedgerApplyOrder) *raftcmdpb.Order {
	return &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{
		Ledger:  "ledger",
		Payload: &raftcmdpb.LedgerScopedOrder_Apply{Apply: apply},
	}}}
}

func tombstoneFSMTransactionOrder(source, destination string, amount uint64) *raftcmdpb.Order {
	return tombstoneFSMApplyOrder(&raftcmdpb.LedgerApplyOrder{
		Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{CreateTransaction: &raftcmdpb.CreateTransactionOrder{
			Postings: []*ledgerpb.Posting{{
				Source: source, Destination: destination,
				Asset: "USD", Amount: ledgerpb.NewUint256FromUint64(amount),
			}},
		}},
	})
}

func readTombstoneFSMVolume(t *testing.T, store *dal.Store, attrs *attributes.Attributes, account string) *raftcmdpb.VolumePair {
	t.Helper()

	require.NoError(t, store.Flush())

	handle, err := store.NewDirectReadHandle()
	require.NoError(t, err)
	defer func() { require.NoError(t, handle.Close()) }()

	volume, err := attrs.Volume.Get(handle, domain.NewVolumeKey("ledger", account, "USD", "").Bytes())
	require.NoError(t, err)

	return volume
}

// A zero-amount posting persists a {0, 0} row for a NORMAL account. That row
// is real: once a TRANSIENT pattern matches the account, the next drain must
// delete it rather than mistake it for an absent cell and leave a ghost
// account behind.
func TestCheckerFSMTransientDrainDeletesPersistedZeroRow(t *testing.T) {
	t.Parallel()

	machine, store, attrs := newTombstoneFSMMachine(t)

	applyTombstoneFSMProposal(t, machine, store, attrs, 1,
		&raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{
			Ledger:  "ledger",
			Payload: &raftcmdpb.LedgerScopedOrder_CreateLedger{CreateLedger: &raftcmdpb.CreateLedgerOrder{}},
		}}})
	applyTombstoneFSMProposal(t, machine, store, attrs, 2,
		tombstoneFSMTransactionOrder("world", "t-13:98", 0))

	persisted := readTombstoneFSMVolume(t, store, attrs, "t-13:98")
	require.NotNil(t, persisted, "a zero-amount posting persists the NORMAL row")
	require.True(t, persisted.GetInput().IsZero() && persisted.GetOutput().IsZero())

	applyTombstoneFSMProposal(t, machine, store, attrs, 3,
		tombstoneFSMApplyOrder(&raftcmdpb.LedgerApplyOrder{
			Data: &raftcmdpb.LedgerApplyOrder_AddAccountType{AddAccountType: &raftcmdpb.AddAccountTypeOrder{
				AccountType: &ledgerpb.AccountType{
					Name:        "t13",
					Pattern:     "t-13:{sub}",
					Persistence: ledgerpb.AccountTypePersistence_ACCOUNT_TYPE_TRANSIENT,
				},
			}},
		}))
	applyTombstoneFSMProposal(t, machine, store, attrs, 4,
		tombstoneFSMTransactionOrder("world", "t-13:98", 5),
		tombstoneFSMTransactionOrder("t-13:98", "world", 5))

	require.Nil(t, readTombstoneFSMVolume(t, store, attrs, "t-13:98"),
		"the transient drain must delete the persisted {0, 0} row")

	var findings []*servicepb.CheckStoreError

	checker := NewChecker(store, attrs, nil, logging.Testing())
	require.NoError(t, checker.Check(context.Background(), func(event *servicepb.CheckStoreEvent) {
		if e, ok := event.GetType().(*servicepb.CheckStoreEvent_Error); ok {
			findings = append(findings, e.Error)
		}
	}))
	require.Empty(t, findings)
}
