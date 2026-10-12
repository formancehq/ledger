package check

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric/noop"
	"google.golang.org/protobuf/proto"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/crypto/keystore"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/cache"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

const fsmPurgeAuditKey = "0123456789abcdef0123456789abcdef"

type fsmPurgeNotifier struct{}

func (fsmPurgeNotifier) NotifyLogsCommitted(uint64) {}
func (fsmPurgeNotifier) NotifyConfigChanged()       {}

// newFSMPurgeMachine builds a real FSM over its own store. The testEngine
// mimic in checker_test.go reproduces WriteSet.Merge's attribute writes but
// not its ephemeral/transient purge phase, so purge assertions need the live
// apply path.
func newFSMPurgeMachine(t *testing.T) (*state.Machine, *dal.Store, *attributes.Attributes) {
	t.Helper()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meterProvider := noop.NewMeterProvider()

	store, err := dal.NewStore(t.TempDir(), logger, meterProvider.Meter("test"), dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	batch := store.OpenWriteSession()
	require.NoError(t, batch.SetBytes([]byte{dal.ZoneGlobal, dal.SubGlobAuditKey}, []byte(fsmPurgeAuditKey)))
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
		fsmPurgeNotifier{},
		nil,
		fsmPurgeAuditKey,
		0,
		func(*raftpb.Entry, *dal.WriteSession) error { return nil },
	)
	require.NoError(t, err)

	machine.State.UpdateClusterPolicy(&commonpb.ClusterPolicy{
		Revision: 1, QueryCheckpointLimit: 10,
		MetadataMaxEntriesPerEntity: domain.DefaultMetadataMaxEntriesPerEntity,
		MetadataMaxKeyBytes:         domain.DefaultMetadataMaxKeyBytes,
		MetadataMaxValueBytes:       domain.DefaultMetadataMaxValueBytes,
		MetadataMaxEntityBytes:      domain.DefaultMetadataMaxEntityBytes,
		MetadataMaxCommandBytes:     domain.DefaultMetadataMaxCommandBytes,
	})

	return machine, store, attrs
}

type fsmCoverageItem struct {
	code byte
	key  []byte
}

// applyFSMPurgeProposal drives one proposal through the real apply path,
// resolving the execution plan the gated attribute reads need.
func applyFSMPurgeProposal(
	t *testing.T,
	machine *state.Machine,
	store *dal.Store,
	attrs *attributes.Attributes,
	index uint64,
	orders ...*raftcmdpb.Order,
) {
	t.Helper()

	var (
		canonicals []fsmCoverageItem
		seen       = map[string]struct{}{}
	)
	add := func(code byte, key []byte) {
		dedupe := string(append([]byte{code}, key...))
		if _, ok := seen[dedupe]; ok {
			return
		}
		seen[dedupe] = struct{}{}
		canonicals = append(canonicals, fsmCoverageItem{code: code, key: key})
	}

	for _, order := range orders {
		ledger := order.GetLedgerScoped().GetLedger()
		add(dal.SubAttrLedger, domain.LedgerKey{Name: ledger}.Bytes())
		add(dal.SubAttrBoundary, domain.LedgerKey{Name: ledger}.Bytes())

		if meta := order.GetLedgerScoped().GetApply().GetAddMetadata(); meta != nil {
			account := meta.GetTarget().GetAccount().GetAddr()

			for key := range meta.GetMetadata() {
				add(dal.SubAttrMetadata, domain.MetadataKey{
					AccountKey: domain.AccountKey{LedgerName: ledger, Account: account},
					Key:        key,
				}.Bytes())
			}
		}

		create := order.GetLedgerScoped().GetApply().GetCreateTransaction()
		if create == nil {
			continue
		}

		if create.GetReference() != "" {
			add(dal.SubAttrReference, domain.TransactionReferenceKey{LedgerName: ledger, Reference: create.GetReference()}.Bytes())
		}

		for _, posting := range create.GetPostings() {
			for _, account := range []string{posting.GetSource(), posting.GetDestination()} {
				add(dal.SubAttrVolume, domain.NewVolumeKey(ledger, account, posting.GetAsset(), "").Bytes())
			}
		}
	}

	handle, err := store.NewDirectReadHandle()
	require.NoError(t, err)

	plans := make([]*raftcmdpb.AttributeCoverage, 0, len(canonicals))

	for _, item := range canonicals {
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
			if value == nil {
				value = &raftcmdpb.VolumePair{Input: commonpb.NewUint256FromUint64(0), Output: commonpb.NewUint256FromUint64(0)}
			}
			raw, err = value.MarshalVT()
		case dal.SubAttrMetadata:
			value, getErr := attrs.Metadata.Get(handle, item.key)
			require.NoError(t, getErr)
			if value != nil {
				raw, err = value.MarshalVT()
			}
		case dal.SubAttrReference:
			value, getErr := attrs.References.Get(handle, item.key)
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
		Date:   &commonpb.Timestamp{Data: 1700000000 + index},
		CallerSnapshot: &commonpb.CallerSnapshot{
			Principal: &commonpb.CallerSnapshot_AuthDisabled{AuthDisabled: &commonpb.AuthDisabledCaller{}},
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

func fsmPurgeCreateLedgerOrder(persistence commonpb.AccountTypePersistence) *raftcmdpb.Order {
	return &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{
		LedgerScoped: &raftcmdpb.LedgerScopedOrder{
			Ledger: "ledger",
			Payload: &raftcmdpb.LedgerScopedOrder_CreateLedger{CreateLedger: &raftcmdpb.CreateLedgerOrder{
				AccountTypes: map[string]*commonpb.AccountType{
					"t13": {Name: "t13", Pattern: "t-13:{sub}", Persistence: persistence},
				},
			}},
		},
	}}
}

func fsmPurgeTransactionOrder(source, destination, reference string) *raftcmdpb.Order {
	return &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{
		LedgerScoped: &raftcmdpb.LedgerScopedOrder{
			Ledger: "ledger",
			Payload: &raftcmdpb.LedgerScopedOrder_Apply{Apply: &raftcmdpb.LedgerApplyOrder{
				Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{CreateTransaction: &raftcmdpb.CreateTransactionOrder{
					Reference: reference,
					Postings: []*commonpb.Posting{{
						Source: source, Destination: destination,
						Asset: "USD", Amount: commonpb.NewUint256FromUint64(5),
					}},
				}},
			}},
		},
	}}
}

func runFSMPurgeCheck(t *testing.T, store *dal.Store, attrs *attributes.Attributes) []*servicepb.CheckStoreError {
	t.Helper()

	var findings []*servicepb.CheckStoreError

	checker := NewChecker(store, attrs, nil, logging.Testing())
	require.NoError(t, checker.Check(context.Background(), func(event *servicepb.CheckStoreEvent) {
		if e, ok := event.GetType().(*servicepb.CheckStoreEvent_Error); ok {
			findings = append(findings, e.Error)
		}
	}))

	return findings
}

// setupPurgedTransientCell funds and drains t-13:98 inside one proposal, so the
// transient cell exists mid-batch and the FSM purges it at the boundary.
func setupPurgedTransientCell(t *testing.T) (*dal.Store, *attributes.Attributes) {
	t.Helper()

	machine, store, attrs := newFSMPurgeMachine(t)
	applyFSMPurgeProposal(t, machine, store, attrs, 1,
		fsmPurgeCreateLedgerOrder(commonpb.AccountTypePersistence_ACCOUNT_TYPE_TRANSIENT))
	applyFSMPurgeProposal(t, machine, store, attrs, 2,
		fsmPurgeTransactionOrder("world", "t-13:98", "fund"),
		fsmPurgeTransactionOrder("t-13:98", "world", "drain"))
	require.NoError(t, store.Flush())

	handle, err := store.NewDirectReadHandle()
	require.NoError(t, err)
	volume, err := attrs.Volume.Get(handle, domain.NewVolumeKey("ledger", "t-13:98", "USD", "").Bytes())
	require.NoError(t, err)
	require.Nil(t, volume, "the transient cell must be purged by live apply")
	require.NoError(t, handle.Close())

	return store, attrs
}

func TestCheckerFSMTransientPurgeIsClean(t *testing.T) {
	t.Parallel()

	store, attrs := setupPurgedTransientCell(t)
	require.Empty(t, runFSMPurgeCheck(t, store, attrs))
}

// A drained transient cell the FSM failed to delete sits at input == output
// with non-zero limbs; the replay expects it absent, so it must be reported.
func TestCheckerFSMRejectsStrandedTransientVolume(t *testing.T) {
	t.Parallel()

	store, attrs := setupPurgedTransientCell(t)

	batch := store.OpenWriteSession()
	_, err := attrs.Volume.Set(batch, domain.NewVolumeKey("ledger", "t-13:98", "USD", "").Bytes(), &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(5),
		Output: commonpb.NewUint256FromUint64(5),
	})
	require.NoError(t, err)
	require.NoError(t, batch.Commit())
	require.NoError(t, store.Flush())

	findings := runFSMPurgeCheck(t, store, attrs)

	var found bool
	for _, finding := range findings {
		if finding.GetErrorType() == servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_VOLUME_MISMATCH &&
			finding.GetAccount() == "t-13:98" && finding.GetAsset() == "USD" {
			found = true

			break
		}
	}

	require.True(t, found, "checker must reject a stranded transient volume row: %v", findings)
}

// A purged volume cell says nothing about the account's metadata: a row the
// audit never wrote must be reported even on an otherwise-dormant account.
func TestCheckerFSMRejectsForgedMetadataOnPurgedTransientAccount(t *testing.T) {
	t.Parallel()

	store, attrs := setupPurgedTransientCell(t)

	batch := store.OpenWriteSession()
	metadataKey := domain.MetadataKey{
		AccountKey: domain.AccountKey{LedgerName: "ledger", Account: "t-13:98"},
		Key:        "injected",
	}
	_, err := attrs.Metadata.Set(batch, metadataKey.Bytes(), commonpb.NewStringValue("forged"))
	require.NoError(t, err)
	require.NoError(t, batch.Commit())
	require.NoError(t, store.Flush())

	findings := runFSMPurgeCheck(t, store, attrs)

	var found bool
	for _, finding := range findings {
		if finding.GetErrorType() == servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_METADATA_MISMATCH &&
			finding.GetAccount() == "t-13:98" {
			found = true

			break
		}
	}

	require.True(t, found, "checker must reject metadata forged onto a purged transient account: %v", findings)
}

// Control: audited metadata on an ephemeral account whose only cell was purged
// is legitimate and must compare clean.
func TestCheckerFSMEphemeralCellPurgeKeepsAuditedMetadataClean(t *testing.T) {
	t.Parallel()

	machine, store, attrs := newFSMPurgeMachine(t)
	applyFSMPurgeProposal(t, machine, store, attrs, 1,
		fsmPurgeCreateLedgerOrder(commonpb.AccountTypePersistence_ACCOUNT_TYPE_EPHEMERAL))
	applyFSMPurgeProposal(t, machine, store, attrs, 2,
		fsmPurgeTransactionOrder("world", "t-13:98", "fund"))
	applyFSMPurgeProposal(t, machine, store, attrs, 3,
		fsmPurgeSaveMetadataOrder("t-13:98", "owner", "alice"))
	applyFSMPurgeProposal(t, machine, store, attrs, 4,
		fsmPurgeTransactionOrder("t-13:98", "world", "drain"))
	require.NoError(t, store.Flush())

	require.Empty(t, runFSMPurgeCheck(t, store, attrs))
}

func fsmPurgeSaveMetadataOrder(account, key, value string) *raftcmdpb.Order {
	return &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{
		LedgerScoped: &raftcmdpb.LedgerScopedOrder{
			Ledger: "ledger",
			Payload: &raftcmdpb.LedgerScopedOrder_Apply{Apply: &raftcmdpb.LedgerApplyOrder{
				Data: &raftcmdpb.LedgerApplyOrder_AddMetadata{AddMetadata: &raftcmdpb.SaveMetadataOrder{
					Target: &commonpb.Target{Target: &commonpb.Target_Account{
						Account: &commonpb.TargetAccount{Addr: account},
					}},
					Metadata: map[string]*commonpb.MetadataValue{key: commonpb.NewStringValue(value)},
				}},
			}},
		},
	}}
}
