package backup

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric/noop"
	"google.golang.org/protobuf/proto"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/application/check"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/crypto/keystore"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/cache"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

const signingParityClusterID = "signing-parity-cluster"

// signingParityNotifier satisfies state.Notifier for a machine driven straight
// through ApplyEntries, where nothing consumes the notifications.
type signingParityNotifier struct{}

func (signingParityNotifier) NotifyLogsCommitted(uint64) {}
func (signingParityNotifier) NotifyConfigChanged()       {}

// newSigningParityMachine builds a real FSM over its own store, so the signing
// logs and audit entries under test are the ones live apply emits rather than
// handcrafted stand-ins.
func newSigningParityMachine(t *testing.T) (*state.Machine, *dal.Store, *attributes.Attributes) {
	t.Helper()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meterProvider := noop.NewMeterProvider()

	store, err := dal.NewStore(t.TempDir(), logger, meterProvider.Meter("test"), dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

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
		signingParityNotifier{},
		nil,
		signingParityClusterID,
		0,
		func(*raftpb.Entry, *dal.WriteSession) error { return nil },
	)
	require.NoError(t, err)
	policy := &commonpb.ClusterPolicy{
		Revision: 1, QueryCheckpointLimit: 10,
		MetadataMaxEntriesPerEntity: domain.DefaultMetadataMaxEntriesPerEntity,
		MetadataMaxKeyBytes:         domain.DefaultMetadataMaxKeyBytes, MetadataMaxValueBytes: domain.DefaultMetadataMaxValueBytes,
		MetadataMaxEntityBytes: domain.DefaultMetadataMaxEntityBytes, MetadataMaxCommandBytes: domain.DefaultMetadataMaxCommandBytes,
	}
	machine.State.UpdateClusterPolicy(policy)

	return machine, store, attrs
}

// applySigningEntry drives one proposal through the real apply path. Signing
// orders read no gated attributes, so the execution plan is empty and the
// coverage bits with it.
func applySigningEntry(t *testing.T, machine *state.Machine, store *dal.Store, index uint64, orders ...*raftcmdpb.Order) {
	t.Helper()

	proposal := &raftcmdpb.Proposal{
		Id:            index,
		Orders:        orders,
		Date:          &commonpb.Timestamp{Data: 1700000000 + index},
		ExecutionPlan: &raftcmdpb.ExecutionPlan{},
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

func applyPurgeParityEntry(t *testing.T, machine *state.Machine, store *dal.Store, attrs *attributes.Attributes, index uint64, order *raftcmdpb.Order) {
	t.Helper()

	ledger := order.GetLedgerScoped().GetLedger()
	canonicals := []struct {
		code byte
		key  []byte
	}{
		{dal.SubAttrLedger, domain.LedgerKey{Name: ledger}.Bytes()},
		{dal.SubAttrBoundary, domain.LedgerKey{Name: ledger}.Bytes()},
	}
	if create := order.GetLedgerScoped().GetApply().GetCreateTransaction(); create != nil {
		if create.GetReference() != "" {
			canonicals = append(canonicals, struct {
				code byte
				key  []byte
			}{dal.SubAttrReference, domain.TransactionReferenceKey{LedgerName: ledger, Reference: create.GetReference()}.Bytes()})
		}
		for _, posting := range create.GetPostings() {
			for _, account := range []string{posting.GetSource(), posting.GetDestination()} {
				canonicals = append(canonicals, struct {
					code byte
					key  []byte
				}{dal.SubAttrVolume, domain.NewVolumeKey(ledger, account, posting.GetAsset(), "").Bytes()})
			}
		}
	}

	handle, err := store.NewDirectReadHandle()
	require.NoError(t, err)
	plans := make([]*raftcmdpb.AttributeCoverage, 0, len(canonicals))
	for _, item := range canonicals {
		id, tag := attributes.MakeKey(item.key)
		plan := &raftcmdpb.AttributeCoverage{Id: &raftcmdpb.AttributeID{Id: id[:], Tag: tag}, AttrCode: uint32(item.code), CanonicalKey: item.key}
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
	order.Technical = &raftcmdpb.OrderTechnical{CoverageBits: bits}
	proposal := &raftcmdpb.Proposal{Id: index, Orders: []*raftcmdpb.Order{order}, Date: &commonpb.Timestamp{Data: 1700000000 + index}, ExecutionPlan: &raftcmdpb.ExecutionPlan{Attributes: plans}}
	data, err := proto.Marshal(proposal)
	require.NoError(t, err)
	result, err := machine.ApplyEntries(context.Background(), store, &raftpb.Entry{Index: new(index), Term: proto.Uint64(1), Type: new(raftpb.EntryNormal), Data: data})
	require.NoError(t, err)
	require.NoError(t, result.Results[0].Error)
}

func signingRegisterOrder(keyID string, publicKey []byte, parentKeyID string) *raftcmdpb.Order {
	return &raftcmdpb.Order{
		Type: &raftcmdpb.Order_SystemScoped{
			SystemScoped: &raftcmdpb.SystemScopedOrder{
				Payload: &raftcmdpb.SystemScopedOrder_RegisterSigningKey{
					RegisterSigningKey: &raftcmdpb.RegisterSigningKeyOrder{
						KeyId:       keyID,
						PublicKey:   publicKey,
						ParentKeyId: parentKeyID,
					},
				},
			},
		},
	}
}

func signingRevokeOrder(keyID string, cascade bool) *raftcmdpb.Order {
	return &raftcmdpb.Order{
		Type: &raftcmdpb.Order_SystemScoped{
			SystemScoped: &raftcmdpb.SystemScopedOrder{
				Payload: &raftcmdpb.SystemScopedOrder_RevokeSigningKey{
					RevokeSigningKey: &raftcmdpb.RevokeSigningKeyOrder{
						KeyId:   keyID,
						Cascade: cascade,
					},
				},
			},
		},
	}
}

func purgeParityCreateLedgerOrder() *raftcmdpb.Order {
	return &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{
		LedgerScoped: &raftcmdpb.LedgerScopedOrder{
			Ledger: "ledger",
			Payload: &raftcmdpb.LedgerScopedOrder_CreateLedger{CreateLedger: &raftcmdpb.CreateLedgerOrder{
				AccountTypes: map[string]*commonpb.AccountType{
					"orders": {Name: "orders", Pattern: "orders:{id}", Persistence: commonpb.AccountTypePersistence_ACCOUNT_TYPE_EPHEMERAL},
				},
			}},
		},
	}}
}

func purgeParityTransactionOrder(source, destination, reference string) *raftcmdpb.Order {
	return &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{
		LedgerScoped: &raftcmdpb.LedgerScopedOrder{
			Ledger: "ledger",
			Payload: &raftcmdpb.LedgerScopedOrder_Apply{Apply: &raftcmdpb.LedgerApplyOrder{
				Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{CreateTransaction: &raftcmdpb.CreateTransactionOrder{
					Reference: reference,
					Postings:  []*commonpb.Posting{{Source: source, Destination: destination, Asset: "USD", Amount: commonpb.NewUint256FromUint64(5)}},
				}},
			}},
		},
	}}
}

func TestBackup_EphemeralPurgeRestoreParity(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	storage := newInMemoryBackupStorage()
	src, srcStore, srcAttrs := newSigningParityMachine(t)
	applyPurgeParityEntry(t, src, srcStore, srcAttrs, 1, purgeParityCreateLedgerOrder())
	applyPurgeParityEntry(t, src, srcStore, srcAttrs, 2, purgeParityTransactionOrder("world", "orders:1", "fund"))
	require.NoError(t, srcStore.Flush())
	_, err := RunBackup(ctx, testLogger(), srcStore, storage, "bucket", "bk-full")
	require.NoError(t, err)

	applyPurgeParityEntry(t, src, srcStore, srcAttrs, 3, purgeParityTransactionOrder("orders:1", "world", "drain"))
	require.NoError(t, srcStore.Flush())
	inc, err := RunIncrementalBackup(ctx, testLogger(), srcStore, storage, "bucket", 0)
	require.NoError(t, err)
	require.Positive(t, inc.LogEntriesExported)
	manifest, err := ReadManifest(ctx, storage, ManifestKey("bucket"))
	require.NoError(t, err)

	dst, dstStore, dstAttrs := newSigningParityMachine(t)
	applyPurgeParityEntry(t, dst, dstStore, dstAttrs, 1, purgeParityCreateLedgerOrder())
	applyPurgeParityEntry(t, dst, dstStore, dstAttrs, 2, purgeParityTransactionOrder("world", "orders:1", "fund"))
	require.NoError(t, dstStore.Flush())
	require.NoError(t, ApplyExportsAndRebuild(ctx, testLogger(), storage, dstStore, manifest))

	handle, err := dstStore.NewDirectReadHandle()
	require.NoError(t, err)
	volume, err := dstAttrs.Volume.Get(handle, domain.NewVolumeKey("ledger", "orders:1", "USD", "").Bytes())
	require.NoError(t, err)
	require.Nil(t, volume, "restored checkpoint-era ephemeral state must be purged")
	tx, err := dstAttrs.Transaction.Get(handle, domain.TransactionKey{LedgerName: "ledger", ID: 2}.Bytes())
	require.NoError(t, err)
	require.NotNil(t, tx, "the draining transaction mapping must survive restore")
	require.NoError(t, handle.Close())

	var findings []*servicepb.CheckStoreError
	checker := check.NewChecker(dstStore, dstAttrs, signingParityClusterID, nil, testLogger())
	require.NoError(t, checker.Check(ctx, func(event *servicepb.CheckStoreEvent) {
		if e, ok := event.GetType().(*servicepb.CheckStoreEvent_Error); ok {
			findings = append(findings, e.Error)
		}
	}))
	require.Empty(t, findings)
}

func signingKeyIDs(t *testing.T, store *dal.Store) []string {
	t.Helper()

	handle, err := store.NewDirectReadHandle()
	require.NoError(t, err)

	defer func() { _ = handle.Close() }()

	rows, malformed, err := query.ReadSigningKeys(handle)
	require.NoError(t, err)
	require.Empty(t, malformed)

	ids := make([]string, 0, len(rows))
	for keyID := range rows {
		ids = append(ids, keyID)
	}

	return ids
}

// TestBackup_SigningCascadeRestoreParity is the cross-lifecycle proof
// (invariant #11) for the EN-2011 cascade.
//
// The isolated RebuildDelta test feeds handcrafted logs, so it can only show
// that a correct cascaded_key_ids replays correctly. This one closes the loop:
// the batch is applied by the real FSM after a full checkpoint, so the exported
// logs carry whatever cascade the live path actually computed, and the restored
// store is compared against the live source and then handed to the checker.
//
// The checkpoint is what makes it sharp. It carries C's row verbatim, because C
// was live when the backup ran, so only the delta can remove it — a cascade that
// missed C on the live path, or a cascaded_key_ids that failed to travel, leaves
// a usable credential on the restored cluster.
func TestBackup_SigningCascadeRestoreParity(t *testing.T) {
	t.Parallel()

	const bucketID = "bucket"

	var (
		parentPub      = bytes.Repeat([]byte{0x11}, 32)
		childPub       = bytes.Repeat([]byte{0x22}, 32)
		replacementPub = bytes.Repeat([]byte{0x33}, 32)
		survivorPub    = bytes.Repeat([]byte{0x44}, 32)
	)

	ctx := context.Background()
	storage := newInMemoryBackupStorage()

	src, srcStore, _ := newSigningParityMachine(t)

	// Pre-checkpoint: P with child C, plus an unrelated root the cascade must
	// leave alone, so the checkpoint is non-trivial and the comparison has a
	// positive case as well as a negative one.
	applySigningEntry(t, src, srcStore, 1, signingRegisterOrder("P", parentPub, ""))
	applySigningEntry(t, src, srcStore, 2, signingRegisterOrder("C", childPub, "P"))
	applySigningEntry(t, src, srcStore, 3, signingRegisterOrder("survivor", survivorPub, ""))
	require.NoError(t, srcStore.Flush())

	full, err := RunBackup(ctx, testLogger(), srcStore, storage, bucketID, "bk-full")
	require.NoError(t, err)
	require.Positive(t, full.LastLogSequence)

	require.ElementsMatch(t, []string{"P", "C", "survivor"}, signingKeyIDs(t, srcStore),
		"the checkpoint is taken while C is live, so it carries C's row")

	// After the checkpoint: the reported sequence, in one proposal.
	applySigningEntry(t, src, srcStore, 4,
		signingRevokeOrder("C", false),
		signingRegisterOrder("C", replacementPub, "P"),
		signingRevokeOrder("P", true),
	)
	require.NoError(t, srcStore.Flush())

	require.ElementsMatch(t, []string{"survivor"}, signingKeyIDs(t, srcStore),
		"live apply must leave only the unrelated root")

	inc, err := RunIncrementalBackup(ctx, testLogger(), srcStore, storage, bucketID, 0)
	require.NoError(t, err)
	require.Positive(t, inc.LogEntriesExported, "the post-checkpoint batch must export")

	manifest, err := ReadManifest(ctx, storage, ManifestKey(bucketID))
	require.NoError(t, err)

	// The restored cluster: the checkpoint's state, then the delta.
	dst, dstStore, dstAttrs := newSigningParityMachine(t)
	applySigningEntry(t, dst, dstStore, 1, signingRegisterOrder("P", parentPub, ""))
	applySigningEntry(t, dst, dstStore, 2, signingRegisterOrder("C", childPub, "P"))
	applySigningEntry(t, dst, dstStore, 3, signingRegisterOrder("survivor", survivorPub, ""))
	require.NoError(t, dstStore.Flush())

	require.NoError(t, ApplyExportsAndRebuild(ctx, testLogger(), storage, dstStore, manifest))

	require.ElementsMatch(t, signingKeyIDs(t, srcStore), signingKeyIDs(t, dstStore),
		"restored signing authority must equal the live source")
	require.NotContains(t, signingKeyIDs(t, dstStore), "C",
		"a cascade-revoked child must not come back through a restore")

	// The checker re-derives the cascade from the audit chain and compares it to
	// the restored rows, so agreement here is the audit side of the same claim.
	var findings []*servicepb.CheckStoreError

	checker := check.NewChecker(dstStore, dstAttrs, signingParityClusterID, nil, testLogger())
	require.NoError(t, checker.Check(ctx, func(event *servicepb.CheckStoreEvent) {
		if e, ok := event.GetType().(*servicepb.CheckStoreEvent_Error); ok {
			findings = append(findings, e.Error)
		}
	}))

	require.Empty(t, findings, "the restored store must carry no integrity findings")
}
