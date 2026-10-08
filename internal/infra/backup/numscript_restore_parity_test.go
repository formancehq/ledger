package backup

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	"github.com/formancehq/ledger/v3/internal/application/admission"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/crypto/keystore"
	"github.com/formancehq/ledger/v3/internal/domain/processing/numscript"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/cache"
	"github.com/formancehq/ledger/v3/internal/infra/health"
	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/infra/plan"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/commands"
	"github.com/formancehq/ledger/v3/internal/pkg/futures"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/pkg/actions"
)

// applyingProposer replaces only Raft transport: the bytes admission's
// Builder.Run produced are applied by the real FSM, so the scripted orders
// under test carry the execution plan and coverage bits production admission
// binds. The logs and audit entries exported below are the ones live apply emits.
type applyingProposer struct {
	t       *testing.T
	machine *state.Machine
	store   *dal.Store
	tracker *node.IndexTracker
}

func (p *applyingProposer) InitialIndex() uint64 {
	return 0
}

func (p *applyingProposer) Propose(ctx context.Context, proposal *node.Proposal) (*futures.Future[state.ApplyResult], error) {
	result, err := p.machine.ApplyEntries(ctx, p.store, &raftpb.Entry{
		Index: new(p.tracker.Next()),
		Term:  new(uint64(1)),
		Type:  new(raftpb.EntryNormal),
		Data:  proposal.Data(),
	})
	require.NoError(p.t, err)
	require.Len(p.t, result.Results, 1)
	require.NoError(p.t, result.Results[0].Error)
	p.tracker.Increment(1)
	proposal.Resolve(nil, nil)

	future := futures.New[state.ApplyResult]()
	future.Resolve(result.Results[0], nil)

	return future, nil
}

// scriptedNode is a real admission in front of a real FSM over one store.
type scriptedNode struct {
	t         *testing.T
	admission *admission.Admission
}

// newScriptedNode recovers an FSM from store and wires admission to it. The
// machine, the plan builder and admission share one attribute cache and index
// tracker, as on a node.
func newScriptedNode(t *testing.T, store *dal.Store) *scriptedNode {
	t.Helper()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meters := noop.NewMeterProvider()

	attrs := attributes.New()
	c, err := cache.New(1000, meters.Meter("test"))
	require.NoError(t, err)

	registry := state.NewStateRegistry(c, attrs)
	keyStore := keystore.NewKeyStore()
	sharedState := state.NewSharedState()

	machine, err := state.NewMachine(logger, registry, state.NewCacheSnapshotter(logger, registry, nil), store,
		dal.NewSentinelFactory(store, false), meters, keyStore, sharedState, signingParityNotifier{}, nil,
		"", 0, func(*raftpb.Entry, *dal.WriteSession) error { return nil })
	require.NoError(t, err)
	require.NoError(t, state.NewRecovery(machine, store).RecoverState())

	// Settle one empty entry so the first admitted proposal starts on a fresh
	// index, then predict from the one after it.
	next := machine.State.LastAppliedIndex + 1
	_, err = machine.ApplyEntries(ctx, store, &raftpb.Entry{Index: new(next), Term: new(uint64(1)), Type: new(raftpb.EntryNormal)})
	require.NoError(t, err)

	tracker := node.NewIndexTracker(next + 1)
	builder := plan.NewBuilder(tracker, c, attrs, store, nil, logger, 0)

	writeGate := health.NewMockWriteGate(gomock.NewController(t))
	writeGate.EXPECT().CheckWritesAllowed().Return(nil).AnyTimes()

	return &scriptedNode{
		t: t,
		admission: admission.NewAdmission(store, logger,
			&applyingProposer{t: t, machine: machine, store: store, tracker: tracker},
			builder, meters, writeGate, keyStore, sharedState, attrs,
			numscript.NewNumscriptCache(0), func(context.Context) error { return nil }),
	}
}

func (n *scriptedNode) apply(reqs ...*servicepb.Request) {
	n.t.Helper()

	ctx := internalauth.WithSystemActor(logging.TestingContext(), commands.ComponentClusterPolicy)
	_, err := n.admission.Admit(ctx, servicepb.UnsignedApplyRequest("", reqs...))
	require.NoError(n.t, err)
}

const (
	numscriptRestoreLedger = "scripted"
	numscriptRestoreAsset  = "USD/2"

	// numscriptRestorePay is the library script: a vars-driven payment out of
	// @alice that also writes account and transaction metadata.
	numscriptRestorePay = `vars {
  monetary $amt
  account $dest
}

send $amt (
  source = @alice
  destination = $dest
)

set_account_meta($dest, "paid_by", "alice")
set_tx_meta("route", "library")`

	// numscriptRestorePayV2 drains @bob before @alice, so its postings depend
	// on balances the delta itself produced.
	numscriptRestorePayV2 = `vars {
  monetary $amt
  account $dest
}

send $amt (
  source = {
    @bob
    @alice
  }
  destination = $dest
)

set_account_meta($dest, "paid_by", "bob-then-alice")
set_tx_meta("route", "library-v2")`
)

var numscriptRestoreAccounts = []string{"world", "alice", "bob", "carol", "dave", "erin"}

// numscriptRestoreView is the business projection of the scripted ledger the
// test compares between the live source and the restored store.
type numscriptRestoreView struct {
	volumes      map[string]*raftcmdpb.VolumePair
	metadata     map[string]*commonpb.MetadataValue
	transactions map[uint64]*commonpb.TransactionState
	library      map[string]*commonpb.NumscriptInfo
}

func readNumscriptRestoreView(t *testing.T, store *dal.Store) numscriptRestoreView {
	t.Helper()

	attrs := attributes.New()

	handle, err := store.NewDirectReadHandle()
	require.NoError(t, err)

	defer func() { require.NoError(t, handle.Close()) }()

	view := numscriptRestoreView{
		volumes:      map[string]*raftcmdpb.VolumePair{},
		metadata:     map[string]*commonpb.MetadataValue{},
		transactions: map[uint64]*commonpb.TransactionState{},
		library:      map[string]*commonpb.NumscriptInfo{},
	}

	for _, account := range numscriptRestoreAccounts {
		volume, err := attrs.Volume.Get(handle, domain.NewVolumeKey(numscriptRestoreLedger, account, numscriptRestoreAsset, "").Bytes())
		require.NoError(t, err)

		if volume != nil {
			view.volumes[account] = volume
		}

		for _, key := range []string{"paid_by", "tier"} {
			value, err := attrs.Metadata.Get(handle, domain.MetadataKey{
				AccountKey: domain.AccountKey{LedgerName: numscriptRestoreLedger, Account: account},
				Key:        key,
			}.Bytes())
			require.NoError(t, err)

			if value != nil {
				view.metadata[account+"/"+key] = value
			}
		}
	}

	// Scan well past the last allocated ID so a duplicated or extra replayed
	// transaction would show up as an unexpected entry.
	for id := range uint64(16) {
		tx, err := attrs.Transaction.Get(handle, domain.TransactionKey{LedgerName: numscriptRestoreLedger, ID: id}.Bytes())
		require.NoError(t, err)

		if tx != nil {
			view.transactions[id] = tx
		}
	}

	for _, version := range []string{"1.0.0", "1.1.0"} {
		info, err := attrs.NumscriptContent.Get(handle, domain.NumscriptEntryKey{
			LedgerName: numscriptRestoreLedger, Name: "pay", Version: version,
		}.Bytes())
		require.NoError(t, err)

		if info != nil {
			view.library[version] = info
		}
	}

	return view
}

func requireNumscriptRestoreViewsEqual(t *testing.T, want, got numscriptRestoreView) {
	t.Helper()

	require.Len(t, got.volumes, len(want.volumes))
	for account, volume := range want.volumes {
		require.Truef(t, proto.Equal(volume, got.volumes[account]), "volume of %s: want %v, got %v", account, volume, got.volumes[account])
	}

	require.Len(t, got.metadata, len(want.metadata))
	for key, value := range want.metadata {
		require.Truef(t, proto.Equal(value, got.metadata[key]), "account metadata %s: want %v, got %v", key, value, got.metadata[key])
	}

	require.Len(t, got.transactions, len(want.transactions))
	for id, tx := range want.transactions {
		require.Truef(t, proto.Equal(tx, got.transactions[id]), "transaction %d: want %v, got %v", id, tx, got.transactions[id])
	}

	require.Len(t, got.library, len(want.library))
	for version, info := range want.library {
		require.Truef(t, proto.Equal(info, got.library[version]), "library pay@%s: want %v, got %v", version, info, got.library[version])
	}
}

// balanceOf is input minus output of an account's USD/2 volume.
func (v numscriptRestoreView) balanceOf(t *testing.T, account string) int64 {
	t.Helper()

	volume := v.volumes[account]
	require.NotNilf(t, volume, "no volume for %s", account)

	return volume.GetInput().ToBigInt().Int64() - volume.GetOutput().ToBigInt().Int64()
}

// TestBackup_NumscriptRestoreParity is the cross-lifecycle proof (invariant
// #11) for VM-executed Numscript: postings and metadata produced by inline and
// library scripts after a full checkpoint must survive checkpoint-plus-delta
// restore. The audit keeps only the business order, never the compiled
// artifact, so the delta carries what live apply produced and the restored
// store is compared against the live source by business value, then handed to
// the checker, which re-runs every audited scripted order itself.
func TestBackup_NumscriptRestoreParity(t *testing.T) {
	t.Parallel()

	const bucketID = "bucket"

	ctx := context.Background()
	storage := newInMemoryBackupStorage()

	srcStore := newBackupTestStore(t)
	seedBackupTestAuditKey(t, srcStore)

	src := newScriptedNode(t, srcStore)

	// The policy goes through an audited order, as on a real cluster, so the
	// checker can account for the stored row.
	src.apply(&servicepb.Request{Type: &servicepb.Request_SetClusterPolicy{
		SetClusterPolicy: &servicepb.SetClusterPolicyRequest{Policy: &commonpb.ClusterPolicy{
			Revision: 1, QueryCheckpointLimit: 10,
			MetadataMaxEntriesPerEntity: domain.DefaultMetadataMaxEntriesPerEntity,
			MetadataMaxKeyBytes:         domain.DefaultMetadataMaxKeyBytes, MetadataMaxValueBytes: domain.DefaultMetadataMaxValueBytes,
			MetadataMaxEntityBytes: domain.DefaultMetadataMaxEntityBytes, MetadataMaxCommandBytes: domain.DefaultMetadataMaxCommandBytes,
		}},
	}})

	// Before the checkpoint: the ledger, the library's first version, and one
	// inline and one library scripted transaction, so the checkpoint seeds
	// non-zero balances and metadata the delta builds on.
	src.apply(actions.CreateLedgerAction(numscriptRestoreLedger, nil))
	src.apply(actions.SaveNumscriptWithVersionAction(numscriptRestoreLedger, "pay", numscriptRestorePay, "1.0.0"))
	src.apply(actions.CreateScriptTransactionAction(numscriptRestoreLedger, `send [USD/2 1000] (
  source = @world
  destination = @alice
)

set_tx_meta("route", "inline")`, nil, nil))
	src.apply(actions.CreateScriptRefTransactionAction(numscriptRestoreLedger, "pay", "1.0.0",
		map[string]string{"amt": "USD/2 100", "dest": "carol"}, nil))
	require.NoError(t, srcStore.Flush())

	_, err := RunBackup(ctx, testLogger(), srcStore, storage, bucketID, "bk-full")
	require.NoError(t, err)

	atCheckpoint := readNumscriptRestoreView(t, srcStore)
	require.Len(t, atCheckpoint.transactions, 2)

	// After the checkpoint, every scripted variant: an inline script reading
	// balances and writing account metadata, an exact library version, a
	// library version saved inside the delta, and "latest" resolving to it.
	src.apply(actions.CreateScriptTransactionAction(numscriptRestoreLedger, `vars {
  monetary $amt
}

send $amt (
  source = @alice
  destination = @bob
)

set_account_meta(@bob, "tier", "gold")
set_tx_meta("route", "inline")`, map[string]string{"amt": "USD/2 300"}, nil))
	src.apply(actions.CreateScriptRefTransactionAction(numscriptRestoreLedger, "pay", "1.0.0",
		map[string]string{"amt": "USD/2 50", "dest": "dave"}, nil))
	src.apply(actions.SaveNumscriptWithVersionAction(numscriptRestoreLedger, "pay", numscriptRestorePayV2, "1.1.0"))
	src.apply(actions.CreateScriptRefTransactionAction(numscriptRestoreLedger, "pay", "latest",
		map[string]string{"amt": "USD/2 350", "dest": "erin"}, nil))
	require.NoError(t, srcStore.Flush())

	live := readNumscriptRestoreView(t, srcStore)
	require.Len(t, live.transactions, 5)
	require.Equal(t, int64(1000-100-300-50-50), live.balanceOf(t, "alice"), "v1.1.0 drains @bob's 300, then 50 from @alice")
	require.Equal(t, int64(0), live.balanceOf(t, "bob"))
	require.Equal(t, int64(350), live.balanceOf(t, "erin"))
	require.Equal(t, "bob-then-alice", live.metadata["erin/paid_by"].GetStringValue())
	require.Len(t, live.library, 2)

	inc, err := RunIncrementalBackup(ctx, testLogger(), srcStore, storage, bucketID, 0)
	require.NoError(t, err)
	require.Positive(t, inc.LogEntriesExported, "the post-checkpoint scripted orders must export")
	require.Positive(t, inc.AuditEntriesExported, "the post-checkpoint scripted orders must export")

	manifest, err := ReadManifest(ctx, storage, ManifestKey(bucketID))
	require.NoError(t, err)
	require.NotNil(t, manifest.Checkpoint)
	require.NotEmpty(t, manifest.Exports)

	// The restored cluster: the checkpoint files, then the delta.
	restored := restoreAuditKeyCheckpoint(t, storage, manifest)

	requireNumscriptRestoreViewsEqual(t, live, readNumscriptRestoreView(t, restored))

	// The checker re-runs every audited order, recompiling each script itself
	// since the audit never holds the artifact, and compares with the restored
	// rows: agreement is the audit side of the same claim.
	auditKeyCheckerClean(t, restored)

	// The next decision consumes the restored projection: a scripted payment
	// out of @alice on the restored store succeeds against her restored
	// balance, which the delta alone produced.
	require.NoError(t, attributes.PrepareForBackup(restored))
	newScriptedNode(t, restored).apply(actions.CreateScriptRefTransactionAction(numscriptRestoreLedger, "pay", "1.0.0",
		map[string]string{"amt": "USD/2 500", "dest": "carol"}, nil))

	after := readNumscriptRestoreView(t, restored)
	require.Equal(t, live.balanceOf(t, "alice")-500, after.balanceOf(t, "alice"))
	require.Equal(t, live.balanceOf(t, "carol")+500, after.balanceOf(t, "carol"))
	auditKeyCheckerClean(t, restored)
}
