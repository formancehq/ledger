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
