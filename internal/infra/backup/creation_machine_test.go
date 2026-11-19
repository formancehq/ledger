package backup

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
	"github.com/formancehq/ledger/v3/internal/pkg/commands"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

const creationTestAuditKey = "0123456789abcdef0123456789abcdef"

// creationTestNotifier satisfies state.Notifier for a machine driven straight
// through ApplyEntries, where nothing consumes the notifications.
type creationTestNotifier struct{}

func (creationTestNotifier) NotifyLogsCommitted(uint64) {}
func (creationTestNotifier) NotifyConfigChanged()       {}

// newCreationTestMachine builds a real FSM over its own store, so the signing
// logs and audit entries under test are the ones live apply emits rather than
// handcrafted stand-ins.
func newCreationTestMachine(t *testing.T) (*state.Machine, *dal.Store, *attributes.Attributes) {
	t.Helper()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meterProvider := noop.NewMeterProvider()

	store, err := dal.NewStore(t.TempDir(), logger, meterProvider.Meter("test"), dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	seedBackupTestAuditKey(t, store)

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
		creationTestNotifier{},
		nil,
		creationTestAuditKey,
		0,
		func(*raftpb.Entry, *dal.WriteSession) error { return nil },
		state.NoopCapabilityPolicyCommitHandler,
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
		Id:             index,
		Orders:         orders,
		Date:           &commonpb.Timestamp{Data: 1700000000 + index},
		ExecutionPlan:  &raftcmdpb.ExecutionPlan{},
		CallerSnapshot: commands.SystemCallerSnapshot(commands.ComponentClusterConfig),
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
