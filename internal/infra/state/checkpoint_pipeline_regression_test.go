package state

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain/crypto/keystore"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/cache"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// TestPipelinedApplyWithCheckpointDoesNotDiverge is the regression test for the
// Antithesis-observed cache/pebble divergence (issue #424 / EN-1235).
//
// The pre-fix race: PrepareEntries(N+1) could call commitAndRequestCheckpoint
// to commit its batch SYNCHRONOUSLY while pb_N's commit was still in flight
// on runCommitter, racing two writers against the same Pebble store.
//
// The fix:
//   - admission + FSM ceinture-bretelles forbid mixing a checkpoint trigger
//     with non-trigger orders unless it's the last order
//   - the FSM never commits synchronously inside PrepareEntries; the
//     PreparedBatch carries the live batch and runCommitter is the single
//     committer
//
// This test exercises the exact shape that used to fail: apply one batch,
// then a second batch where the LAST entry is a CreateQueryCheckpoint, both
// with sentinelMode on. The post-commit sentinel must not report a divergence
// and the checkpoint flag must propagate.
func TestPipelinedApplyWithCheckpointDoesNotDiverge(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meterProvider := noop.NewMeterProvider()
	meter := meterProvider.Meter("test")

	dataStore, err := dal.NewStore(t.TempDir(), logger, meter, dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = dataStore.Close() })

	c, err := cache.New(1000, meter)
	require.NoError(t, err)

	reg := NewStateRegistry(c, attributes.New(), 0)
	snap := NewCacheSnapshotter(logger, reg, nil)
	machine, err := NewMachine(
		logger, reg, snap, dataStore, dal.NewSentinelFactory(dataStore, true), meterProvider,
		keystore.NewKeyStore(), NewSharedState(), newNoopNotifier(t), nil,
		"test-cluster",
		0,
		false,
		noopConfChangeHandler,
	)
	require.NoError(t, err)
	require.NoError(t, NewRecovery(machine, dataStore).RecoverState())

	// Batch 1: a normal CreateLedger.
	_, err = machine.ApplyEntries(context.Background(), dataStore,
		makeEntry(t, 1, makeProposal(1, createLedgerOrder("ledger-a"))),
	)
	require.NoError(t, err)

	// Batch 2: another CreateLedger immediately followed by a
	// CreateQueryCheckpoint. The trigger is the LAST entry of the slice,
	// which is the contract the applier pre-split must maintain.
	createOther := makeEntry(t, 2, makeProposal(2, createLedgerOrder("ledger-b")))
	checkpoint := makeEntry(t, 3, makeProposal(3,
		&raftcmdpb.Order{Type: &raftcmdpb.Order_SystemScoped{
			SystemScoped: &raftcmdpb.SystemScopedOrder{
				Payload: &raftcmdpb.SystemScopedOrder_CreateQueryCheckpoint{
					CreateQueryCheckpoint: &raftcmdpb.CreateQueryCheckpointOrder{},
				},
			},
		}},
	))

	result, err := machine.ApplyEntries(context.Background(), dataStore, createOther, checkpoint)
	require.NoError(t, err, "pipelined apply with trigger-as-last-entry must commit without sentinel divergence")
	require.True(t, result.CheckpointRequired)
	require.NotZero(t, result.QueryCheckpointID)
	require.Len(t, result.Results, 2)
}

// TestPrepareEntries_RejectsTwoCheckpointsSameBatch ensures that even if an
// adversarial caller constructs a slice where two entries are checkpoint
// triggers (the first not last), the FSM defensive check refuses to commit.
// In practice the applier pre-split should never produce such a slice.
func TestPrepareEntries_RejectsTwoCheckpointsSameBatch(t *testing.T) {
	t.Parallel()

	machine, dataStore, _ := newTestMachine(t)
	ctx := context.Background()

	_, err := machine.ApplyEntries(ctx, dataStore, makeEntry(t, 1, makeProposal(1, createLedgerOrder("ledger-1"))))
	require.NoError(t, err)

	chkptOrder := func() *raftcmdpb.Order {
		return &raftcmdpb.Order{Type: &raftcmdpb.Order_SystemScoped{
			SystemScoped: &raftcmdpb.SystemScopedOrder{
				Payload: &raftcmdpb.SystemScopedOrder_CreateQueryCheckpoint{
					CreateQueryCheckpoint: &raftcmdpb.CreateQueryCheckpointOrder{},
				},
			},
		}}
	}

	first := makeEntry(t, 2, makeProposal(10, chkptOrder()))
	second := makeEntry(t, 3, makeProposal(11, chkptOrder()))

	_, err = machine.PrepareEntries(ctx, dataStore, first, second)
	require.Error(t, err)
	require.Contains(t, err.Error(), "applier must pre-split")
}
