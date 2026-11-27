package plan

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/infra/preload"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/commands"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func BenchmarkPrepareCapabilityProposal(b *testing.B) {
	store, err := dal.NewStore(b.TempDir(), logging.Testing(), noop.Meter{}, dal.DefaultConfig())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = store.Close() })
	batch := store.OpenWriteSession()
	if err := state.SaveClusterPolicy(batch, &commonpb.ClusterPolicy{
		CapabilityActivations: []*commonpb.CapabilityActivation{{Id: 1, Active: true}},
	}); err != nil {
		b.Fatal(err)
	}
	if err := batch.Commit(); err != nil {
		b.Fatal(err)
	}
	builder := &Builder{store: store}
	for _, count := range []int{1, 100, 1000} {
		b.Run(fmt.Sprintf("orders-%d", count), func(b *testing.B) {
			orders := make([]*raftcmdpb.Order, count)
			for i := range orders {
				orders[i] = &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{
					Ledger: "ledger", Payload: &raftcmdpb.LedgerScopedOrder_Apply{Apply: &raftcmdpb.LedgerApplyOrder{
						Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{CreateTransaction: &raftcmdpb.CreateTransactionOrder{}},
					}},
				}}}
			}
			b.ReportAllocs()
			for b.Loop() {
				cmd := &raftcmdpb.Proposal{Id: 1, Orders: orders}
				if _, err := builder.prepareProposal(cmd); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestRunnerSelectsCapabilityFromCommittedPolicyBeforeSerialization(t *testing.T) {
	t.Parallel()
	tracker := node.NewIndexTracker(1)
	builder := newSubmitTestBuilder(t, tracker)
	proposer := &recordingProposer{tracker: tracker}

	proposeTransaction := func() error {
		cmd := commands.NewCommand(&raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{
			Ledger: "ledger", Payload: &raftcmdpb.LedgerScopedOrder_Apply{Apply: &raftcmdpb.LedgerApplyOrder{
				Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{CreateTransaction: &raftcmdpb.CreateTransactionOrder{}},
			}},
		}}})
		build := &BuildResult{aggregate: NewCoverage(), token: &preload.CleanupToken{}}
		result, err := builder.Run(t.Context(), cmd, build, func(*raftcmdpb.Proposal) error { return nil }, proposer)
		if err != nil {
			return err
		}
		result.Guard.ReleaseLoaders()

		return nil
	}

	prepared := make(chan struct{})
	resume := make(chan struct{})
	builder.afterPrepare = func() {
		close(prepared)
		<-resume
	}
	firstResult := make(chan error, 1)
	go func() { firstResult <- proposeTransaction() }()
	<-prepared
	batch := builder.store.OpenWriteSession()
	require.NoError(t, state.SaveClusterPolicy(batch, &commonpb.ClusterPolicy{
		CapabilityActivations: []*commonpb.CapabilityActivation{{Id: 1, Active: true}},
	}))
	require.NoError(t, batch.Commit())
	close(resume)
	require.NoError(t, <-firstResult)
	builder.afterPrepare = nil
	require.NoError(t, proposeTransaction())

	proposals := proposer.capturedProposals(t)
	require.Len(t, proposals, 2)
	require.Empty(t, proposals[0].GetRequiredCapabilities(), "the proposal prepared before activation keeps its semantics at submission")
	require.Equal(t, []uint32{1}, proposals[1].GetRequiredCapabilities())
}

func TestRunnerSerializesPayloadOnEveryRetry(t *testing.T) {
	t.Parallel()
	for _, policy := range []bool{false, true} {
		t.Run(map[bool]string{false: "technical", true: "policy"}[policy], func(t *testing.T) {
			t.Parallel()
			tracker := node.NewIndexTracker(1)
			builder := newSubmitTestBuilder(t, tracker)
			proposer := &recordingProposer{tracker: tracker, staleRemain: 1}
			cmd := commands.NewTechnicalCommand(&raftcmdpb.TechnicalUpdate{Kind: &raftcmdpb.TechnicalUpdate_AuditKey{AuditKey: make([]byte, 32)}})
			cmd.CallerSnapshot = commands.SystemCallerSnapshot(commands.ComponentClusterConfig)
			ops := []WriteOperation{{Target: &cmd.GetBatch().TechnicalUpdates[0].CoverageBits}}
			if policy {
				cmd.Payload = &raftcmdpb.Proposal_SetClusterPolicy{SetClusterPolicy: &raftcmdpb.SetClusterPolicyProposal{Policy: &commonpb.ClusterPolicy{Revision: 2}}}
				cmd.CallerSnapshot = commands.SystemCallerSnapshot(commands.ComponentClusterPolicy)
				ops = nil
			}
			require.NoError(t, SubmitTechnical(t.Context(), builder, proposer, cmd, ops, "payload"))
			all := proposer.capturedProposals(t)
			require.Len(t, all, 2)
			for _, p := range all {
				require.NotNil(t, p.GetPayload())
				require.Empty(t, p.GetOrders())
				require.Empty(t, p.GetTechnicalUpdates())
				require.NotZero(t, p.GetPredictedIndex())
				if policy {
					require.NotNil(t, p.GetSetClusterPolicy())
					require.Equal(t, uint64(2), p.GetSetClusterPolicy().GetPolicy().GetRevision())
				} else {
					require.Len(t, p.GetBatch().GetTechnicalUpdates(), 1)
					require.Len(t, p.GetBatch().GetTechnicalUpdates()[0].GetAuditKey(), 32)
				}
			}
			require.NotEqual(t, all[0].GetId(), all[1].GetId())
			require.Greater(t, all[1].GetPredictedIndex(), all[0].GetPredictedIndex())
			require.NotNil(t, cmd.GetPayload(), "serialization must leave the proposal intact")
			require.Empty(t, cmd.GetOrders())
			require.Empty(t, cmd.GetTechnicalUpdates())
		})
	}
}
