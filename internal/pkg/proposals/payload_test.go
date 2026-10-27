package proposals

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/pkg/commands"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/signaturepb"
)

func TestLegacyWirePreservesPayloadAndCoverageTargets(t *testing.T) {
	t.Parallel()
	order := &raftcmdpb.Order{Technical: &raftcmdpb.OrderTechnical{}}
	logical := &raftcmdpb.Proposal{Id: 3, PredictedIndex: 8, Payload: &raftcmdpb.Proposal_Batch{Batch: &raftcmdpb.ExecutionBatch{Orders: []*raftcmdpb.Order{order}}}}
	before := logical.CloneVT()
	wire, err := LegacyWire(logical)
	require.NoError(t, err)
	require.Nil(t, wire.GetPayload())
	require.Same(t, order, wire.GetOrders()[0])
	require.True(t, proto.Equal(before, logical), "encoding must not mutate the logical proposal")
	order.Technical.CoverageBits = []byte{7}
	require.Equal(t, []byte{7}, wire.GetOrders()[0].GetTechnical().GetCoverageBits())
	require.Equal(t, logical.GetPredictedIndex(), wire.GetPredictedIndex())
}

func TestPolicyEncodingKeepsAuditIntent(t *testing.T) {
	t.Parallel()
	policy := &commonpb.ClusterPolicy{Revision: 7}
	control := &raftcmdpb.Proposal{
		CallerSnapshot: commands.SystemCallerSnapshot(commands.ComponentClusterPolicy),
		Payload:        &raftcmdpb.Proposal_SetClusterPolicy{SetClusterPolicy: &raftcmdpb.SetClusterPolicyProposal{Policy: policy}},
	}
	legacy, err := LegacyWire(control)
	require.NoError(t, err)
	require.Nil(t, legacy.GetPayload())
	require.True(t, proto.Equal(PolicyOrder(policy), legacy.GetOrders()[0]))
	control.Idempotency = &commonpb.Idempotency{Key: "client-key"}
	require.ErrorContains(t, ValidateShape(control), "idempotency or signature")
	control.Idempotency = nil
	control.Signature = &signaturepb.SignedApplyBatch{}
	require.ErrorContains(t, ValidateShape(control), "idempotency or signature")
}

func TestValidateShapeRejectsAmbiguousAndMixedPayloads(t *testing.T) {
	t.Parallel()
	for name, proposal := range map[string]*raftcmdpb.Proposal{
		"nil":                         nil,
		"empty batch":                 {Payload: &raftcmdpb.Proposal_Batch{Batch: &raftcmdpb.ExecutionBatch{}}},
		"legacy plus payload":         {Orders: []*raftcmdpb.Order{{}}, Payload: &raftcmdpb.Proposal_Batch{Batch: &raftcmdpb.ExecutionBatch{Orders: []*raftcmdpb.Order{{}}}}},
		"policy in batch":             {Payload: &raftcmdpb.Proposal_Batch{Batch: &raftcmdpb.ExecutionBatch{Orders: []*raftcmdpb.Order{PolicyOrder(nil)}}}},
		"nil order":                   {Payload: &raftcmdpb.Proposal_Batch{Batch: &raftcmdpb.ExecutionBatch{Orders: []*raftcmdpb.Order{nil}}}},
		"nil update":                  {Payload: &raftcmdpb.Proposal_Batch{Batch: &raftcmdpb.ExecutionBatch{TechnicalUpdates: []*raftcmdpb.TechnicalUpdate{nil}}}},
		"unattributed policy":         {Payload: &raftcmdpb.Proposal_SetClusterPolicy{SetClusterPolicy: &raftcmdpb.SetClusterPolicyProposal{}}},
		"unrelated update with order": {Payload: &raftcmdpb.Proposal_Batch{Batch: &raftcmdpb.ExecutionBatch{Orders: []*raftcmdpb.Order{{}}, TechnicalUpdates: []*raftcmdpb.TechnicalUpdate{{Kind: &raftcmdpb.TechnicalUpdate_AuditKey{AuditKey: make([]byte, 32)}}}}}},
	} {
		t.Run(name, func(t *testing.T) { require.Error(t, ValidateShape(proposal)) })
	}
	// A historical composition must remain readable even when new writers
	// cannot emit it. Never apply new shape restrictions to the old format.
	require.NoError(t, ValidateShape(&raftcmdpb.Proposal{Orders: []*raftcmdpb.Order{PolicyOrder(nil)}, TechnicalUpdates: []*raftcmdpb.TechnicalUpdate{{}}}))
	require.NoError(t, ValidateShape(&raftcmdpb.Proposal{}))
}

func TestMixedMirrorBatchIsLimitedToItsLedger(t *testing.T) {
	t.Parallel()
	order := &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{
		Ledger: "mirror", Payload: &raftcmdpb.LedgerScopedOrder_MirrorIngest{MirrorIngest: &raftcmdpb.MirrorIngestOrder{}},
	}}}
	update := &raftcmdpb.TechnicalUpdate{Kind: &raftcmdpb.TechnicalUpdate_MirrorSync{MirrorSync: &raftcmdpb.MirrorSyncUpdate{LedgerName: "mirror"}}}
	p := &raftcmdpb.Proposal{Payload: &raftcmdpb.Proposal_Batch{Batch: &raftcmdpb.ExecutionBatch{Orders: []*raftcmdpb.Order{order}, TechnicalUpdates: []*raftcmdpb.TechnicalUpdate{update}}}}
	require.NoError(t, ValidateShape(p))
	update.GetMirrorSync().LedgerName = "other"
	require.ErrorContains(t, ValidateShape(p), "same ledger")
}

// Compare exact bytes, not only decoded fields: the expansion phase must not
// change what an old replica receives, including the shared header.
func TestLegacyWireMatchesReleasedEncoding(t *testing.T) {
	t.Parallel()
	order := &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{Ledger: "ledger", Payload: &raftcmdpb.LedgerScopedOrder_CreateLedger{CreateLedger: &raftcmdpb.CreateLedgerOrder{}}}}, Technical: &raftcmdpb.OrderTechnical{CoverageBits: []byte{1}}}
	update := &raftcmdpb.TechnicalUpdate{Kind: &raftcmdpb.TechnicalUpdate_EventsSink{EventsSink: &raftcmdpb.EventsSinkUpdate{SinkName: "sink", Cursor: 2}}}
	policy := &commonpb.ClusterPolicy{Revision: 3}
	for _, kind := range []string{"business", "technical", "policy", "barrier"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			legacy := &raftcmdpb.Proposal{Id: 7, Date: &commonpb.Timestamp{Data: 9}, PredictedIndex: 11, ExecutionPlan: &raftcmdpb.ExecutionPlan{CacheEpoch: 2}, CallerSnapshot: commands.SystemCallerSnapshot(commands.ComponentClusterPolicy)}
			logical := legacy.CloneVT()
			switch kind {
			case "business":
				legacy.Orders = []*raftcmdpb.Order{order}
				legacy.Idempotency = &commonpb.Idempotency{Key: "key"}
				legacy.Signature = &signaturepb.SignedApplyBatch{}
				logical.Idempotency = legacy.GetIdempotency()
				logical.Signature = legacy.GetSignature()
				logical.Payload = &raftcmdpb.Proposal_Batch{Batch: &raftcmdpb.ExecutionBatch{Orders: []*raftcmdpb.Order{order}}}
			case "technical":
				legacy.TechnicalUpdates = []*raftcmdpb.TechnicalUpdate{update}
				logical.Payload = &raftcmdpb.Proposal_Batch{Batch: &raftcmdpb.ExecutionBatch{TechnicalUpdates: []*raftcmdpb.TechnicalUpdate{update}}}
			case "policy":
				legacy.Orders = []*raftcmdpb.Order{PolicyOrder(policy)}
				logical.Payload = &raftcmdpb.Proposal_SetClusterPolicy{SetClusterPolicy: &raftcmdpb.SetClusterPolicyProposal{Policy: policy}}
			}
			before := logical.CloneVT()
			wire, err := LegacyWire(logical)
			require.NoError(t, err)
			expected, err := legacy.MarshalVT()
			require.NoError(t, err)
			actual, err := wire.MarshalVT()
			require.NoError(t, err)
			require.Equal(t, expected, actual)
			require.True(t, proto.Equal(before, logical))
		})
	}
}

func BenchmarkLegacyWire(b *testing.B) {
	for _, count := range []int{1, 100, 1000} {
		b.Run(fmt.Sprintf("orders=%d", count), func(b *testing.B) {
			orders := make([]*raftcmdpb.Order, count)
			for i := range orders {
				orders[i] = &raftcmdpb.Order{Technical: &raftcmdpb.OrderTechnical{CoverageBits: []byte{1}}}
			}
			logical := &raftcmdpb.Proposal{Id: 1, Payload: &raftcmdpb.Proposal_Batch{Batch: &raftcmdpb.ExecutionBatch{Orders: orders}}}
			b.ReportAllocs()
			for b.Loop() {
				wire, err := LegacyWire(logical)
				if err != nil {
					b.Fatal(err)
				}
				if _, err = wire.MarshalVT(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
