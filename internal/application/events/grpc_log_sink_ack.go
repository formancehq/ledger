package events

import (
	"context"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/plan"
	"github.com/formancehq/ledger/v3/internal/pkg/commands"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// GrpcLogSinkAcker is the named system producer for inbound stream cursor
// updates. The public adapter validates the stream; this producer owns the
// system attribution, coverage declaration, and Raft submission.
type GrpcLogSinkAcker struct {
	builder  *plan.Builder
	proposer Proposer
}

func NewGrpcLogSinkAcker(builder *plan.Builder, proposer Proposer) *GrpcLogSinkAcker {
	return &GrpcLogSinkAcker{builder: builder, proposer: proposer}
}

func (a *GrpcLogSinkAcker) Ack(ctx context.Context, sinkName string, incarnation, sequence uint64) error {
	update := &raftcmdpb.TechnicalUpdate{Kind: &raftcmdpb.TechnicalUpdate_GrpcLogSinkAck{
		GrpcLogSinkAck: &raftcmdpb.GrpcLogSinkAck{SinkName: sinkName, Incarnation: incarnation, Sequence: sequence},
	}}
	proposal := &raftcmdpb.Proposal{
		Payload: &raftcmdpb.Proposal_Batch{Batch: &raftcmdpb.ExecutionBatch{
			TechnicalUpdates: []*raftcmdpb.TechnicalUpdate{update},
		}},
		CallerSnapshot: commands.SystemCallerSnapshot(commands.ComponentEventsSink),
	}
	coverage := plan.NewCoverage()
	coverage.Add(dal.SubAttrSinkConfig, domain.SinkConfigKey{Name: sinkName}.Bytes())

	return plan.SubmitTechnical(ctx, a.builder, a.proposer, proposal,
		[]plan.WriteOperation{{Coverage: coverage, Target: &update.CoverageBits}}, "grpcLogSinkAck")
}
