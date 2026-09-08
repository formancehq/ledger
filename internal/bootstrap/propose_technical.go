package bootstrap

import (
	"context"

	"github.com/formancehq/ledger/v3/internal/infra/plan"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// proposeTechnical submits a technical Raft proposal through the shared
// plan.SubmitTechnical submitter. It blocks until the FSM applies. Used by
// callers that previously went through NodeProposer.ProposeProposal —
// cluster config updates, idempotency eviction, backup orders.
//
// The retry/cleanup/wait lifecycle (per-attempt reset, coverage/guard
// cleanup, Raft-acceptance/FSM-apply waits, bounded stale retries) is
// owned by plan.SubmitTechnical; this wrapper only pins bootstrap's
// caller identity. The caller still supplies the technical updates, the
// declared coverage via operations, and the cancellation context.
func proposeTechnical(ctx context.Context, builder *plan.Builder, proposer plan.Proposer, cmd *raftcmdpb.Proposal, operations []plan.WriteOperation) error {
	return plan.SubmitTechnical(ctx, builder, proposer, cmd, operations, "proposeTechnical")
}
