package bootstrap

import (
	"context"

	"github.com/formancehq/ledger/v3/internal/infra/plan"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// backupProposer satisfies application/backup.Proposer by routing each
// BackupOrder / IncrementalBackupOrder TechnicalUpdate through
// proposeTechnical. This:
//   - Serialises the IndexTracker increment with the proposal under the
//     tracker mutex (via plan.Builder.Run), so a backup Start / Progress /
//     Complete / Fail cannot interleave with an admission proposal in a
//     way that hands the other proposal a stale PredictedIndex.
//   - Uses WaitContext on both the Raft acceptance future and the FSM
//     apply future, so a leadership loss after Raft accept does not
//     leave the orchestrator goroutine pinned forever — TerminalProposeTimeout
//     actually bounds the terminal hop, and a cancelled cleanup loop ctx
//     unblocks failStaleJob.
//
// Lives in bootstrap (not application/backup) because plan depends on
// state for ApplyResult, which would create an import cycle if the
// application layer imported plan directly. Mirrors the existing
// indexReadyProposerAdapter and metadataBatchProposer adapters.
type backupProposer struct {
	builder  *plan.Builder
	proposer plan.Proposer
}

func newBackupProposer(builder *plan.Builder, proposer plan.Proposer) *backupProposer {
	return &backupProposer{builder: builder, proposer: proposer}
}

// Propose pushes the proposal through proposeTechnical with empty Needs
// per TU. The backup apply handlers (state.applyBackupOrder /
// applyIncrementalBackupOrder) do not read from the FSM cache — they
// only mutate BackupJobsState's in-memory map and write to Pebble — so
// the coverage_bits stay empty and the scope passed to the handler
// admits nothing.
func (b *backupProposer) Propose(ctx context.Context, cmd *raftcmdpb.Proposal) error {
	tus := cmd.GetTechnicalUpdates()
	operations := make([]plan.WriteOperation, 0, len(tus))

	for i := range tus {
		operations = append(operations, plan.WriteOperation{
			Needs: nil, // backup apply does not read FSM cache
			SetCoverage: func(bits []byte) {
				cmd.GetTechnicalUpdates()[i].CoverageBits = bits
			},
		})
	}

	return proposeTechnical(ctx, b.builder, b.proposer, cmd, operations)
}
