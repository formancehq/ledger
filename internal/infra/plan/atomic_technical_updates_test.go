package plan

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func TestRun_BridgeAlwaysEmitsLegacyTechnicalUpdates(t *testing.T) {
	t.Parallel()
	for _, attributesNeeded := range []bool{false, true} {
		t.Run(map[bool]string{false: "no preload", true: "attribute preload"}[attributesNeeded], func(t *testing.T) {
			t.Parallel()
			tracker := node.NewIndexTracker(1)
			builder := newSubmitTestBuilder(t, tracker)
			needs := NewCoverage()
			if attributesNeeded {
				needs.Add(dal.SubAttrLedger, (domain.LedgerKey{Name: "bridge"}).Bytes())
			}
			build, err := builder.Build(needs, nil)
			require.NoError(t, err)
			cmd := &raftcmdpb.Proposal{AtomicTechnicalUpdates: true}
			proposer := &recordingProposer{tracker: tracker}
			_, err = builder.Run(t.Context(), cmd, build, func(p *raftcmdpb.Proposal) ([]byte, error) {
				require.False(t, p.GetAtomicTechnicalUpdates())

				return p.MarshalVT()
			}, proposer)
			require.NoError(t, err)
			captured := proposer.capturedProposals(t)
			require.Len(t, captured, 1)
			require.False(t, captured[0].GetAtomicTechnicalUpdates())
		})
	}
}
