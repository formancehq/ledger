package plan

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func TestRun_RejectsUnsupportedTechnicalComposition(t *testing.T) {
	t.Parallel()
	updates := []*raftcmdpb.TechnicalUpdate{
		{Kind: &raftcmdpb.TechnicalUpdate_EventsSink{EventsSink: &raftcmdpb.EventsSinkUpdate{SinkName: "sink", Cursor: 7}}},
		{Kind: &raftcmdpb.TechnicalUpdate_ClusterConfig{}},
		{Kind: &raftcmdpb.TechnicalUpdate_IdempotencyEviction{}},
		{Kind: &raftcmdpb.TechnicalUpdate_BackupOrder{}},
		{Kind: &raftcmdpb.TechnicalUpdate_IncrementalBackupOrder{}},
		{Kind: &raftcmdpb.TechnicalUpdate_AuditKey{AuditKey: make([]byte, 32)}},
	}
	for _, needsAttributes := range []bool{false, true} {
		for _, update := range updates {
			for _, withOrders := range []bool{false, true} {
				t.Run(fmt.Sprintf("%T/attributes=%t/orders=%t", update.GetKind(), needsAttributes, withOrders), func(t *testing.T) {
					t.Parallel()
					tracker := node.NewIndexTracker(1)
					builder := newSubmitTestBuilder(t, tracker)
					needs := NewCoverage()
					if needsAttributes {
						needs.Add(dal.SubAttrLedger, (domain.LedgerKey{Name: "mirror"}).Bytes())
					}
					build, err := builder.Build(needs, nil)
					require.NoError(t, err)
					cmd := &raftcmdpb.Proposal{TechnicalUpdates: []*raftcmdpb.TechnicalUpdate{update}}
					if withOrders {
						cmd.Orders = []*raftcmdpb.Order{{}}
					} else {
						cmd.TechnicalUpdates = append(cmd.TechnicalUpdates, &raftcmdpb.TechnicalUpdate{
							Kind: &raftcmdpb.TechnicalUpdate_MirrorSync{MirrorSync: &raftcmdpb.MirrorSyncUpdate{LedgerName: "mirror"}},
						})
					}
					proposer := &recordingProposer{tracker: tracker}
					marshalled := false
					result, err := builder.Run(t.Context(), cmd, build, func(p *raftcmdpb.Proposal) ([]byte, error) {
						marshalled = true

						return p.MarshalVT()
					}, proposer)
					require.ErrorIs(t, err, ErrUnsupportedTechnicalComposition)
					require.Nil(t, result)
					require.False(t, marshalled)
					require.Empty(t, proposer.capturedProposals(t))
					require.Nil(t, build.token, "rejection must release preload loaders")
				})
			}
		}
	}
}

func TestRun_AllowsSupportedTechnicalComposition(t *testing.T) {
	t.Parallel()
	for _, withOrders := range []bool{false, true} {
		t.Run("mirror", func(t *testing.T) {
			t.Parallel()
			tracker := node.NewIndexTracker(1)
			builder := newSubmitTestBuilder(t, tracker)
			build, err := builder.Build(NewCoverage(), nil)
			require.NoError(t, err)
			cmd := &raftcmdpb.Proposal{TechnicalUpdates: []*raftcmdpb.TechnicalUpdate{{
				Kind: &raftcmdpb.TechnicalUpdate_MirrorSync{MirrorSync: &raftcmdpb.MirrorSyncUpdate{LedgerName: "mirror"}},
			}}}
			if withOrders {
				cmd.Orders = []*raftcmdpb.Order{{}}
			}
			proposeErr := errors.New("reached proposer")
			_, err = builder.Run(t.Context(), cmd, build, (*raftcmdpb.Proposal).MarshalVT, stubFailingProposer{err: proposeErr})
			require.ErrorIs(t, err, proposeErr)
		})
	}
}

func TestRun_AllowsStandaloneOperationalUpdate(t *testing.T) {
	t.Parallel()
	updates := []*raftcmdpb.TechnicalUpdate{
		{Kind: &raftcmdpb.TechnicalUpdate_EventsSink{}},
		{Kind: &raftcmdpb.TechnicalUpdate_ClusterConfig{}},
		{Kind: &raftcmdpb.TechnicalUpdate_IdempotencyEviction{}},
		{Kind: &raftcmdpb.TechnicalUpdate_BackupOrder{}},
		{Kind: &raftcmdpb.TechnicalUpdate_IncrementalBackupOrder{}},
		{Kind: &raftcmdpb.TechnicalUpdate_AuditKey{AuditKey: make([]byte, 32)}},
	}
	for _, update := range updates {
		t.Run(fmt.Sprintf("%T", update.GetKind()), func(t *testing.T) {
			t.Parallel()
			tracker := node.NewIndexTracker(1)
			builder := newSubmitTestBuilder(t, tracker)
			build, err := builder.Build(NewCoverage(), nil)
			require.NoError(t, err)
			cmd := &raftcmdpb.Proposal{TechnicalUpdates: []*raftcmdpb.TechnicalUpdate{update}}
			proposeErr := errors.New("reached proposer")
			_, err = builder.Run(t.Context(), cmd, build, (*raftcmdpb.Proposal).MarshalVT, stubFailingProposer{err: proposeErr})
			require.ErrorIs(t, err, proposeErr)
		})
	}
}
