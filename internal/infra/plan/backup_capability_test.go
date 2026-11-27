package plan

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/infra/capabilities"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func TestBuilderPreparesBackupStartFromCommittedActivation(t *testing.T) {
	t.Parallel()
	builder := newSubmitTestBuilder(t, nil)
	start := func() *raftcmdpb.Proposal {
		return &raftcmdpb.Proposal{Id: 1, TechnicalUpdates: []*raftcmdpb.TechnicalUpdate{{
			Kind: &raftcmdpb.TechnicalUpdate_BackupOrder{BackupOrder: &raftcmdpb.BackupOrder{
				Op: &raftcmdpb.BackupOrder_Start{Start: &raftcmdpb.BackupOrderStart{JobId: 1}},
			}},
		}}}
	}
	before, err := builder.prepareProposal(start())
	require.NoError(t, err)
	require.Empty(t, before.RequiredCapabilities())

	batch := builder.store.OpenWriteSession()
	require.NoError(t, batch.SetProto([]byte{dal.ZoneGlobal, dal.SubGlobClusterPolicy}, &commonpb.ClusterPolicy{
		CapabilityActivations: []*commonpb.CapabilityActivation{{Id: uint32(capabilities.BackupDestinationAlias), Active: true}},
	}))
	require.NoError(t, batch.Commit())

	after, err := builder.prepareProposal(start())
	require.NoError(t, err)
	require.Equal(t, []capabilities.CapabilityID{capabilities.BackupDestinationAlias}, after.RequiredCapabilities())
}
