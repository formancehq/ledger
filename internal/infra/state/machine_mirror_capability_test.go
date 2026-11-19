package state

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/capabilities"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func TestMirrorPromotionCapabilitySelectsHistoricalAndCheckedApply(t *testing.T) {
	for _, checked := range []bool{false, true} {
		t.Run(map[bool]string{false: "historical", true: "checked"}[checked], func(t *testing.T) {
			machine, store, _ := newTestMachine(t)
			create := createLedgerOrder("mirror")
			create.GetLedgerScoped().GetCreateLedger().Mode = commonpb.LedgerMode_LEDGER_MODE_MIRROR
			result, err := machine.ApplyEntries(t.Context(), store, makeEntry(t, 1, makeProposal(1, create)))
			require.NoError(t, err)
			require.NoError(t, result.Results[0].Error)

			promotion := &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: "mirror", Payload: &raftcmdpb.LedgerScopedOrder_PromoteLedger{PromoteLedger: &raftcmdpb.PromoteLedgerOrder{}},
			}}}
			proposal := makeProposal(2, promotion)
			if checked {
				proposal.RequiredCapabilities = []uint32{uint32(capabilities.MirrorPromotionCatchUp)}
			}
			result, err = machine.ApplyEntries(t.Context(), store, makeEntry(t, 2, proposal))
			require.NoError(t, err)
			if checked {
				var lag *domain.ErrMirrorNotCaughtUp
				require.ErrorAs(t, result.Results[0].Error, &lag)
				require.Zero(t, lag.Head)
			} else {
				require.NoError(t, result.Results[0].Error)
			}
		})
	}
}
