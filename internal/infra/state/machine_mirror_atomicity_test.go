package state

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func TestMirrorSync_AtomicWithIngestAfterPromotion(t *testing.T) {
	t.Parallel()
	for _, promoted := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "promotion races ingestion"}[promoted], func(t *testing.T) {
			t.Parallel()
			machine, store, _ := newTestMachine(t)
			create := createLedgerOrder("mirror")
			create.GetLedgerScoped().GetCreateLedger().Mode = commonpb.LedgerMode_LEDGER_MODE_MIRROR
			result, err := machine.ApplyEntries(t.Context(), store, makeEntry(t, 1, makeProposal(1, create)))
			require.NoError(t, err)
			require.NoError(t, result.Results[0].Error)

			// Establish a prior committed head; the next update must either
			// publish with ingestion or leave this value intact on rejection.
			prior := makeProposal(2)
			id, _ := attributes.MakeKey((domain.LedgerKey{Name: "mirror"}).Bytes())
			prior.ExecutionPlan.Attributes = []*raftcmdpb.AttributeCoverage{declareTestPlan(id, dal.SubAttrLedger)}
			prior.TechnicalUpdates = []*raftcmdpb.TechnicalUpdate{{Kind: &raftcmdpb.TechnicalUpdate_MirrorSync{
				MirrorSync: &raftcmdpb.MirrorSyncUpdate{LedgerName: "mirror", SourceLogCount: 7, Error: &commonpb.MirrorSyncError{Message: "previous error"}},
			}}}
			result, err = machine.ApplyEntries(t.Context(), store, makeEntry(t, 2, prior))
			require.NoError(t, err)
			require.NoError(t, result.Results[0].Error)

			if promoted {
				promotion := &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{
					Ledger: "mirror", Payload: &raftcmdpb.LedgerScopedOrder_PromoteLedger{PromoteLedger: &raftcmdpb.PromoteLedgerOrder{}},
				}}}
				result, err = machine.ApplyEntries(t.Context(), store, makeEntry(t, 3, makeProposal(3, promotion)))
				require.NoError(t, err)
				require.NoError(t, result.Results[0].Error)
			}

			ingest := &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: "mirror", Payload: &raftcmdpb.LedgerScopedOrder_MirrorIngest{MirrorIngest: &raftcmdpb.MirrorIngestOrder{
					Entry: &raftcmdpb.MirrorLogEntry{V2LogId: 1, Data: &raftcmdpb.MirrorLogEntry_FillGap{FillGap: &raftcmdpb.MirrorFillGap{}}},
				}},
			}}}
			proposal := makeProposal(4, ingest)
			proposal.TechnicalUpdates = []*raftcmdpb.TechnicalUpdate{{Kind: &raftcmdpb.TechnicalUpdate_MirrorSync{
				MirrorSync: &raftcmdpb.MirrorSyncUpdate{LedgerName: "mirror", SourceLogCount: 99, ClearError: true},
			}}}
			index := uint64(3)
			if promoted {
				index = 4
			}
			result, err = machine.ApplyEntries(t.Context(), store, makeEntry(t, index, proposal))
			require.NoError(t, err)
			wantHead, wantPosition := uint64(99), uint64(1)
			if promoted {
				var rejection *domain.ErrLedgerNotInMirrorMode
				require.ErrorAs(t, result.Results[0].Error, &rejection)
				wantHead, wantPosition = 7, 0
			} else {
				require.NoError(t, result.Results[0].Error)
			}
			for _, restarted := range []bool{false, true} {
				if restarted {
					recovered := recoverMachineOnStore(t, store)
					require.Equal(t, index, recovered.LastAppliedIndex())
				}
				boundary, err := attributes.New().Boundary.Get(store, (domain.LedgerKey{Name: "mirror"}).Bytes())
				require.NoError(t, err)
				require.Equal(t, wantPosition, boundary.GetLastMirrorV2LogId())
				head, err := query.ReadMirrorSourceHead(store, "mirror")
				require.NoError(t, err)
				require.Equal(t, wantHead, head)
				status, err := query.ReadMirrorStatus(store, "mirror")
				require.NoError(t, err)
				if promoted {
					require.Equal(t, "previous error", status.GetMessage())
				} else {
					require.Nil(t, status)
				}
			}
		})
	}
}
