package processing

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

const tombstonedLedger = "tombstoned-ledger"

// EN-2045 — the deleted-ledger gate covers directly dispatched ledger-scoped
// writes, not just apply-scoped ones.
//
// DeleteLedger keeps the LedgerInfo as a tombstone with DeletedAt set.
// processApply reads that tombstone and rejects before dispatching any
// apply-scoped write, but processLedgerScoped also dispatches commands that
// never reach processApply. Each command below writes state a tombstoned
// ledger can never serve back — query.GetLedgerByName and ListLedgers both
// drop the ledger on DeletedAt before ledger metadata, numscripts, prepared
// queries or mode are read — so accepting one commits a log, and with it an
// audit entry, for a mutation no read can confirm.
//
// The apply case is carried alongside the directly dispatched ones so the
// original processApply trigger keeps an independent regression case of its
// own rather than being represented only through the new gate.
//
// The two commands deliberately outside the gate have no case here:
// DeleteLedger must resolve the tombstone it re-stamps, and MirrorIngest is
// already rejected by loadBoundaries — DeleteLedger drops the boundary row —
// so gating it would only relabel a rejection the FSM already makes.
func TestLedgerScopedWrite_RejectsTombstonedLedger(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		// mode of the tombstoned ledger the Scope resolves. PromoteLedger
		// rejects a non-mirror ledger on its own, so its case needs a
		// tombstone that would otherwise be promotable.
		mode  commonpb.LedgerMode
		order *raftcmdpb.LedgerScopedOrder
	}{
		{
			name: "save ledger metadata",
			order: &raftcmdpb.LedgerScopedOrder{
				Ledger: tombstonedLedger,
				Payload: &raftcmdpb.LedgerScopedOrder_SaveLedgerMetadata{
					SaveLedgerMetadata: &raftcmdpb.SaveLedgerMetadataOrder{
						Metadata: map[string]*commonpb.MetadataValue{
							"color": commonpb.NewStringValue("blue"),
						},
					},
				},
			},
		},
		{
			name: "delete ledger metadata",
			order: &raftcmdpb.LedgerScopedOrder{
				Ledger: tombstonedLedger,
				Payload: &raftcmdpb.LedgerScopedOrder_DeleteLedgerMetadata{
					DeleteLedgerMetadata: &raftcmdpb.DeleteLedgerMetadataOrder{Key: "color"},
				},
			},
		},
		{
			name: "save numscript",
			order: &raftcmdpb.LedgerScopedOrder{
				Ledger: tombstonedLedger,
				Payload: &raftcmdpb.LedgerScopedOrder_SaveNumscript{
					SaveNumscript: &raftcmdpb.SaveNumscriptOrder{
						Name:    "pay",
						Content: validNumscriptContent,
						Version: "1.0.0",
					},
				},
			},
		},
		{
			name: "create prepared query",
			order: &raftcmdpb.LedgerScopedOrder{
				Ledger: tombstonedLedger,
				Payload: &raftcmdpb.LedgerScopedOrder_CreatePreparedQuery{
					CreatePreparedQuery: &raftcmdpb.CreatePreparedQueryOrder{
						Query: &commonpb.PreparedQuery{
							Name:   "q1",
							Target: commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
						},
					},
				},
			},
		},
		{
			name: "update prepared query",
			order: &raftcmdpb.LedgerScopedOrder{
				Ledger: tombstonedLedger,
				Payload: &raftcmdpb.LedgerScopedOrder_UpdatePreparedQuery{
					UpdatePreparedQuery: &raftcmdpb.UpdatePreparedQueryOrder{Name: "q1"},
				},
			},
		},
		{
			name: "delete prepared query",
			order: &raftcmdpb.LedgerScopedOrder{
				Ledger: tombstonedLedger,
				Payload: &raftcmdpb.LedgerScopedOrder_DeletePreparedQuery{
					DeletePreparedQuery: &raftcmdpb.DeletePreparedQueryOrder{Name: "q1"},
				},
			},
		},
		{
			name: "promote ledger",
			mode: commonpb.LedgerMode_LEDGER_MODE_MIRROR,
			order: &raftcmdpb.LedgerScopedOrder{
				Ledger: tombstonedLedger,
				Payload: &raftcmdpb.LedgerScopedOrder_PromoteLedger{
					PromoteLedger: &raftcmdpb.PromoteLedgerOrder{},
				},
			},
		},
		{
			name: "apply add account type",
			order: &raftcmdpb.LedgerScopedOrder{
				Ledger: tombstonedLedger,
				Payload: &raftcmdpb.LedgerScopedOrder_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{
						Data: &raftcmdpb.LedgerApplyOrder_AddAccountType{
							AddAccountType: &raftcmdpb.AddAccountTypeOrder{
								AccountType: &commonpb.AccountType{Name: "customer", Pattern: "customer:*"},
							},
						},
					},
				},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			// The ledger read is the only registered Scope access, so any
			// store call a handler made before rejecting fails the test as an
			// unexpected call. That is what pins "no state was mutated"
			// alongside the nil log payload below.
			mockStore := NewMockScope(ctrl)
			expectGetLedger(mockStore, domain.LedgerKey{Name: tombstonedLedger},
				(&commonpb.LedgerInfo{
					Name:      tombstonedLedger,
					Id:        9,
					Mode:      tc.mode,
					DeletedAt: &commonpb.Timestamp{Data: 1},
				}).AsReader(), nil)

			processor, err := NewRequestProcessor(nil, 0)
			require.NoError(t, err)

			payload, derr := processor.ProcessOrder(
				&raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: tc.order}},
				mockStore,
			)

			require.Nil(t, payload, "a rejected command must commit no log")
			require.NotNil(t, derr)

			// Assert the tombstone branch specifically: a live ledger that
			// merely failed to resolve surfaces ErrLedgerNotFound, and a
			// storage fault surfaces ErrStorageOperation. Either would pass a
			// bare require.Error.
			var deleted *domain.ErrLedgerDeleted
			require.ErrorAs(t, derr, &deleted)
			require.Equal(t, tombstonedLedger, deleted.Name)
			require.Equal(t, domain.ErrReasonLedgerDeleted, derr.Reason())
		})
	}
}
