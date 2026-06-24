package admission

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// TestRequestToOrder_WrapsEveryRequestVariant pins the contract that every
// servicepb.Request variant gets converted to an Order with the matching
// wrapper (LedgerScopedOrder for ledger-scoped commands, SystemScopedOrder
// for cluster-global ones) and that the ledger name is propagated to the
// wrapper envelope rather than leaking into the payload sub-message.
//
// This is the structural invariant that #511 relies on: the audit log reads
// the ledger off the wrapper, so a new request variant that lands in a
// payload sub-message without a matching wrapping leaks past audit
// attribution. Adding such a variant fails this test as a missing case.
func TestRequestToOrder_WrapsEveryRequestVariant(t *testing.T) {
	t.Parallel()

	const ledger = "wrap-test"

	type wrapKind int
	const (
		wrapLedger wrapKind = iota
		wrapSystem
	)

	type expect struct {
		kind   wrapKind
		ledger string // empty for system-scoped
		// payload sniffer: each case asserts the inner payload type lines up.
		payloadAssert func(t *testing.T, order *raftcmdpb.Order)
	}

	mustLedgerScoped := func(t *testing.T, order *raftcmdpb.Order) *raftcmdpb.LedgerScopedOrder {
		t.Helper()
		ls := order.GetLedgerScoped()
		require.NotNil(t, ls, "expected ledger-scoped wrapper")
		require.Nil(t, order.GetSystemScoped(), "must not also be system-scoped")

		return ls
	}
	mustSystemScoped := func(t *testing.T, order *raftcmdpb.Order) *raftcmdpb.SystemScopedOrder {
		t.Helper()
		ss := order.GetSystemScoped()
		require.NotNil(t, ss, "expected system-scoped wrapper")
		require.Nil(t, order.GetLedgerScoped(), "must not also be ledger-scoped")

		return ss
	}

	cases := []struct {
		name   string
		req    *servicepb.Request
		expect expect
	}{
		{
			name: "create_ledger",
			req: &servicepb.Request{Type: &servicepb.Request_CreateLedger{
				CreateLedger: &servicepb.CreateLedgerRequest{Name: ledger},
			}},
			expect: expect{
				kind:   wrapLedger,
				ledger: ledger,
				payloadAssert: func(t *testing.T, o *raftcmdpb.Order) {
					require.NotNil(t, mustLedgerScoped(t, o).GetCreateLedger())
				},
			},
		},
		{
			name: "delete_ledger",
			req: &servicepb.Request{Type: &servicepb.Request_DeleteLedger{
				DeleteLedger: &servicepb.DeleteLedgerRequest{Name: ledger},
			}},
			expect: expect{
				kind:   wrapLedger,
				ledger: ledger,
				payloadAssert: func(t *testing.T, o *raftcmdpb.Order) {
					require.NotNil(t, mustLedgerScoped(t, o).GetDeleteLedger())
				},
			},
		},
		{
			name: "promote_ledger",
			req: &servicepb.Request{Type: &servicepb.Request_PromoteLedger{
				PromoteLedger: &servicepb.PromoteLedgerRequest{Ledger: ledger},
			}},
			expect: expect{
				kind:   wrapLedger,
				ledger: ledger,
				payloadAssert: func(t *testing.T, o *raftcmdpb.Order) {
					require.NotNil(t, mustLedgerScoped(t, o).GetPromoteLedger())
				},
			},
		},
		{
			name: "save_ledger_metadata",
			req: &servicepb.Request{Type: &servicepb.Request_SaveLedgerMetadata{
				SaveLedgerMetadata: &servicepb.SaveLedgerMetadataRequest{
					Ledger: ledger,
					Metadata: map[string]*commonpb.MetadataValue{
						"owner": {Type: &commonpb.MetadataValue_StringValue{StringValue: "team"}},
					},
				},
			}},
			expect: expect{
				kind:   wrapLedger,
				ledger: ledger,
				payloadAssert: func(t *testing.T, o *raftcmdpb.Order) {
					sm := mustLedgerScoped(t, o).GetSaveLedgerMetadata()
					require.NotNil(t, sm)
					require.Contains(t, sm.GetMetadata(), "owner")
				},
			},
		},
		{
			name: "delete_ledger_metadata",
			req: &servicepb.Request{Type: &servicepb.Request_DeleteLedgerMetadata{
				DeleteLedgerMetadata: &servicepb.DeleteLedgerMetadataRequest{
					Ledger: ledger,
					Key:    "owner",
				},
			}},
			expect: expect{
				kind:   wrapLedger,
				ledger: ledger,
				payloadAssert: func(t *testing.T, o *raftcmdpb.Order) {
					dm := mustLedgerScoped(t, o).GetDeleteLedgerMetadata()
					require.NotNil(t, dm)
					require.Equal(t, "owner", dm.GetKey())
				},
			},
		},
		{
			name: "create_prepared_query",
			req: &servicepb.Request{Type: &servicepb.Request_CreatePreparedQuery{
				CreatePreparedQuery: &servicepb.CreatePreparedQueryRequest{
					Ledger: ledger,
					Query:  &commonpb.PreparedQuery{Name: "q"},
				},
			}},
			expect: expect{
				kind:   wrapLedger,
				ledger: ledger,
				payloadAssert: func(t *testing.T, o *raftcmdpb.Order) {
					require.NotNil(t, mustLedgerScoped(t, o).GetCreatePreparedQuery())
				},
			},
		},
		{
			name: "update_prepared_query",
			req: &servicepb.Request{Type: &servicepb.Request_UpdatePreparedQuery{
				UpdatePreparedQuery: &servicepb.UpdatePreparedQueryRequest{Ledger: ledger, Name: "q"},
			}},
			expect: expect{
				kind:   wrapLedger,
				ledger: ledger,
				payloadAssert: func(t *testing.T, o *raftcmdpb.Order) {
					up := mustLedgerScoped(t, o).GetUpdatePreparedQuery()
					require.NotNil(t, up)
					require.Equal(t, "q", up.GetName())
				},
			},
		},
		{
			name: "delete_prepared_query",
			req: &servicepb.Request{Type: &servicepb.Request_DeletePreparedQuery{
				DeletePreparedQuery: &servicepb.DeletePreparedQueryRequest{Ledger: ledger, Name: "q"},
			}},
			expect: expect{
				kind:   wrapLedger,
				ledger: ledger,
				payloadAssert: func(t *testing.T, o *raftcmdpb.Order) {
					dp := mustLedgerScoped(t, o).GetDeletePreparedQuery()
					require.NotNil(t, dp)
					require.Equal(t, "q", dp.GetName())
				},
			},
		},
		{
			name: "save_numscript",
			req: &servicepb.Request{Type: &servicepb.Request_SaveNumscript{
				SaveNumscript: &servicepb.SaveNumscriptRequest{
					Ledger:  ledger,
					Name:    "tx",
					Content: "x",
					Version: "1.0.0",
				},
			}},
			expect: expect{
				kind:   wrapLedger,
				ledger: ledger,
				payloadAssert: func(t *testing.T, o *raftcmdpb.Order) {
					ns := mustLedgerScoped(t, o).GetSaveNumscript()
					require.NotNil(t, ns)
					require.Equal(t, "tx", ns.GetName())
					require.Equal(t, "1.0.0", ns.GetVersion())
				},
			},
		},
		{
			name: "delete_numscript",
			req: &servicepb.Request{Type: &servicepb.Request_DeleteNumscript{
				DeleteNumscript: &servicepb.DeleteNumscriptRequest{Ledger: ledger, Name: "tx"},
			}},
			expect: expect{
				kind:   wrapLedger,
				ledger: ledger,
				payloadAssert: func(t *testing.T, o *raftcmdpb.Order) {
					dn := mustLedgerScoped(t, o).GetDeleteNumscript()
					require.NotNil(t, dn)
					require.Equal(t, "tx", dn.GetName())
				},
			},
		},
		{
			name: "apply/set_metadata_field_type",
			req: &servicepb.Request{Type: &servicepb.Request_SetMetadataFieldType{
				SetMetadataFieldType: &servicepb.SetMetadataFieldTypeRequest{
					Ledger:     ledger,
					TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
					Key:        "age",
					Type:       commonpb.MetadataType_METADATA_TYPE_INT64,
				},
			}},
			expect: expect{
				kind:   wrapLedger,
				ledger: ledger,
				payloadAssert: func(t *testing.T, o *raftcmdpb.Order) {
					ap := mustLedgerScoped(t, o).GetApply()
					require.NotNil(t, ap)
					require.NotNil(t, ap.GetSetMetadataFieldType())
				},
			},
		},
		{
			name: "apply/remove_metadata_field_type",
			req: &servicepb.Request{Type: &servicepb.Request_RemoveMetadataFieldType{
				RemoveMetadataFieldType: &servicepb.RemoveMetadataFieldTypeRequest{
					Ledger:     ledger,
					TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
					Key:        "age",
				},
			}},
			expect: expect{
				kind:   wrapLedger,
				ledger: ledger,
				payloadAssert: func(t *testing.T, o *raftcmdpb.Order) {
					ap := mustLedgerScoped(t, o).GetApply()
					require.NotNil(t, ap)
					require.NotNil(t, ap.GetRemoveMetadataFieldType())
				},
			},
		},
		{
			name: "apply/create_index",
			req: &servicepb.Request{Type: &servicepb.Request_CreateIndex{
				CreateIndex: &servicepb.CreateIndexRequest{
					Ledger: ledger,
					Id:     &commonpb.IndexID{Kind: &commonpb.IndexID_AccountBuiltin{AccountBuiltin: commonpb.AccountBuiltinIndex_ACCT_BUILTIN_INDEX_UNSPECIFIED}},
				},
			}},
			expect: expect{
				kind:   wrapLedger,
				ledger: ledger,
				payloadAssert: func(t *testing.T, o *raftcmdpb.Order) {
					require.NotNil(t, mustLedgerScoped(t, o).GetApply().GetCreateIndex())
				},
			},
		},
		{
			name: "apply/drop_index",
			req: &servicepb.Request{Type: &servicepb.Request_DropIndex{
				DropIndex: &servicepb.DropIndexRequest{
					Ledger: ledger,
					Id:     &commonpb.IndexID{Kind: &commonpb.IndexID_AccountBuiltin{AccountBuiltin: commonpb.AccountBuiltinIndex_ACCT_BUILTIN_INDEX_UNSPECIFIED}},
				},
			}},
			expect: expect{
				kind:   wrapLedger,
				ledger: ledger,
				payloadAssert: func(t *testing.T, o *raftcmdpb.Order) {
					require.NotNil(t, mustLedgerScoped(t, o).GetApply().GetDropIndex())
				},
			},
		},
		{
			name: "apply/add_account_type",
			req: &servicepb.Request{Type: &servicepb.Request_AddAccountType{
				AddAccountType: &servicepb.AddAccountTypeLedgerRequest{
					Ledger:      ledger,
					AccountType: &commonpb.AccountType{Name: "user"},
				},
			}},
			expect: expect{
				kind:   wrapLedger,
				ledger: ledger,
				payloadAssert: func(t *testing.T, o *raftcmdpb.Order) {
					require.NotNil(t, mustLedgerScoped(t, o).GetApply().GetAddAccountType())
				},
			},
		},
		{
			name: "apply/remove_account_type",
			req: &servicepb.Request{Type: &servicepb.Request_RemoveAccountType{
				RemoveAccountType: &servicepb.RemoveAccountTypeLedgerRequest{
					Ledger: ledger,
					Name:   "user",
				},
			}},
			expect: expect{
				kind:   wrapLedger,
				ledger: ledger,
				payloadAssert: func(t *testing.T, o *raftcmdpb.Order) {
					require.NotNil(t, mustLedgerScoped(t, o).GetApply().GetRemoveAccountType())
				},
			},
		},
		{
			name: "apply/set_default_enforcement_mode",
			req: &servicepb.Request{Type: &servicepb.Request_SetDefaultEnforcementMode{
				SetDefaultEnforcementMode: &servicepb.SetDefaultEnforcementModeLedgerRequest{
					Ledger:          ledger,
					EnforcementMode: commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT,
				},
			}},
			expect: expect{
				kind:   wrapLedger,
				ledger: ledger,
				payloadAssert: func(t *testing.T, o *raftcmdpb.Order) {
					require.NotNil(t, mustLedgerScoped(t, o).GetApply().GetUpdateDefaultEnforcementMode())
				},
			},
		},

		// System-scoped variants.
		{
			name: "register_signing_key",
			req: &servicepb.Request{Type: &servicepb.Request_RegisterSigningKey{
				RegisterSigningKey: &servicepb.RegisterSigningKeyRequest{KeyId: "k1", PublicKey: []byte("pk")},
			}},
			expect: expect{
				kind: wrapSystem,
				payloadAssert: func(t *testing.T, o *raftcmdpb.Order) {
					require.NotNil(t, mustSystemScoped(t, o).GetRegisterSigningKey())
				},
			},
		},
		{
			name: "revoke_signing_key",
			req: &servicepb.Request{Type: &servicepb.Request_RevokeSigningKey{
				RevokeSigningKey: &servicepb.RevokeSigningKeyRequest{KeyId: "k1"},
			}},
			expect: expect{
				kind: wrapSystem,
				payloadAssert: func(t *testing.T, o *raftcmdpb.Order) {
					require.NotNil(t, mustSystemScoped(t, o).GetRevokeSigningKey())
				},
			},
		},
		{
			name: "set_signing_config",
			req: &servicepb.Request{Type: &servicepb.Request_SetSigningConfig{
				SetSigningConfig: &servicepb.SetSigningConfigRequest{RequireSignatures: true},
			}},
			expect: expect{
				kind: wrapSystem,
				payloadAssert: func(t *testing.T, o *raftcmdpb.Order) {
					require.NotNil(t, mustSystemScoped(t, o).GetSetSigningConfig())
				},
			},
		},
		{
			name: "add_events_sink",
			req: &servicepb.Request{Type: &servicepb.Request_AddEventsSink{
				AddEventsSink: &servicepb.AddEventsSinkRequest{Config: &commonpb.SinkConfig{Name: "s"}},
			}},
			expect: expect{
				kind: wrapSystem,
				payloadAssert: func(t *testing.T, o *raftcmdpb.Order) {
					require.NotNil(t, mustSystemScoped(t, o).GetAddEventsSink())
				},
			},
		},
		{
			name: "remove_events_sink",
			req: &servicepb.Request{Type: &servicepb.Request_RemoveEventsSink{
				RemoveEventsSink: &servicepb.RemoveEventsSinkRequest{Name: "s"},
			}},
			expect: expect{
				kind: wrapSystem,
				payloadAssert: func(t *testing.T, o *raftcmdpb.Order) {
					require.NotNil(t, mustSystemScoped(t, o).GetRemoveEventsSink())
				},
			},
		},
		{
			name: "close_chapter",
			req: &servicepb.Request{Type: &servicepb.Request_CloseChapter{
				CloseChapter: &servicepb.CloseChapterRequest{},
			}},
			expect: expect{
				kind: wrapSystem,
				payloadAssert: func(t *testing.T, o *raftcmdpb.Order) {
					require.NotNil(t, mustSystemScoped(t, o).GetCloseChapter())
				},
			},
		},
		{
			name: "seal_chapter",
			req: &servicepb.Request{Type: &servicepb.Request_SealChapter{
				SealChapter: &servicepb.SealChapterRequest{ChapterId: 1},
			}},
			expect: expect{
				kind: wrapSystem,
				payloadAssert: func(t *testing.T, o *raftcmdpb.Order) {
					require.NotNil(t, mustSystemScoped(t, o).GetSealChapter())
				},
			},
		},
		{
			name: "confirm_archive_chapter",
			req: &servicepb.Request{Type: &servicepb.Request_ConfirmArchiveChapter{
				ConfirmArchiveChapter: &servicepb.ConfirmArchiveChapterRequest{ChapterId: 1},
			}},
			expect: expect{
				kind: wrapSystem,
				payloadAssert: func(t *testing.T, o *raftcmdpb.Order) {
					require.NotNil(t, mustSystemScoped(t, o).GetConfirmArchiveChapter())
				},
			},
		},
		{
			name: "set_maintenance_mode",
			req: &servicepb.Request{Type: &servicepb.Request_SetMaintenanceMode{
				SetMaintenanceMode: &servicepb.SetMaintenanceModeRequest{Enabled: true},
			}},
			expect: expect{
				kind: wrapSystem,
				payloadAssert: func(t *testing.T, o *raftcmdpb.Order) {
					require.NotNil(t, mustSystemScoped(t, o).GetSetMaintenanceMode())
				},
			},
		},
		{
			name: "set_chapter_schedule",
			req: &servicepb.Request{Type: &servicepb.Request_SetChapterSchedule{
				SetChapterSchedule: &servicepb.SetChapterScheduleRequest{Cron: "0 0 1 * *"},
			}},
			expect: expect{
				kind: wrapSystem,
				payloadAssert: func(t *testing.T, o *raftcmdpb.Order) {
					require.NotNil(t, mustSystemScoped(t, o).GetSetChapterSchedule())
				},
			},
		},
		{
			name: "delete_chapter_schedule",
			req: &servicepb.Request{Type: &servicepb.Request_DeleteChapterSchedule{
				DeleteChapterSchedule: &servicepb.DeleteChapterScheduleRequest{},
			}},
			expect: expect{
				kind: wrapSystem,
				payloadAssert: func(t *testing.T, o *raftcmdpb.Order) {
					require.NotNil(t, mustSystemScoped(t, o).GetDeleteChapterSchedule())
				},
			},
		},
		{
			name: "create_query_checkpoint",
			req: &servicepb.Request{Type: &servicepb.Request_CreateQueryCheckpoint{
				CreateQueryCheckpoint: &servicepb.CreateQueryCheckpointRequest{},
			}},
			expect: expect{
				kind: wrapSystem,
				payloadAssert: func(t *testing.T, o *raftcmdpb.Order) {
					require.NotNil(t, mustSystemScoped(t, o).GetCreateQueryCheckpoint())
				},
			},
		},
		{
			name: "delete_query_checkpoint",
			req: &servicepb.Request{Type: &servicepb.Request_DeleteQueryCheckpoint{
				DeleteQueryCheckpoint: &servicepb.DeleteQueryCheckpointRequest{CheckpointId: 1},
			}},
			expect: expect{
				kind: wrapSystem,
				payloadAssert: func(t *testing.T, o *raftcmdpb.Order) {
					require.NotNil(t, mustSystemScoped(t, o).GetDeleteQueryCheckpoint())
				},
			},
		},
		{
			name: "set_query_checkpoint_schedule",
			req: &servicepb.Request{Type: &servicepb.Request_SetQueryCheckpointSchedule{
				SetQueryCheckpointSchedule: &servicepb.SetQueryCheckpointScheduleRequest{Cron: "0 0 1 * *"},
			}},
			expect: expect{
				kind: wrapSystem,
				payloadAssert: func(t *testing.T, o *raftcmdpb.Order) {
					require.NotNil(t, mustSystemScoped(t, o).GetSetQueryCheckpointSchedule())
				},
			},
		},
		{
			name: "delete_query_checkpoint_schedule",
			req: &servicepb.Request{Type: &servicepb.Request_DeleteQueryCheckpointSchedule{
				DeleteQueryCheckpointSchedule: &servicepb.DeleteQueryCheckpointScheduleRequest{},
			}},
			expect: expect{
				kind: wrapSystem,
				payloadAssert: func(t *testing.T, o *raftcmdpb.Order) {
					require.NotNil(t, mustSystemScoped(t, o).GetDeleteQueryCheckpointSchedule())
				},
			},
		},
	}

	store := createTestStore(t)
	admission, _ := createTestAdmission(t, store)

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			order, err := admission.requestToOrder(context.Background(), tc.req, nil, newBulkOverlay())
			require.NoError(t, err)
			require.NotNil(t, order)

			switch tc.expect.kind {
			case wrapLedger:
				ls := order.GetLedgerScoped()
				require.NotNil(t, ls, "%s: must be ledger-scoped", tc.name)
				require.Equal(t, tc.expect.ledger, ls.GetLedger(),
					"%s: wrapper ledger must match the request-level ledger", tc.name)
			case wrapSystem:
				require.NotNil(t, order.GetSystemScoped(), "%s: must be system-scoped", tc.name)
				require.Nil(t, order.GetLedgerScoped(), "%s: must not be ledger-scoped", tc.name)
			}

			tc.expect.payloadAssert(t, order)
		})
	}
}
