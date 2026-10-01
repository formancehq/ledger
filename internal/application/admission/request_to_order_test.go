package admission

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"testing"

	"github.com/stretchr/testify/require"

	commonpb "github.com/formancehq/ledger/pkg/client/v3/grpc"

	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// TestRequestToOrder_WrapsEveryRequestVariant pins the contract that every
// commonpb.Request variant gets converted to an Order with the matching
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
		req    *commonpb.Request
		expect expect
	}{
		{
			name: "create_ledger",
			req: &commonpb.Request{Type: &commonpb.Request_CreateLedger{
				CreateLedger: &commonpb.CreateLedgerRequest{Name: ledger},
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
			req: &commonpb.Request{Type: &commonpb.Request_DeleteLedger{
				DeleteLedger: &commonpb.DeleteLedgerRequest{Name: ledger},
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
			req: &commonpb.Request{Type: &commonpb.Request_PromoteLedger{
				PromoteLedger: &commonpb.PromoteLedgerRequest{Ledger: ledger},
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
			req: &commonpb.Request{Type: &commonpb.Request_SaveLedgerMetadata{
				SaveLedgerMetadata: &commonpb.SaveLedgerMetadataRequest{
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
			req: &commonpb.Request{Type: &commonpb.Request_DeleteLedgerMetadata{
				DeleteLedgerMetadata: &commonpb.DeleteLedgerMetadataRequest{
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
			req: &commonpb.Request{Type: &commonpb.Request_CreatePreparedQuery{
				CreatePreparedQuery: &commonpb.CreatePreparedQueryRequest{
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
			req: &commonpb.Request{Type: &commonpb.Request_UpdatePreparedQuery{
				UpdatePreparedQuery: &commonpb.UpdatePreparedQueryRequest{Ledger: ledger, Name: "q"},
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
			req: &commonpb.Request{Type: &commonpb.Request_DeletePreparedQuery{
				DeletePreparedQuery: &commonpb.DeletePreparedQueryRequest{Ledger: ledger, Name: "q"},
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
			req: &commonpb.Request{Type: &commonpb.Request_SaveNumscript{
				SaveNumscript: &commonpb.SaveNumscriptRequest{
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
			name: "apply/set_metadata_field_type",
			req: &commonpb.Request{Type: &commonpb.Request_SetMetadataFieldType{
				SetMetadataFieldType: &commonpb.SetMetadataFieldTypeRequest{
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
			req: &commonpb.Request{Type: &commonpb.Request_RemoveMetadataFieldType{
				RemoveMetadataFieldType: &commonpb.RemoveMetadataFieldTypeRequest{
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
			req: &commonpb.Request{Type: &commonpb.Request_CreateIndex{
				CreateIndex: &commonpb.CreateIndexRequest{
					Ledger: ledger,
					// Must be a builder-supported IndexID: validateOrderCreateIndex
					// (run by requestToOrder→validateOrder) rejects unsupported
					// kinds like the ACCT_BUILTIN UNSPECIFIED sentinel.
					Id: &commonpb.IndexID{Kind: &commonpb.IndexID_AccountBuiltin{AccountBuiltin: commonpb.AccountBuiltinIndex_ACCT_BUILTIN_INDEX_ASSET}},
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
			req: &commonpb.Request{Type: &commonpb.Request_DropIndex{
				DropIndex: &commonpb.DropIndexRequest{
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
			req: &commonpb.Request{Type: &commonpb.Request_AddAccountType{
				AddAccountType: &commonpb.AddAccountTypeLedgerRequest{
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
			req: &commonpb.Request{Type: &commonpb.Request_RemoveAccountType{
				RemoveAccountType: &commonpb.RemoveAccountTypeLedgerRequest{
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
			req: &commonpb.Request{Type: &commonpb.Request_SetDefaultEnforcementMode{
				SetDefaultEnforcementMode: &commonpb.SetDefaultEnforcementModeLedgerRequest{
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
			req: &commonpb.Request{Type: &commonpb.Request_RegisterSigningKey{
				RegisterSigningKey: &commonpb.RegisterSigningKeyRequest{KeyId: "k1", PublicKey: bytes.Repeat([]byte{0x11}, ed25519.PublicKeySize)},
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
			req: &commonpb.Request{Type: &commonpb.Request_RevokeSigningKey{
				RevokeSigningKey: &commonpb.RevokeSigningKeyRequest{KeyId: "k1"},
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
			req: &commonpb.Request{Type: &commonpb.Request_SetSigningConfig{
				SetSigningConfig: &commonpb.SetSigningConfigRequest{RequireSignatures: true},
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
			req: &commonpb.Request{Type: &commonpb.Request_AddEventsSink{
				AddEventsSink: &commonpb.AddEventsSinkRequest{Config: &commonpb.SinkConfig{Name: "s"}},
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
			req: &commonpb.Request{Type: &commonpb.Request_RemoveEventsSink{
				RemoveEventsSink: &commonpb.RemoveEventsSinkRequest{Name: "s", ControllerId: "uid-1"},
			}},
			expect: expect{
				kind: wrapSystem,
				payloadAssert: func(t *testing.T, o *raftcmdpb.Order) {
					require.Equal(t, "uid-1", mustSystemScoped(t, o).GetRemoveEventsSink().GetControllerId())
				},
			},
		},
		{
			name: "set_maintenance_mode",
			req: &commonpb.Request{Type: &commonpb.Request_SetMaintenanceMode{
				SetMaintenanceMode: &commonpb.SetMaintenanceModeRequest{Enabled: true},
			}},
			expect: expect{
				kind: wrapSystem,
				payloadAssert: func(t *testing.T, o *raftcmdpb.Order) {
					require.NotNil(t, mustSystemScoped(t, o).GetSetMaintenanceMode())
				},
			},
		},
		{
			name: "create_query_checkpoint",
			req: &commonpb.Request{Type: &commonpb.Request_CreateQueryCheckpoint{
				CreateQueryCheckpoint: &commonpb.CreateQueryCheckpointRequest{},
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
			req: &commonpb.Request{Type: &commonpb.Request_DeleteQueryCheckpoint{
				DeleteQueryCheckpoint: &commonpb.DeleteQueryCheckpointRequest{CheckpointId: 1},
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
			req: &commonpb.Request{Type: &commonpb.Request_SetQueryCheckpointSchedule{
				SetQueryCheckpointSchedule: &commonpb.SetQueryCheckpointScheduleRequest{Cron: "0 0 1 * *"},
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
			req: &commonpb.Request{Type: &commonpb.Request_DeleteQueryCheckpointSchedule{
				DeleteQueryCheckpointSchedule: &commonpb.DeleteQueryCheckpointScheduleRequest{},
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
