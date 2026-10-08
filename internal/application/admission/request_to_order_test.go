package admission

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"testing"

	"github.com/stretchr/testify/require"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"

	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// TestRequestToOrder_WrapsEveryRequestVariant pins the contract that every
// ledgerpb.Request variant gets converted to an Order with the matching
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
		req    *ledgerpb.Request
		expect expect
	}{
		{
			name: "create_ledger",
			req: &ledgerpb.Request{Type: &ledgerpb.Request_CreateLedger{
				CreateLedger: &ledgerpb.CreateLedgerRequest{Name: ledger},
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
			req: &ledgerpb.Request{Type: &ledgerpb.Request_DeleteLedger{
				DeleteLedger: &ledgerpb.DeleteLedgerRequest{Name: ledger},
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
			req: &ledgerpb.Request{Type: &ledgerpb.Request_PromoteLedger{
				PromoteLedger: &ledgerpb.PromoteLedgerRequest{Ledger: ledger},
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
			req: &ledgerpb.Request{Type: &ledgerpb.Request_SaveLedgerMetadata{
				SaveLedgerMetadata: &ledgerpb.SaveLedgerMetadataRequest{
					Ledger: ledger,
					Metadata: map[string]*ledgerpb.MetadataValue{
						"owner": {Type: &ledgerpb.MetadataValue_StringValue{StringValue: "team"}},
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
			req: &ledgerpb.Request{Type: &ledgerpb.Request_DeleteLedgerMetadata{
				DeleteLedgerMetadata: &ledgerpb.DeleteLedgerMetadataRequest{
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
			req: &ledgerpb.Request{Type: &ledgerpb.Request_CreatePreparedQuery{
				CreatePreparedQuery: &ledgerpb.CreatePreparedQueryRequest{
					Ledger: ledger,
					Query:  &ledgerpb.PreparedQuery{Name: "q"},
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
			req: &ledgerpb.Request{Type: &ledgerpb.Request_UpdatePreparedQuery{
				UpdatePreparedQuery: &ledgerpb.UpdatePreparedQueryRequest{Ledger: ledger, Name: "q"},
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
			req: &ledgerpb.Request{Type: &ledgerpb.Request_DeletePreparedQuery{
				DeletePreparedQuery: &ledgerpb.DeletePreparedQueryRequest{Ledger: ledger, Name: "q"},
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
			req: &ledgerpb.Request{Type: &ledgerpb.Request_SaveNumscript{
				SaveNumscript: &ledgerpb.SaveNumscriptRequest{
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
			req: &ledgerpb.Request{Type: &ledgerpb.Request_SetMetadataFieldType{
				SetMetadataFieldType: &ledgerpb.SetMetadataFieldTypeRequest{
					Ledger:     ledger,
					TargetType: ledgerpb.TargetType_TARGET_TYPE_ACCOUNT,
					Key:        "age",
					Type:       ledgerpb.MetadataType_METADATA_TYPE_INT64,
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
			req: &ledgerpb.Request{Type: &ledgerpb.Request_RemoveMetadataFieldType{
				RemoveMetadataFieldType: &ledgerpb.RemoveMetadataFieldTypeRequest{
					Ledger:     ledger,
					TargetType: ledgerpb.TargetType_TARGET_TYPE_ACCOUNT,
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
			req: &ledgerpb.Request{Type: &ledgerpb.Request_CreateIndex{
				CreateIndex: &ledgerpb.CreateIndexRequest{
					Ledger: ledger,
					// Must be a builder-supported IndexID: validateOrderCreateIndex
					// (run by requestToOrder→validateOrder) rejects unsupported
					// kinds like the ACCT_BUILTIN UNSPECIFIED sentinel.
					Id: &ledgerpb.IndexID{Kind: &ledgerpb.IndexID_AccountBuiltin{AccountBuiltin: ledgerpb.AccountBuiltinIndex_ACCT_BUILTIN_INDEX_ASSET}},
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
			req: &ledgerpb.Request{Type: &ledgerpb.Request_DropIndex{
				DropIndex: &ledgerpb.DropIndexRequest{
					Ledger: ledger,
					Id:     &ledgerpb.IndexID{Kind: &ledgerpb.IndexID_AccountBuiltin{AccountBuiltin: ledgerpb.AccountBuiltinIndex_ACCT_BUILTIN_INDEX_UNSPECIFIED}},
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
			req: &ledgerpb.Request{Type: &ledgerpb.Request_AddAccountType{
				AddAccountType: &ledgerpb.AddAccountTypeLedgerRequest{
					Ledger:      ledger,
					AccountType: &ledgerpb.AccountType{Name: "user"},
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
			req: &ledgerpb.Request{Type: &ledgerpb.Request_RemoveAccountType{
				RemoveAccountType: &ledgerpb.RemoveAccountTypeLedgerRequest{
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
			req: &ledgerpb.Request{Type: &ledgerpb.Request_SetDefaultEnforcementMode{
				SetDefaultEnforcementMode: &ledgerpb.SetDefaultEnforcementModeLedgerRequest{
					Ledger:          ledger,
					EnforcementMode: ledgerpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT,
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
			req: &ledgerpb.Request{Type: &ledgerpb.Request_RegisterSigningKey{
				RegisterSigningKey: &ledgerpb.RegisterSigningKeyRequest{KeyId: "k1", PublicKey: bytes.Repeat([]byte{0x11}, ed25519.PublicKeySize)},
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
			req: &ledgerpb.Request{Type: &ledgerpb.Request_RevokeSigningKey{
				RevokeSigningKey: &ledgerpb.RevokeSigningKeyRequest{KeyId: "k1"},
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
			req: &ledgerpb.Request{Type: &ledgerpb.Request_SetSigningConfig{
				SetSigningConfig: &ledgerpb.SetSigningConfigRequest{RequireSignatures: true},
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
			req: &ledgerpb.Request{Type: &ledgerpb.Request_AddEventsSink{
				AddEventsSink: &ledgerpb.AddEventsSinkRequest{Config: &ledgerpb.SinkConfig{Name: "s"}},
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
			req: &ledgerpb.Request{Type: &ledgerpb.Request_RemoveEventsSink{
				RemoveEventsSink: &ledgerpb.RemoveEventsSinkRequest{Name: "s", ControllerId: "uid-1"},
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
			req: &ledgerpb.Request{Type: &ledgerpb.Request_SetMaintenanceMode{
				SetMaintenanceMode: &ledgerpb.SetMaintenanceModeRequest{Enabled: true},
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
			req: &ledgerpb.Request{Type: &ledgerpb.Request_CreateQueryCheckpoint{
				CreateQueryCheckpoint: &ledgerpb.CreateQueryCheckpointRequest{},
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
			req: &ledgerpb.Request{Type: &ledgerpb.Request_DeleteQueryCheckpoint{
				DeleteQueryCheckpoint: &ledgerpb.DeleteQueryCheckpointRequest{CheckpointId: 1},
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
			req: &ledgerpb.Request{Type: &ledgerpb.Request_SetQueryCheckpointSchedule{
				SetQueryCheckpointSchedule: &ledgerpb.SetQueryCheckpointScheduleRequest{Cron: "0 0 1 * *"},
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
			req: &ledgerpb.Request{Type: &ledgerpb.Request_DeleteQueryCheckpointSchedule{
				DeleteQueryCheckpointSchedule: &ledgerpb.DeleteQueryCheckpointScheduleRequest{},
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
