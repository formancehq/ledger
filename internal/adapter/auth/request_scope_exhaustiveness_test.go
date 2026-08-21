package auth

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// scopeCase pairs a well-formed request with the scope its dedicated
// HTTP route or gRPC RPC requires. The dedicated surface is the source of
// truth: a batch path must never demand a different scope for the same
// operation than the single-shot path does.
type scopeCase struct {
	req      *servicepb.Request
	expected Scope
}

// TestRequiredScopeForRequest_ProtoExhaustive walks the Request.type oneof
// descriptor and asserts that every declared variant (a) appears in the case
// table, (b) has an explicit arm in requiredScopeForRequest rather than
// falling through to the fail-closed default, and (c) resolves to the scope
// its dedicated surface requires.
//
// Assertion (b) is the one a "covered field names" guard cannot make: a
// variant with no arm returns ScopeOpsWrite, which is indistinguishable from
// a deliberate ScopeOpsWrite unless production code reports decidedness.
// See internal/application/admission/wrapper_exhaustiveness_test.go for the
// weaker form of this pattern.
func TestRequiredScopeForRequest_ProtoExhaustive(t *testing.T) {
	t.Parallel()

	cases := requestScopeCases()

	// ByName, not Get(0): proto3 `optional` fields generate synthetic oneofs,
	// so the real oneof is not reliably the first one.
	typeOneof := (&servicepb.Request{}).ProtoReflect().Descriptor().Oneofs().ByName("type")
	require.NotNil(t, typeOneof, "servicepb.Request must declare a oneof named 'type'")

	fields := typeOneof.Fields()
	for i := range fields.Len() {
		name := string(fields.Get(i).Name())

		tc, ok := cases[name]
		require.True(t, ok,
			"Request.type.%s is declared in the proto but has no entry in requestScopeCases — "+
				"add one carrying the scope its dedicated HTTP route or gRPC RPC requires", name)

		scope, decided := requiredScopeForRequest(tc.req)
		require.True(t, decided,
			"Request.type.%s has no explicit arm in requiredScopeForRequest and is falling "+
				"through to the fail-closed default — classify it explicitly", name)
		require.Equal(t, tc.expected, scope, "Request.type.%s resolved to the wrong scope", name)
	}

	// Reverse direction: a stale table entry means the proto dropped a variant
	// and the table was not cleaned up.
	for name := range cases {
		require.NotNil(t, fields.ByName(protoreflect.Name(name)),
			"requestScopeCases has entry %q which Request.type no longer declares — remove it", name)
	}
}

// TestRequiredScopeForLedgerApply_ProtoExhaustive is the same guard for the
// inner LedgerAction.data oneof, reachable over gRPC as
// Request_Apply{Apply: {Action: ...}}.
func TestRequiredScopeForLedgerApply_ProtoExhaustive(t *testing.T) {
	t.Parallel()

	cases := ledgerActionScopeCases()

	dataOneof := (&servicepb.LedgerAction{}).ProtoReflect().Descriptor().Oneofs().ByName("data")
	require.NotNil(t, dataOneof, "servicepb.LedgerAction must declare a oneof named 'data'")

	fields := dataOneof.Fields()
	for i := range fields.Len() {
		name := string(fields.Get(i).Name())

		expected, ok := cases[name]
		require.True(t, ok,
			"LedgerAction.data.%s is declared in the proto but has no entry in "+
				"ledgerActionScopeCases — add one", name)

		scope, decided := requiredScopeForLedgerApply(&servicepb.LedgerApplyRequest{
			Action: actionForField(name),
		})
		require.True(t, decided,
			"LedgerAction.data.%s has no explicit arm in requiredScopeForLedgerApply and is "+
				"falling through to the fail-closed default — classify it explicitly", name)
		require.Equal(t, expected, scope, "LedgerAction.data.%s resolved to the wrong scope", name)
	}

	for name := range cases {
		require.NotNil(t, fields.ByName(protoreflect.Name(name)),
			"ledgerActionScopeCases has entry %q which LedgerAction.data no longer declares", name)
	}
}

// requestScopeCases maps every Request.type field name to a well-formed
// request and the scope its dedicated surface requires.
//
// Entries must be well-formed: the "apply" entry carries a real inner action,
// because an Apply with a nil action is malformed input and correctly reports
// decided=false. Nil and empty inputs are asserted in request_scope_test.go,
// not here.
func requestScopeCases() map[string]scopeCase {
	return map[string]scopeCase{
		// Ledger lifecycle — HTTP requireLedgersWrite group (handler.go:172-179).
		"create_ledger":  {&servicepb.Request{Type: &servicepb.Request_CreateLedger{}}, ScopeLedgersWrite},
		"delete_ledger":  {&servicepb.Request{Type: &servicepb.Request_DeleteLedger{}}, ScopeLedgersWrite},
		"promote_ledger": {&servicepb.Request{Type: &servicepb.Request_PromoteLedger{}}, ScopeLedgersWrite},
		"create_index":   {&servicepb.Request{Type: &servicepb.Request_CreateIndex{}}, ScopeLedgersWrite},
		"drop_index":     {&servicepb.Request{Type: &servicepb.Request_DropIndex{}}, ScopeLedgersWrite},
		"save_numscript": {&servicepb.Request{Type: &servicepb.Request_SaveNumscript{}}, ScopeLedgersWrite},

		// Cluster operations — ClusterService requires ledger:ClusterWrite
		// (server_cluster.go:442, 481), granted only by ledger:admin.
		"create_query_checkpoint":          {&servicepb.Request{Type: &servicepb.Request_CreateQueryCheckpoint{}}, ScopeClusterWrite},
		"delete_query_checkpoint":          {&servicepb.Request{Type: &servicepb.Request_DeleteQueryCheckpoint{}}, ScopeClusterWrite},
		"set_query_checkpoint_schedule":    {&servicepb.Request{Type: &servicepb.Request_SetQueryCheckpointSchedule{}}, ScopeClusterWrite},
		"delete_query_checkpoint_schedule": {&servicepb.Request{Type: &servicepb.Request_DeleteQueryCheckpointSchedule{}}, ScopeClusterWrite},
		"set_cluster_policy":               {&servicepb.Request{Type: &servicepb.Request_SetClusterPolicy{}}, ScopeClusterWrite},

		// Metadata & chart of accounts — HTTP requireMetadataWrite group
		// (handler.go:188-200).
		"set_metadata_field_type":      {&servicepb.Request{Type: &servicepb.Request_SetMetadataFieldType{}}, ScopeMetadataWrite},
		"remove_metadata_field_type":   {&servicepb.Request{Type: &servicepb.Request_RemoveMetadataFieldType{}}, ScopeMetadataWrite},
		"save_ledger_metadata":         {&servicepb.Request{Type: &servicepb.Request_SaveLedgerMetadata{}}, ScopeMetadataWrite},
		"delete_ledger_metadata":       {&servicepb.Request{Type: &servicepb.Request_DeleteLedgerMetadata{}}, ScopeMetadataWrite},
		"add_account_type":             {&servicepb.Request{Type: &servicepb.Request_AddAccountType{}}, ScopeMetadataWrite},
		"remove_account_type":          {&servicepb.Request{Type: &servicepb.Request_RemoveAccountType{}}, ScopeMetadataWrite},
		"set_default_enforcement_mode": {&servicepb.Request{Type: &servicepb.Request_SetDefaultEnforcementMode{}}, ScopeMetadataWrite},

		// Prepared queries — HTTP requireQueriesWrite group (handler.go:212-216).
		"create_prepared_query": {&servicepb.Request{Type: &servicepb.Request_CreatePreparedQuery{}}, ScopeQueriesWrite},
		"update_prepared_query": {&servicepb.Request{Type: &servicepb.Request_UpdatePreparedQuery{}}, ScopeQueriesWrite},
		"delete_prepared_query": {&servicepb.Request{Type: &servicepb.Request_DeletePreparedQuery{}}, ScopeQueriesWrite},

		// Operator surface — no business identity, ledger:OpsWrite by design.
		"register_signing_key":    {&servicepb.Request{Type: &servicepb.Request_RegisterSigningKey{}}, ScopeOpsWrite},
		"revoke_signing_key":      {&servicepb.Request{Type: &servicepb.Request_RevokeSigningKey{}}, ScopeOpsWrite},
		"set_signing_config":      {&servicepb.Request{Type: &servicepb.Request_SetSigningConfig{}}, ScopeOpsWrite},
		"add_events_sink":         {&servicepb.Request{Type: &servicepb.Request_AddEventsSink{}}, ScopeOpsWrite},
		"remove_events_sink":      {&servicepb.Request{Type: &servicepb.Request_RemoveEventsSink{}}, ScopeOpsWrite},
		"close_chapter":           {&servicepb.Request{Type: &servicepb.Request_CloseChapter{}}, ScopeOpsWrite},
		"seal_chapter":            {&servicepb.Request{Type: &servicepb.Request_SealChapter{}}, ScopeOpsWrite},
		"archive_chapter":         {&servicepb.Request{Type: &servicepb.Request_ArchiveChapter{}}, ScopeOpsWrite},
		"confirm_archive_chapter": {&servicepb.Request{Type: &servicepb.Request_ConfirmArchiveChapter{}}, ScopeOpsWrite},
		"set_maintenance_mode":    {&servicepb.Request{Type: &servicepb.Request_SetMaintenanceMode{}}, ScopeOpsWrite},
		"set_chapter_schedule":    {&servicepb.Request{Type: &servicepb.Request_SetChapterSchedule{}}, ScopeOpsWrite},
		"delete_chapter_schedule": {&servicepb.Request{Type: &servicepb.Request_DeleteChapterSchedule{}}, ScopeOpsWrite},

		// Batch apply — delegates to the LedgerAction classifier. A real
		// action is required; see the doc comment above.
		"apply": {
			&servicepb.Request{Type: &servicepb.Request_Apply{
				Apply: &servicepb.LedgerApplyRequest{
					Action: &servicepb.LedgerAction{
						Data: &servicepb.LedgerAction_CreateTransaction{},
					},
				},
			}},
			ScopeTransactionsWrite,
		},
	}
}

// ledgerActionScopeCases maps every LedgerAction.data field name to its scope.
func ledgerActionScopeCases() map[string]Scope {
	return map[string]Scope{
		"create_transaction": ScopeTransactionsWrite,
		"revert_transaction": ScopeTransactionsWrite,
		"add_metadata":       ScopeMetadataWrite,
		"delete_metadata":    ScopeMetadataWrite,
		// Same three operations as their top-level twins, and the same HTTP
		// routes (handler.go:197-199) — so the same scope.
		"add_account_type":             ScopeMetadataWrite,
		"remove_account_type":          ScopeMetadataWrite,
		"set_default_enforcement_mode": ScopeMetadataWrite,
	}
}

// actionForField builds a LedgerAction whose oneof is set to the named field.
func actionForField(name string) *servicepb.LedgerAction {
	switch name {
	case "create_transaction":
		return &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{}}
	case "revert_transaction":
		return &servicepb.LedgerAction{Data: &servicepb.LedgerAction_RevertTransaction{}}
	case "add_metadata":
		return &servicepb.LedgerAction{Data: &servicepb.LedgerAction_AddMetadata{
			AddMetadata: &commonpb.SaveMetadataCommand{},
		}}
	case "delete_metadata":
		return &servicepb.LedgerAction{Data: &servicepb.LedgerAction_DeleteMetadata{
			DeleteMetadata: &commonpb.DeleteMetadataCommand{},
		}}
	case "add_account_type":
		return &servicepb.LedgerAction{Data: &servicepb.LedgerAction_AddAccountType{}}
	case "remove_account_type":
		return &servicepb.LedgerAction{Data: &servicepb.LedgerAction_RemoveAccountType{}}
	case "set_default_enforcement_mode":
		return &servicepb.LedgerAction{Data: &servicepb.LedgerAction_SetDefaultEnforcementMode{}}
	default:
		// Unreachable: the exhaustiveness test fails on an unknown field name
		// before it gets here, via the ledgerActionScopeCases lookup.
		return nil
	}
}
