package auth

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func TestRequiredScopeForRequest_CreateLedger(t *testing.T) {
	t.Parallel()

	req := &servicepb.Request{Type: &servicepb.Request_CreateLedger{}}
	assert.Equal(t, ScopeLedgersWrite, RequiredScopeForRequest(req))
}

func TestRequiredScopeForRequest_DeleteLedger(t *testing.T) {
	t.Parallel()

	req := &servicepb.Request{Type: &servicepb.Request_DeleteLedger{}}
	assert.Equal(t, ScopeLedgersWrite, RequiredScopeForRequest(req))
}

func TestRequiredScopeForRequest_PromoteLedger(t *testing.T) {
	t.Parallel()

	req := &servicepb.Request{Type: &servicepb.Request_PromoteLedger{}}
	assert.Equal(t, ScopeLedgersWrite, RequiredScopeForRequest(req))
}

// TestRequiredScopeForRequest_IndexManagement pins index create/drop to
// ledger:LedgerWrite. This is the cross-transport parity guard: the HTTP routes
// POST/DELETE /v3/{ledgerName}/indexes[/{canonicalId}] are mounted under the
// ledger:LedgerWrite group (see internal/adapter/http/handler.go), so the gRPC
// Apply path must resolve to the same scope. Without the explicit cases these
// requests fell through to the ledger:OpsWrite default, letting an OpsWrite-only
// token manage indexes over gRPC while HTTP required LedgerWrite.
func TestRequiredScopeForRequest_IndexManagement(t *testing.T) {
	t.Parallel()

	indexRequests := []struct {
		name string
		req  *servicepb.Request
	}{
		{"CreateIndex", &servicepb.Request{Type: &servicepb.Request_CreateIndex{}}},
		{"DropIndex", &servicepb.Request{Type: &servicepb.Request_DropIndex{}}},
	}

	for _, tc := range indexRequests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, ScopeLedgersWrite, RequiredScopeForRequest(tc.req))
		})
	}
}

func TestRequiredScopeForRequest_OpsWrite(t *testing.T) {
	t.Parallel()

	opsWriteRequests := []struct {
		name string
		req  *servicepb.Request
	}{
		{"RegisterSigningKey", &servicepb.Request{Type: &servicepb.Request_RegisterSigningKey{}}},
		{"RevokeSigningKey", &servicepb.Request{Type: &servicepb.Request_RevokeSigningKey{}}},
		{"SetSigningConfig", &servicepb.Request{Type: &servicepb.Request_SetSigningConfig{}}},
		{"AddEventsSink", &servicepb.Request{Type: &servicepb.Request_AddEventsSink{}}},
		{"RemoveEventsSink", &servicepb.Request{Type: &servicepb.Request_RemoveEventsSink{}}},
		{"SetMaintenanceMode", &servicepb.Request{Type: &servicepb.Request_SetMaintenanceMode{}}},
	}

	for _, tc := range opsWriteRequests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, ScopeOpsWrite, RequiredScopeForRequest(tc.req))
		})
	}
}

func TestRequiredScopeForRequest_MetadataWrite(t *testing.T) {
	t.Parallel()

	metadataWriteRequests := []struct {
		name string
		req  *servicepb.Request
	}{
		{"SetMetadataFieldType", &servicepb.Request{Type: &servicepb.Request_SetMetadataFieldType{}}},
		{"RemoveMetadataFieldType", &servicepb.Request{Type: &servicepb.Request_RemoveMetadataFieldType{}}},
	}

	for _, tc := range metadataWriteRequests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, ScopeMetadataWrite, RequiredScopeForRequest(tc.req))
		})
	}
}

func TestRequiredScopeForRequest_QueriesWrite(t *testing.T) {
	t.Parallel()

	queriesWriteRequests := []struct {
		name string
		req  *servicepb.Request
	}{
		{"CreatePreparedQuery", &servicepb.Request{Type: &servicepb.Request_CreatePreparedQuery{}}},
		{"UpdatePreparedQuery", &servicepb.Request{Type: &servicepb.Request_UpdatePreparedQuery{}}},
		{"DeletePreparedQuery", &servicepb.Request{Type: &servicepb.Request_DeletePreparedQuery{}}},
	}

	for _, tc := range queriesWriteRequests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, ScopeQueriesWrite, RequiredScopeForRequest(tc.req))
		})
	}
}

func TestRequiredScopeForLedgerApply_CreateTransaction(t *testing.T) {
	t.Parallel()

	req := &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Action: &servicepb.LedgerAction{
					Data: &servicepb.LedgerAction_CreateTransaction{},
				},
			},
		},
	}
	assert.Equal(t, ScopeTransactionsWrite, RequiredScopeForRequest(req))
}

func TestRequiredScopeForLedgerApply_RevertTransaction(t *testing.T) {
	t.Parallel()

	req := &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Action: &servicepb.LedgerAction{
					Data: &servicepb.LedgerAction_RevertTransaction{},
				},
			},
		},
	}
	assert.Equal(t, ScopeTransactionsWrite, RequiredScopeForRequest(req))
}

func TestRequiredScopeForLedgerApply_AddMetadata(t *testing.T) {
	t.Parallel()

	req := &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Action: &servicepb.LedgerAction{
					Data: &servicepb.LedgerAction_AddMetadata{},
				},
			},
		},
	}
	assert.Equal(t, ScopeMetadataWrite, RequiredScopeForRequest(req))
}

func TestRequiredScopeForLedgerApply_DeleteMetadata(t *testing.T) {
	t.Parallel()

	req := &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Action: &servicepb.LedgerAction{
					Data: &servicepb.LedgerAction_DeleteMetadata{},
				},
			},
		},
	}
	assert.Equal(t, ScopeMetadataWrite, RequiredScopeForRequest(req))
}

func TestRequiredScopeForLedgerApply_NilApply(t *testing.T) {
	t.Parallel()

	req := &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: nil,
		},
	}
	// Nil apply defaults to ledger:OpsWrite (most restrictive)
	assert.Equal(t, ScopeOpsWrite, RequiredScopeForRequest(req))
}

// TestRequiredScopeForRequest_BusinessVariantsDoNotRequireOpsWrite is the
// EN-1506 regression guard. Each of these operations has a dedicated HTTP
// route under a business scope; before the fix they fell through to
// ledger:OpsWrite over gRPC Apply, so an operator scope was needed to perform
// a business action — and ledger:OpsWrite also grants maintenance mode,
// signing-key control.
func TestRequiredScopeForRequest_BusinessVariantsDoNotRequireOpsWrite(t *testing.T) {
	t.Parallel()

	businessRequests := []struct {
		name     string
		req      *servicepb.Request
		expected Scope
	}{
		{"SaveNumscript", &servicepb.Request{Type: &servicepb.Request_SaveNumscript{}}, ScopeLedgersWrite},
		{"SaveLedgerMetadata", &servicepb.Request{Type: &servicepb.Request_SaveLedgerMetadata{}}, ScopeMetadataWrite},
		{"DeleteLedgerMetadata", &servicepb.Request{Type: &servicepb.Request_DeleteLedgerMetadata{}}, ScopeMetadataWrite},
		{"AddAccountType", &servicepb.Request{Type: &servicepb.Request_AddAccountType{}}, ScopeMetadataWrite},
		{"RemoveAccountType", &servicepb.Request{Type: &servicepb.Request_RemoveAccountType{}}, ScopeMetadataWrite},
		{"SetDefaultEnforcementMode", &servicepb.Request{Type: &servicepb.Request_SetDefaultEnforcementMode{}}, ScopeMetadataWrite},
	}

	for _, tc := range businessRequests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := RequiredScopeForRequest(tc.req)
			assert.Equal(t, tc.expected, got)
			assert.NotEqual(t, ScopeOpsWrite, got, "business operation must not require an operator scope")
		})
	}
}

// TestRequiredScopeForRequest_QueryCheckpointsRequireClusterWrite guards the
// privilege-escalation half of EN-1506. ClusterService gates these operations
// on ledger:ClusterWrite (server_cluster.go:442, 481), which DefaultMapping
// grants only via ledger:admin. While Apply accepted ledger:OpsWrite, any
// ledger:write token could perform them.
func TestRequiredScopeForRequest_QueryCheckpointsRequireClusterWrite(t *testing.T) {
	t.Parallel()

	checkpointRequests := []struct {
		name string
		req  *servicepb.Request
	}{
		{"CreateQueryCheckpoint", &servicepb.Request{Type: &servicepb.Request_CreateQueryCheckpoint{}}},
		{"DeleteQueryCheckpoint", &servicepb.Request{Type: &servicepb.Request_DeleteQueryCheckpoint{}}},
		{"SetQueryCheckpointSchedule", &servicepb.Request{Type: &servicepb.Request_SetQueryCheckpointSchedule{}}},
		{"DeleteQueryCheckpointSchedule", &servicepb.Request{Type: &servicepb.Request_DeleteQueryCheckpointSchedule{}}},
	}

	for _, tc := range checkpointRequests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := RequiredScopeForRequest(tc.req)
			assert.Equal(t, ScopeClusterWrite, got)
			assert.NotEqual(t, ScopeOpsWrite, got,
				"an admin-only cluster operation must not be reachable with ledger:OpsWrite")
		})
	}
}

// TestRequiredScopeForLedgerApply_AccountTypeActions covers the LedgerAction
// twins of the account-type operations, reachable as
// Request_Apply{Apply: {Action: ...}}. Each case is spelled out rather than
// table-driven over the oneof wrapper: the generated isLedgerAction_Data
// interface is unexported outside servicepb, so a shared table field cannot
// name its type.
func TestRequiredScopeForLedgerApply_AccountTypeActions(t *testing.T) {
	t.Parallel()

	applyWith := func(action *servicepb.LedgerAction) *servicepb.Request {
		return &servicepb.Request{Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{Action: action},
		}}
	}

	t.Run("AddAccountType", func(t *testing.T) {
		t.Parallel()

		req := applyWith(&servicepb.LedgerAction{
			Data: &servicepb.LedgerAction_AddAccountType{},
		})
		assert.Equal(t, ScopeMetadataWrite, RequiredScopeForRequest(req))
	})

	t.Run("RemoveAccountType", func(t *testing.T) {
		t.Parallel()

		req := applyWith(&servicepb.LedgerAction{
			Data: &servicepb.LedgerAction_RemoveAccountType{},
		})
		assert.Equal(t, ScopeMetadataWrite, RequiredScopeForRequest(req))
	})

	t.Run("SetDefaultEnforcementMode", func(t *testing.T) {
		t.Parallel()

		req := applyWith(&servicepb.LedgerAction{
			Data: &servicepb.LedgerAction_SetDefaultEnforcementMode{},
		})
		assert.Equal(t, ScopeMetadataWrite, RequiredScopeForRequest(req))
	})
}

// TestRequiredScopeForRequest_FailsClosed pins the fail-closed contract for
// input the classifier cannot recognize. These cases are deliberately excluded
// from the exhaustiveness table, which requires well-formed entries.
func TestRequiredScopeForRequest_FailsClosed(t *testing.T) {
	t.Parallel()

	failClosed := []struct {
		name string
		req  *servicepb.Request
	}{
		{"nil request", nil},
		{"no oneof set", &servicepb.Request{}},
		{"apply with nil LedgerApplyRequest", &servicepb.Request{Type: &servicepb.Request_Apply{}}},
		{"apply with nil action", &servicepb.Request{Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{},
		}}},
		{"apply with empty action", &servicepb.Request{Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{Action: &servicepb.LedgerAction{}},
		}}},
	}

	for _, tc := range failClosed {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, ScopeOpsWrite, RequiredScopeForRequest(tc.req))
		})
	}
}
