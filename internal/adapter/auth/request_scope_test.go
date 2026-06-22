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
		{"CloseChapter", &servicepb.Request{Type: &servicepb.Request_CloseChapter{}}},
		{"SealChapter", &servicepb.Request{Type: &servicepb.Request_SealChapter{}}},
		{"ArchiveChapter", &servicepb.Request{Type: &servicepb.Request_ArchiveChapter{}}},
		{"ConfirmArchiveChapter", &servicepb.Request{Type: &servicepb.Request_ConfirmArchiveChapter{}}},
		{"SetMaintenanceMode", &servicepb.Request{Type: &servicepb.Request_SetMaintenanceMode{}}},
		{"SetChapterSchedule", &servicepb.Request{Type: &servicepb.Request_SetChapterSchedule{}}},
		{"DeleteChapterSchedule", &servicepb.Request{Type: &servicepb.Request_DeleteChapterSchedule{}}},
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
	// Nil apply defaults to ops:write (most restrictive)
	assert.Equal(t, ScopeOpsWrite, RequiredScopeForRequest(req))
}
