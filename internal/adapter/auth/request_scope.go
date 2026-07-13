package auth

import (
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// RequiredScopeForRequest returns the granular scope required to execute the given Request.
func RequiredScopeForRequest(req *servicepb.Request) Scope {
	switch req.GetType().(type) {
	case *servicepb.Request_Apply:
		return requiredScopeForLedgerApply(req.GetApply())
	case *servicepb.Request_CreateLedger:
		return ScopeLedgersWrite
	case *servicepb.Request_DeleteLedger:
		return ScopeLedgersWrite
	case *servicepb.Request_PromoteLedger:
		return ScopeLedgersWrite
	case *servicepb.Request_CreateIndex:
		// Index management is a per-ledger write; the HTTP routes
		// (POST/DELETE /v3/{ledgerName}/indexes[/{canonicalId}]) sit under
		// the ledger:LedgerWrite group, so gRPC must agree — otherwise the
		// same operation demands ledger:OpsWrite over gRPC (default fallthrough)
		// and ledger:LedgerWrite over HTTP.
		return ScopeLedgersWrite
	case *servicepb.Request_DropIndex:
		return ScopeLedgersWrite
	case *servicepb.Request_RegisterSigningKey:
		return ScopeOpsWrite
	case *servicepb.Request_RevokeSigningKey:
		return ScopeOpsWrite
	case *servicepb.Request_SetSigningConfig:
		return ScopeOpsWrite
	case *servicepb.Request_AddEventsSink:
		return ScopeOpsWrite
	case *servicepb.Request_RemoveEventsSink:
		return ScopeOpsWrite
	case *servicepb.Request_CloseChapter:
		return ScopeOpsWrite
	case *servicepb.Request_SealChapter:
		return ScopeOpsWrite
	case *servicepb.Request_ArchiveChapter:
		return ScopeOpsWrite
	case *servicepb.Request_ConfirmArchiveChapter:
		return ScopeOpsWrite
	case *servicepb.Request_SetMaintenanceMode:
		return ScopeOpsWrite
	case *servicepb.Request_SetChapterSchedule:
		return ScopeOpsWrite
	case *servicepb.Request_DeleteChapterSchedule:
		return ScopeOpsWrite
	case *servicepb.Request_SetMetadataFieldType:
		return ScopeMetadataWrite
	case *servicepb.Request_RemoveMetadataFieldType:
		return ScopeMetadataWrite
	case *servicepb.Request_CreatePreparedQuery:
		return ScopeQueriesWrite
	case *servicepb.Request_UpdatePreparedQuery:
		return ScopeQueriesWrite
	case *servicepb.Request_DeletePreparedQuery:
		return ScopeQueriesWrite
	default:
		// Unknown request type — default to ledger:OpsWrite (most restrictive write scope)
		return ScopeOpsWrite
	}
}

// requiredScopeForLedgerApply returns the granular scope for inner Apply request types.
func requiredScopeForLedgerApply(req *servicepb.LedgerApplyRequest) Scope {
	if req == nil {
		return ScopeOpsWrite
	}

	switch req.GetAction().GetData().(type) {
	case *servicepb.LedgerAction_CreateTransaction:
		return ScopeTransactionsWrite
	case *servicepb.LedgerAction_RevertTransaction:
		return ScopeTransactionsWrite
	case *servicepb.LedgerAction_AddMetadata:
		return ScopeMetadataWrite
	case *servicepb.LedgerAction_DeleteMetadata:
		return ScopeMetadataWrite
	default:
		return ScopeOpsWrite
	}
}
