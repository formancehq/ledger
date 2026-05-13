package auth

import (
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
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
	case *servicepb.Request_ClosePeriod:
		return ScopeOpsWrite
	case *servicepb.Request_SealPeriod:
		return ScopeOpsWrite
	case *servicepb.Request_ArchivePeriod:
		return ScopeOpsWrite
	case *servicepb.Request_ConfirmArchivePeriod:
		return ScopeOpsWrite
	case *servicepb.Request_SetMaintenanceMode:
		return ScopeOpsWrite
	case *servicepb.Request_SetPeriodSchedule:
		return ScopeOpsWrite
	case *servicepb.Request_DeletePeriodSchedule:
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
		// Unknown request type — default to ops:write (most restrictive write scope)
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
