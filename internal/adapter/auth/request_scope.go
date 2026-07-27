package auth

import (
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// RequiredScopeForRequest returns the granular scope required to execute the
// given Request. Variants this build does not know about — malformed input, or
// a proto variant added without a scope decision — fail closed on
// ledger:OpsWrite, the most restrictive write scope.
func RequiredScopeForRequest(req *servicepb.Request) Scope {
	scope, decided := requiredScopeForRequest(req)
	if !decided {
		return ScopeOpsWrite
	}

	return scope
}

// requiredScopeForRequest returns the scope for a Request and whether the
// variant carries an explicit decision.
//
// decided=false means "this build has no arm for this variant" — never "the
// answer is OpsWrite". Keeping the two apart is the whole point: a silent
// fall-through used to be indistinguishable from a deliberate OpsWrite, so no
// test could tell that a variant had never been classified (EN-1506).
// TestRequiredScopeForRequest_ProtoExhaustive asserts decided=true for every
// variant the proto declares, so a new oneof field fails CI until someone
// decides its scope.
func requiredScopeForRequest(req *servicepb.Request) (Scope, bool) {
	switch req.GetType().(type) {
	case *servicepb.Request_Apply:
		return requiredScopeForLedgerApply(req.GetApply())
	case *servicepb.Request_CreateLedger:
		return ScopeLedgersWrite, true
	case *servicepb.Request_DeleteLedger:
		return ScopeLedgersWrite, true
	case *servicepb.Request_PromoteLedger:
		return ScopeLedgersWrite, true
	case *servicepb.Request_CreateIndex:
		// Index management is a per-ledger write; the HTTP routes
		// (POST/DELETE /v3/{ledgerName}/indexes[/{canonicalId}]) sit under
		// the ledger:LedgerWrite group, so gRPC must agree — otherwise the
		// same operation demands ledger:OpsWrite over gRPC (default fallthrough)
		// and ledger:LedgerWrite over HTTP.
		return ScopeLedgersWrite, true
	case *servicepb.Request_DropIndex:
		return ScopeLedgersWrite, true
	case *servicepb.Request_RegisterSigningKey:
		return ScopeOpsWrite, true
	case *servicepb.Request_RevokeSigningKey:
		return ScopeOpsWrite, true
	case *servicepb.Request_SetSigningConfig:
		return ScopeOpsWrite, true
	case *servicepb.Request_AddEventsSink:
		return ScopeOpsWrite, true
	case *servicepb.Request_RemoveEventsSink:
		return ScopeOpsWrite, true
	case *servicepb.Request_CloseChapter:
		return ScopeOpsWrite, true
	case *servicepb.Request_SealChapter:
		return ScopeOpsWrite, true
	case *servicepb.Request_ArchiveChapter:
		return ScopeOpsWrite, true
	case *servicepb.Request_ConfirmArchiveChapter:
		return ScopeOpsWrite, true
	case *servicepb.Request_SetMaintenanceMode:
		return ScopeOpsWrite, true
	case *servicepb.Request_SetChapterSchedule:
		return ScopeOpsWrite, true
	case *servicepb.Request_DeleteChapterSchedule:
		return ScopeOpsWrite, true
	case *servicepb.Request_SetMetadataFieldType:
		return ScopeMetadataWrite, true
	case *servicepb.Request_RemoveMetadataFieldType:
		return ScopeMetadataWrite, true
	case *servicepb.Request_CreatePreparedQuery:
		return ScopeQueriesWrite, true
	case *servicepb.Request_UpdatePreparedQuery:
		return ScopeQueriesWrite, true
	case *servicepb.Request_DeletePreparedQuery:
		return ScopeQueriesWrite, true
	default:
		return ScopeOpsWrite, false
	}
}

// requiredScopeForLedgerApply returns the granular scope for inner Apply
// request types, and whether the action variant carries an explicit decision.
// A nil request or nil action is malformed input, not an undecided variant,
// but both fail closed the same way.
func requiredScopeForLedgerApply(req *servicepb.LedgerApplyRequest) (Scope, bool) {
	if req == nil {
		return ScopeOpsWrite, false
	}

	switch req.GetAction().GetData().(type) {
	case *servicepb.LedgerAction_CreateTransaction:
		return ScopeTransactionsWrite, true
	case *servicepb.LedgerAction_RevertTransaction:
		return ScopeTransactionsWrite, true
	case *servicepb.LedgerAction_AddMetadata:
		return ScopeMetadataWrite, true
	case *servicepb.LedgerAction_DeleteMetadata:
		return ScopeMetadataWrite, true
	default:
		return ScopeOpsWrite, false
	}
}
