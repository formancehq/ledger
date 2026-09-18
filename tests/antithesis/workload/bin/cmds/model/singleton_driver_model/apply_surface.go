package main

import (
	"github.com/antithesishq/antithesis-sdk-go/random"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

func requestedEnforcementMode(req *servicepb.Request) *commonpb.ChartEnforcementMode {
	if setter := req.GetSetDefaultEnforcementMode(); setter != nil {
		mode := setter.GetEnforcementMode()
		return &mode
	}
	if setter := req.GetApply().GetAction().GetSetDefaultEnforcementMode(); setter != nil {
		mode := setter.GetEnforcementMode()
		return &mode
	}
	return nil
}

// maybeAddSkippableReason composes skip opt-ins with ordinary generated slots,
// preserving their position, ledger selection, and surrounding bulk shape.
func maybeAddSkippableReason(ls oracle.LedgerState, req *servicepb.Request) *servicepb.Request {
	if random.RandomChoice([]uint8{0, 1, 2, 3}) != 0 {
		return req
	}

	// The ordinary chart generators use the top-level request forms. Skippable
	// reasons live on Apply, so retain their generated payload while nesting it.
	switch top := req.GetType().(type) {
	case *servicepb.Request_AddAccountType:
		req = &servicepb.Request{Type: &servicepb.Request_Apply{Apply: &servicepb.LedgerApplyRequest{
			Ledger: top.AddAccountType.GetLedger(),
			Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_AddAccountType{
				AddAccountType: &servicepb.AddAccountTypeRequest{AccountType: top.AddAccountType.GetAccountType()},
			}},
		}}}
	case *servicepb.Request_RemoveAccountType:
		req = &servicepb.Request{Type: &servicepb.Request_Apply{Apply: &servicepb.LedgerApplyRequest{
			Ledger: top.RemoveAccountType.GetLedger(),
			Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_RemoveAccountType{
				RemoveAccountType: &servicepb.RemoveAccountTypeRequest{Name: top.RemoveAccountType.GetName()},
			}},
		}}}
	}

	reason, ok := generatedSkippableReason(req)
	if !ok {
		return req
	}
	if create := req.GetApply().GetAction().GetCreateTransaction(); create != nil {
		// Ordinary creates carry a globally-unique reference, so opting one into
		// REFERENCE_CONFLICT without rebinding the reference asks for a skip the
		// FSM can never perform. Half keep their unique reference, so the opt-in
		// that never fires stays covered too.
		if ref, _, ok := pickTxRef(ls); ok && random.RandomChoice([]uint8{0, 1}) == 0 {
			create.Reference = ref
		}

		return applyCreate(req.GetApply().GetLedger(), create, reason)
	}

	req.GetApply().SkippableReasons = []commonpb.ErrorReason{reason}
	return req
}

// generatedSkippableReason maps existing Apply actions to a skip reason.
// Mode setters deliberately use a disallowed reason to cover admission rejection.
func generatedSkippableReason(req *servicepb.Request) (commonpb.ErrorReason, bool) {
	if reason, ok := allowedSkippableReason(req); ok {
		return reason, true
	}
	if req.GetApply().GetAction().GetSetDefaultEnforcementMode() != nil {
		return commonpb.ErrorReason_ERROR_REASON_ACCOUNT_TYPE_NOT_FOUND, true
	}
	return commonpb.ErrorReason_ERROR_REASON_UNSPECIFIED, false
}

// allowedSkippableReason is the model's independent copy of the public
// per-action whitelist. It must not use admission's generated lookup table.
func allowedSkippableReason(req *servicepb.Request) (commonpb.ErrorReason, bool) {
	action := req.GetApply().GetAction()
	switch action.GetData().(type) {
	case *servicepb.LedgerAction_CreateTransaction:
		return commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT, true
	case *servicepb.LedgerAction_RevertTransaction:
		return commonpb.ErrorReason_ERROR_REASON_TRANSACTION_ALREADY_REVERTED, true
	case *servicepb.LedgerAction_DeleteMetadata:
		return commonpb.ErrorReason_ERROR_REASON_METADATA_NOT_FOUND, true
	case *servicepb.LedgerAction_AddAccountType:
		return commonpb.ErrorReason_ERROR_REASON_ACCOUNT_TYPE_ALREADY_EXISTS, true
	case *servicepb.LedgerAction_RemoveAccountType:
		return commonpb.ErrorReason_ERROR_REASON_ACCOUNT_TYPE_NOT_FOUND, true
	default:
		return commonpb.ErrorReason_ERROR_REASON_UNSPECIFIED, false
	}
}

func bulkHasInvalidSkippableReason(bulk oracle.Bulk) bool {
	for _, req := range bulk.Requests {
		reasons := req.GetApply().GetSkippableReasons()
		if len(reasons) == 0 {
			continue
		}
		allowed, ok := allowedSkippableReason(req)
		for _, reason := range reasons {
			if !ok || reason != allowed {
				return true
			}
		}
	}
	return false
}

var skippedReasons = []commonpb.ErrorReason{
	commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT,
	commonpb.ErrorReason_ERROR_REASON_TRANSACTION_ALREADY_REVERTED,
	commonpb.ErrorReason_ERROR_REASON_METADATA_NOT_FOUND,
	commonpb.ErrorReason_ERROR_REASON_ACCOUNT_TYPE_ALREADY_EXISTS,
	commonpb.ErrorReason_ERROR_REASON_ACCOUNT_TYPE_NOT_FOUND,
}

func skipCoverageMessage(reason commonpb.ErrorReason) string {
	return coveragePrefix + "Apply skipped " + reason.String() + " and continued"
}

func modeCoverageMessage(mode commonpb.ChartEnforcementMode, nested bool) string {
	form := "top-level"
	if nested {
		form = "ledger-action"
	}
	return coveragePrefix + "Apply " + form + " set " + mode.String()
}

const invalidSkipCoverageMessage = coveragePrefix + "Apply rejected invalid skip opt-in"

func applyCoverageMessages() []string {
	messages := []string{invalidSkipCoverageMessage}
	for _, reason := range skippedReasons {
		messages = append(messages, skipCoverageMessage(reason))
	}
	for _, mode := range []commonpb.ChartEnforcementMode{commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT, commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT} {
		for _, nested := range []bool{false, true} {
			messages = append(messages, modeCoverageMessage(mode, nested))
		}
	}
	return messages
}

// Called only after every commit check succeeded: selection alone never proves
// either the skipped outcome or execution of the following order.
func noteApplyCoverage(bulk oracle.Bulk, result oracle.ApplyResult) {
	hits := applyCoverageHits(bulk, result)
	for _, message := range applyCoverageMessages() {
		emitCoverage(hits[message], message, nil, coverageHit)
	}
}

// applyCoverageHits decides every Apply sonde for each validated commit.
func applyCoverageHits(bulk oracle.Bulk, result oracle.ApplyResult) map[string]bool {
	hits := make(map[string]bool, len(applyCoverageMessages()))
	for _, message := range applyCoverageMessages() {
		hits[message] = false
	}
	for i, order := range result.Orders {
		if order.Skipped != nil && i+1 < len(result.Orders) && result.Orders[i+1].Skipped == nil {
			hits[skipCoverageMessage(order.Skipped.GetReason())] = true
		}
		if mode := requestedEnforcementMode(bulk.Requests[i]); mode != nil {
			hits[modeCoverageMessage(*mode, bulk.Requests[i].GetApply() != nil)] = true
		}
	}
	return hits
}
