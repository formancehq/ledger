package main

import (
	"github.com/antithesishq/antithesis-sdk-go/random"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

func requestedEnforcementMode(req *ledgerpb.Request) *ledgerpb.ChartEnforcementMode {
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
func maybeAddSkippableReason(ls oracle.LedgerState, req *ledgerpb.Request) *ledgerpb.Request {
	if random.RandomChoice([]uint8{0, 1, 2, 3}) != 0 {
		return req
	}

	// The ordinary chart generators use the top-level request forms. Skippable
	// reasons live on Apply, so retain their generated payload while nesting it.
	switch top := req.GetType().(type) {
	case *ledgerpb.Request_AddAccountType:
		req = &ledgerpb.Request{Type: &ledgerpb.Request_Apply{Apply: &ledgerpb.LedgerApplyRequest{
			Ledger: top.AddAccountType.GetLedger(),
			Action: &ledgerpb.LedgerAction{Data: &ledgerpb.LedgerAction_AddAccountType{
				AddAccountType: &ledgerpb.AddAccountTypeRequest{AccountType: top.AddAccountType.GetAccountType()},
			}},
		}}}
	case *ledgerpb.Request_RemoveAccountType:
		req = &ledgerpb.Request{Type: &ledgerpb.Request_Apply{Apply: &ledgerpb.LedgerApplyRequest{
			Ledger: top.RemoveAccountType.GetLedger(),
			Action: &ledgerpb.LedgerAction{Data: &ledgerpb.LedgerAction_RemoveAccountType{
				RemoveAccountType: &ledgerpb.RemoveAccountTypeRequest{Name: top.RemoveAccountType.GetName()},
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

	req.GetApply().SkippableReasons = []ledgerpb.ErrorReason{reason}

	return req
}

// generatedSkippableReason maps existing Apply actions to a skip reason.
// Mode setters deliberately use a disallowed reason to cover admission rejection.
func generatedSkippableReason(req *ledgerpb.Request) (ledgerpb.ErrorReason, bool) {
	if reason, ok := allowedSkippableReason(req); ok {
		return reason, true
	}
	if req.GetApply().GetAction().GetSetDefaultEnforcementMode() != nil {
		return ledgerpb.ErrorReason_ERROR_REASON_ACCOUNT_TYPE_NOT_FOUND, true
	}

	return ledgerpb.ErrorReason_ERROR_REASON_UNSPECIFIED, false
}

// allowedSkippableReason is the model's independent copy of the public
// per-action whitelist. It must not use admission's generated lookup table.
func allowedSkippableReason(req *ledgerpb.Request) (ledgerpb.ErrorReason, bool) {
	action := req.GetApply().GetAction()
	switch action.GetData().(type) {
	case *ledgerpb.LedgerAction_CreateTransaction:
		return ledgerpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT, true
	case *ledgerpb.LedgerAction_RevertTransaction:
		return ledgerpb.ErrorReason_ERROR_REASON_TRANSACTION_ALREADY_REVERTED, true
	case *ledgerpb.LedgerAction_DeleteMetadata:
		return ledgerpb.ErrorReason_ERROR_REASON_METADATA_NOT_FOUND, true
	case *ledgerpb.LedgerAction_AddAccountType:
		return ledgerpb.ErrorReason_ERROR_REASON_ACCOUNT_TYPE_ALREADY_EXISTS, true
	case *ledgerpb.LedgerAction_RemoveAccountType:
		return ledgerpb.ErrorReason_ERROR_REASON_ACCOUNT_TYPE_NOT_FOUND, true
	default:
		return ledgerpb.ErrorReason_ERROR_REASON_UNSPECIFIED, false
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

var skippedReasons = []ledgerpb.ErrorReason{
	ledgerpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT,
	ledgerpb.ErrorReason_ERROR_REASON_TRANSACTION_ALREADY_REVERTED,
	ledgerpb.ErrorReason_ERROR_REASON_METADATA_NOT_FOUND,
	ledgerpb.ErrorReason_ERROR_REASON_ACCOUNT_TYPE_ALREADY_EXISTS,
	ledgerpb.ErrorReason_ERROR_REASON_ACCOUNT_TYPE_NOT_FOUND,
}

func skipCoverageMessage(reason ledgerpb.ErrorReason) string {
	return coveragePrefix + "Apply skipped " + reason.String() + " and continued"
}

func modeCoverageMessage(mode ledgerpb.ChartEnforcementMode, nested bool) string {
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
	for _, mode := range []ledgerpb.ChartEnforcementMode{ledgerpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT, ledgerpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT} {
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
		emitCoverage(hits[message], message, nil)
	}
}

// applyCoverageHits decides every Apply probe for each validated commit.
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
