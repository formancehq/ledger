package main

import (
	"math/big"

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

// generateSkippableBulk targets the small collision pools used by ordinary
// writers. The selected failure may disappear before commit; the oracle decides
// whether the order succeeded, skipped, or failed in the actual serialization.
func generateSkippableBulk(ledger string, ls oracle.LedgerState) oracle.Bulk {
	var action *servicepb.LedgerAction
	var reason commonpb.ErrorReason
	switch random.RandomChoice([]uint8{0, 1, 2, 3, 4, 5}) {
	case 0:
		ref, _, ok := pickTxRef(ls)
		if !ok {
			return oracle.Bulk{}
		}
		action = &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{CreateTransaction: &servicepb.CreateTransactionPayload{
			Reference: ref, Postings: []*commonpb.Posting{commonpb.NewPosting("world", poolAddress(), "USD", big.NewInt(1))},
		}}}
		reason = commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT
	case 1:
		req := generateRevert(ledger, ls)
		if req == nil {
			return oracle.Bulk{}
		}
		action = req.GetApply().GetAction()
		reason = commonpb.ErrorReason_ERROR_REASON_TRANSACTION_ALREADY_REVERTED
	case 2:
		action = &servicepb.LedgerAction{Data: &servicepb.LedgerAction_DeleteMetadata{DeleteMetadata: &commonpb.DeleteMetadataCommand{
			Target: &commonpb.Target{Target: &commonpb.Target_Account{Account: &commonpb.TargetAccount{Addr: poolAddress()}}}, Key: "model-skip-missing",
		}}}
		reason = commonpb.ErrorReason_ERROR_REASON_METADATA_NOT_FOUND
	case 3:
		action = &servicepb.LedgerAction{Data: &servicepb.LedgerAction_AddAccountType{AddAccountType: &servicepb.AddAccountTypeRequest{AccountType: generateAddAccountType(ledger).GetAddAccountType().GetAccountType()}}}
		reason = commonpb.ErrorReason_ERROR_REASON_ACCOUNT_TYPE_ALREADY_EXISTS
	case 4:
		action = &servicepb.LedgerAction{Data: &servicepb.LedgerAction_RemoveAccountType{RemoveAccountType: &servicepb.RemoveAccountTypeRequest{Name: poolName()}}}
		reason = commonpb.ErrorReason_ERROR_REASON_ACCOUNT_TYPE_NOT_FOUND
	case 5:
		// Mode setters are not on the whitelist: this opt-in must fail admission,
		// even though the mode switch would otherwise be valid.
		req := generateEnforcementMode(ledger)
		mode := requestedEnforcementMode(req)
		action = &servicepb.LedgerAction{Data: &servicepb.LedgerAction_SetDefaultEnforcementMode{SetDefaultEnforcementMode: &servicepb.SetDefaultEnforcementModeRequest{EnforcementMode: *mode}}}
		reason = commonpb.ErrorReason_ERROR_REASON_ACCOUNT_TYPE_NOT_FOUND
	}
	req := &servicepb.Request{Type: &servicepb.Request_Apply{Apply: &servicepb.LedgerApplyRequest{Ledger: ledger, Action: action, SkippableReasons: []commonpb.ErrorReason{reason}}}}
	following := generateEnforcementMode(ledger)
	if random.RandomChoice([]uint8{0, 1, 2, 3, 4, 5, 6, 7}) == 0 {
		// A second, non-opted-in copy can reject the whole proposal after a skip.
		following = req.CloneVT()
		following.GetApply().SkippableReasons = nil
	}
	return oracle.Bulk{Requests: []*servicepb.Request{req, following}}
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
	hits := map[string]bool{}
	for i, order := range result.Orders {
		if order.Skipped != nil && i+1 < len(result.Orders) && result.Orders[i+1].Skipped == nil {
			hits[skipCoverageMessage(order.Skipped.GetReason())] = true
		}
		if mode := requestedEnforcementMode(bulk.Requests[i]); mode != nil {
			hits[modeCoverageMessage(*mode, bulk.Requests[i].GetApply() != nil)] = true
		}
	}
	for _, message := range applyCoverageMessages() {
		emitCoverage(hits[message], message, nil, coverageHit)
	}
}
