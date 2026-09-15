package oracle

import (
	"sort"
	"strconv"
	"strings"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// The oracle declares the documented whitelist independently of the generated
// admission table, so changing that table cannot silently change expectations.
func validSkippableReasons(req *servicepb.LedgerApplyRequest) bool {
	var allowed commonpb.ErrorReason
	switch req.GetAction().GetData().(type) {
	case *servicepb.LedgerAction_CreateTransaction:
		allowed = commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT
	case *servicepb.LedgerAction_RevertTransaction:
		allowed = commonpb.ErrorReason_ERROR_REASON_TRANSACTION_ALREADY_REVERTED
	case *servicepb.LedgerAction_DeleteMetadata:
		allowed = commonpb.ErrorReason_ERROR_REASON_METADATA_NOT_FOUND
	case *servicepb.LedgerAction_AddAccountType:
		allowed = commonpb.ErrorReason_ERROR_REASON_ACCOUNT_TYPE_ALREADY_EXISTS
	case *servicepb.LedgerAction_RemoveAccountType:
		allowed = commonpb.ErrorReason_ERROR_REASON_ACCOUNT_TYPE_NOT_FOUND
	}
	for _, reason := range req.GetSkippableReasons() {
		if allowed == commonpb.ErrorReason_ERROR_REASON_UNSPECIFIED || reason != allowed {
			return false
		}
	}

	return true
}

// chartRequest lets both wire forms share chart prediction without mutating
// the request retained for idempotency and replay.
func chartRequest(req *servicepb.Request) *servicepb.Request {
	switch a := req.GetApply().GetAction().GetData().(type) {
	case *servicepb.LedgerAction_AddAccountType:
		return &servicepb.Request{Type: &servicepb.Request_AddAccountType{AddAccountType: &servicepb.AddAccountTypeLedgerRequest{Ledger: req.GetApply().GetLedger(), AccountType: a.AddAccountType.GetAccountType()}}}
	case *servicepb.LedgerAction_RemoveAccountType:
		return &servicepb.Request{Type: &servicepb.Request_RemoveAccountType{RemoveAccountType: &servicepb.RemoveAccountTypeLedgerRequest{Ledger: req.GetApply().GetLedger(), Name: a.RemoveAccountType.GetName()}}}
	default:
		return req
	}
}

func predictSkippedLog(s LedgerState, req *servicepb.Request, reason string) *commonpb.OrderSkippedLog {
	context := map[string]string{}
	action := req.GetApply().GetAction()
	switch reason {
	case domain.ErrReasonTransactionReferenceConflict:
		ref := action.GetCreateTransaction().GetReference()
		id, ok := s.txByRef.Get(ref)
		if !ok {
			panic("model: reference-conflict skip without an existing transaction")
		}
		context["ledger"] = req.GetApply().GetLedger()
		context["reference"] = ref
		context["existingTransactionId"] = strconv.Itoa(id)
	case domain.ErrReasonTransactionAlreadyReverted:
		context["transactionId"] = strconv.FormatUint(action.GetRevertTransaction().GetTransactionId(), 10)
	case domain.ErrReasonMetadataNotFound:
		cmd := action.GetDeleteMetadata()
		context["key"] = cmd.GetKey()
		if account := cmd.GetTarget().GetAccount(); account != nil {
			context["target"] = account.GetAddr()
		} else {
			context["target"] = strconv.FormatUint(cmd.GetTarget().GetTransactionId(), 10)
		}
	case domain.ErrReasonAccountTypeAlreadyExists:
		context["name"] = action.GetAddAccountType().GetAccountType().GetName()
	case domain.ErrReasonAccountTypeNotFound:
		context["name"] = action.GetRemoveAccountType().GetName()
	default:
		panic("model: unmodeled skipped reason " + reason)
	}

	return &commonpb.OrderSkippedLog{Reason: domain.ReasonCode(reason), Context: context}
}

func canonicalSkippedLog(log *commonpb.OrderSkippedLog) string {
	keys := make([]string, 0, len(log.GetContext()))
	for key := range log.GetContext() {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := []string{strconv.Itoa(int(log.GetReason()))}
	for _, key := range keys {
		parts = append(parts, strconv.Quote(key)+"="+strconv.Quote(log.GetContext()[key]))
	}

	return strings.Join(parts, "|")
}
