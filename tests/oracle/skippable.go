package oracle

import (
	"sort"
	"strconv"
	"strings"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"

	"github.com/formancehq/ledger/v3/internal/domain"
)

// The oracle declares the documented whitelist independently of the generated
// admission table, so changing that table cannot silently change expectations.
func validSkippableReasons(req *ledgerpb.LedgerApplyRequest) bool {
	var allowed ledgerpb.ErrorReason
	switch req.GetAction().GetData().(type) {
	case *ledgerpb.LedgerAction_CreateTransaction:
		allowed = ledgerpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT
	case *ledgerpb.LedgerAction_RevertTransaction:
		allowed = ledgerpb.ErrorReason_ERROR_REASON_TRANSACTION_ALREADY_REVERTED
	case *ledgerpb.LedgerAction_DeleteMetadata:
		allowed = ledgerpb.ErrorReason_ERROR_REASON_METADATA_NOT_FOUND
	case *ledgerpb.LedgerAction_AddAccountType:
		allowed = ledgerpb.ErrorReason_ERROR_REASON_ACCOUNT_TYPE_ALREADY_EXISTS
	case *ledgerpb.LedgerAction_RemoveAccountType:
		allowed = ledgerpb.ErrorReason_ERROR_REASON_ACCOUNT_TYPE_NOT_FOUND
	}
	for _, reason := range req.GetSkippableReasons() {
		if allowed == ledgerpb.ErrorReason_ERROR_REASON_UNSPECIFIED || reason != allowed {
			return false
		}
	}

	return true
}

// chartRequest lets both wire forms share chart prediction without mutating
// the request retained for idempotency and replay.
func chartRequest(req *ledgerpb.Request) *ledgerpb.Request {
	switch a := req.GetApply().GetAction().GetData().(type) {
	case *ledgerpb.LedgerAction_AddAccountType:
		return &ledgerpb.Request{Type: &ledgerpb.Request_AddAccountType{AddAccountType: &ledgerpb.AddAccountTypeLedgerRequest{Ledger: req.GetApply().GetLedger(), AccountType: a.AddAccountType.GetAccountType()}}}
	case *ledgerpb.LedgerAction_RemoveAccountType:
		return &ledgerpb.Request{Type: &ledgerpb.Request_RemoveAccountType{RemoveAccountType: &ledgerpb.RemoveAccountTypeLedgerRequest{Ledger: req.GetApply().GetLedger(), Name: a.RemoveAccountType.GetName()}}}
	default:
		return req
	}
}

func predictSkippedLog(s LedgerState, req *ledgerpb.Request, reason string) *ledgerpb.OrderSkippedLog {
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

	return &ledgerpb.OrderSkippedLog{Reason: domain.ReasonCode(reason), Context: context}
}

func canonicalSkippedLog(log *ledgerpb.OrderSkippedLog) string {
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
