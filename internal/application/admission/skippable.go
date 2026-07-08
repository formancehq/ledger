package admission

import (
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// Per-action business whitelists. Each set lists the ErrorReason values
// the action accepts in `skippable_reasons`. Adding a value requires
// auditing the sub-processor for the "dry before mutate" invariant
// documented in internal/domain/processing/processor_skippable.go.
//
// Structural reasons (KindInternal) are never accepted; the FSM applies the
// same gate as defense in depth.
var (
	createTransactionSkippable = map[commonpb.ErrorReason]struct{}{
		commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT: {},
	}
)

// extractSkippableReasonsFromApply pulls and validates the top-level
// `skippable_reasons` list off a LedgerApplyRequest. Returns the typed enum
// slice ready to assign onto raftcmdpb.Order.SkippableReasons, or an error
// wrapped in BusinessError when any entry is rejected.
//
// Validation rules:
//   - Empty lists pass through as no-op (skip disabled).
//   - UNSPECIFIED is always rejected (caller forgot to set the enum value).
//   - Each entry must be in the action-specific business whitelist.
//   - A non-empty list on an action that does not support skip is rejected
//     — silently dropping the intent would surprise the caller.
func extractSkippableReasonsFromApply(apply *servicepb.LedgerApplyRequest) ([]commonpb.ErrorReason, error) {
	reasons := apply.GetSkippableReasons()
	if len(reasons) == 0 {
		return nil, nil
	}

	action := apply.GetAction()
	if action == nil {
		return nil, &domain.BusinessError{Err: &domain.ErrInvalidSkippableReason{Provided: reasons[0]}}
	}

	var allowed map[commonpb.ErrorReason]struct{}

	switch action.GetData().(type) {
	case *servicepb.LedgerAction_CreateTransaction:
		allowed = createTransactionSkippable
	default:
		// The caller opted into skip on an action that has no whitelist —
		// reject rather than silently drop the intent.
		return nil, &domain.BusinessError{Err: &domain.ErrInvalidSkippableReason{Provided: reasons[0]}}
	}

	for _, r := range reasons {
		if r == commonpb.ErrorReason_ERROR_REASON_UNSPECIFIED {
			return nil, &domain.BusinessError{Err: &domain.ErrInvalidSkippableReason{Provided: r}}
		}

		if _, ok := allowed[r]; !ok {
			return nil, &domain.BusinessError{Err: &domain.ErrInvalidSkippableReason{Provided: r}}
		}
	}

	return reasons, nil
}
