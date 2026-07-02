package admission

import (
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// Per-operation business whitelists. Each set lists the ErrorReason values
// the operation accepts in `skippable_reasons`. Adding a value requires
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

// extractSkippableReasonsFromApply pulls and validates the `skippable_reasons`
// list off the public payload of an Apply request. Returns the typed enum
// slice ready to assign onto raftcmdpb.Order.SkippableReasons, or an error
// wrapped in BusinessError when any entry is rejected.
//
// Validation rules:
//   - UNSPECIFIED is always rejected (caller forgot to set the enum value).
//   - Each entry must be in the per-operation business whitelist.
//   - Operations without a whitelist treat any non-empty list as invalid.
//
// Empty lists (the default) pass through and disable the skip mechanism.
func extractSkippableReasonsFromApply(apply *servicepb.LedgerApplyRequest) ([]commonpb.ErrorReason, error) {
	action := apply.GetAction()
	if action == nil {
		return nil, nil
	}

	var (
		reasons []commonpb.ErrorReason
		allowed map[commonpb.ErrorReason]struct{}
	)

	switch data := action.GetData().(type) {
	case *servicepb.LedgerAction_CreateTransaction:
		reasons = data.CreateTransaction.GetSkippableReasons()
		allowed = createTransactionSkippable
	default:
		// Operations without skip support today: any non-empty list is
		// rejected so a typo on the wrong payload doesn't silently lose
		// the opt-in intent.
		return nil, nil
	}

	if len(reasons) == 0 {
		return nil, nil
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
