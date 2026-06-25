package processing

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// TestMatchOrderSkip_RequiresNonEmptyWhitelist pins the contract that an
// order without skippable_reasons never gets its failure converted to a skip —
// the historical zero-cost path stays intact for callers that did not opt in.
func TestMatchOrderSkip_RequiresNonEmptyWhitelist(t *testing.T) {
	t.Parallel()

	order := &raftcmdpb.Order{}
	err := &domain.ErrTransactionReferenceConflict{Ledger: "L", Reference: "ref-1"}

	payload, matched := matchOrderSkip(order, err)
	require.False(t, matched)
	require.Nil(t, payload)
}

// TestMatchOrderSkip_AllowsListedReason validates the success path: a reason
// the order explicitly authorised converts the failure into an
// OrderSkippedLog carrying the error's Metadata() on its `context` field
// (clients correlate via the existing tx id without an extra lookup).
func TestMatchOrderSkip_AllowsListedReason(t *testing.T) {
	t.Parallel()

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{Ledger: "L"},
		},
		SkippableReasons: []commonpb.ErrorReason{commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT},
	}
	err := &domain.ErrTransactionReferenceConflict{Ledger: "L", Reference: "ref-1", ExistingTransactionID: 42}

	payload, matched := matchOrderSkip(order, err)
	require.True(t, matched)
	require.NotNil(t, payload)

	apply := payload.GetApply()
	require.NotNil(t, apply)
	require.Equal(t, "L", apply.GetLedgerName())

	skipped := apply.GetLog().GetData().GetOrderSkipped()
	require.NotNil(t, skipped)
	require.Equal(t, commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT, skipped.GetReason())

	ctx := skipped.GetContext()
	require.Equal(t, "L", ctx["ledger"])
	require.Equal(t, "ref-1", ctx["reference"])
	require.Equal(t, "42", ctx["existingTransactionId"])
}

// TestMatchOrderSkip_RejectsNonWhitelistedReason exercises the case where the
// order opts into skip but the actual failure is a different reason. The
// failure must propagate unchanged so the proposal still fails loudly.
func TestMatchOrderSkip_RejectsNonWhitelistedReason(t *testing.T) {
	t.Parallel()

	order := &raftcmdpb.Order{
		SkippableReasons: []commonpb.ErrorReason{commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT},
	}
	err := &domain.ErrLedgerNotFound{Name: "missing"}

	payload, matched := matchOrderSkip(order, err)
	require.False(t, matched)
	require.Nil(t, payload)
}

// TestMatchOrderSkip_RejectsKindInternal is the structural defense-in-depth
// gate: even if admission failed to strip a KindInternal reason from the
// whitelist, the FSM must refuse to convert it to a skip. Structural failures
// indicate broken invariants that callers cannot resolve by retrying.
func TestMatchOrderSkip_RejectsKindInternal(t *testing.T) {
	t.Parallel()

	internalReason := commonpb.ErrorReason_ERROR_REASON_INVALID_EXECUTION_PLAN
	require.Equal(t, domain.KindInternal, domain.KindForReason(internalReason),
		"this test assumes INVALID_EXECUTION_PLAN classifies as KindInternal")

	order := &raftcmdpb.Order{
		SkippableReasons: []commonpb.ErrorReason{internalReason},
	}
	err := &domain.ErrInvalidExecutionPlan{Reason_: "boom"}

	payload, matched := matchOrderSkip(order, err)
	require.False(t, matched)
	require.Nil(t, payload)
}

// TestMatchOrderSkip_UnspecifiedReason — when the error's Reason() does not
// map to a known ErrorReason (escape hatch on a non-Describable), the skip
// must NOT engage. The default route is to fail loudly.
func TestMatchOrderSkip_UnspecifiedReason(t *testing.T) {
	t.Parallel()

	order := &raftcmdpb.Order{
		SkippableReasons: []commonpb.ErrorReason{commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT},
	}

	payload, matched := matchOrderSkip(order, unknownReasonErr{})
	require.False(t, matched)
	require.Nil(t, payload)
}

type unknownReasonErr struct{}

func (unknownReasonErr) Error() string               { return "unknown" }
func (unknownReasonErr) Reason() string              { return "THIS_IS_NOT_A_KNOWN_REASON" }
func (unknownReasonErr) Metadata() map[string]string { return nil }

var _ domain.Describable = unknownReasonErr{}
