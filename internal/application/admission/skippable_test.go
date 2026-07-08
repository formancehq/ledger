package admission

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// TestExtractSkippableReasonsFromApply_AcceptsWhitelistedReason validates the
// success path: an Apply request listing only whitelisted reasons passes
// through as the typed enum slice. Pins the supported business whitelist for
// CreateTransaction (extend the assertion if a new reason is added to
// createTransactionSkippable).
func TestExtractSkippableReasonsFromApply_AcceptsWhitelistedReason(t *testing.T) {
	t.Parallel()

	apply := &servicepb.LedgerApplyRequest{
		Ledger: "L",
		Action: &servicepb.LedgerAction{
			Data: &servicepb.LedgerAction_CreateTransaction{
				CreateTransaction: &servicepb.CreateTransactionPayload{},
			},
		},
		SkippableReasons: []commonpb.ErrorReason{
			commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT,
		},
	}

	got, err := extractSkippableReasonsFromApply(apply)
	require.NoError(t, err)
	require.Equal(t, []commonpb.ErrorReason{commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT}, got)
}

// TestExtractSkippableReasonsFromApply_EmptyListReturnsNil verifies the
// historical no-opt-in default: an empty/missing skippable_reasons disables
// the skip mechanism and returns (nil, nil) so the order's SkippableReasons
// field stays unset.
func TestExtractSkippableReasonsFromApply_EmptyListReturnsNil(t *testing.T) {
	t.Parallel()

	apply := &servicepb.LedgerApplyRequest{
		Ledger: "L",
		Action: &servicepb.LedgerAction{
			Data: &servicepb.LedgerAction_CreateTransaction{
				CreateTransaction: &servicepb.CreateTransactionPayload{},
			},
		},
	}

	got, err := extractSkippableReasonsFromApply(apply)
	require.NoError(t, err)
	require.Nil(t, got)
}

// TestExtractSkippableReasonsFromApply_RejectsUnspecified pins the
// "explicit-only" rule: an UNSPECIFIED entry is treated as a caller bug
// (forgot to set the enum value) and rejected up front rather than silently
// dropped.
func TestExtractSkippableReasonsFromApply_RejectsUnspecified(t *testing.T) {
	t.Parallel()

	apply := &servicepb.LedgerApplyRequest{
		Ledger: "L",
		Action: &servicepb.LedgerAction{
			Data: &servicepb.LedgerAction_CreateTransaction{
				CreateTransaction: &servicepb.CreateTransactionPayload{},
			},
		},
		SkippableReasons: []commonpb.ErrorReason{
			commonpb.ErrorReason_ERROR_REASON_UNSPECIFIED,
		},
	}

	got, err := extractSkippableReasonsFromApply(apply)
	require.Nil(t, got)
	requireInvalidSkippableReason(t, err, commonpb.ErrorReason_ERROR_REASON_UNSPECIFIED)
}

// TestExtractSkippableReasonsFromApply_RejectsOutOfWhitelist exercises the
// per-action business whitelist: a reason that is real but not allowed for
// this action must fail admission with a typed business error so the gRPC
// adapter can render a clean validation error.
func TestExtractSkippableReasonsFromApply_RejectsOutOfWhitelist(t *testing.T) {
	t.Parallel()

	apply := &servicepb.LedgerApplyRequest{
		Ledger: "L",
		Action: &servicepb.LedgerAction{
			Data: &servicepb.LedgerAction_CreateTransaction{
				CreateTransaction: &servicepb.CreateTransactionPayload{},
			},
		},
		SkippableReasons: []commonpb.ErrorReason{
			commonpb.ErrorReason_ERROR_REASON_INSUFFICIENT_FUNDS,
		},
	}

	got, err := extractSkippableReasonsFromApply(apply)
	require.Nil(t, got)
	requireInvalidSkippableReason(t, err, commonpb.ErrorReason_ERROR_REASON_INSUFFICIENT_FUNDS)
}

// TestExtractSkippableReasonsFromApply_RejectsOnUnsupportedAction covers
// the "wrong action" case: the caller opted into skip on an action that has
// no whitelist (e.g. AddMetadata) — the helper rejects rather than silently
// dropping the intent so a copy-paste from a CreateTransaction entry does
// not silently disarm the skip on the wrong action.
func TestExtractSkippableReasonsFromApply_RejectsOnUnsupportedAction(t *testing.T) {
	t.Parallel()

	apply := &servicepb.LedgerApplyRequest{
		Ledger: "L",
		Action: &servicepb.LedgerAction{
			Data: &servicepb.LedgerAction_AddMetadata{
				AddMetadata: &commonpb.SaveMetadataCommand{},
			},
		},
		SkippableReasons: []commonpb.ErrorReason{
			commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT,
		},
	}

	got, err := extractSkippableReasonsFromApply(apply)
	require.Nil(t, got)
	requireInvalidSkippableReason(t, err, commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT)
}

func requireInvalidSkippableReason(t *testing.T, err error, expected commonpb.ErrorReason) {
	t.Helper()
	require.Error(t, err)

	var be *domain.BusinessError

	require.True(t, errors.As(err, &be), "expected BusinessError wrap")

	var inner *domain.ErrInvalidSkippableReason

	require.True(t, errors.As(be.Err, &inner), "expected ErrInvalidSkippableReason inside BusinessError")
	require.Equal(t, expected, inner.Provided)
}
