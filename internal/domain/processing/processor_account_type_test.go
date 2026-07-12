package processing

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// TestProcessRemoveAccountType_EmptyNameRejectedAsValidation pins the fix for
// finding checker.go:3472: a RemoveAccountType targeting an empty name is a
// validation error (INVALID_PATTERN), NOT a legitimate ACCOUNT_TYPE_NOT_FOUND
// outcome. Because INVALID_PATTERN is not in any skippable whitelist, the FSM
// can never emit a degenerate OrderSkippedLog with context.name="" for such
// an order. Symmetric with processAddAccountType, which already rejects an
// empty name.
func TestProcessRemoveAccountType_EmptyNameRejectedAsValidation(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	expectGetLedger(mockStore, domain.LedgerKey{Name: "L"}, (&commonpb.LedgerInfo{Name: "L", Id: 1}).AsReader(), nil).AnyTimes()
	// No Ledgers().Put must happen — the order is rejected before any mutation.

	payload, describable := processRemoveAccountType("L", &raftcmdpb.RemoveAccountTypeOrder{Name: ""}, &Context{Scope: mockStore})

	require.Nil(t, payload)
	require.NotNil(t, describable)

	var invalidPattern *domain.ErrInvalidPattern
	require.True(t, errors.As(describable, &invalidPattern),
		"empty account-type name must be a validation error (INVALID_PATTERN), not a skippable ACCOUNT_TYPE_NOT_FOUND")

	// Guard the invariant the fix relies on: INVALID_PATTERN is a
	// non-skippable kind, so the failure can never become an OrderSkippedLog.
	require.Equal(t, domain.KindValidation, domain.KindForReason(domain.ReasonCode(describable.Reason())))
}
