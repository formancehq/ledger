package processing

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBusinessError(t *testing.T) {
	t.Parallel()

	inner := errors.New("something went wrong")
	bErr := &BusinessError{Err: inner}

	require.Equal(t, "something went wrong", bErr.Error())
	require.ErrorIs(t, bErr, inner)
	require.Equal(t, inner, bErr.Unwrap())
}

func TestErrorTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		err      error
		expected string
	}{
		{
			name:     "ErrLedgerAlreadyExists",
			err:      &ErrLedgerAlreadyExists{Name: "default"},
			expected: "ledger already exists: default",
		},
		{
			name:     "ErrLedgerNotFound",
			err:      &ErrLedgerNotFound{Name: "default"},
			expected: "ledger does not exist: default",
		},
		{
			name:     "ErrIdempotencyKeyConflict",
			err:      &ErrIdempotencyKeyConflict{Key: "ik-001"},
			expected: `idempotency key conflict: key "ik-001" used with different request content`,
		},
		{
			name:     "ErrTransactionReferenceConflict",
			err:      &ErrTransactionReferenceConflict{LedgerID: 1, Reference: "ref-001"},
			expected: `transaction reference "ref-001" already exists in ledger 1`,
		},
		{
			name:     "ErrTransactionNotFound",
			err:      &ErrTransactionNotFound{TransactionID: 42},
			expected: "transaction 42 does not exist",
		},
		{
			name:     "ErrTransactionAlreadyReverted",
			err:      &ErrTransactionAlreadyReverted{TransactionID: 42},
			expected: "transaction 42 is already reverted",
		},
		{
			name:     "ErrInsufficientFunds",
			err:      &ErrInsufficientFunds{Account: "bank", Asset: "USD", Amount: "1000", Balance: "500"},
			expected: `insufficient funds on account "bank" for asset USD: needed 1000, available 500`,
		},
		{
			name:     "ErrBalanceNotFound",
			err:      &ErrBalanceNotFound{Account: "bank", Asset: "USD"},
			expected: `balance not found for account "bank" asset "USD"`,
		},
		{
			name:     "ErrSinkAlreadyExists",
			err:      &ErrSinkAlreadyExists{Name: "nats-1"},
			expected: "event sink already exists: nats-1",
		},
		{
			name:     "ErrSinkNotFound",
			err:      &ErrSinkNotFound{Name: "nats-1"},
			expected: "event sink not found: nats-1",
		},
		{
			name:     "ErrMetadataNotFound",
			err:      &ErrMetadataNotFound{Target: "users:123", Key: "status"},
			expected: `metadata key "status" not found on users:123`,
		},
		{
			name:     "ErrPeriodNotFound",
			err:      &ErrPeriodNotFound{PeriodID: 99},
			expected: "period 99 not found",
		},
		{
			name:     "ErrPeriodNotClosing",
			err:      &ErrPeriodNotClosing{PeriodID: 5},
			expected: "period 5 is not in CLOSING state",
		},
		{
			name:     "ErrPeriodNotClosed",
			err:      &ErrPeriodNotClosed{PeriodID: 5},
			expected: "period 5 is not in CLOSED state",
		},
		{
			name:     "ErrPeriodNotArchiving",
			err:      &ErrPeriodNotArchiving{PeriodID: 5},
			expected: "period 5 is not in ARCHIVING state",
		},
		{
			name:     "ErrInvalidCronExpression",
			err:      &ErrInvalidCronExpression{Expression: "bad", Details: "parse failed"},
			expected: `invalid cron expression "bad": parse failed`,
		},
		{
			name:     "ErrInvalidReceipt",
			err:      &ErrInvalidReceipt{Reason: "expired"},
			expected: "invalid receipt: expired",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.expected, tc.err.Error())
		})
	}
}
