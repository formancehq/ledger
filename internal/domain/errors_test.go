package domain

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
			err:      &ErrTransactionReferenceConflict{Ledger: "test", Reference: "ref-001"},
			expected: `transaction reference "ref-001" already exists in ledger test`,
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
			name:     "ErrTransactionReferenceNotFound",
			err:      &ErrTransactionReferenceNotFound{Reference: "invoice:42"},
			expected: `transaction with reference "invoice:42" does not exist`,
		},
		{
			name:     "ErrTransactionTargetMissing",
			err:      ErrTransactionTargetMissing,
			expected: "transaction target requires either id or reference",
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

func TestWrapCompileError(t *testing.T) {
	t.Parallel()

	t.Run("raw error passes through unchanged (sanitiser handles it)", func(t *testing.T) {
		t.Parallel()

		// After flemzord's review: raw errors at the WrapCompileError
		// boundary are no longer re-wrapped — they pass through so the
		// convertToGRPCError sanitiser default returns codes.Unknown with
		// a correlation ID instead of leaking the message via
		// ErrFilterCompilation.Detail. Client-actionable validation
		// errors are typed at source (compile.go validation helpers via
		// NewFilterCompilationError).
		raw := errors.New(`creating account iterator: pebble: not found at /var/lib/ledger/...`)
		wrapped := WrapCompileError(raw)

		require.Same(t, raw, wrapped)
	})

	t.Run("existing BusinessError passes through unchanged", func(t *testing.T) {
		t.Parallel()

		// ErrIndexNotFound is already wrapped by query.Compile and has its own
		// gRPC mapping (FailedPrecondition). Re-wrapping it as
		// ErrFilterCompilation would shadow the specific mapping with the
		// generic InvalidArgument — regression caught by E2E
		// indexes_test.go:63.
		original := &BusinessError{Err: &ErrIndexNotFound{Index: `metadata["category"] on accounts`}}
		got := WrapCompileError(original)
		require.Same(t, original, got)

		var notFound *ErrIndexNotFound
		require.ErrorAs(t, got, &notFound)
		require.Equal(t, `metadata["category"] on accounts`, notFound.Index)
	})

	t.Run("wrapped BusinessError passes through unchanged", func(t *testing.T) {
		t.Parallel()

		// Defensive: even if a caller wraps with fmt.Errorf, errors.As must
		// still find the BusinessError underneath and we must NOT re-wrap.
		original := &BusinessError{Err: &ErrIndexNotFound{Index: "idx"}}
		wrapped := errors.Join(original)
		got := WrapCompileError(wrapped)
		require.Equal(t, wrapped, got)
	})
}

func TestNewFilterCompilationError(t *testing.T) {
	t.Parallel()

	err := NewFilterCompilationError("field %q is declared as %s, cannot use string condition", "age", "INT64")

	var biz *BusinessError
	require.ErrorAs(t, err, &biz)

	var compile *ErrFilterCompilation
	require.ErrorAs(t, err, &compile)
	require.Equal(t, `field "age" is declared as INT64, cannot use string condition`, compile.Detail)
}
