package ledger

import (
	"errors"
	"testing"
)

func TestErrInvalidIdempotencyInput_Is(t *testing.T) {
	originalErr := ErrInvalidIdempotencyInput{
		idempotencyKey:          "test-key",
		expectedIdempotencyHash: "expected-hash",
		computedIdempotencyHash: "computed-hash",
	}

	tests := []struct {
		name     string
		err      error
		target   error
		expected bool
	}{
		{
			name:     "same value type",
			err:      originalErr,
			target:   ErrInvalidIdempotencyInput{},
			expected: true,
		},
		{
			name:     "pointer to value type",
			err:      &originalErr,
			target:   ErrInvalidIdempotencyInput{},
			expected: true,
		},
		{
			name:     "value to pointer type",
			err:      originalErr,
			target:   &ErrInvalidIdempotencyInput{},
			expected: true,
		},
		{
			name:     "different error type",
			err:      originalErr,
			target:   errors.New("different error"),
			expected: false,
		},
	}

	for _, tc := range tests {
		tc := tc // capture range variable
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := errors.Is(tc.err, tc.target)
			if result != tc.expected {
				t.Errorf("errors.Is(%v, %v) = %v, expected %v", tc.err, tc.target, result, tc.expected)
			}
		})
	}
}
