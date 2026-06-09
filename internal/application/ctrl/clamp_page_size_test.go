package ctrl

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestClampPageSize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    uint32
		expected uint32
	}{
		{
			name:     "zero returns default",
			input:    0,
			expected: DefaultPageSize,
		},
		{
			name:     "within range is unchanged",
			input:    50,
			expected: 50,
		},
		{
			name:     "exceeds max returns max",
			input:    5000,
			expected: MaxPageSize,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.expected, ClampPageSize(tt.input))
		})
	}
}
