package cmdutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWrapText(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		text      string
		maxWidth  int
		separator string
		expected  []string
	}{
		{
			name:      "short text no wrap",
			text:      "users:bank",
			maxWidth:  20,
			separator: ":",
			expected:  []string{"users:bank"},
		},
		{
			name:      "exact fit",
			text:      "users:bank",
			maxWidth:  10,
			separator: ":",
			expected:  []string{"users:bank"},
		},
		{
			name:      "wrap at separator",
			text:      "users:bank:payments:eu:france",
			maxWidth:  20,
			separator: ":",
			expected:  []string{"users:bank:payments:", "eu:france"},
		},
		{
			name:      "multiple wraps",
			text:      "users:bank:payments:eu:france:paris:12345678",
			maxWidth:  20,
			separator: ":",
			expected:  []string{"users:bank:payments:", "eu:france:paris:", "12345678"},
		},
		{
			name:      "hard break no separator found",
			text:      "abcdefghijklmnopqrstuvwxyz",
			maxWidth:  10,
			separator: ":",
			expected:  []string{"abcdefghij", "klmnopqrst", "uvwxyz"},
		},
		{
			name:      "empty separator hard break",
			text:      "abcdefghijklmno",
			maxWidth:  10,
			separator: "",
			expected:  []string{"abcdefghij", "klmno"},
		},
		{
			name:      "zero max width returns as-is",
			text:      "anything",
			maxWidth:  0,
			separator: ":",
			expected:  []string{"anything"},
		},
		{
			name:      "empty text",
			text:      "",
			maxWidth:  10,
			separator: ":",
			expected:  []string{""},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.expected, WrapText(tt.text, tt.maxWidth, tt.separator))
		})
	}
}
