package bytesize

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    string
		expected ByteSize
	}{
		// Plain integers (bytes)
		{"0", 0},
		{"1", 1},
		{"1024", 1024},
		{"1048576", 1048576},

		// Binary suffixes
		{"1Ki", 1024},
		{"256Ki", 256 * 1024},
		{"1Mi", 1024 * 1024},
		{"64Mi", 64 * 1024 * 1024},
		{"1Gi", 1024 * 1024 * 1024},
		{"2Gi", 2 * 1024 * 1024 * 1024},
		{"1Ti", 1024 * 1024 * 1024 * 1024},
		{"1Pi", 1024 * 1024 * 1024 * 1024 * 1024},

		// Decimal suffixes
		{"1k", 1000},
		{"10k", 10000},
		{"1M", 1000000},
		{"1G", 1000000000},
		{"1T", 1000000000000},
		{"1P", 1000000000000000},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()

			got, err := Parse(tt.input)
			require.NoError(t, err)
			require.Equal(t, tt.expected, got)
		})
	}
}

func TestParseErrors(t *testing.T) {
	t.Parallel()

	tests := []string{
		"",
		"abc",
		"1.5Gi",
		"Mi",
		"1Xi",
	}

	for _, input := range tests {
		t.Run(input, func(t *testing.T) {
			t.Parallel()

			_, err := Parse(input)
			require.Error(t, err)
		})
	}
}

func TestString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		value    ByteSize
		expected string
	}{
		{0, "0"},
		{1, "1"},
		{512, "512"},
		{1024, "1Ki"},
		{1024 * 1024, "1Mi"},
		{64 * 1024 * 1024, "64Mi"},
		{1024 * 1024 * 1024, "1Gi"},
		{10 * 1024 * 1024 * 1024, "10Gi"},
		{1536, "1536"}, // 1.5Ki — not exact, stays as bytes
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.expected, tt.value.String())
		})
	}
}

func TestRoundTrip(t *testing.T) {
	t.Parallel()

	inputs := []string{"0", "1Ki", "256Mi", "1Gi", "10Gi"}
	for _, input := range inputs {
		t.Run(input, func(t *testing.T) {
			t.Parallel()

			parsed, err := Parse(input)
			require.NoError(t, err)
			require.Equal(t, input, parsed.String())
		})
	}
}
