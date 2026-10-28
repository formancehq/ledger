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
		{" 1Ki ", 1024},
		{"+1Ki", 1024},
		{"0Ei", 0},
		{"0E", 0},

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
		"9223372036854775808Ki",
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

// TestParseRejectsOverflowIntoDefault pins EN-2924: 16Ei must not become
// the zero sentinel used by server flags to select consumer defaults.
func TestParseRejectsOverflowIntoDefault(t *testing.T) {
	t.Parallel()
	_, err := Parse("16Ei")
	require.ErrorContains(t, err, "out of range")
}

func TestParseRejectsNegativeSizes(t *testing.T) {
	t.Parallel()
	for _, input := range []string{"-1", "-1Ki", "-1Ei", "-16Ei", "-1k", "-1E", "-9223372036854775808"} {
		t.Run(input, func(t *testing.T) {
			t.Parallel()
			_, err := Parse(input)
			require.ErrorContains(t, err, "must be non-negative")
		})
	}
}

func TestParseBoundaries(t *testing.T) {
	t.Parallel()
	tests := []struct {
		valid    string
		expected ByteSize
		overflow string
	}{
		{"9223372036854775807", 9223372036854775807, "9223372036854775808"},
		{"9007199254740991Ki", 9223372036854774784, "9007199254740992Ki"},
		{"8796093022207Mi", 9223372036853727232, "8796093022208Mi"},
		{"8589934591Gi", 9223372035781033984, "8589934592Gi"},
		{"8388607Ti", 9223370937343148032, "8388608Ti"},
		{"8191Pi", 9222246136947933184, "8192Pi"},
		{"7Ei", 8070450532247928832, "8Ei"},
		{"9223372036854775k", 9223372036854775000, "9223372036854776k"},
		{"9223372036854M", 9223372036854000000, "9223372036855M"},
		{"9223372036G", 9223372036000000000, "9223372037G"},
		{"9223372T", 9223372000000000000, "9223373T"},
		{"9223P", 9223000000000000000, "9224P"},
		{"9E", 9000000000000000000, "10E"},
	}
	for _, tt := range tests {
		t.Run(tt.valid, func(t *testing.T) {
			t.Parallel()
			got, err := Parse(tt.valid)
			require.NoError(t, err)
			require.Equal(t, tt.expected, got)
			_, err = Parse(tt.overflow)
			require.ErrorContains(t, err, "out of range")
		})
	}
}
