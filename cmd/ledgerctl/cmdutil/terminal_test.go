package cmdutil

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTerminalSafe(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		in   string
		want string
	}{
		{"plain", "Pay out a user", "Pay out a user"},
		{"unicode", "Payé — Straße", "Payé — Straße"},
		{"csi", "\x1b[2Jcleared", `\x1b[2Jcleared`},
		{"osc 52", "\x1b]52;c;cHduZWQ=\x07", `\x1b]52;c;cHduZWQ=\a`},
		{"newline and tab", "line1\nline2\tend", `line1\nline2\tend`},
		{"carriage return", "real\rfake", `real\rfake`},
		{"del and c1", "a\x7fb\u009bc", `a\x7fb\u009bc`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, TerminalSafe(tc.in))
		})
	}
}
