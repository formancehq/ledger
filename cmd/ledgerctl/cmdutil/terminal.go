package cmdutil

import (
	"strconv"
	"strings"
	"unicode"
)

// TerminalSafe renders server-supplied free text for human-readable output.
// Control characters, newline and tab included, are written as Go escapes
// (for example `\x1b`) so the value cannot drive the terminal or split a table
// row. Structured output keeps the original value.
func TerminalSafe(s string) string {
	var b strings.Builder

	for _, r := range s {
		if unicode.IsControl(r) {
			quoted := strconv.QuoteRune(r)
			b.WriteString(quoted[1 : len(quoted)-1])

			continue
		}

		b.WriteRune(r)
	}

	return b.String()
}
