package filterexpr

import (
	"testing"
)

// FuzzFilterExprParse fuzzes the filter expression DSL parser.
// This targets the participle-based parser with arbitrary strings looking for
// panics, infinite loops, or unexpected behavior on malformed input.
func FuzzFilterExprParse(f *testing.F) {
	// Seed corpus: valid expressions from the grammar.
	f.Add(`metadata[key] == "value"`)
	f.Add(`metadata[key] exists`)
	f.Add(`metadata[key] != "value"`)
	f.Add(`metadata[key] > 42`)
	f.Add(`metadata[key] >= 0`)
	f.Add(`metadata[key] < -1`)
	f.Add(`metadata[key] <= 999`)
	f.Add(`metadata[key] in ("a", "b", "c")`)
	f.Add(`address == "users:alice"`)
	f.Add(`address ^= "users:"`)
	f.Add(`source == "bank"`)
	f.Add(`destination == "users:bob"`)
	f.Add(`address in ("a", "b")`)
	f.Add(`ledger == "default"`)
	f.Add(`metadata[key] == $param`)
	f.Add(`metadata[key] == true`)
	f.Add(`metadata[key] == false`)
	f.Add(`metadata[key] == 42`)
	f.Add(`not metadata[key] exists`)
	f.Add(`metadata[a] == "x" and metadata[b] == "y"`)
	f.Add(`metadata[a] == "x" or metadata[b] == "y"`)
	f.Add(`(metadata[a] == "x" or metadata[b] == "y") and address == "z"`)
	f.Add(`not (metadata[a] == "x" and metadata[b] == "y")`)
	// Edge cases
	f.Add(``)
	f.Add(`(`)
	f.Add(`)`)
	f.Add(`metadata`)
	f.Add(`metadata[]`)
	f.Add(`metadata[key]`)
	f.Add(`and or not`)
	f.Add(`metadata[key] == ""`)
	f.Add(`metadata[key] == -9223372036854775808`)
	f.Add(`address == "a:b:c:d:e:f:g"`)

	f.Fuzz(func(t *testing.T, input string) {
		// Parse must not panic on any input.
		// Errors are expected for invalid input.
		_, _ = Parse(input)
	})
}
