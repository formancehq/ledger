package filterexpr

import (
	"testing"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
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
	// Bare, target-aware fields (EN-1549): resolved differently per target.
	f.Add(`timestamp >= "2023-11-14T22:13:20Z"`)
	f.Add(`ledger == main`)
	f.Add(`outcome == failure`)
	f.Add(`seq between 1000 and 2000`)
	f.Add(`order_type in (create_transaction, revert_transaction)`)
	f.Add(`proposal_id == 42`)
	f.Add(`outcome == failure and ledger == main`)

	f.Fuzz(func(t *testing.T, input string) {
		// Parse must not panic on any input, on any target. Errors are expected
		// for invalid input; the audit target additionally exercises the bare
		// audit-field resolution path.
		for _, target := range []commonpb.QueryTarget{
			commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS,
			commonpb.QueryTarget_QUERY_TARGET_AUDIT,
		} {
			_, _ = Parse(input, target)
		}
	})
}
