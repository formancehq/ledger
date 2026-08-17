package commonpb

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/adapter/json"
)

// aboveJSNumberLimit is 2^53 + 1, the smallest integer a JavaScript double
// cannot represent exactly. Every string-amount assertion uses it so the test
// fails if the value is ever routed through a float.
const aboveJSNumberLimit = "9007199254740993"

func TestPosting_DefaultWireIsUnquoted(t *testing.T) {
	t.Parallel()

	amount, ok := new(big.Int).SetString(aboveJSNumberLimit, 10)
	require.True(t, ok)

	p := NewPosting("world", "alice", "USD/2", amount)

	got, err := json.Marshal(p)
	require.NoError(t, err)

	require.JSONEq(t,
		`{"source":"world","destination":"alice","amount":`+aboveJSNumberLimit+`,"asset":"USD/2","color":""}`,
		string(got))
	require.Contains(t, string(got), `"amount":`+aboveJSNumberLimit,
		"default wire must carry the bare number with every digit intact")
}

func TestPosting_StringAmountWireIsQuoted(t *testing.T) {
	t.Parallel()

	amount, ok := new(big.Int).SetString(aboveJSNumberLimit, 10)
	require.True(t, ok)

	p := NewPosting("world", "alice", "USD/2", amount)

	got, err := json.Marshal(StringAmountPostings([]*Posting{p}))
	require.NoError(t, err)

	require.JSONEq(t,
		`[{"source":"world","destination":"alice","amount":"`+aboveJSNumberLimit+`","asset":"USD/2","color":""}]`,
		string(got))
}

func TestPosting_NilAmountStaysOmitted(t *testing.T) {
	t.Parallel()

	// The amount field is `omitempty`. Retyping it to `any` would make a nil
	// amount emit `null` unless the nil guard in buildAux holds, so assert the
	// absence explicitly in both modes.
	p := &Posting{Source: "world", Destination: "alice", Asset: "USD/2"}

	def, err := json.Marshal(p)
	require.NoError(t, err)
	require.NotContains(t, string(def), "amount")

	str, err := json.Marshal(StringAmountPostings([]*Posting{p}))
	require.NoError(t, err)
	require.NotContains(t, string(str), "amount")
}

func TestPosting_EmptySliceMarshalsAsArray(t *testing.T) {
	t.Parallel()

	// nil is the input that actually occurs in production: tx.GetPostings()
	// returns nil for a posting-less transaction, never an empty non-nil
	// slice. Both must still marshal as `[]`, not `null`.
	testCases := map[string][]*Posting{
		"nil slice":   nil,
		"empty slice": {},
	}

	for name, postings := range testCases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got, err := json.Marshal(StringAmountPostings(postings))
			require.NoError(t, err)
			require.Equal(t, "[]", string(got),
				"must not become null: Transaction.postings has no omitempty")
		})
	}
}

func TestPosting_NilElementStaysNullInBothModes(t *testing.T) {
	t.Parallel()

	// StringAmountPosting.buildAux is nil-receiver-safe via GetX() accessors,
	// so a nil embedded *Posting would silently produce a full zero-valued
	// object instead of null unless the wrapper's MarshalJSON guards for it.
	// The opt-in header must not change the shape of a nil element, only the
	// amount's formatting.
	amount, ok := new(big.Int).SetString(aboveJSNumberLimit, 10)
	require.True(t, ok)

	postings := []*Posting{NewPosting("world", "alice", "USD/2", amount), nil}

	def, err := json.Marshal(postings)
	require.NoError(t, err)
	require.JSONEq(t,
		`[{"source":"world","destination":"alice","amount":`+aboveJSNumberLimit+`,"asset":"USD/2","color":""},null]`,
		string(def))

	str, err := json.Marshal(StringAmountPostings(postings))
	require.NoError(t, err)
	require.JSONEq(t,
		`[{"source":"world","destination":"alice","amount":"`+aboveJSNumberLimit+`","asset":"USD/2","color":""},null]`,
		string(str))
}
