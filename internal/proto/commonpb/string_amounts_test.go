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

// newTestTransaction returns a transaction whose single posting carries an
// amount above the JavaScript integer limit, so any accidental float round-trip
// anywhere in the chain fails the test rather than passing with a plausible
// number.
func newTestTransaction(t *testing.T) *Transaction {
	t.Helper()

	amount, ok := new(big.Int).SetString(aboveJSNumberLimit, 10)
	require.True(t, ok)

	return &Transaction{
		Id:        7,
		Reference: "ref-1",
		Postings: []*Posting{
			NewPosting("world", "alice", "USD/2", amount),
		},
	}
}

// requireOnlyAmountsDiffer decodes both bodies and asserts they are structurally
// identical once every postings[].amount is normalised to its decimal string.
// This is the load-bearing guarantee of EN-1779: the opt-in wire may differ from
// the default wire at posting amounts and nowhere else.
//
// Caveat: the repo json wrapper (internal/adapter/json, backed by sonic) decodes
// numbers into `any` as float64 with no UseNumber-style option, which is lossy
// above 2^53 - confirmed empirically: 9007199254740993 decodes as
// 9007199254740992. So this helper alone cannot validate exactness above the
// limit; callers that need the differential check must use an amount below
// 2^53, and assert the above-2^53 digit-for-digit behaviour separately via
// require.Contains against the raw wire bytes.
func requireOnlyAmountsDiffer(t *testing.T, defaultBody, stringBody []byte) {
	t.Helper()

	var a, b any

	require.NoError(t, json.Unmarshal(defaultBody, &a))
	require.NoError(t, json.Unmarshal(stringBody, &b))

	require.Equal(t, normalizeAmounts(a), normalizeAmounts(b))
}

// normalizeAmounts walks a decoded JSON tree and rewrites every "amount" value
// to its decimal string, so a number and its quoted form compare equal.
func normalizeAmounts(v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, val := range t {
			if k == "amount" {
				out[k] = amountToString(val)

				continue
			}

			out[k] = normalizeAmounts(val)
		}

		return out
	case []any:
		out := make([]any, 0, len(t))
		for _, val := range t {
			out = append(out, normalizeAmounts(val))
		}

		return out
	default:
		return v
	}
}

func amountToString(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case float64:
		return new(big.Float).SetFloat64(t).Text('f', 0)
	default:
		return ""
	}
}

func TestTransaction_StringAmountDiffersOnlyAtAmount(t *testing.T) {
	t.Parallel()

	tx := newTestTransaction(t)

	def, err := json.Marshal(tx)
	require.NoError(t, err)

	str, err := json.Marshal(StringAmountTransaction{Transaction: tx})
	require.NoError(t, err)

	require.NotEqual(t, string(def), string(str))
	require.Contains(t, string(str), `"amount":"`+aboveJSNumberLimit+`"`,
		"the opt-in wire must carry the above-2^53 amount digit for digit")
}

func TestTransaction_StringAmountOnlyChangesAmountShape(t *testing.T) {
	t.Parallel()

	// requireOnlyAmountsDiffer decodes numbers through float64 (see its doc
	// comment), which is lossy above 2^53. Use a small amount here so the
	// structural comparison itself is not defeated by float rounding; the
	// above-2^53 exactness is covered separately by
	// TestTransaction_StringAmountDiffersOnlyAtAmount.
	amount := big.NewInt(4200)
	tx := &Transaction{
		Id:        7,
		Reference: "ref-1",
		Postings: []*Posting{
			NewPosting("world", "alice", "USD/2", amount),
		},
	}

	def, err := json.Marshal(tx)
	require.NoError(t, err)

	str, err := json.Marshal(StringAmountTransaction{Transaction: tx})
	require.NoError(t, err)

	requireOnlyAmountsDiffer(t, def, str)
}

func TestTransaction_NilPostingsStillEmitsEmptyArray(t *testing.T) {
	t.Parallel()

	// postings has no omitempty and the schema types it non-nullable, so a nil
	// slice behind the `any` field must still marshal as [] and never null.
	tx := &Transaction{Id: 1}

	def, err := json.Marshal(tx)
	require.NoError(t, err)
	require.Contains(t, string(def), `"postings":[]`)

	str, err := json.Marshal(StringAmountTransaction{Transaction: tx})
	require.NoError(t, err)
	require.Contains(t, string(str), `"postings":[]`)
}

func TestTransaction_NilElementStaysNullInBothModes(t *testing.T) {
	t.Parallel()

	txs := []*Transaction{newTestTransaction(t), nil}

	def, err := json.Marshal(txs)
	require.NoError(t, err)

	str, err := json.Marshal(StringAmountTransactions(txs))
	require.NoError(t, err)

	require.Contains(t, string(def), `,null]`)
	require.Contains(t, string(str), `,null]`,
		"a nil element must stay null: the header may change amount formatting, never shape")
}

func TestTransactions_EmptyAndNilSliceMarshalAsArray(t *testing.T) {
	t.Parallel()

	for name, in := range map[string][]*Transaction{"nil slice": nil, "empty slice": {}} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got, err := json.Marshal(StringAmountTransactions(in))
			require.NoError(t, err)
			require.Equal(t, "[]", string(got))
		})
	}
}
