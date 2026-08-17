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

// newTestTransaction returns a transaction whose single posting carries the
// given amount. Callers needing to exercise the JavaScript integer limit pass
// a value parsed from aboveJSNumberLimit; callers exercising
// requireOnlyAmountsDiffer must stay below it (see that helper's doc comment).
func newTestTransaction(t *testing.T, amount *big.Int) *Transaction {
	t.Helper()

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

	require.Equal(t, normalizeAmounts(t, a), normalizeAmounts(t, b))
}

// normalizeAmounts walks a decoded JSON tree and, inside every "postings"
// array it finds, rewrites each posting's "amount" value to its decimal
// string, so a number and its quoted form compare equal. The rewrite is
// scoped to postings[].amount rather than every "amount" key at any depth:
// the repository has other unrelated "amount" JSON keys (events, adapter/v2
// types, receipts), and a document embedding one of those must not have it
// silently normalised away by this test helper.
func normalizeAmounts(t *testing.T, v any) any {
	t.Helper()

	switch node := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(node))
		for k, val := range node {
			if k == "postings" {
				out[k] = normalizePostingsAmounts(t, val)

				continue
			}

			out[k] = normalizeAmounts(t, val)
		}

		return out
	case []any:
		out := make([]any, 0, len(node))
		for _, val := range node {
			out = append(out, normalizeAmounts(t, val))
		}

		return out
	default:
		return v
	}
}

// normalizePostingsAmounts rewrites the "amount" field of every posting in a
// decoded postings array to its decimal string. A posting element may be nil
// (decoded as a non-map value), which is passed through unchanged.
func normalizePostingsAmounts(t *testing.T, v any) any {
	t.Helper()

	postings, ok := v.([]any)
	if !ok {
		return v
	}

	out := make([]any, 0, len(postings))

	for _, posting := range postings {
		p, ok := posting.(map[string]any)
		if !ok {
			out = append(out, posting)

			continue
		}

		normalized := make(map[string]any, len(p))
		for k, val := range p {
			if k == "amount" {
				normalized[k] = amountToString(t, val)

				continue
			}

			normalized[k] = val
		}

		out = append(out, normalized)
	}

	return out
}

// amountToString renders a decoded "amount" value as a decimal string. It
// fails the test rather than masking a regression: a silent "" default for an
// unexpected type would let a future bug that turns amount into null or an
// object pass requireOnlyAmountsDiffer, because both sides would normalise to
// the same empty string.
func amountToString(t *testing.T, v any) string {
	t.Helper()

	switch n := v.(type) {
	case string:
		return n
	case float64:
		return new(big.Float).SetFloat64(n).Text('f', 0)
	default:
		t.Fatalf("amount decoded as unexpected type %T (%#v); expected string or number", v, v)

		return ""
	}
}

func TestTransaction_StringAmountKeepsAboveJSLimitExact(t *testing.T) {
	t.Parallel()

	amount, ok := new(big.Int).SetString(aboveJSNumberLimit, 10)
	require.True(t, ok)

	tx := newTestTransaction(t, amount)

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
	// TestTransaction_StringAmountKeepsAboveJSLimitExact.
	tx := newTestTransaction(t, big.NewInt(4200))

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

	amount, ok := new(big.Int).SetString(aboveJSNumberLimit, 10)
	require.True(t, ok)

	txs := []*Transaction{newTestTransaction(t, amount), nil}

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

func TestCreatedTransaction_StringAmountKeepsAboveJSLimitExact(t *testing.T) {
	t.Parallel()

	amount, ok := new(big.Int).SetString(aboveJSNumberLimit, 10)
	require.True(t, ok)

	ct := &CreatedTransaction{Transaction: newTestTransaction(t, amount)}

	def, err := json.Marshal(ct)
	require.NoError(t, err)

	str, err := json.Marshal(StringAmountCreatedTransaction{CreatedTransaction: ct})
	require.NoError(t, err)

	require.NotEqual(t, string(def), string(str))
	require.Contains(t, string(str), `"amount":"`+aboveJSNumberLimit+`"`,
		"the opt-in wire must carry the above-2^53 amount digit for digit")
}

func TestCreatedTransaction_StringAmountOnlyChangesAmountShape(t *testing.T) {
	t.Parallel()

	ct := &CreatedTransaction{Transaction: newTestTransaction(t, big.NewInt(4200))}

	def, err := json.Marshal(ct)
	require.NoError(t, err)

	str, err := json.Marshal(StringAmountCreatedTransaction{CreatedTransaction: ct})
	require.NoError(t, err)

	requireOnlyAmountsDiffer(t, def, str)
}

func TestCreatedTransaction_NilChildStaysOmittedInBothModes(t *testing.T) {
	t.Parallel()

	// Transaction is `omitempty`. Retyping it to `any` would make a nil
	// transaction emit `null` unless the nil guard holds, so assert the
	// absence explicitly in both modes. Asserting on the field name alone is
	// sufficient: the counterfactual `"transaction":null` output also
	// contains the field name, so this loses no detection power over also
	// asserting "null" separately.
	ct := &CreatedTransaction{ChapterId: 3}

	def, err := json.Marshal(ct)
	require.NoError(t, err)
	require.NotContains(t, string(def), "transaction")

	str, err := json.Marshal(StringAmountCreatedTransaction{CreatedTransaction: ct})
	require.NoError(t, err)
	require.NotContains(t, string(str), "transaction")
}

func TestCreatedTransaction_NilReceiverIsNull(t *testing.T) {
	t.Parallel()

	got, err := json.Marshal(StringAmountCreatedTransaction{CreatedTransaction: nil})
	require.NoError(t, err)
	require.Equal(t, "null", string(got))
}

func TestRevertedTransaction_StringAmountKeepsAboveJSLimitExact(t *testing.T) {
	t.Parallel()

	amount, ok := new(big.Int).SetString(aboveJSNumberLimit, 10)
	require.True(t, ok)

	rt := &RevertedTransaction{RevertTransaction: newTestTransaction(t, amount)}

	def, err := json.Marshal(rt)
	require.NoError(t, err)

	str, err := json.Marshal(StringAmountRevertedTransaction{RevertedTransaction: rt})
	require.NoError(t, err)

	require.NotEqual(t, string(def), string(str))
	require.Contains(t, string(str), `"amount":"`+aboveJSNumberLimit+`"`,
		"the opt-in wire must carry the above-2^53 amount digit for digit")
}

func TestRevertedTransaction_StringAmountOnlyChangesAmountShape(t *testing.T) {
	t.Parallel()

	rt := &RevertedTransaction{RevertTransaction: newTestTransaction(t, big.NewInt(4200))}

	def, err := json.Marshal(rt)
	require.NoError(t, err)

	str, err := json.Marshal(StringAmountRevertedTransaction{RevertedTransaction: rt})
	require.NoError(t, err)

	requireOnlyAmountsDiffer(t, def, str)
}

func TestRevertedTransaction_NilChildStaysOmittedInBothModes(t *testing.T) {
	t.Parallel()

	// RevertTransaction is `omitempty`. Retyping it to `any` would make a nil
	// transaction emit `null` unless the nil guard holds, so assert the
	// absence explicitly in both modes. Asserting on the field name alone is
	// sufficient: the counterfactual `"revertTransaction":null` output also
	// contains the field name, so this loses no detection power over also
	// asserting "null" separately.
	rt := &RevertedTransaction{RevertedTransactionId: 4}

	def, err := json.Marshal(rt)
	require.NoError(t, err)
	require.NotContains(t, string(def), "revertTransaction")

	str, err := json.Marshal(StringAmountRevertedTransaction{RevertedTransaction: rt})
	require.NoError(t, err)
	require.NotContains(t, string(str), "revertTransaction")
}

func TestRevertedTransaction_NilReceiverIsNull(t *testing.T) {
	t.Parallel()

	got, err := json.Marshal(StringAmountRevertedTransaction{RevertedTransaction: nil})
	require.NoError(t, err)
	require.Equal(t, "null", string(got))
}
