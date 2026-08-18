package commonpb

import (
	"bytes"
	"math/big"
	"strconv"
	"testing"
	libtime "time"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/go-libs/v5/pkg/types/time"

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

// newTestLog wraps a ledger-log payload in the full five-level chain a log
// route actually serves: Log -> LogPayload_Apply -> ApplyLedgerLog ->
// LedgerLog -> LedgerLogPayload. Every level must forward the mode for the
// amount at the bottom to come out quoted.
func newTestLog(t *testing.T, payload *LedgerLogPayload) *Log {
	t.Helper()

	return &Log{
		Sequence: 5,
		Receipt:  "receipt-1",
		Payload: &LogPayload{
			Type: &LogPayload_Apply{
				Apply: &ApplyLedgerLog{
					LedgerName: "ledger0",
					Log: &LedgerLog{
						Id:   9,
						Data: payload,
					},
				},
			},
		},
	}
}

func TestLog_StringAmountReachesPostingThroughCreatedTransaction(t *testing.T) {
	t.Parallel()

	amount, ok := new(big.Int).SetString(aboveJSNumberLimit, 10)
	require.True(t, ok)

	log := newTestLog(t, &LedgerLogPayload{
		Payload: &LedgerLogPayload_CreatedTransaction{
			CreatedTransaction: &CreatedTransaction{Transaction: newTestTransaction(t, amount)},
		},
	})

	def, err := json.Marshal(log)
	require.NoError(t, err)
	require.Contains(t, string(def), `"amount":`+aboveJSNumberLimit,
		"the default wire must stay the bare number at full depth")

	str, err := json.Marshal(StringAmountLog{Log: log})
	require.NoError(t, err)
	require.Contains(t, string(str), `"amount":"`+aboveJSNumberLimit+`"`,
		"the mode must survive all five log levels down to the posting")
}

func TestLog_StringAmountReachesPostingThroughRevertedTransaction(t *testing.T) {
	t.Parallel()

	amount, ok := new(big.Int).SetString(aboveJSNumberLimit, 10)
	require.True(t, ok)

	log := newTestLog(t, &LedgerLogPayload{
		Payload: &LedgerLogPayload_RevertedTransaction{
			RevertedTransaction: &RevertedTransaction{
				RevertedTransactionId: 3,
				RevertTransaction:     newTestTransaction(t, amount),
			},
		},
	})

	def, err := json.Marshal(log)
	require.NoError(t, err)
	require.Contains(t, string(def), `"amount":`+aboveJSNumberLimit,
		"the default wire must stay the bare number at full depth")

	str, err := json.Marshal(StringAmountLog{Log: log})
	require.NoError(t, err)
	require.Contains(t, string(str), `"amount":"`+aboveJSNumberLimit+`"`,
		"the mode must survive all five log levels down to the posting")
}

func TestLog_StringAmountOnlyChangesAmountShapeAtFullDepth(t *testing.T) {
	t.Parallel()

	// A small amount: requireOnlyAmountsDiffer decodes numbers through float64
	// (see its doc comment), which is lossy above 2^53. Exactness above the
	// limit is covered by the two reach tests above.
	log := newTestLog(t, &LedgerLogPayload{
		Payload: &LedgerLogPayload_CreatedTransaction{
			CreatedTransaction: &CreatedTransaction{Transaction: newTestTransaction(t, big.NewInt(4200))},
		},
	})

	def, err := json.Marshal(log)
	require.NoError(t, err)

	str, err := json.Marshal(StringAmountLog{Log: log})
	require.NoError(t, err)

	requireOnlyAmountsDiffer(t, def, str)
}

func TestLog_NonPostingPayloadIsByteIdentical(t *testing.T) {
	t.Parallel()

	// The opt-in path rebuilds only the posting-bearing variants; every other
	// variant delegates to MarshalJSON. Assert byte equality rather than
	// structural equality: this is the check that catches a hand-rolled variant
	// struct drifting away from the delegated output.
	testCases := map[string]*Log{
		"ledger-level variant carrying no postings": {
			Sequence: 2,
			Payload: &LogPayload{
				Type: &LogPayload_DeleteLedger{
					DeleteLedger: &DeletedLedgerLog{Name: "ledger0"},
				},
			},
		},
		"ledger-log payload variant carrying no postings": newTestLog(t, &LedgerLogPayload{
			Payload: &LedgerLogPayload_SavedMetadata{
				SavedMetadata: &SavedMetadata{
					Target: &Target{Target: &Target_TransactionId{TransactionId: 7}},
				},
			},
		}),
	}

	for name, log := range testCases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			def, err := json.Marshal(log)
			require.NoError(t, err)

			str, err := json.Marshal(StringAmountLog{Log: log})
			require.NoError(t, err)

			require.Equal(t, string(def), string(str),
				"a payload carrying no amount must be byte-identical in both modes")
		})
	}
}

func TestLog_NilPayloadStaysOmittedInBothModes(t *testing.T) {
	t.Parallel()

	// Log.payload is `omitempty`. Retyping it to `any` would make a nil payload
	// emit `null` unless the childValue guard holds.
	log := &Log{Sequence: 1}

	def, err := json.Marshal(log)
	require.NoError(t, err)
	require.NotContains(t, string(def), "payload")

	str, err := json.Marshal(StringAmountLog{Log: log})
	require.NoError(t, err)
	require.NotContains(t, string(str), "payload")
}

func TestApplyLedgerLog_NilLogStaysOmittedInBothModes(t *testing.T) {
	t.Parallel()

	// ApplyLedgerLog.log is `omitempty`, same guard as Log.payload.
	apply := &ApplyLedgerLog{LedgerName: "ledger0"}

	def, err := json.Marshal(apply)
	require.NoError(t, err)
	require.NotContains(t, string(def), `"log"`)

	str, err := json.Marshal(stringAmountApplyLedgerLog{ApplyLedgerLog: apply})
	require.NoError(t, err)
	require.NotContains(t, string(str), `"log"`)
}

func TestLedgerLog_NilDataStaysNullInBothModes(t *testing.T) {
	t.Parallel()

	// LedgerLog.data has NO omitempty, so a nil payload renders as `null`
	// today. The retyped `any` field must keep emitting exactly that: the
	// childValue guard leaves a nil interface, which renders `null` as well.
	ledgerLog := &LedgerLog{}

	def, err := json.Marshal(ledgerLog)
	require.NoError(t, err)
	require.Contains(t, string(def), `"data":null`)

	str, err := json.Marshal(stringAmountLedgerLog{LedgerLog: ledgerLog})
	require.NoError(t, err)
	require.Contains(t, string(str), `"data":null`)

	require.Equal(t, string(def), string(str))
}

func TestLog_NilReceiverIsNull(t *testing.T) {
	t.Parallel()

	got, err := json.Marshal(StringAmountLog{Log: nil})
	require.NoError(t, err)
	require.Equal(t, "null", string(got))
}

func TestLogs_NilElementStaysNullInBothModes(t *testing.T) {
	t.Parallel()

	amount, ok := new(big.Int).SetString(aboveJSNumberLimit, 10)
	require.True(t, ok)

	logs := []*Log{
		newTestLog(t, &LedgerLogPayload{
			Payload: &LedgerLogPayload_CreatedTransaction{
				CreatedTransaction: &CreatedTransaction{Transaction: newTestTransaction(t, amount)},
			},
		}),
		nil,
	}

	def, err := json.Marshal(logs)
	require.NoError(t, err)
	require.Contains(t, string(def), `,null]`)

	str, err := json.Marshal(StringAmountLogs(logs))
	require.NoError(t, err)
	require.Contains(t, string(str), `,null]`,
		"a nil element must stay null: the header may change amount formatting, never shape")
	require.Contains(t, string(str), `"amount":"`+aboveJSNumberLimit+`"`)
}

func TestLogs_EmptyAndNilSliceMarshalAsArray(t *testing.T) {
	t.Parallel()

	for name, in := range map[string][]*Log{"nil slice": nil, "empty slice": {}} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got, err := json.Marshal(StringAmountLogs(in))
			require.NoError(t, err)
			require.Equal(t, "[]", string(got))
		})
	}
}

// newTestCursorAccount returns an account page entry. Nothing below Account
// carries a Uint256 or a bare JSON number, so this exists to prove an
// ACCOUNTS-target cursor is byte-identical in both modes.
func newTestCursorAccount(t *testing.T) *Account {
	t.Helper()

	return &Account{
		Address: "alice",
		Volumes: []*AccountVolume{
			{
				Asset: "USD/2",
				Volumes: &VolumesWithBalance{
					Input:   aboveJSNumberLimit,
					Output:  "0",
					Balance: aboveJSNumberLimit,
				},
			},
		},
	}
}

// newTestCursorLog wraps a transaction carrying the given amount in the full
// nine-level chain a LOGS-target cursor serves.
func newTestCursorLog(t *testing.T, amount *big.Int) *Log {
	t.Helper()

	return newTestLog(t, &LedgerLogPayload{
		Payload: &LedgerLogPayload_CreatedTransaction{
			CreatedTransaction: &CreatedTransaction{Transaction: newTestTransaction(t, amount)},
		},
	})
}

func TestPreparedQueryCursor_StringAmountReachesTransactionData(t *testing.T) {
	t.Parallel()

	amount, ok := new(big.Int).SetString(aboveJSNumberLimit, 10)
	require.True(t, ok)

	cursor := &PreparedQueryCursor{
		PageSize:        15,
		TransactionData: []*Transaction{newTestTransaction(t, amount)},
	}

	def, err := json.Marshal(cursor)
	require.NoError(t, err)
	require.Contains(t, string(def), `"amount":`+aboveJSNumberLimit,
		"the default cursor wire must stay an unquoted number")

	str, err := json.Marshal(StringAmountPreparedQueryCursor{PreparedQueryCursor: cursor})
	require.NoError(t, err)
	require.Contains(t, string(str), `"amount":"`+aboveJSNumberLimit+`"`,
		"the cursor must forward the mode into its transaction page")
}

func TestPreparedQueryCursor_StringAmountReachesLogData(t *testing.T) {
	t.Parallel()

	amount, ok := new(big.Int).SetString(aboveJSNumberLimit, 10)
	require.True(t, ok)

	cursor := &PreparedQueryCursor{
		PageSize: 15,
		LogData:  []*Log{newTestCursorLog(t, amount)},
	}

	def, err := json.Marshal(cursor)
	require.NoError(t, err)
	require.Contains(t, string(def), `"amount":`+aboveJSNumberLimit,
		"the default cursor wire must stay an unquoted number")

	str, err := json.Marshal(StringAmountPreparedQueryCursor{PreparedQueryCursor: cursor})
	require.NoError(t, err)
	require.Contains(t, string(str), `"amount":"`+aboveJSNumberLimit+`"`,
		"the cursor must forward the mode down the whole log chain")
}

func TestPreparedQueryCursor_StringAmountOnlyChangesAmountShape(t *testing.T) {
	t.Parallel()

	// A small amount: requireOnlyAmountsDiffer decodes numbers through float64
	// and is lossy above 2^53 (see its doc comment). The above-2^53 exactness
	// is covered by the two require.Contains tests above.
	cursors := map[string]*PreparedQueryCursor{
		"transactions target": {
			PageSize:        15,
			HasMore:         true,
			Next:            "cursor-next",
			TransactionData: []*Transaction{newTestTransaction(t, big.NewInt(4200))},
		},
		"logs target": {
			PageSize: 15,
			HasMore:  true,
			Next:     "cursor-next",
			LogData:  []*Log{newTestCursorLog(t, big.NewInt(4200))},
		},
	}

	for name, cursor := range cursors {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			def, err := json.Marshal(cursor)
			require.NoError(t, err)

			str, err := json.Marshal(StringAmountPreparedQueryCursor{PreparedQueryCursor: cursor})
			require.NoError(t, err)

			requireOnlyAmountsDiffer(t, def, str)
		})
	}
}

func TestPreparedQueryCursor_AccountsTargetIsByteIdenticalInBothModes(t *testing.T) {
	t.Parallel()

	cursor := &PreparedQueryCursor{
		PageSize:    15,
		HasMore:     true,
		Previous:    "cursor-previous",
		AccountData: []*Account{newTestCursorAccount(t)},
	}

	def, err := json.Marshal(cursor)
	require.NoError(t, err)

	str, err := json.Marshal(StringAmountPreparedQueryCursor{PreparedQueryCursor: cursor})
	require.NoError(t, err)

	require.Equal(t, string(def), string(str),
		"an accounts page carries no bare number: the opt-in wire must be byte-identical")

	// The two unpopulated data fields must stay absent, not become `[]`: this is
	// what the len()==0 guard in sliceValue buys over a `!= nil` check.
	for _, body := range []string{string(def), string(str)} {
		require.NotContains(t, body, "transactionData")
		require.NotContains(t, body, "logData")
	}
}

func TestPreparedQueryCursor_EmptyDataSliceStaysOmittedInBothModes(t *testing.T) {
	t.Parallel()

	cursors := map[string]struct {
		cursor  *PreparedQueryCursor
		absent  string
		present string
	}{
		"empty transaction page": {
			cursor:  &PreparedQueryCursor{PageSize: 15, TransactionData: []*Transaction{}},
			absent:  "transactionData",
			present: `{"pageSize":15,"hasMore":false}`,
		},
		"empty log page": {
			cursor:  &PreparedQueryCursor{PageSize: 15, LogData: []*Log{}},
			absent:  "logData",
			present: `{"pageSize":15,"hasMore":false}`,
		},
	}

	for name, tc := range cursors {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			def, err := json.Marshal(tc.cursor)
			require.NoError(t, err)

			str, err := json.Marshal(StringAmountPreparedQueryCursor{PreparedQueryCursor: tc.cursor})
			require.NoError(t, err)

			require.NotContains(t, string(def), tc.absent)
			require.NotContains(t, string(str), tc.absent,
				"an empty non-nil page must stay omitted, never render as []")
			require.JSONEq(t, tc.present, string(def))
			require.JSONEq(t, tc.present, string(str))
		})
	}
}

func TestPreparedQueryCursor_DrainedCursorIsByteIdenticalInBothModes(t *testing.T) {
	t.Parallel()

	// emptyListResponse (internal/query/executor.go) builds exactly this: a page
	// size and nothing else.
	cursor := &PreparedQueryCursor{PageSize: 15}

	def, err := json.Marshal(cursor)
	require.NoError(t, err)
	require.JSONEq(t, `{"pageSize":15,"hasMore":false}`, string(def))

	str, err := json.Marshal(StringAmountPreparedQueryCursor{PreparedQueryCursor: cursor})
	require.NoError(t, err)
	require.Equal(t, string(def), string(str))
}

func TestPreparedQueryCursor_NilReceiverIsNull(t *testing.T) {
	t.Parallel()

	got, err := json.Marshal(StringAmountPreparedQueryCursor{PreparedQueryCursor: nil})
	require.NoError(t, err)
	require.Equal(t, "null", string(got))
}

// --- EN-1779 default-wire golden gate ---------------------------------------
//
// TestDefaultWireGolden pins the DEFAULT wire byte for byte for every
// marshaller EN-1779 rewrote.
//
// A FAILURE HERE MEANS THE CHANGE IS WRONG. These bytes are what every client
// that does not send Formance-Bigint-As-String already receives, and every
// expectation below was verified against the pre-change base branch. Never edit
// a `want` value to make a new implementation pass: fix the implementation, or
// escalate if the wire genuinely has to move.
//
// Two encoders are asserted because they disagree, and only one of them is
// production:
//   - json.Marshal is sonic ConfigDefault. It does NOT escape HTML, so a
//     golden captured with it alone would not describe what clients receive.
//   - json.MarshalWrite is sonic ConfigStd, which is what writeJSONResponse
//     (internal/adapter/http/response.go) serves: it escapes `<`, `>` and `&`,
//     and appends a trailing newline that is part of the response body.
//
// Every fixture carries at most ONE metadata key. sonic does not sort map keys
// in either config, so a byte comparison over two or more keys would flake on
// iteration order.

// goldenAmount is aboveJSNumberLimit as a uint64. Fixtures build their amount
// from it while the expectations concatenate aboveJSNumberLimit, and
// TestDefaultWireGolden asserts the two agree, so neither can drift alone.
const goldenAmount uint64 = 9007199254740993

// goldenDate is the single instant every golden fixture carries. RFC3339Nano
// drops the fractional part at whole seconds, so the rendered form is stable.
const goldenDate = "2026-08-18T10:00:00Z"

// goldenPostingJSON is the pinned default wire of goldenPosting.
const goldenPostingJSON = `{"source":"world","destination":"alice","amount":` +
	aboveJSNumberLimit + `,"asset":"USD/2","color":"red"}`

// goldenTransactionJSON is the pinned default wire of goldenTransaction. One
// line per field, in the order Transaction.buildAux declares them: that order
// is part of the wire contract, not an implementation detail.
//
// The `weight` metadata value is deliberately 2^64-1 and renders as a bare JSON
// number. Metadata is the counter-example to "nothing below Account carries a
// bare number", and it is unaffected by the header: only amounts move.
const goldenTransactionJSON = `{"postings":[` + goldenPostingJSON + `]` +
	`,"metadata":{"weight":18446744073709551615}` +
	`,"timestamp":"` + goldenDate + `"` +
	`,"reference":"ref-1"` +
	`,"id":42` +
	`,"insertedAt":"` + goldenDate + `"` +
	`,"updatedAt":"` + goldenDate + `"` +
	`,"revertedAt":"` + goldenDate + `"` +
	`,"revertedByTransactionId":43` +
	`,"revertsTransactionId":41` +
	`,"reverted":true` +
	`,"postCommitVolumes":{"world":[{"asset":"USD/2","color":"","input":"0"` +
	`,"output":"` + aboveJSNumberLimit + `"}]}}`

// goldenCreatedTransactionJSON is the pinned default wire of
// goldenCreatedTransaction.
const goldenCreatedTransactionJSON = `{"transaction":` + goldenTransactionJSON +
	`,"accountMetadata":{"alice":{"tier":"gold"}}` +
	`,"chapterId":3}`

// goldenLedgerLogJSON is the pinned default wire of goldenLedgerLog.
const goldenLedgerLogJSON = `{"type":"NEW_TRANSACTION"` +
	`,"data":{"createdTransaction":` + goldenCreatedTransactionJSON + `}` +
	`,"date":"` + goldenDate + `"` +
	`,"id":9}`

// goldenApplyLedgerLogJSON is the pinned default wire of goldenApplyLedgerLog.
const goldenApplyLedgerLogJSON = `{"ledgerName":"ledger0","log":` + goldenLedgerLogJSON + `}`

// goldenLogJSON is the pinned default wire of goldenLog.
//
// responseSignature is `{}` and not absent even though the log carries no
// signature: Log.buildAux fills it through protoFieldJSON, whose `msg == nil`
// guard cannot see a typed nil behind the proto.Message interface, so
// protojson renders the nil message as `{}` and `omitempty` keeps a two-byte
// value. That is pre-existing behaviour on both sides of EN-1779 (the base
// branch builds the field the same way), so it is pinned here rather than
// quietly corrected: changing it would move the default wire.
const goldenLogJSON = `{"sequence":5` +
	`,"payload":{"apply":` + goldenApplyLedgerLogJSON + `}` +
	`,"receipt":"receipt-1"` +
	`,"responseSignature":{}}`

// goldenTimestamp returns goldenDate as the proto Timestamp the payload types
// carry.
func goldenTimestamp() *Timestamp {
	return NewTimestamp(time.New(libtime.Date(2026, 8, 18, 10, 0, 0, 0, libtime.UTC)))
}

// goldenPosting returns the posting every golden fixture carries: one
// above-2^53 amount, so the wire is pinned exactly where a JavaScript client
// would truncate it.
func goldenPosting() *Posting {
	return &Posting{
		Source:      "world",
		Destination: "alice",
		Amount:      NewUint256FromUint64(goldenAmount),
		Asset:       "USD/2",
		Color:       "red",
	}
}

// goldenTransaction returns a transaction with every optional field populated,
// so the golden pins the position of each one rather than only the postings.
func goldenTransaction() *Transaction {
	at := goldenTimestamp()

	return &Transaction{
		Postings:              []*Posting{goldenPosting()},
		Metadata:              map[string]*MetadataValue{"weight": NewUintValue(18446744073709551615)},
		Timestamp:             at,
		Reference:             "ref-1",
		Id:                    42,
		InsertedAt:            at,
		UpdatedAt:             at,
		RevertedAt:            at,
		RevertedByTransaction: 43,
		RevertsTransaction:    41,
		Reverted:              true,
		PostCommitVolumes: &PostCommitVolumes{
			VolumesByAccount: map[string]*VolumesByAssets{
				"world": {
					Volumes: []*VolumeEntry{
						{Asset: "USD/2", Volumes: &Volumes{Input: "0", Output: aboveJSNumberLimit}},
					},
				},
			},
		},
	}
}

// goldenCreatedTransaction returns the created-transaction payload the log
// chain carries.
func goldenCreatedTransaction() *CreatedTransaction {
	return &CreatedTransaction{
		Transaction: goldenTransaction(),
		AccountMetadata: map[string]*MetadataMap{
			"alice": {Values: map[string]*MetadataValue{"tier": NewStringValue("gold")}},
		},
		ChapterId: 3,
	}
}

// goldenLedgerLog returns the ledger log wrapping goldenCreatedTransaction.
func goldenLedgerLog() *LedgerLog {
	return &LedgerLog{
		Id:   9,
		Date: goldenTimestamp(),
		Data: &LedgerLogPayload{
			Payload: &LedgerLogPayload_CreatedTransaction{
				CreatedTransaction: goldenCreatedTransaction(),
			},
		},
	}
}

// goldenApplyLedgerLog returns the apply payload wrapping goldenLedgerLog.
func goldenApplyLedgerLog() *ApplyLedgerLog {
	return &ApplyLedgerLog{LedgerName: "ledger0", Log: goldenLedgerLog()}
}

// goldenLog returns the nine-level payload a logs response serves: Log ->
// LogPayload_Apply -> ApplyLedgerLog -> LedgerLog ->
// LedgerLogPayload_CreatedTransaction -> CreatedTransaction -> Transaction ->
// Posting -> amount.
func goldenLog() *Log {
	return &Log{
		Sequence: 5,
		Payload: &LogPayload{
			Type: &LogPayload_Apply{Apply: goldenApplyLedgerLog()},
		},
		Receipt: "receipt-1",
	}
}

// defaultWireGolden pins one payload to the exact bytes the default wire must
// produce. wantProduction is the json.MarshalWrite (ConfigStd) form without its
// trailing newline; leave it empty when ConfigStd cannot differ from
// ConfigDefault, which is every fixture carrying no `<`, `>` or `&`.
type defaultWireGolden struct {
	value          any
	want           string
	wantProduction string
}

// defaultWireGoldens returns the pinned default wire of every marshaller
// EN-1779 rewrote, plus the omitted-field and empty-collection cases whose
// shape the opt-in mechanism could otherwise have changed unnoticed.
func defaultWireGoldens() map[string]defaultWireGolden {
	return map[string]defaultWireGolden{
		"posting": {
			value: goldenPosting(),
			want:  goldenPostingJSON,
		},
		"posting without an amount": {
			// amount is `omitempty` and typed `any`, so it must stay absent
			// rather than start emitting null.
			value: &Posting{Source: "world", Destination: "alice", Asset: "USD/2"},
			want:  `{"source":"world","destination":"alice","asset":"USD/2","color":""}`,
		},
		"posting with an HTML-escapable account address": {
			// The only fixture where the two encoders diverge, and the reason
			// the golden cannot be captured with json.Marshal alone.
			value: &Posting{
				Source:      "orders:<a&b>",
				Destination: "alice",
				Amount:      NewUint256FromUint64(1),
				Asset:       "USD/2",
			},
			want: `{"source":"orders:<a&b>","destination":"alice"` +
				`,"amount":1,"asset":"USD/2","color":""}`,
			wantProduction: `{"source":"orders:\u003ca\u0026b\u003e","destination":"alice"` +
				`,"amount":1,"asset":"USD/2","color":""}`,
		},
		"transaction": {
			value: goldenTransaction(),
			want:  goldenTransactionJSON,
		},
		"transaction with nil postings": {
			// postings has no `omitempty` and the OpenAPI schema types it as a
			// non-nullable required array, so a posting-less transaction renders
			// `[]` and never null.
			value: &Transaction{Id: 7},
			want:  `{"postings":[],"metadata":{},"id":7,"reverted":false}`,
		},
		"created transaction": {
			value: goldenCreatedTransaction(),
			want:  goldenCreatedTransactionJSON,
		},
		"created transaction without a transaction": {
			// transaction is an `omitempty` pointer retyped to `any`: absent,
			// not null.
			value: &CreatedTransaction{ChapterId: 3},
			want:  `{"chapterId":3}`,
		},
		"reverted transaction": {
			value: &RevertedTransaction{RevertedTransactionId: 3, RevertTransaction: goldenTransaction()},
			want:  `{"revertedTransactionId":3,"revertTransaction":` + goldenTransactionJSON + `}`,
		},
		"reverted transaction without a revert transaction": {
			// revertTransaction is an `omitempty` pointer retyped to `any`:
			// absent, not null.
			value: &RevertedTransaction{RevertedTransactionId: 3},
			want:  `{"revertedTransactionId":3}`,
		},
		"ledger log payload carrying a created transaction": {
			value: goldenLedgerLog().GetData(),
			want:  `{"createdTransaction":` + goldenCreatedTransactionJSON + `}`,
		},
		"ledger log payload carrying no amount": {
			// The oneof variants that carry no amount are deliberately not
			// re-listed in marshalStringAmounts, so this pins the branch both
			// modes share.
			value: &LedgerLogPayload{
				Payload: &LedgerLogPayload_SavedMetadata{
					SavedMetadata: &SavedMetadata{
						Target:   &Target{Target: &Target_TransactionId{TransactionId: 7}},
						Metadata: map[string]*MetadataValue{"tier": NewStringValue("gold")},
					},
				},
			},
			want: `{"savedMetadata":{"targetType":"TRANSACTION","transactionId":7` +
				`,"metadata":{"tier":"gold"}}}`,
		},
		"ledger log": {
			value: goldenLedgerLog(),
			want:  goldenLedgerLogJSON,
		},
		"apply ledger log": {
			value: goldenApplyLedgerLog(),
			want:  goldenApplyLedgerLogJSON,
		},
		"log payload": {
			value: goldenLog().GetPayload(),
			want:  `{"apply":` + goldenApplyLedgerLogJSON + `}`,
		},
		"log at full depth": {
			value: goldenLog(),
			want:  goldenLogJSON,
		},
		"prepared query cursor drained": {
			// The shape emptyListResponse (internal/query/executor.go) produces.
			value: &PreparedQueryCursor{PageSize: 15},
			want:  `{"pageSize":15,"hasMore":false}`,
		},
		"prepared query cursor on accounts": {
			value: &PreparedQueryCursor{
				PageSize: 15,
				HasMore:  true,
				Previous: "prev-token",
				Next:     "next-token",
				AccountData: []*Account{
					{
						Address: "alice",
						Volumes: []*AccountVolume{
							{
								Asset: "USD/2",
								Volumes: &VolumesWithBalance{
									Input:   aboveJSNumberLimit,
									Output:  "0",
									Balance: aboveJSNumberLimit,
								},
							},
						},
					},
				},
			},
			want: `{"pageSize":15,"hasMore":true,"previous":"prev-token","next":"next-token"` +
				`,"accountData":[{"address":"alice","volumes":[{"asset":"USD/2","color":""` +
				`,"volumes":{"input":"` + aboveJSNumberLimit + `","output":"0"` +
				`,"balance":"` + aboveJSNumberLimit + `"}}]}]}`,
		},
		"prepared query cursor on transactions": {
			value: &PreparedQueryCursor{PageSize: 15, TransactionData: []*Transaction{goldenTransaction()}},
			want:  `{"pageSize":15,"hasMore":false,"transactionData":[` + goldenTransactionJSON + `]}`,
		},
		"prepared query cursor on logs": {
			value: &PreparedQueryCursor{PageSize: 15, LogData: []*Log{goldenLog()}},
			want:  `{"pageSize":15,"hasMore":false,"logData":[` + goldenLogJSON + `]}`,
		},
	}
}

// TestDefaultWireGolden asserts the default wire is byte-identical to what it
// was before EN-1779 rewrote the marshaller chain. Read the note above
// defaultWireGolden before changing anything here: a failure means the
// implementation moved the wire, not that the expectation is stale.
func TestDefaultWireGolden(t *testing.T) {
	t.Parallel()

	require.Equal(t, aboveJSNumberLimit, strconv.FormatUint(goldenAmount, 10),
		"the fixture amount and the expectation constant must be the same number")

	for name, tc := range defaultWireGoldens() {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got, err := json.Marshal(tc.value)
			require.NoError(t, err)
			require.Equal(t, tc.want, string(got),
				"the default wire moved: fix the implementation, never this expectation")

			wantProduction := tc.want
			if tc.wantProduction != "" {
				wantProduction = tc.wantProduction
			}

			var buf bytes.Buffer

			require.NoError(t, json.MarshalWrite(&buf, tc.value))
			// The trailing newline is appended by sonic's Encoder, so it is part
			// of the body writeJSONResponse sends.
			require.Equal(t, wantProduction+"\n", buf.String(),
				"the production wire moved: fix the implementation, never this expectation")
		})
	}
}
