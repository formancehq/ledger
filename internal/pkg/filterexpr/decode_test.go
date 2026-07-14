package filterexpr

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// TestDecodeDualFormat_Equivalence is the core EN-1511 acceptance check: the
// textual filterexpr form and the structured v2 JSON DSL form of the SAME
// logical filter decode to an identical *commonpb.QueryFilter.
func TestDecodeDualFormat_Equivalence(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		text   string
		json   string
		target commonpb.QueryTarget
	}{
		{
			name:   "metadata match",
			text:   `metadata[status] == "active"`,
			json:   `{"$match":{"metadata[status]":"active"}}`,
			target: commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
		},
		{
			name:   "address prefix",
			text:   `address ^= "users:"`,
			json:   `{"$match":{"address":"users:"}}`,
			target: commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
		},
		{
			// Values are non-numeric strings on purpose: the textual grammar
			// coerces a bare/quoted numeric metadata value to an IntCondition, while
			// the JSON $match value stays a StringCondition, so the two forms only
			// decode byte-for-byte identically for genuinely string-typed values.
			name:   "and of two conditions",
			text:   `metadata[a] == "x" and metadata[b] == "y"`,
			json:   `{"$and":[{"$match":{"metadata[a]":"x"}},{"$match":{"metadata[b]":"y"}}]}`,
			target: commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS,
		},
		{
			name:   "or of two conditions",
			text:   `metadata[a] == "x" or metadata[b] == "y"`,
			json:   `{"$or":[{"$match":{"metadata[a]":"x"}},{"$match":{"metadata[b]":"y"}}]}`,
			target: commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS,
		},
		{
			name:   "ledger condition on logs",
			text:   `ledger == "main"`,
			json:   `{"$match":{"ledger":"main"}}`,
			target: commonpb.QueryTarget_QUERY_TARGET_LOGS,
		},
		{
			// EN-1544: the textual `date` field and the structured `date` bound both
			// coerce the same RFC3339 string to a log date condition.
			name:   "date RFC3339 on logs",
			text:   `date >= "2023-11-14T22:13:20Z"`,
			json:   `{"$gte":{"date":"2023-11-14T22:13:20Z"}}`,
			target: commonpb.QueryTarget_QUERY_TARGET_LOGS,
		},
		{
			// EN-1544: the textual `timestamp` field and the structured `timestamp`
			// bound both coerce the same RFC3339 string to a tx timestamp condition.
			name:   "timestamp RFC3339 on transactions",
			text:   `timestamp >= "2023-11-14T22:13:20Z"`,
			json:   `{"$gte":{"timestamp":"2023-11-14T22:13:20Z"}}`,
			target: commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			fromText, err := DecodeDualFormat([]byte(tc.text), tc.target)
			require.NoError(t, err, "textual form must decode")
			require.NotNil(t, fromText)

			fromJSON, err := DecodeDualFormat([]byte(tc.json), tc.target)
			require.NoError(t, err, "structured form must decode")
			require.NotNil(t, fromJSON)

			require.True(t, proto.Equal(fromText, fromJSON),
				"text and JSON forms must decode to the same QueryFilter\n text: %v\n json: %v",
				fromText, fromJSON)
		})
	}
}

// TestDecodeDualFormat_DatePerTarget locks the per-target validity of the EN-1544
// date/timestamp fields: `date` compiles to a log condition (valid on LOGS only)
// and `timestamp` to a transaction condition (valid on TRANSACTIONS only). The
// gate runs on the decoded proto, so it applies to BOTH serializations
// identically — using the wrong target is a 400 regardless of the form used.
func TestDecodeDualFormat_DatePerTarget(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		raw     string
		target  commonpb.QueryTarget
		wantErr string // empty => must succeed
	}{
		{
			name:   "textual date on logs is valid",
			raw:    `date >= "2023-11-14T22:13:20Z"`,
			target: commonpb.QueryTarget_QUERY_TARGET_LOGS,
		},
		{
			name:   "structured date on logs is valid",
			raw:    `{"$gte":{"date":"2023-11-14T22:13:20Z"}}`,
			target: commonpb.QueryTarget_QUERY_TARGET_LOGS,
		},
		{
			name:    "textual date on transactions is rejected",
			raw:     `date >= "2023-11-14T22:13:20Z"`,
			target:  commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS,
			wantErr: "not valid on transactions",
		},
		{
			name:    "structured date on transactions is rejected",
			raw:     `{"$gte":{"date":"2023-11-14T22:13:20Z"}}`,
			target:  commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS,
			wantErr: "not valid on transactions",
		},
		{
			name:   "textual timestamp on transactions is valid",
			raw:    `timestamp >= "2023-11-14T22:13:20Z"`,
			target: commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS,
		},
		{
			name:    "textual timestamp on logs is rejected",
			raw:     `timestamp >= "2023-11-14T22:13:20Z"`,
			target:  commonpb.QueryTarget_QUERY_TARGET_LOGS,
			wantErr: "not valid on logs",
		},
		{
			name:    "structured timestamp on accounts is rejected",
			raw:     `{"$gte":{"timestamp":"2023-11-14T22:13:20Z"}}`,
			target:  commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
			wantErr: "not valid on accounts",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			filter, err := DecodeDualFormat([]byte(tc.raw), tc.target)
			if tc.wantErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.wantErr)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, filter)
		})
	}
}

// TestDecodeDualFormat_DateRejectsPreEpoch locks pre-epoch rejection at the DSL
// layer for both serializations (EN-1544), consistent with the transport-level
// rejection (EN-1542).
func TestDecodeDualFormat_DateRejectsPreEpoch(t *testing.T) {
	t.Parallel()

	cases := []struct {
		raw    string
		target commonpb.QueryTarget
	}{
		{`date >= "1969-12-31T00:00:00Z"`, commonpb.QueryTarget_QUERY_TARGET_LOGS},
		{`{"$gte":{"date":"1969-12-31T00:00:00Z"}}`, commonpb.QueryTarget_QUERY_TARGET_LOGS},
		{`timestamp >= "1969-12-31T00:00:00Z"`, commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS},
		{`{"$gte":{"timestamp":"1969-12-31T00:00:00Z"}}`, commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS},
	}

	for _, tc := range cases {
		_, err := DecodeDualFormat([]byte(tc.raw), tc.target)
		require.Error(t, err, tc.raw)
		require.Contains(t, err.Error(), "Unix epoch", tc.raw)
	}
}

// TestDecodeDualFormat_JSONQuotedText covers the body-field form where textual
// filterexpr is carried inside a JSON string ("filter": "metadata[k] == v").
func TestDecodeDualFormat_JSONQuotedText(t *testing.T) {
	t.Parallel()

	quoted := []byte(`"metadata[status] == \"active\""`)
	fromQuoted, err := DecodeDualFormat(quoted, commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS)
	require.NoError(t, err)
	require.NotNil(t, fromQuoted)

	fromBare, err := DecodeDualFormat([]byte(`metadata[status] == "active"`), commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS)
	require.NoError(t, err)

	require.True(t, proto.Equal(fromQuoted, fromBare),
		"JSON-quoted textual filter must decode identically to the bare textual form")
}

// TestDecodeDualFormat_Empty covers the no-filter cases: nil, empty, whitespace,
// JSON null, empty JSON string.
func TestDecodeDualFormat_Empty(t *testing.T) {
	t.Parallel()

	for _, raw := range []string{"", "   ", "null", `""`} {
		filter, err := DecodeDualFormat([]byte(raw), commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS)
		require.NoError(t, err, "empty-ish input %q must not error", raw)
		require.Nil(t, filter, "empty-ish input %q must yield a nil filter", raw)
	}

	filter, err := DecodeDualFormat(nil, commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS)
	require.NoError(t, err)
	require.Nil(t, filter)
}

// TestDecodeDualFormat_Malformed covers rejection of malformed input in each
// form, and rejection of a condition invalid on the given target.
func TestDecodeDualFormat_Malformed(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		raw     string
		target  commonpb.QueryTarget
		wantErr string
	}{
		{
			name:    "malformed textual",
			raw:     `metadata[k] ==`,
			target:  commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
			wantErr: "filter:",
		},
		{
			name:    "malformed json - unknown operator",
			raw:     `{"$bogus":{}}`,
			target:  commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
			wantErr: "unknown operator",
		},
		{
			name:    "malformed json - empty object",
			raw:     `{}`,
			target:  commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
			wantErr: "empty object",
		},
		{
			name:    "malformed json - not an object body",
			raw:     `{"$match":42}`,
			target:  commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
			wantErr: "filter:",
		},
		{
			// `ledger` is a valid textual condition but is only valid on LOGS/AUDIT
			// targets, so it is rejected on ACCOUNTS by the per-target gate.
			name:    "condition invalid on target (textual)",
			raw:     `ledger == "main"`,
			target:  commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
			wantErr: "not valid on accounts",
		},
		{
			name:    "condition invalid on target (json)",
			raw:     `{"$match":{"reference":"x"}}`,
			target:  commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
			wantErr: "not valid on accounts",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := DecodeDualFormat([]byte(tc.raw), tc.target)
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

// TestDecodeDualFormat_AuditTextOnly documents and locks the audit-arm decision:
// an audit condition parses only via the textual form; the structured JSON DSL
// has no representation for it and is rejected by the codec (EN-1241). Both flow
// through the single shared decoder.
func TestDecodeDualFormat_AuditTextOnly(t *testing.T) {
	t.Parallel()

	fromText, err := DecodeDualFormat([]byte(`audit[outcome] == failure`), commonpb.QueryTarget_QUERY_TARGET_AUDIT)
	require.NoError(t, err, "textual audit filter must decode")
	require.NotNil(t, fromText)
	require.IsType(t, &commonpb.QueryFilter_Audit{}, fromText.GetFilter())

	// There is no structured JSON representation of an audit condition — the codec
	// rejects any attempt to carry one (its field names collide with the
	// transaction/log conditions the JSON DSL already claims).
	_, err = DecodeDualFormat([]byte(`{"$match":{"outcome":"failure"}}`), commonpb.QueryTarget_QUERY_TARGET_AUDIT)
	require.Error(t, err, "audit has no structured JSON form; a structured filter must not decode to a valid audit condition")
}

// TestDecodeDualFormatStructuralOnly_SkipsTargetGate confirms the update-path
// variant decodes both forms but does NOT apply the per-target validity gate
// (the FSM applies it against the stored target).
func TestDecodeDualFormatStructuralOnly_SkipsTargetGate(t *testing.T) {
	t.Parallel()

	// `ledger` is invalid on ACCOUNTS, but the structural-only decoder does not
	// know the target and must accept it (the gate runs later against the stored
	// target) in both forms.
	fromText, err := DecodeDualFormatStructuralOnly([]byte(`ledger == "main"`))
	require.NoError(t, err)
	require.NotNil(t, fromText)

	fromJSON, err := DecodeDualFormatStructuralOnly([]byte(`{"$match":{"ledger":"main"}}`))
	require.NoError(t, err)
	require.NotNil(t, fromJSON)

	require.True(t, proto.Equal(fromText, fromJSON))
}
