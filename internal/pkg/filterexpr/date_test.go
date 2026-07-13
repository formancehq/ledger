package filterexpr

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// 2023-11-14T22:13:20Z == 1_700_000_000 s == 1_700_000_000_000_000 µs.
const wantDateMicros = uint64(1_700_000_000_000_000)

func TestParseDate_LogRFC3339AndRawMatch(t *testing.T) {
	t.Parallel()

	// Both the RFC3339 string and the raw-microsecond form must compile to the
	// same log date condition.
	for _, in := range []string{
		`date >= "2023-11-14T22:13:20Z"`,
		"date >= 1700000000000000",
	} {
		filter, err := Parse(in)
		require.NoError(t, err, in)

		lc := filter.GetLogBuiltinUint()
		require.NotNil(t, lc, in)
		assert.Equal(t, commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE, lc.GetField(), in)
		assert.Equal(t, wantDateMicros, lc.GetCond().GetMin(), in)
		assert.False(t, lc.GetCond().GetMinExclusive(), in)
	}
}

func TestParseTimestamp_TxRFC3339AndRawMatch(t *testing.T) {
	t.Parallel()

	for _, in := range []string{
		`timestamp >= "2023-11-14T22:13:20Z"`,
		"timestamp >= 1700000000000000",
	} {
		filter, err := Parse(in)
		require.NoError(t, err, in)

		bc := filter.GetBuiltinUint()
		require.NotNil(t, bc, in)
		assert.Equal(t, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP, bc.GetField(), in)
		assert.Equal(t, wantDateMicros, bc.GetCond().GetMin(), in)
	}
}

// TestParseDate_ClosedRange asserts that an `and` of a lower and an upper bound
// on the same date field folds into a single closed LogBuiltinUintCondition,
// mirroring the JSON codec's foldRangeAnd. The fold is what lets Format emit the
// two exclusive comparison clauses and still round-trip to one condition.
func TestParseDate_ClosedRange(t *testing.T) {
	t.Parallel()

	filter, err := Parse(`date >= "2023-11-14T22:13:20Z" and date < "2023-11-15T22:13:20Z"`)
	require.NoError(t, err)

	// No enclosing $and: the two complementary bounds fold into one range.
	require.Nil(t, filter.GetAnd())

	lc := filter.GetLogBuiltinUint()
	require.NotNil(t, lc)
	assert.Equal(t, commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE, lc.GetField())
	assert.Equal(t, wantDateMicros, lc.GetCond().GetMin())
	assert.False(t, lc.GetCond().GetMinExclusive())
	assert.Equal(t, wantDateMicros+24*60*60*1_000_000, lc.GetCond().GetMax())
	assert.True(t, lc.GetCond().GetMaxExclusive())
}

// TestParseDate_FoldOnlyComplementaryBounds guards that the date-range fold fires
// only for exactly one lower + one upper bound on the same field. Non-foldable
// `and` combinations stay as an explicit AndFilter.
func TestParseDate_FoldOnlyComplementaryBounds(t *testing.T) {
	t.Parallel()

	cases := []string{
		// Two lower bounds (same direction) — not complementary.
		`date >= "2023-11-14T22:13:20Z" and date > "2023-11-13T22:13:20Z"`,
		// Different fields — date vs timestamp.
		`date >= "2023-11-14T22:13:20Z" and timestamp < "2023-11-15T22:13:20Z"`,
		// A closed range already (between) AND a further bound.
		`date between "2023-11-14T22:13:20Z" and "2023-11-16T22:13:20Z" and date < "2023-11-15T22:13:20Z"`,
	}

	for _, in := range cases {
		t.Run(in, func(t *testing.T) {
			t.Parallel()

			filter, err := Parse(in)
			require.NoError(t, err, in)
			require.NotNil(t, filter.GetAnd(), "expected an unfolded AndFilter for %q", in)
		})
	}
}

func TestParseDate_Between(t *testing.T) {
	t.Parallel()

	filter, err := Parse(`timestamp between "2023-11-14T22:13:20Z" and 1700086400000000`)
	require.NoError(t, err)

	bc := filter.GetBuiltinUint()
	require.NotNil(t, bc)
	assert.Equal(t, wantDateMicros, bc.GetCond().GetMin())
	assert.Equal(t, uint64(1700086400000000), bc.GetCond().GetMax())
}

// TestFormatDate_RoundTrip is the regression for the NumaryBot MINOR: Format had
// no case for QueryFilter_LogBuiltinUint / QueryFilter_BuiltinUint, so a textual
// date/timestamp filter rendered as "<unknown filter>" (prepared-query listings
// call Format). Date-semantic bounds render as quoted RFC3339 so they parse back
// to the same filter.
func TestFormatDate_RoundTrip(t *testing.T) {
	t.Parallel()

	cases := []struct {
		in   string
		want string
	}{
		{`date >= "2023-11-14T22:13:20Z"`, `date >= "2023-11-14T22:13:20Z"`},
		{`timestamp >= "2023-11-14T22:13:20Z"`, `timestamp >= "2023-11-14T22:13:20Z"`},
		{`timestamp == "2023-11-14T22:13:20Z"`, `timestamp == "2023-11-14T22:13:20Z"`},
		{`date < "2023-11-14T22:13:20Z"`, `date < "2023-11-14T22:13:20Z"`},
		// Raw micros normalize to the canonical quoted RFC3339 rendering.
		{`timestamp >= 1700000000000000`, `timestamp >= "2023-11-14T22:13:20Z"`},
	}

	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			t.Parallel()

			f, err := Parse(tc.in)
			require.NoError(t, err)

			got := Format(f)
			require.Equal(t, tc.want, got)

			// And the formatted string must parse back to an equivalent filter.
			reparsed, err := Parse(got)
			require.NoError(t, err)
			require.True(t, proto.Equal(f, reparsed),
				"Format output must round-trip\n first: %v\n reparsed: %v", f, reparsed)
		})
	}
}

// TestFormatDate_PreservesExclusiveClosedRange is the regression for the flemzord
// HIGH: a structured JSON `$and` of `$gt`/`$lt` on a date field folds into one
// UintCondition with both exclusive flags. Format used to emit `between`, which
// parses back inclusive on both ends — silently widening an exported prepared
// query at the boundaries. Format must now emit the two comparison clauses, which
// the parser folds back into the identical exclusive closed range.
func TestFormatDate_PreservesExclusiveClosedRange(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		json     string
		wantText string
	}{
		{
			name:     "timestamp gt/lt (both exclusive)",
			json:     `{"$and":[{"$gt":{"timestamp":"2023-11-14T22:13:20Z"}},{"$lt":{"timestamp":"2023-11-15T22:13:20Z"}}]}`,
			wantText: `timestamp > "2023-11-14T22:13:20Z" and timestamp < "2023-11-15T22:13:20Z"`,
		},
		{
			name:     "date gte/lt (upper exclusive only)",
			json:     `{"$and":[{"$gte":{"date":"2023-11-14T22:13:20Z"}},{"$lt":{"date":"2023-11-15T22:13:20Z"}}]}`,
			wantText: `date >= "2023-11-14T22:13:20Z" and date < "2023-11-15T22:13:20Z"`,
		},
		{
			name:     "timestamp gt/lte (lower exclusive only)",
			json:     `{"$and":[{"$gt":{"timestamp":"2023-11-14T22:13:20Z"}},{"$lte":{"timestamp":"2023-11-15T22:13:20Z"}}]}`,
			wantText: `timestamp > "2023-11-14T22:13:20Z" and timestamp <= "2023-11-15T22:13:20Z"`,
		},
		{
			name:     "date between (both inclusive) still uses between",
			json:     `{"$and":[{"$gte":{"date":"2023-11-14T22:13:20Z"}},{"$lte":{"date":"2023-11-15T22:13:20Z"}}]}`,
			wantText: `date between "2023-11-14T22:13:20Z" and "2023-11-15T22:13:20Z"`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// Build the folded proto exactly the way the JSON DSL does.
			folded := &commonpb.QueryFilter{}
			require.NoError(t, json.Unmarshal([]byte(tc.json), folded))
			// Precondition: the JSON side really folded to one condition, not an $and.
			require.Nil(t, folded.GetAnd(), "JSON input must fold to a single condition")

			got := Format(folded)
			require.Equal(t, tc.wantText, got)

			reparsed, err := Parse(got)
			require.NoError(t, err)
			require.True(t, proto.Equal(folded, reparsed),
				"Format output must round-trip to the original exclusive range\n first: %v\n reparsed: %v", folded, reparsed)
		})
	}
}

// TestFormatDate_NotWrapsExclusiveRange is the regression for the flemzord HIGH:
// an exclusive two-sided date range renders as two clauses joined by `and`, so it
// carries `and` precedence, not leaf precedence. Under a wrapping `not` the pair
// must be parenthesized — otherwise Format emits `not a and b`, which reparses as
// `(not a) and b` and silently changes the filter's meaning.
func TestFormatDate_NotWrapsExclusiveRange(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		json     string
		wantText string
	}{
		{
			name:     "not over exclusive timestamp range",
			json:     `{"$and":[{"$gt":{"timestamp":"2023-11-14T22:13:20Z"}},{"$lt":{"timestamp":"2023-11-15T22:13:20Z"}}]}`,
			wantText: `not (timestamp > "2023-11-14T22:13:20Z" and timestamp < "2023-11-15T22:13:20Z")`,
		},
		{
			name:     "not over exclusive date range",
			json:     `{"$and":[{"$gt":{"date":"2023-11-14T22:13:20Z"}},{"$lt":{"date":"2023-11-15T22:13:20Z"}}]}`,
			wantText: `not (date > "2023-11-14T22:13:20Z" and date < "2023-11-15T22:13:20Z")`,
		},
		{
			name:     "not over inclusive range keeps between (leaf, no parens)",
			json:     `{"$and":[{"$gte":{"timestamp":"2023-11-14T22:13:20Z"}},{"$lte":{"timestamp":"2023-11-15T22:13:20Z"}}]}`,
			wantText: `not timestamp between "2023-11-14T22:13:20Z" and "2023-11-15T22:13:20Z"`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			inner := &commonpb.QueryFilter{}
			require.NoError(t, json.Unmarshal([]byte(tc.json), inner))
			require.Nil(t, inner.GetAnd(), "JSON input must fold to a single condition")

			notFilter := &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Not{
				Not: &commonpb.NotFilter{Filter: inner},
			}}

			got := Format(notFilter)
			require.Equal(t, tc.wantText, got)

			reparsed, err := Parse(got)
			require.NoError(t, err)
			require.True(t, proto.Equal(notFilter, reparsed),
				"Format output must round-trip under not\n first: %v\n reparsed: %v", notFilter, reparsed)
		})
	}
}

// TestFormatDate_RawMicrosOutsideRFC3339Range is the regression for the flemzord
// MEDIUM: the decoder accepts the full uint64 raw-microsecond range, but the
// formatter always emitted RFC3339 — which wraps for v > math.MaxInt64 (pre-epoch,
// rejected by the decoder) and emits a non-RFC3339 5-digit-year string for years
// past 9999. Both fail Parse(Format(f)). Format must fall back to raw micros for
// bounds RFC3339 cannot round-trip.
func TestFormatDate_RawMicrosOutsideRFC3339Range(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		field commonpb.TransactionBuiltinIndex
		v     uint64
		want  string
	}{
		{
			name: "year beyond 9999 (still <= MaxInt64)",
			v:    300000000000000000, // ~year 11500 — RFC3339 emits a 5-digit year
			want: `timestamp >= 300000000000000000`,
		},
		{
			name: "above MaxInt64 (int64 conversion wraps)",
			v:    18446744073709551615, // math.MaxUint64
			want: `timestamp >= 18446744073709551615`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			min := tc.v
			f := &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_BuiltinUint{
				BuiltinUint: &commonpb.BuiltinUintCondition{
					Field: commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP,
					Cond:  &commonpb.UintCondition{Min: &min},
				},
			}}

			got := Format(f)
			require.Equal(t, tc.want, got, "must render raw micros, not an unparseable RFC3339 string")

			reparsed, err := Parse(got)
			require.NoError(t, err, "raw-micros output must parse back")
			require.True(t, proto.Equal(f, reparsed),
				"Format output must round-trip for out-of-RFC3339-range bounds\n first: %v\n reparsed: %v", f, reparsed)
		})
	}

	// Same guarantee on the audit[timestamp] arm.
	t.Run("audit[timestamp] above MaxInt64", func(t *testing.T) {
		t.Parallel()

		min := uint64(18446744073709551615)
		f := &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Audit{
			Audit: &commonpb.AuditCondition{
				Field: commonpb.AuditField_AUDIT_FIELD_TIMESTAMP,
				Condition: &commonpb.AuditCondition_UintCond{
					UintCond: &commonpb.UintCondition{Min: &min},
				},
			},
		}}

		got := Format(f)
		require.Equal(t, `audit[timestamp] >= 18446744073709551615`, got)

		reparsed, err := Parse(got)
		require.NoError(t, err)
		require.True(t, proto.Equal(f, reparsed), "audit[timestamp] raw micros must round-trip")
	})
}

// TestFormatDate_OnlyEmitsParseableFields is the regression for the flemzord
// MEDIUM: Format used to emit id/insertedAt/revertedAt expressions the textual
// parser (DateCond) cannot read back, so Parse(Format(f)) failed for those arms.
// Format now renders only the fields the grammar admits (timestamp, date); other
// builtin fields render as an explicit unknown marker rather than a lie that
// won't reparse.
func TestFormatDate_OnlyEmitsParseableFields(t *testing.T) {
	t.Parallel()

	nonParseable := []commonpb.TransactionBuiltinIndex{
		commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ID,
		commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_INSERTED_AT,
		commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REVERTED_AT,
	}

	lo := wantDateMicros
	for _, field := range nonParseable {
		f := &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_BuiltinUint{
			BuiltinUint: &commonpb.BuiltinUintCondition{
				Field: field,
				Cond:  &commonpb.UintCondition{Min: &lo},
			},
		}}
		assert.Equal(t, "<unknown builtin field>", Format(f), field.String())
	}

	// And the two fields we DO emit must round-trip through Parse for every
	// single-bound operator shape (== is covered by TestFormatDate_RoundTrip).
	for _, field := range []string{"timestamp", "date"} {
		for _, op := range []string{">", ">=", "<", "<="} {
			in := field + " " + op + ` "2023-11-14T22:13:20Z"`
			t.Run(in, func(t *testing.T) {
				t.Parallel()

				f, err := Parse(in)
				require.NoError(t, err)
				got := Format(f)
				reparsed, err := Parse(got)
				require.NoError(t, err, got)
				require.True(t, proto.Equal(f, reparsed),
					"Format must emit a parseable field\n first: %v\n reparsed: %v", f, reparsed)
			})
		}
	}
}

func TestParseDate_RejectsPreEpoch(t *testing.T) {
	t.Parallel()

	for _, in := range []string{
		`date >= "1969-12-31T00:00:00Z"`,
		`timestamp >= "1969-12-31T00:00:00Z"`,
	} {
		_, err := Parse(in)
		require.Error(t, err, in)
		assert.Contains(t, err.Error(), "Unix epoch", in)
	}
}

func TestParseDate_RejectsGarbageValue(t *testing.T) {
	t.Parallel()

	_, err := Parse(`date >= "not-a-date"`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "RFC3339")
}

func TestParseDate_RejectsParam(t *testing.T) {
	t.Parallel()

	// date/timestamp are index-resolved range fields evaluated without a
	// parameter-resolution context; a $param operand is rejected, mirroring the
	// audit datetime field.
	_, err := Parse(`date >= $since`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not support parameters")
}

// TestParseDate_KeywordsUsableAsBareValues guards that `date`/`timestamp` remain
// usable as bare metadata values. Because they are matched as plain Idents (not
// lexer keywords), the bare `Value.Bare` production captures them naturally.
func TestParseDate_KeywordsUsableAsBareValues(t *testing.T) {
	t.Parallel()

	for _, in := range []string{
		"metadata[kind] == date",
		"metadata[kind] == timestamp",
	} {
		filter, err := Parse(in)
		require.NoError(t, err, in)
		require.NotNil(t, filter.GetField(), in)
	}
}

// TestParseDate_DoesNotMistokenizeIdentifierPrefix is the regression for the
// NumaryBot MAJOR: adding `date`/`timestamp` as lexer keywords with a `\b`
// boundary would tokenize only the prefix of an identifier — reject filters that
// parsed before. They are matched as grammar literals against the whole Ident
// token instead, so a bare identifier that merely *starts* with `date`/`timestamp`
// stays one token.
//
// Reconciled with the EN-1547 lexer tightening (Ident = ^[a-zA-Z_][a-zA-Z0-9_]*;
// special characters must be quoted): the bare cases use valid Ident-continuation
// keys, and keys/values carrying special chars (`-`, `.`, `/`, `:`) are quoted —
// they still must not be mistokenized by the `date`/`timestamp` grammar literals.
func TestParseDate_DoesNotMistokenizeIdentifierPrefix(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		in   string
	}{
		{"date-prefixed bare metadata key", `metadata[date_range] == "x"`},
		{"timestamp-prefixed bare metadata key", `metadata[timestamp_created] == "x"`},
		{"quoted dotted date key", `metadata["date.start"] == "x"`},
		{"quoted slashed timestamp key", `metadata["timestamp/utc"] == "x"`},
		{"quoted date-prefixed value", `metadata[k] == "date-2023"`},
		{"quoted colon timestamp value", `metadata[k] == "timestamp:created"`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			filter, err := Parse(tc.in)
			require.NoError(t, err, tc.in)
			require.NotNil(t, filter.GetField(), tc.in)
		})
	}
}
