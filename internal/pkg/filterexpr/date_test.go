package filterexpr

import (
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

func TestParseDate_ClosedRange(t *testing.T) {
	t.Parallel()

	filter, err := Parse(`date >= "2023-11-14T22:13:20Z" and date < "2023-11-15T22:13:20Z"`)
	require.NoError(t, err)

	// Two same-field bounds are two separate leaves under an $and here (the DSL
	// does not fold textual conditions the way the JSON codec folds a range $and),
	// but both must carry the coerced micros.
	and := filter.GetAnd()
	require.NotNil(t, and)
	require.Len(t, and.GetFilters(), 2)

	lo := and.GetFilters()[0].GetLogBuiltinUint()
	require.NotNil(t, lo)
	assert.Equal(t, wantDateMicros, lo.GetCond().GetMin())

	hi := and.GetFilters()[1].GetLogBuiltinUint()
	require.NotNil(t, hi)
	assert.Equal(t, wantDateMicros+24*60*60*1_000_000, hi.GetCond().GetMax())
	assert.True(t, hi.GetCond().GetMaxExclusive())
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
// boundary would tokenize only the prefix of an identifier that continues with an
// Ident-continuation char (`-`, `:`, `.`, `/`) — the keyword boundary matches
// before those characters — and reject filters that parsed before. Matching them
// as plain Idents instead keeps the whole identifier intact.
func TestParseDate_DoesNotMistokenizeIdentifierPrefix(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		in   string
	}{
		{"date-prefixed metadata key", `metadata[date-range] == "x"`},
		{"timestamp-prefixed metadata key", `metadata[timestamp-created] == "x"`},
		{"dotted date key", `metadata[date.start] == "x"`},
		{"slashed timestamp key", `metadata[timestamp/utc] == "x"`},
		{"date-prefixed bare value", `metadata[k] == date-2023`},
		{"colon timestamp bare value", `metadata[k] == timestamp:created`},
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
