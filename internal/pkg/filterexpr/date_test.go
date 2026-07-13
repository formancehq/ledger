package filterexpr

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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

// TestParseDate_TimestampStillUsableAsBareValue guards the grammar change: making
// date/timestamp lexer keywords must not stop them being usable as bare metadata
// values (they are in the Value.Kw allow-list).
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
