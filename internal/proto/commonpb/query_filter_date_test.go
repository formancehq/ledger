package commonpb

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// 2023-11-14T22:13:20Z == 1_700_000_000 s == 1_700_000_000_000_000 µs.
const structuredDateMicros = uint64(1_700_000_000_000_000)

// TestStructuredDateRFC3339EqualsRawMicros asserts the structured JSON DSL
// date/timestamp bounds compile identically whether written as an RFC3339 string
// or as raw microseconds (EN-1544). Both go through the shared
// CoerceDatetimeMicros, so the decoded QueryFilter must be proto.Equal.
func TestStructuredDateRFC3339EqualsRawMicros(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		rfc3339   string
		rawMicros string
	}{
		{
			name:      "log date",
			rfc3339:   `{"$gte":{"date":"2023-11-14T22:13:20Z"}}`,
			rawMicros: `{"$gte":{"date":"1700000000000000"}}`,
		},
		{
			name:      "tx timestamp",
			rfc3339:   `{"$gte":{"timestamp":"2023-11-14T22:13:20Z"}}`,
			rawMicros: `{"$gte":{"timestamp":"1700000000000000"}}`,
		},
		{
			name:      "tx insertedAt",
			rfc3339:   `{"$lt":{"insertedAt":"2023-11-14T22:13:20Z"}}`,
			rawMicros: `{"$lt":{"insertedAt":"1700000000000000"}}`,
		},
		{
			name:      "tx revertedAt closed range",
			rfc3339:   `{"$and":[{"$gte":{"revertedAt":"2023-11-14T22:13:20Z"}},{"$lt":{"revertedAt":"2023-11-14T22:13:20Z"}}]}`,
			rawMicros: `{"$and":[{"$gte":{"revertedAt":"1700000000000000"}},{"$lt":{"revertedAt":"1700000000000000"}}]}`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			fromRFC := &QueryFilter{}
			require.NoError(t, json.Unmarshal([]byte(tc.rfc3339), fromRFC))

			fromRaw := &QueryFilter{}
			require.NoError(t, json.Unmarshal([]byte(tc.rawMicros), fromRaw))

			require.True(t, proto.Equal(fromRFC, fromRaw),
				"RFC3339 and raw-micros must decode to the same filter\n rfc: %v\n raw: %v", fromRFC, fromRaw)
		})
	}
}

// TestStructuredDateNumericMicrosStillParses guards that a JSON-number bound
// (not a string) keeps working for date fields — the coercer falls back to
// asUint64 when the literal is not a string.
func TestStructuredDateNumericMicrosStillParses(t *testing.T) {
	t.Parallel()

	got := &QueryFilter{}
	require.NoError(t, json.Unmarshal([]byte(`{"$gte":{"date":1700000000000000}}`), got))
	require.NotNil(t, got.GetLogBuiltinUint())
	require.Equal(t, structuredDateMicros, got.GetLogBuiltinUint().GetCond().GetMin())
}

func TestStructuredDateValue(t *testing.T) {
	t.Parallel()

	got := &QueryFilter{}
	require.NoError(t, json.Unmarshal([]byte(`{"$gte":{"date":"2023-11-14T22:13:20Z"}}`), got))
	lc := got.GetLogBuiltinUint()
	require.NotNil(t, lc)
	require.Equal(t, LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE, lc.GetField())
	require.Equal(t, structuredDateMicros, lc.GetCond().GetMin())
}

func TestStructuredDateRejectsPreEpoch(t *testing.T) {
	t.Parallel()

	for _, in := range []string{
		`{"$gte":{"date":"1969-12-31T00:00:00Z"}}`,
		`{"$gte":{"timestamp":"1969-12-31T00:00:00Z"}}`,
	} {
		err := json.Unmarshal([]byte(in), &QueryFilter{})
		require.Error(t, err, in)
		require.Contains(t, err.Error(), "Unix epoch", in)
	}
}

// TestStructuredNonDateFieldRejectsRFC3339 guards that RFC3339 coercion is scoped
// to date-semantic fields: `id` is a plain count and must still reject an RFC3339
// string (it is not a valid uint).
func TestStructuredNonDateFieldRejectsRFC3339(t *testing.T) {
	t.Parallel()

	err := json.Unmarshal([]byte(`{"$gte":{"id":"2023-11-14T22:13:20Z"}}`), &QueryFilter{})
	require.Error(t, err)
}

// TestCoerceDatetimeMicros exercises the shared coercion directly.
func TestCoerceDatetimeMicros(t *testing.T) {
	t.Parallel()

	rfc, err := CoerceDatetimeMicros("2023-11-14T22:13:20Z")
	require.NoError(t, err)
	require.Equal(t, structuredDateMicros, rfc)

	raw, err := CoerceDatetimeMicros("1700000000000000")
	require.NoError(t, err)
	require.Equal(t, structuredDateMicros, raw)

	_, err = CoerceDatetimeMicros("1969-12-31T00:00:00Z")
	require.ErrorIs(t, err, ErrDatetimeBeforeEpoch)

	_, err = CoerceDatetimeMicros("not-a-date")
	require.Error(t, err)
	require.Contains(t, err.Error(), "RFC3339")
}
