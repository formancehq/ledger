package http

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// TestToTemplateUsageJSON_EpochLastUsed is the regression guard for the
// dropped-epoch bug: a non-nil lastUsed at Data==0 (the Unix epoch) must be
// emitted, not silently omitted. nil is the only "never used" sentinel.
func TestToTemplateUsageJSON_EpochLastUsed(t *testing.T) {
	t.Parallel()

	out := toTemplateUsageJSON(&commonpb.TemplateUsage{
		Count:    3,
		LastUsed: &commonpb.Timestamp{Data: 0},
	})

	require.NotNil(t, out.LastUsed, "a non-nil epoch timestamp must be present, not omitted")
	assert.Equal(t, "1970-01-01T00:00:00Z", *out.LastUsed, "Data==0 is the Unix epoch, formatted from microseconds")
	assert.Equal(t, uint64(3), out.Count)
}

// TestToTemplateUsageJSON_NilLastUsedOmitted confirms nil (never invoked)
// still drops lastUsed from the JSON (omitempty on the pointer).
func TestToTemplateUsageJSON_NilLastUsedOmitted(t *testing.T) {
	t.Parallel()

	out := toTemplateUsageJSON(&commonpb.TemplateUsage{Count: 0})
	require.Nil(t, out.LastUsed)

	raw, err := json.Marshal(out)
	require.NoError(t, err)
	assert.JSONEq(t, `{"count":0}`, string(raw), "nil lastUsed must be omitted, not serialized as null")
}

// TestToTemplateUsageJSON_MicrosecondUnitAndCamelCase locks the two sibling
// contract points flemzord flagged alongside the epoch bug:
//   - the DTO serializes camelCase (`lastUsed`, `count`) with no raw protobuf
//     tags (`last_used`) or wire encoding (`{data: <int64>}`);
//   - Timestamp.Data is interpreted as microseconds, not nanoseconds.
func TestToTemplateUsageJSON_MicrosecondUnitAndCamelCase(t *testing.T) {
	t.Parallel()

	// 1_700_000_000_000_000 µs = 2023-11-14T22:13:20Z. If Data were treated
	// as nanoseconds the year would collapse to 1970.
	out := toTemplateUsageJSON(&commonpb.TemplateUsage{
		Count:    42,
		LastUsed: &commonpb.Timestamp{Data: 1_700_000_000_000_000},
	})

	require.NotNil(t, out.LastUsed)
	assert.Equal(t, "2023-11-14T22:13:20Z", *out.LastUsed, "Data must be read as microseconds")

	raw, err := json.Marshal(out)
	require.NoError(t, err)
	assert.JSONEq(t, `{"count":42,"lastUsed":"2023-11-14T22:13:20Z"}`, string(raw),
		"DTO must be camelCase with no raw protobuf field names or wire encoding")
}
