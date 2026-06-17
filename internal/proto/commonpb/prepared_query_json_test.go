package commonpb

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/adapter/json"
)

// TestPreparedQuery_MarshalJSON_CamelCaseAndEnumString guards the regression
// where PreparedQuery shipped through sonic/encoding/json emitted PascalCase
// oneof keys (`"Filter":{"Reference":{"cond":{"Value":{"Hardcoded":...}}}}`)
// and a raw enum int (`"target":1`) — see #478. The fix routes the value
// through protojson so the response matches the camelCase contract every
// other endpoint follows and the input side already accepts via
// decodePreparedQueryFilter.
func TestPreparedQuery_MarshalJSON_CamelCaseAndEnumString(t *testing.T) {
	t.Parallel()

	pq := &PreparedQuery{
		Name:   "q1",
		Ledger: "test",
		Target: QueryTarget_QUERY_TARGET_TRANSACTIONS,
		Filter: &QueryFilter{
			Filter: &QueryFilter_Reference{
				Reference: &ReferenceCondition{
					Cond: &StringCondition{
						Value: &StringCondition_Hardcoded{Hardcoded: "order-123"},
					},
				},
			},
		},
	}

	data, err := json.Marshal(pq)
	require.NoError(t, err)

	out := string(data)
	require.Contains(t, out, `"name":"q1"`)
	require.Contains(t, out, `"ledger":"test"`)
	require.Contains(t, out, `"target":"QUERY_TARGET_TRANSACTIONS"`,
		"enum must be emitted as its string constant, not a raw int")
	require.Contains(t, out, `"reference"`,
		"oneof variants must be lowercased per protojson")
	require.Contains(t, out, `"hardcoded":"order-123"`,
		"nested oneof variants must be camelCase too")

	// Belt-and-braces: the broken shape must not survive.
	require.False(t, strings.Contains(out, `"Filter"`),
		"PascalCase oneof container leak")
	require.False(t, strings.Contains(out, `"Reference"`),
		"PascalCase oneof variant leak")
	require.False(t, strings.Contains(out, `"Hardcoded"`),
		"PascalCase nested-oneof variant leak")
	require.False(t, strings.Contains(out, `"target":1`),
		"raw-int enum leak")
}
