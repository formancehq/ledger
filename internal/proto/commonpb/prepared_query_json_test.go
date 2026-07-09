package commonpb

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/adapter/json"
)

// TestPreparedQuery_MarshalJSON_CamelCaseAndEnumString guards two regressions:
//   - #478: default encoding/json emitted PascalCase oneof keys
//     (`"Filter":{"Reference":{...}}`) and a raw enum int (`"target":1`).
//   - EN-1465: the earlier protojson fix leaked the proto-internal oneof/wrapper
//     names of QueryFilter (`"reference":{"cond":{"hardcoded":...}}`) and the
//     QUERY_TARGET_* enum prefix onto the public surface.
//
// The current contract is the canonical flat shape: target is the bare string
// enum (ACCOUNTS/TRANSACTIONS/LOGS) and the filter uses `match` + tagged-union
// conditions (see query_filter.go).
func TestPreparedQuery_MarshalJSON_CamelCaseAndEnumString(t *testing.T) {
	t.Parallel()

	pq := &PreparedQuery{
		Name:   "q1",
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
	require.Contains(t, out, `"target":"TRANSACTIONS"`,
		"enum must be emitted as the bare string constant, not a raw int or the QUERY_TARGET_* prefix")
	require.Contains(t, out, `"match":{"type":"reference"`,
		"filter must use the canonical flat match shape")
	require.Contains(t, out, `"equals":"order-123"`,
		"string condition must use the public `equals` key")

	// The PreparedQuery value no longer carries a ledger field — the ledger
	// lives on the storage key, on the surrounding RPC request, and (for
	// write commands) on the LedgerScopedOrder wrapper.
	require.NotContains(t, out, `"ledger"`,
		"ledger field must not appear in the marshalled PreparedQuery")

	// The broken shape (#478) must not survive.
	require.NotContains(t, out, `"Filter"`, "PascalCase oneof container leak")
	require.NotContains(t, out, `"Reference"`, "PascalCase oneof variant leak")
	require.NotContains(t, out, `"Hardcoded"`, "PascalCase nested-oneof variant leak")
	require.NotContains(t, out, `"target":1`, "raw-int enum leak")

	// The protojson leak (EN-1465) must not survive either.
	require.NotContains(t, out, `QUERY_TARGET_`, "proto enum prefix leak")
	require.NotContains(t, out, `"cond"`, "protojson-internal condition wrapper leak")
	require.NotContains(t, out, `"hardcoded"`, "protojson-internal string oneof leak")
}
