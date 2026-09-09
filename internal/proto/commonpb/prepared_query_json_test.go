package commonpb

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/adapter/json"
)

func TestPreparedQueryCursor_LogDataRoundTrip(t *testing.T) {
	t.Parallel()

	wantLog := &LedgerLog{
		Id:   7,
		Date: &Timestamp{Data: 1_700_000_000_000_000},
		Data: &LedgerLogPayload{Payload: &LedgerLogPayload_CreatedTransaction{
			CreatedTransaction: &CreatedTransaction{
				Transaction: &Transaction{
					Id:        9,
					Reference: "order-789",
					Postings: []*Posting{
						NewColoredPosting("world", "alice", "USD/2", "pending", big.NewInt(1000)),
					},
					Metadata: map[string]*MetadataValue{"note": NewStringValue("checkout")},
				},
				AccountMetadata: map[string]*MetadataMap{
					"alice": {Values: map[string]*MetadataValue{"tier": NewStringValue("gold")}},
				},
			},
		}},
	}
	cursor := &PreparedQueryCursor{
		PageSize: 10,
		LogData: []*Log{{
			Sequence: 42,
			Payload: &LogPayload{Type: &LogPayload_Apply{
				Apply: &ApplyLedgerLog{LedgerName: "orders", Log: wantLog},
			}},
		}},
	}

	data, err := json.Marshal(cursor)
	require.NoError(t, err)
	require.Contains(t, string(data), `"logData":`)
	require.NotContains(t, string(data), `"createdTransaction":`)
	require.Contains(t, string(data), `"data":{"transaction":`)
	require.Contains(t, string(data), `"type":"NEW_TRANSACTION"`)
	var response struct {
		LogData []struct {
			Payload struct {
				Apply struct {
					Log *LedgerLog `json:"log"`
				} `json:"apply"`
			} `json:"payload"`
		} `json:"logData"`
	}
	require.NoError(t, json.Unmarshal(data, &response))
	require.Len(t, response.LogData, 1)
	require.True(t, proto.Equal(wantLog, response.LogData[0].Payload.Apply.Log), "prepared-query logData must preserve the complete payload during hydration")
}

// TestPreparedQuery_MarshalJSON_CamelCaseAndEnumString guards two regressions:
//   - #478: default encoding/json emitted PascalCase oneof keys
//     (`"Filter":{"Reference":{...}}`) and a raw enum int (`"target":1`).
//   - EN-1465: protojson leaked the proto-internal oneof/wrapper names of
//     QueryFilter and the QUERY_TARGET_* enum prefix onto the public surface.
//
// The current contract is the v2-aligned query DSL: target is the bare string
// enum (ACCOUNTS/TRANSACTIONS/LOGS) and the filter uses `$`-prefixed operators
// (`$match`/`$gt`/…) with single-key operator->{field:value} bodies (see
// query_filter.go).
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
	require.Contains(t, out, `"$match":{"reference":"order-123"}`,
		"filter must use the v2-aligned query DSL shape")

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
