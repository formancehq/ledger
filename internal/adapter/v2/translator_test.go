package v2

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func TestTranslateBatch_NewTransaction(t *testing.T) {
	t.Parallel()

	v2Logs := []V2Log{{
		ID:   1,
		Type: "NEW_TRANSACTION",
		Data: mustMarshal(t, V2NewTransactionData{
			Transaction: V2Transaction{
				ID: 0,
				Postings: []V2Posting{{
					Source:      "world",
					Destination: "users:001",
					Amount:      "100",
					Asset:       "USD/2",
				}},
				Timestamp: "2023-11-14T22:13:20Z",
				Reference: "tx-ref-001",
			},
		}),
	}}

	orders, nextLogID, nextTxID, err := TranslateBatch("default", v2Logs, 1, 0)
	require.NoError(t, err)
	require.Len(t, orders, 1)
	require.Equal(t, uint64(2), nextLogID)
	require.Equal(t, uint64(1), nextTxID)

	ingest := orders[0].GetMirrorIngest()
	require.NotNil(t, ingest)
	require.Equal(t, "default", ingest.GetLedger())

	ct := ingest.GetEntry().GetCreatedTransaction()
	require.NotNil(t, ct)
	require.Equal(t, uint64(0), ct.GetTransactionId())
	require.Equal(t, "tx-ref-001", ct.GetReference())
	require.Len(t, ct.GetPostings(), 1)
	require.Equal(t, "world", ct.GetPostings()[0].GetSource())
	require.Equal(t, "users:001", ct.GetPostings()[0].GetDestination())
	require.Equal(t, "USD/2", ct.GetPostings()[0].GetAsset())
}

func TestTranslateBatch_SetMetadata_Account(t *testing.T) {
	t.Parallel()

	v2Logs := []V2Log{{
		ID:   1,
		Type: "SET_METADATA",
		Data: mustMarshal(t, V2SetMetadataData{
			TargetType: "ACCOUNT",
			TargetID:   json.RawMessage(`"users:001"`),
			Metadata:   map[string]string{"role": "admin"},
		}),
	}}

	orders, _, _, err := TranslateBatch("default", v2Logs, 1, 1)
	require.NoError(t, err)
	require.Len(t, orders, 1)

	sm := orders[0].GetMirrorIngest().GetEntry().GetSavedMetadata()
	require.NotNil(t, sm)

	account := sm.GetTarget().GetAccount()
	require.NotNil(t, account)
	require.Equal(t, "users:001", account.GetAddr())
	require.Len(t, sm.GetMetadata(), 1)
	require.Contains(t, sm.GetMetadata(), "role")
	require.Equal(t, "admin", sm.GetMetadata()["role"].GetStringValue())
}

func TestTranslateBatch_SetMetadata_Transaction(t *testing.T) {
	t.Parallel()

	v2Logs := []V2Log{{
		ID:   1,
		Type: "SET_METADATA",
		Data: mustMarshal(t, V2SetMetadataData{
			TargetType: "TRANSACTION",
			TargetID:   json.RawMessage(`42`),
			Metadata:   map[string]string{"status": "confirmed"},
		}),
	}}

	orders, _, _, err := TranslateBatch("default", v2Logs, 1, 1)
	require.NoError(t, err)
	require.Len(t, orders, 1)

	sm := orders[0].GetMirrorIngest().GetEntry().GetSavedMetadata()
	require.NotNil(t, sm)

	require.Equal(t, uint64(42), sm.GetTarget().GetTransactionId())
}

func TestTranslateBatch_SetMetadata_StringMetadataPreserved(t *testing.T) {
	t.Parallel()

	v2Logs := []V2Log{{
		ID:   1,
		Type: "SET_METADATA",
		Data: json.RawMessage(`{
			"targetType": "ACCOUNT",
			"targetId": "users:001",
			"metadata": {
				"unsafe": "9007199254740993"
			}
		}`),
	}}

	orders, _, _, err := TranslateBatch("default", v2Logs, 1, 1)
	require.NoError(t, err)
	require.Len(t, orders, 1)

	metadata := orders[0].GetMirrorIngest().GetEntry().GetSavedMetadata().GetMetadata()
	require.Contains(t, metadata, "unsafe")
	require.Equal(t, "9007199254740993", metadata["unsafe"].GetStringValue())
}

func TestTranslateBatch_SetMetadata_RejectsNonStringMetadata(t *testing.T) {
	t.Parallel()

	v2Logs := []V2Log{{
		ID:   1,
		Type: "SET_METADATA",
		Data: json.RawMessage(`{
				"targetType": "ACCOUNT",
				"targetId": "users:001",
				"metadata": {
					"unsafe": 9007199254740993
				}
			}`),
	}}

	orders, _, _, err := TranslateBatch("default", v2Logs, 1, 1)
	require.Error(t, err)
	require.Nil(t, orders)
	require.Contains(t, err.Error(), "unmarshaling SET_METADATA data")
}

func TestTranslateBatch_RevertedTransaction(t *testing.T) {
	t.Parallel()

	v2Logs := []V2Log{{
		ID:   3,
		Type: "REVERTED_TRANSACTION",
		Data: mustMarshal(t, V2RevertedTransactionData{
			RevertedTransactionID: 1,
			RevertTransaction: V2Transaction{
				ID: 5,
				Postings: []V2Posting{{
					Source:      "users:001",
					Destination: "world",
					Amount:      "100",
					Asset:       "USD/2",
				}},
				Timestamp: "2023-11-14T22:14:00Z",
			},
		}),
	}}

	orders, _, nextTxID, err := TranslateBatch("default", v2Logs, 3, 1)
	require.NoError(t, err)
	require.Len(t, orders, 1)
	require.Equal(t, uint64(6), nextTxID)

	rt := orders[0].GetMirrorIngest().GetEntry().GetRevertedTransaction()
	require.NotNil(t, rt)
	require.Equal(t, uint64(1), rt.GetRevertedTransactionId())
	require.Equal(t, uint64(5), rt.GetNewTransactionId())
	require.Len(t, rt.GetReversePostings(), 1)
}

func TestTranslateBatch_DeleteMetadata(t *testing.T) {
	t.Parallel()

	v2Logs := []V2Log{{
		ID:   1,
		Type: "DELETE_METADATA",
		Data: mustMarshal(t, V2DeleteMetadataData{
			TargetType: "ACCOUNT",
			TargetID:   json.RawMessage(`"users:001"`),
			Key:        "role",
		}),
	}}

	orders, _, _, err := TranslateBatch("default", v2Logs, 1, 1)
	require.NoError(t, err)
	require.Len(t, orders, 1)

	dm := orders[0].GetMirrorIngest().GetEntry().GetDeletedMetadata()
	require.NotNil(t, dm)
	require.Equal(t, "role", dm.GetKey())
	require.Equal(t, "users:001", dm.GetTarget().GetAccount().GetAddr())
}

func TestTranslateBatch_UnknownLogType_FillGap(t *testing.T) {
	t.Parallel()

	v2Logs := []V2Log{{
		ID:   1,
		Type: "INSERTED_SCHEMA",
		Data: json.RawMessage(`{}`),
	}}

	orders, _, _, err := TranslateBatch("default", v2Logs, 1, 1)
	require.NoError(t, err)
	require.Len(t, orders, 1)

	gap := orders[0].GetMirrorIngest().GetEntry().GetFillGap()
	require.NotNil(t, gap)
}

func TestTranslateBatch_LogIDGapDetection(t *testing.T) {
	t.Parallel()

	// Logs with a gap: expected 1, got 3
	v2Logs := []V2Log{{
		ID:   3,
		Type: "SET_METADATA",
		Data: mustMarshal(t, V2SetMetadataData{
			TargetType: "ACCOUNT",
			TargetID:   json.RawMessage(`"users:001"`),
			Metadata:   map[string]string{"key": "val"},
		}),
	}}

	orders, nextLogID, _, err := TranslateBatch("default", v2Logs, 1, 1)
	require.NoError(t, err)
	// 2 fill-gap orders (for IDs 1, 2) + 1 real order (for ID 3)
	require.Len(t, orders, 3)
	require.Equal(t, uint64(4), nextLogID)

	// First two should be fill gaps
	require.NotNil(t, orders[0].GetMirrorIngest().GetEntry().GetFillGap())
	require.Equal(t, uint64(1), orders[0].GetMirrorIngest().GetEntry().GetV2LogId())
	require.NotNil(t, orders[1].GetMirrorIngest().GetEntry().GetFillGap())
	require.Equal(t, uint64(2), orders[1].GetMirrorIngest().GetEntry().GetV2LogId())

	// Third should be the actual metadata
	require.NotNil(t, orders[2].GetMirrorIngest().GetEntry().GetSavedMetadata())
}

func TestTranslateBatch_EmptyInput(t *testing.T) {
	t.Parallel()

	orders, nextLogID, nextTxID, err := TranslateBatch("default", nil, 1, 1)
	require.NoError(t, err)
	require.Empty(t, orders)
	require.Equal(t, uint64(1), nextLogID)
	require.Equal(t, uint64(1), nextTxID)
}

func TestTranslateBatch_MultipleLogs(t *testing.T) {
	t.Parallel()

	v2Logs := []V2Log{
		{
			ID:   1,
			Type: "NEW_TRANSACTION",
			Data: mustMarshal(t, V2NewTransactionData{
				Transaction: V2Transaction{
					ID:       0,
					Postings: []V2Posting{{Source: "world", Destination: "a", Amount: "50", Asset: "EUR"}},
				},
			}),
		},
		{
			ID:   2,
			Type: "SET_METADATA",
			Data: mustMarshal(t, V2SetMetadataData{
				TargetType: "ACCOUNT",
				TargetID:   json.RawMessage(`"a"`),
				Metadata:   map[string]string{"type": "asset"},
			}),
		},
	}

	orders, nextLogID, nextTxID, err := TranslateBatch("ledger1", v2Logs, 1, 0)
	require.NoError(t, err)
	require.Len(t, orders, 2)
	require.Equal(t, uint64(3), nextLogID)
	require.Equal(t, uint64(1), nextTxID)

	require.NotNil(t, orders[0].GetMirrorIngest().GetEntry().GetCreatedTransaction())
	require.NotNil(t, orders[1].GetMirrorIngest().GetEntry().GetSavedMetadata())
}

func TestTranslateBatch_AccountMetadata(t *testing.T) {
	t.Parallel()

	v2Logs := []V2Log{{
		ID:   1,
		Type: "NEW_TRANSACTION",
		Data: mustMarshal(t, V2NewTransactionData{
			Transaction: V2Transaction{
				ID:       0,
				Postings: []V2Posting{{Source: "world", Destination: "a", Amount: "100", Asset: "USD"}},
			},
			AccountMetadata: map[string]map[string]string{
				"a": {"type": "asset"},
			},
		}),
	}}

	orders, _, _, err := TranslateBatch("default", v2Logs, 1, 0)
	require.NoError(t, err)
	require.Len(t, orders, 1)

	ct := orders[0].GetMirrorIngest().GetEntry().GetCreatedTransaction()
	require.NotNil(t, ct)
	require.Contains(t, ct.GetAccountMetadata(), "a")
}

func TestTranslatePostings_LargeAmount(t *testing.T) {
	t.Parallel()

	postings := []V2Posting{{
		Source:      "world",
		Destination: "vault",
		Amount:      "999999999999999999999",
		Asset:       "BTC/8",
	}}

	result, err := translatePostings(postings)
	require.NoError(t, err)
	require.Len(t, result, 1)
	require.NotNil(t, result[0].GetAmount())
}

func TestTranslatePostings_NegativeAmount(t *testing.T) {
	t.Parallel()

	postings := []V2Posting{{
		Source:      "world",
		Destination: "vault",
		Amount:      "-100",
		Asset:       "USD",
	}}

	_, err := translatePostings(postings)
	require.Error(t, err)
	require.Contains(t, err.Error(), "negative amount")
}

func TestTranslatePostings_InvalidAmount(t *testing.T) {
	t.Parallel()

	postings := []V2Posting{{
		Source:      "world",
		Destination: "vault",
		Amount:      "not-a-number",
		Asset:       "USD",
	}}

	_, err := translatePostings(postings)
	require.Error(t, err)
}

func TestTranslateTarget_TransactionString(t *testing.T) {
	t.Parallel()

	// v2 sometimes encodes transaction IDs as strings
	target, err := translateTarget("TRANSACTION", json.RawMessage(`"42"`))
	require.NoError(t, err)
	require.Equal(t, uint64(42), target.GetTransactionId())
}

func TestTranslateTarget_TransactionUint(t *testing.T) {
	t.Parallel()

	target, err := translateTarget("TRANSACTION", json.RawMessage(`42`))
	require.NoError(t, err)
	require.Equal(t, uint64(42), target.GetTransactionId())
}

func TestTranslateTarget_Account(t *testing.T) {
	t.Parallel()

	target, err := translateTarget("ACCOUNT", json.RawMessage(`"users:001"`))
	require.NoError(t, err)
	require.Equal(t, "users:001", target.GetAccount().GetAddr())
}

func TestTranslateTarget_UnknownType(t *testing.T) {
	t.Parallel()

	_, err := translateTarget("UNKNOWN", json.RawMessage(`"whatever"`))
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown target type")
}

func TestTranslateMetadataMap_String(t *testing.T) {
	t.Parallel()

	result := translateMetadataMap(map[string]string{"hello": "world"})
	require.Equal(t, "world", result["hello"].GetStringValue())
}

func TestTranslateMetadataMap_MultipleStringValues(t *testing.T) {
	t.Parallel()

	result := translateMetadataMap(map[string]string{
		"status": "confirmed",
		"ref":    "order-123",
	})
	require.Equal(t, "confirmed", result["status"].GetStringValue())
	require.Equal(t, "order-123", result["ref"].GetStringValue())
}

func TestTranslateMetadataMap_Nil(t *testing.T) {
	t.Parallel()

	result := translateMetadataMap(nil)
	require.Nil(t, result)
}

func TestTranslateBatch_TxIDSkippedWhenGap(t *testing.T) {
	t.Parallel()

	// Transaction ID is 5 but expectedNextTxID is 2
	// The translator should record the gap in skippedTxIDs (though currently unused)
	// and advance nextTxID to 6
	v2Logs := []V2Log{{
		ID:   1,
		Type: "NEW_TRANSACTION",
		Data: mustMarshal(t, V2NewTransactionData{
			Transaction: V2Transaction{
				ID:       5,
				Postings: []V2Posting{{Source: "world", Destination: "a", Amount: "100", Asset: "USD"}},
			},
		}),
	}}

	orders, _, nextTxID, err := TranslateBatch("default", v2Logs, 1, 2)
	require.NoError(t, err)
	require.Len(t, orders, 1)
	require.Equal(t, uint64(6), nextTxID)
}

func TestTranslateBatch_InvalidJSON(t *testing.T) {
	t.Parallel()

	v2Logs := []V2Log{{
		ID:   1,
		Type: "NEW_TRANSACTION",
		Data: json.RawMessage(`{invalid json`),
	}}

	_, _, _, err := TranslateBatch("default", v2Logs, 1, 1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "translating v2 log 1")
}

func TestTranslateBatch_TrailingJSONRejected(t *testing.T) {
	t.Parallel()

	v2Logs := []V2Log{{
		ID:   1,
		Type: "SET_METADATA",
		Data: json.RawMessage(`{
			"targetType": "ACCOUNT",
			"targetId": "users:001",
			"metadata": {"key": "value"}
		}{}`),
	}}

	_, _, _, err := TranslateBatch("default", v2Logs, 1, 1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "translating v2 log 1")
}

func TestMakeMirrorOrder(t *testing.T) {
	t.Parallel()

	entry := &raftcmdpb.MirrorLogEntry{
		V2LogId: 42,
		Data: &raftcmdpb.MirrorLogEntry_FillGap{
			FillGap: &raftcmdpb.MirrorFillGap{},
		},
	}

	order := makeMirrorOrder("test-ledger", entry)
	require.Equal(t, "test-ledger", order.GetMirrorIngest().GetLedger())
	require.Equal(t, uint64(42), order.GetMirrorIngest().GetEntry().GetV2LogId())
}

func mustMarshal(t *testing.T, v any) json.RawMessage {
	t.Helper()

	data, err := json.Marshal(v)
	require.NoError(t, err)

	return data
}

func BenchmarkTranslateBatch(b *testing.B) {
	// Build a realistic batch of 100 NEW_TRANSACTION logs with 2 postings each.
	const batchSize = 100

	v2Logs := make([]V2Log, batchSize)
	for i := range v2Logs {
		data, err := json.Marshal(V2NewTransactionData{
			Transaction: V2Transaction{
				ID: uint64(i),
				Postings: []V2Posting{
					{Source: "world", Destination: "users:001", Amount: "150000", Asset: "USD/2"},
					{Source: "users:001", Destination: "merchants:042", Amount: "150000", Asset: "USD/2"},
				},
				Timestamp: "2024-06-15T10:30:00Z",
				Metadata:  map[string]string{"ref": "order-12345", "type": "payment"},
			},
		})
		if err != nil {
			b.Fatalf("json.Marshal: %v", err)
		}
		v2Logs[i] = V2Log{
			ID:   uint64(i + 1),
			Type: "NEW_TRANSACTION",
			Data: data,
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		orders, _, _, err := TranslateBatch("default", v2Logs, 1, 0)
		if err != nil {
			b.Fatal(err)
		}

		_ = orders
	}
}
