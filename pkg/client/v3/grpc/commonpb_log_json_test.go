package grpc_test

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
	ledgerjson "github.com/formancehq/ledger/pkg/client/v3/internal/json"
)

func TestLedgerLogJSONOutput(t *testing.T) {
	t.Parallel()
	timestamp := &ledgerpb.Timestamp{Data: 1_700_000_000_123_456}
	metadata := map[string]*ledgerpb.MetadataValue{
		"label": ledgerpb.NewStringValue("customer"), "active": ledgerpb.NewBoolValue(true),
		"count": ledgerpb.NewUintValue(math.MaxUint64), "debt": ledgerpb.NewIntValue(math.MinInt64),
		"null": ledgerpb.NewNullValue(""),
	}
	transaction := &ledgerpb.Transaction{
		Id: 9007199254740993, Reference: "ref-1", Timestamp: timestamp, InsertedAt: timestamp, UpdatedAt: timestamp,
		Reverted: true, RevertedAt: timestamp, RevertedByTransaction: 9007199254740994,
		Metadata: metadata,
		Postings: []*ledgerpb.Posting{{Source: "world", Destination: "users:alice", Asset: "USD/2", Color: "GOLD", Amount: &ledgerpb.Uint256{V1: 1 << 36}}},
		PostCommitVolumes: &ledgerpb.PostCommitVolumes{VolumesByAccount: map[string]*ledgerpb.VolumesByAssets{
			"users:alice": {Volumes: []*ledgerpb.VolumeEntry{
				{Asset: "USD/2", Color: "", Volumes: &ledgerpb.Volumes{Input: "123", Output: "0"}},
				{Asset: "USD/2", Color: "GOLD", Volumes: &ledgerpb.Volumes{Input: "1267650600228229401496703205376", Output: "0"}},
			}},
		}},
	}
	revert := proto.Clone(transaction).(*ledgerpb.Transaction)
	revert.Id = 9007199254740994
	revert.Reverted = false
	revert.RevertedAt = nil
	revert.RevertedByTransaction = 0
	revert.RevertsTransaction = transaction.GetId()
	type logCase struct {
		name    string
		kind    ledgerpb.LogType
		payload *ledgerpb.LedgerLogPayload
	}
	cases := []logCase{
		{"created", ledgerpb.NewTransactionLogType, &ledgerpb.LedgerLogPayload{Payload: &ledgerpb.LedgerLogPayload_CreatedTransaction{CreatedTransaction: &ledgerpb.CreatedTransaction{Transaction: transaction, AccountMetadata: map[string]*ledgerpb.MetadataMap{"users:alice": {Values: metadata}}}}}},
		{"reverted", ledgerpb.RevertedTransactionLogType, &ledgerpb.LedgerLogPayload{Payload: &ledgerpb.LedgerLogPayload_RevertedTransaction{RevertedTransaction: &ledgerpb.RevertedTransaction{RevertedTransactionId: transaction.GetId(), RevertTransaction: revert}}}},
		{"set-field", ledgerpb.SetMetadataFieldTypeLogType, &ledgerpb.LedgerLogPayload{Payload: &ledgerpb.LedgerLogPayload_SetMetadataFieldType{SetMetadataFieldType: &ledgerpb.SetMetadataFieldTypeLog{TargetType: ledgerpb.TargetType_TARGET_TYPE_TRANSACTION, Key: "count", Type: ledgerpb.MetadataType_METADATA_TYPE_UINT64}}}},
		{"remove-field", ledgerpb.RemovedMetadataFieldTypeLogType, &ledgerpb.LedgerLogPayload{Payload: &ledgerpb.LedgerLogPayload_RemovedMetadataFieldType{RemovedMetadataFieldType: &ledgerpb.RemovedMetadataFieldTypeLog{TargetType: ledgerpb.TargetType_TARGET_TYPE_TRANSACTION, Key: "count", DroppedIndex: &ledgerpb.IndexID{Kind: &ledgerpb.IndexID_Metadata{Metadata: &ledgerpb.MetadataIndexID{Target: ledgerpb.TargetType_TARGET_TYPE_TRANSACTION, Key: "count"}}}}}}},
		{"skipped", ledgerpb.OrderSkippedLogType, &ledgerpb.LedgerLogPayload{Payload: &ledgerpb.LedgerLogPayload_OrderSkipped{OrderSkipped: &ledgerpb.OrderSkippedLog{Reason: ledgerpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT, Context: map[string]string{"reference": "ref-1", "existingTransactionId": "9007199254740993"}}}}},
	}
	for name, target := range map[string]*ledgerpb.Target{
		"zero":        {Target: &ledgerpb.Target_TransactionId{TransactionId: 0}},
		"account":     {Target: &ledgerpb.Target_Account{Account: &ledgerpb.TargetAccount{Addr: "users:alice"}}},
		"transaction": {Target: &ledgerpb.Target_TransactionId{TransactionId: transaction.GetId()}},
	} {
		cases = append(
			cases,
			logCase{"saved-" + name, ledgerpb.SetMetadataLogType, &ledgerpb.LedgerLogPayload{Payload: &ledgerpb.LedgerLogPayload_SavedMetadata{SavedMetadata: &ledgerpb.SavedMetadata{Target: target, Metadata: metadata}}}},
			logCase{"deleted-" + name, ledgerpb.DeleteMetadataLogType, &ledgerpb.LedgerLogPayload{Payload: &ledgerpb.LedgerLogPayload_DeletedMetadata{DeletedMetadata: &ledgerpb.DeletedMetadata{Target: target, Key: "label"}}}},
		)
	}
	for _, tc := range []struct {
		kind ledgerpb.LogType
		wire string
	}{
		{ledgerpb.FillGapLogType, `{"fillGap":{"originalId":"18446744073709551615"}}`},
		{ledgerpb.CreateIndexLogType, `{"createIndex":{"id":{"metadata":{"target":"TARGET_TYPE_TRANSACTION","key":"label"}},"boundType":"METADATA_TYPE_UINT64","boundTypeDeclared":true}}`},
		{ledgerpb.DropIndexLogType, `{"dropIndex":{"id":{"metadata":{"target":"TARGET_TYPE_ACCOUNT","key":"label"}}}}`},
		{ledgerpb.AddedAccountTypeLogType, `{"addedAccountType":{"accountType":{"name":"users","pattern":"users:{id}","persistence":"ACCOUNT_TYPE_EPHEMERAL","segmentTypes":{"id":{"uint64":{}}}}}}`},
		{ledgerpb.RemovedAccountTypeLogType, `{"removedAccountType":{"name":"users"}}`},
		{ledgerpb.UpdatedDefaultEnforcementModeLogType, `{"updatedDefaultEnforcementMode":{"enforcementMode":"CHART_ENFORCEMENT_AUDIT"}}`},
	} {
		payload := &ledgerpb.LedgerLogPayload{}
		require.NoError(t, protojson.Unmarshal([]byte(tc.wire), payload))
		field := payload.ProtoReflect().WhichOneof(payload.ProtoReflect().Descriptor().Oneofs().Get(0))
		cases = append(cases, logCase{field.JSONName(), tc.kind, payload})
	}
	covered := map[string]bool{}
	discriminators := map[ledgerpb.LogType]string{}
	for _, tc := range cases {
		field := tc.payload.ProtoReflect().WhichOneof(tc.payload.ProtoReflect().Descriptor().Oneofs().Get(0))
		covered[string(field.Name())] = true
		if previous, ok := discriminators[tc.kind]; ok {
			require.Equal(t, previous, string(field.Name()), "each payload variant needs a distinct log type")
		}
		discriminators[tc.kind] = string(field.Name())
	}
	require.Len(t, covered, (&ledgerpb.LedgerLogPayload{}).ProtoReflect().Descriptor().Oneofs().Get(0).Fields().Len(), "extend the matrix for each new variant")
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			original := &ledgerpb.LedgerLog{Id: 9007199254740995, Date: timestamp, Data: tc.payload}
			for name, codec := range map[string]struct {
				marshal func(any) ([]byte, error)
			}{
				"standard": {json.Marshal}, "sonic": {ledgerjson.Marshal},
			} {
				t.Run(name, func(t *testing.T) {
					encoded, err := codec.marshal(original)
					require.NoError(t, err)
					var envelope struct {
						Type ledgerpb.LogType `json:"type"`
						Data json.RawMessage  `json:"data"`
						ID   json.RawMessage  `json:"id"`
						Date string           `json:"date"`
					}
					require.NoError(t, json.Unmarshal(encoded, &envelope))
					require.Equal(t, tc.kind, envelope.Type)
					require.Equal(t, "9007199254740995", string(envelope.ID))
					require.Equal(t, "2023-11-14T22:13:20.123456Z", envelope.Date)
					var wireData map[string]json.RawMessage
					require.NoError(t, json.Unmarshal(envelope.Data, &wireData))
					field := tc.payload.ProtoReflect().WhichOneof(tc.payload.ProtoReflect().Descriptor().Oneofs().Get(0))
					require.NotContains(t, wireData, field.JSONName(), "data must be the direct payload")
					message := tc.payload.ProtoReflect().Get(field).Message().Interface()
					var expected []byte
					if custom, ok := message.(json.Marshaler); ok {
						expected, err = custom.MarshalJSON()
					} else {
						expected, err = protojson.Marshal(message)
					}
					require.NoError(t, err)
					require.JSONEq(t, string(expected), string(envelope.Data))
					if tc.name == "created" || tc.name == "reverted" {
						transactionField := "transaction"
						wantID := "9007199254740993"
						if tc.name == "reverted" {
							transactionField, wantID = "revertTransaction", "9007199254740994"
							require.Equal(t, "9007199254740993", string(wireData["revertedTransactionId"]))
						}
						var tx map[string]json.RawMessage
						require.NoError(t, json.Unmarshal(wireData[transactionField], &tx))
						require.Equal(t, wantID, string(tx["id"]))
						if tc.name == "reverted" {
							require.Equal(t, "9007199254740993", string(tx["revertsTransactionId"]))
						} else {
							require.Equal(t, "9007199254740994", string(tx["revertedByTransactionId"]))
						}
						var postings []map[string]json.RawMessage
						require.NoError(t, json.Unmarshal(tx["postings"], &postings))
						require.Len(t, postings, 1)
						require.Equal(t, "1267650600228229401496703205376", string(postings[0]["amount"]))
						require.Equal(t, `"GOLD"`, string(postings[0]["color"]))
						require.JSONEq(t, `{"users:alice":[{"asset":"USD/2","color":"","input":"123","output":"0"},{"asset":"USD/2","color":"GOLD","input":"1267650600228229401496703205376","output":"0"}]}`, string(tx["postCommitVolumes"]))
					}
					if tc.kind == ledgerpb.SetMetadataLogType || tc.kind == ledgerpb.DeleteMetadataLogType {
						require.Contains(t, wireData, "targetId")
						require.NotContains(t, wireData, "accountId")
						require.NotContains(t, wireData, "transactionId")
						switch tc.name {
						case "saved-zero", "deleted-zero":
							require.Equal(t, "0", string(wireData["targetId"]))
						case "saved-transaction", "deleted-transaction":
							require.Equal(t, "9007199254740993", string(wireData["targetId"]))
						case "saved-account", "deleted-account":
							require.Equal(t, `"users:alice"`, string(wireData["targetId"]))
						}
					}
					if tc.kind == ledgerpb.SetMetadataLogType {
						var values map[string]json.RawMessage
						require.NoError(t, json.Unmarshal(wireData["metadata"], &values))
						require.Equal(t, "18446744073709551615", string(values["count"]))
						require.Equal(t, "-9223372036854775808", string(values["debt"]))
						require.Equal(t, "null", string(values["null"]))
						require.Equal(t, "true", string(values["active"]))
					}
				})
			}
		})
	}
}

// The JSON projection preserves metadata values but cannot reconstruct internal
// provenance that MarshalJSON omits (signed-positive/datetime/null originals).
func TestLedgerLogJSONMetadataProjection(t *testing.T) {
	t.Parallel()
	original := &ledgerpb.LedgerLog{Data: &ledgerpb.LedgerLogPayload{Payload: &ledgerpb.LedgerLogPayload_SavedMetadata{SavedMetadata: &ledgerpb.SavedMetadata{
		Target: &ledgerpb.Target{Target: &ledgerpb.Target_TransactionId{TransactionId: 0}},
		Metadata: map[string]*ledgerpb.MetadataValue{
			"signed": ledgerpb.NewIntValue(42), "datetime": ledgerpb.NewDatetimeValue(1700000000123456), "null": ledgerpb.NewNullValue("invalid original"),
		},
	}}}}
	encoded, err := json.Marshal(original)
	require.NoError(t, err)
	require.JSONEq(t, `{"type":"SET_METADATA","data":{"targetType":"TRANSACTION","targetId":0,"metadata":{"signed":42,"datetime":"2023-11-14T22:13:20.123456Z","null":null}}}`, string(encoded))
}

func TestLedgerLogJSONNullAccountMetadata(t *testing.T) {
	t.Parallel()
	original := &ledgerpb.LedgerLog{Data: &ledgerpb.LedgerLogPayload{Payload: &ledgerpb.LedgerLogPayload_CreatedTransaction{CreatedTransaction: &ledgerpb.CreatedTransaction{
		AccountMetadata: map[string]*ledgerpb.MetadataMap{"nil": nil, "nil-values": {}, "empty": {Values: map[string]*ledgerpb.MetadataValue{}}},
	}}}}
	encoded, err := json.Marshal(original)
	require.NoError(t, err)
	require.JSONEq(t, `{"type":"NEW_TRANSACTION","data":{"accountMetadata":{"nil":null,"nil-values":null,"empty":{}}}}`, string(encoded))
}

func TestMetadataJSONRejectsNilAccount(t *testing.T) {
	t.Parallel()
	target := &ledgerpb.Target{Target: &ledgerpb.Target_Account{}}
	for name, message := range map[string]json.Marshaler{
		"saved":   &ledgerpb.SavedMetadata{Target: target},
		"deleted": &ledgerpb.DeletedMetadata{Target: target},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := message.MarshalJSON()
			require.EqualError(t, err, "missing metadata account target")
		})
	}
}

func TestMetadataJSONRejectsNilTransaction(t *testing.T) {
	t.Parallel()
	target := &ledgerpb.Target{Target: (*ledgerpb.Target_TransactionId)(nil)}
	for name, message := range map[string]json.Marshaler{
		"saved":   &ledgerpb.SavedMetadata{Target: target},
		"deleted": &ledgerpb.DeletedMetadata{Target: target},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := message.MarshalJSON()
			require.EqualError(t, err, "missing metadata transaction target")
		})
	}
}
