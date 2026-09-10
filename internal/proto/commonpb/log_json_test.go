package commonpb_test

import (
	"encoding/json"
	"math"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	ledgerjson "github.com/formancehq/ledger/v3/internal/adapter/json"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestLedgerLogJSONOutput(t *testing.T) {
	t.Parallel()
	timestamp := &commonpb.Timestamp{Data: 1_700_000_000_123_456}
	metadata := map[string]*commonpb.MetadataValue{
		"label": commonpb.NewStringValue("customer"), "active": commonpb.NewBoolValue(true),
		"count": commonpb.NewUintValue(math.MaxUint64), "debt": commonpb.NewIntValue(math.MinInt64),
		"null": commonpb.NewNullValue(""),
	}
	transaction := &commonpb.Transaction{
		Id: 9007199254740993, Reference: "ref-1", Timestamp: timestamp, InsertedAt: timestamp, UpdatedAt: timestamp,
		Reverted: true, RevertedAt: timestamp, RevertedByTransaction: 9007199254740994,
		Metadata: metadata,
		Postings: []*commonpb.Posting{commonpb.NewColoredPosting("world", "users:alice", "USD/2", "GOLD", new(big.Int).Lsh(big.NewInt(1), 100))},
		PostCommitVolumes: &commonpb.PostCommitVolumes{VolumesByAccount: map[string]*commonpb.VolumesByAssets{
			"users:alice": {Volumes: []*commonpb.VolumeEntry{
				{Asset: "USD/2", Color: "", Volumes: &commonpb.Volumes{Input: "123", Output: "0"}},
				{Asset: "USD/2", Color: "GOLD", Volumes: &commonpb.Volumes{Input: "1267650600228229401496703205376", Output: "0"}},
			}},
		}},
	}
	revert := proto.Clone(transaction).(*commonpb.Transaction)
	revert.Id = 9007199254740994
	revert.Reverted = false
	revert.RevertedAt = nil
	revert.RevertedByTransaction = 0
	revert.RevertsTransaction = transaction.GetId()
	type logCase struct {
		name    string
		kind    commonpb.LogType
		payload *commonpb.LedgerLogPayload
	}
	cases := []logCase{
		{"created", commonpb.NewTransactionLogType, &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_CreatedTransaction{CreatedTransaction: &commonpb.CreatedTransaction{Transaction: transaction, AccountMetadata: map[string]*commonpb.MetadataMap{"users:alice": {Values: metadata}}}}}},
		{"reverted", commonpb.RevertedTransactionLogType, &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_RevertedTransaction{RevertedTransaction: &commonpb.RevertedTransaction{RevertedTransactionId: transaction.GetId(), RevertTransaction: revert}}}},
		{"set-field", commonpb.SetMetadataFieldTypeLogType, &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_SetMetadataFieldType{SetMetadataFieldType: &commonpb.SetMetadataFieldTypeLog{TargetType: commonpb.TargetType_TARGET_TYPE_TRANSACTION, Key: "count", Type: commonpb.MetadataType_METADATA_TYPE_UINT64}}}},
		{"remove-field", commonpb.RemovedMetadataFieldTypeLogType, &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_RemovedMetadataFieldType{RemovedMetadataFieldType: &commonpb.RemovedMetadataFieldTypeLog{TargetType: commonpb.TargetType_TARGET_TYPE_TRANSACTION, Key: "count", DroppedIndex: &commonpb.IndexID{Kind: &commonpb.IndexID_Metadata{Metadata: &commonpb.MetadataIndexID{Target: commonpb.TargetType_TARGET_TYPE_TRANSACTION, Key: "count"}}}}}}},
		{"skipped", commonpb.OrderSkippedLogType, &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_OrderSkipped{OrderSkipped: &commonpb.OrderSkippedLog{Reason: commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT, Context: map[string]string{"reference": "ref-1", "existingTransactionId": "9007199254740993"}}}}},
	}
	for name, target := range map[string]*commonpb.Target{
		"zero":        {Target: &commonpb.Target_TransactionId{TransactionId: 0}},
		"account":     {Target: &commonpb.Target_Account{Account: &commonpb.TargetAccount{Addr: "users:alice"}}},
		"transaction": {Target: &commonpb.Target_TransactionId{TransactionId: transaction.GetId()}},
	} {
		cases = append(
			cases,
			logCase{"saved-" + name, commonpb.SetMetadataLogType, &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_SavedMetadata{SavedMetadata: &commonpb.SavedMetadata{Target: target, Metadata: metadata}}}},
			logCase{"deleted-" + name, commonpb.DeleteMetadataLogType, &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_DeletedMetadata{DeletedMetadata: &commonpb.DeletedMetadata{Target: target, Key: "label"}}}},
		)
	}
	for _, tc := range []struct {
		kind commonpb.LogType
		wire string
	}{
		{commonpb.FillGapLogType, `{"fillGap":{"originalId":"18446744073709551615"}}`},
		{commonpb.CreateIndexLogType, `{"createIndex":{"id":{"metadata":{"target":"TARGET_TYPE_TRANSACTION","key":"label"}},"initial":true,"boundType":"METADATA_TYPE_UINT64","boundTypeDeclared":true}}`},
		{commonpb.DropIndexLogType, `{"dropIndex":{"id":{"metadata":{"target":"TARGET_TYPE_ACCOUNT","key":"label"}}}}`},
		{commonpb.AddedAccountTypeLogType, `{"addedAccountType":{"accountType":{"name":"users","pattern":"users:{id}","persistence":"ACCOUNT_TYPE_EPHEMERAL","segmentTypes":{"id":{"uint64":{}}}}}}`},
		{commonpb.RemovedAccountTypeLogType, `{"removedAccountType":{"name":"users"}}`},
		{commonpb.UpdatedDefaultEnforcementModeLogType, `{"updatedDefaultEnforcementMode":{"enforcementMode":"CHART_ENFORCEMENT_AUDIT"}}`},
	} {
		payload := &commonpb.LedgerLogPayload{}
		require.NoError(t, protojson.Unmarshal([]byte(tc.wire), payload))
		field := payload.ProtoReflect().WhichOneof(payload.ProtoReflect().Descriptor().Oneofs().Get(0))
		cases = append(cases, logCase{field.JSONName(), tc.kind, payload})
	}
	covered := map[string]bool{}
	discriminators := map[commonpb.LogType]string{}
	for _, tc := range cases {
		field := tc.payload.ProtoReflect().WhichOneof(tc.payload.ProtoReflect().Descriptor().Oneofs().Get(0))
		covered[string(field.Name())] = true
		if previous, ok := discriminators[tc.kind]; ok {
			require.Equal(t, previous, string(field.Name()), "each payload variant needs a distinct log type")
		}
		discriminators[tc.kind] = string(field.Name())
	}
	require.Len(t, covered, (&commonpb.LedgerLogPayload{}).ProtoReflect().Descriptor().Oneofs().Get(0).Fields().Len(), "extend the matrix for each new variant")
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			original := &commonpb.LedgerLog{Id: 9007199254740995, Date: timestamp, Data: tc.payload}
			for name, codec := range map[string]struct {
				marshal func(any) ([]byte, error)
			}{
				"standard": {json.Marshal}, "sonic": {ledgerjson.Marshal},
			} {
				t.Run(name, func(t *testing.T) {
					encoded, err := codec.marshal(original)
					require.NoError(t, err)
					var envelope struct {
						Type commonpb.LogType `json:"type"`
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
					if tc.kind == commonpb.SetMetadataLogType || tc.kind == commonpb.DeleteMetadataLogType {
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
					if tc.kind == commonpb.SetMetadataLogType {
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
	original := &commonpb.LedgerLog{Data: &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_SavedMetadata{SavedMetadata: &commonpb.SavedMetadata{
		Target: &commonpb.Target{Target: &commonpb.Target_TransactionId{TransactionId: 0}},
		Metadata: map[string]*commonpb.MetadataValue{
			"signed": commonpb.NewIntValue(42), "datetime": commonpb.NewDatetimeValue(1700000000123456), "null": commonpb.NewNullValue("invalid original"),
		},
	}}}}
	encoded, err := json.Marshal(original)
	require.NoError(t, err)
	require.JSONEq(t, `{"type":"SET_METADATA","data":{"targetType":"TRANSACTION","targetId":0,"metadata":{"signed":42,"datetime":"2023-11-14T22:13:20.123456Z","null":null}}}`, string(encoded))
}

func TestLedgerLogJSONNullAccountMetadata(t *testing.T) {
	t.Parallel()
	original := &commonpb.LedgerLog{Data: &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_CreatedTransaction{CreatedTransaction: &commonpb.CreatedTransaction{
		AccountMetadata: map[string]*commonpb.MetadataMap{"nil": nil, "nil-values": {}, "empty": {Values: map[string]*commonpb.MetadataValue{}}},
	}}}}
	encoded, err := json.Marshal(original)
	require.NoError(t, err)
	require.JSONEq(t, `{"type":"NEW_TRANSACTION","data":{"accountMetadata":{"nil":null,"nil-values":null,"empty":{}}}}`, string(encoded))
}
