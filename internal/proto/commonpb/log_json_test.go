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

func TestLedgerLogJSONRoundTrip(t *testing.T) {
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
		"account":     {Target: &commonpb.Target_Account{Account: &commonpb.TargetAccount{Addr: "users:alice"}}},
		"transaction": {Target: &commonpb.Target_TransactionId{TransactionId: transaction.GetId()}},
	} {
		cases = append(
			cases,
			logCase{"saved-" + name, commonpb.SetMetadataLogType, &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_SavedMetadata{SavedMetadata: &commonpb.SavedMetadata{Target: target, Metadata: metadata}}}},
			logCase{"deleted-" + name, commonpb.DeleteMetadataLogType, &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_DeletedMetadata{DeletedMetadata: &commonpb.DeletedMetadata{Target: target, Key: "label"}}}},
		)
	}
	for _, wire := range []string{
		`{"fillGap":{"originalId":"18446744073709551615"}}`,
		`{"createIndex":{"id":{"metadata":{"target":"TARGET_TYPE_TRANSACTION","key":"label"}},"initial":true,"boundType":"METADATA_TYPE_UINT64","boundTypeDeclared":true}}`,
		`{"dropIndex":{"id":{"metadata":{"target":"TARGET_TYPE_ACCOUNT","key":"label"}}}}`,
		`{"addedAccountType":{"accountType":{"name":"users","pattern":"users:{id}","persistence":"ACCOUNT_TYPE_EPHEMERAL","segmentTypes":{"id":{"uint64":{}}}}}}`,
		`{"removedAccountType":{"name":"users"}}`,
		`{"updatedDefaultEnforcementMode":{"enforcementMode":"CHART_ENFORCEMENT_AUDIT"}}`,
	} {
		payload := &commonpb.LedgerLogPayload{}
		require.NoError(t, protojson.Unmarshal([]byte(wire), payload))
		field := payload.ProtoReflect().WhichOneof(payload.ProtoReflect().Descriptor().Oneofs().Get(0))
		cases = append(cases, logCase{field.JSONName(), commonpb.SetMetadataLogType, payload})
	}
	covered := map[string]bool{}
	for _, tc := range cases {
		field := tc.payload.ProtoReflect().WhichOneof(tc.payload.ProtoReflect().Descriptor().Oneofs().Get(0))
		covered[string(field.Name())] = true
	}
	require.Len(t, covered, (&commonpb.LedgerLogPayload{}).ProtoReflect().Descriptor().Oneofs().Get(0).Fields().Len(), "extend the matrix for each new variant")
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			original := &commonpb.LedgerLog{Id: 9007199254740995, Date: timestamp, Data: tc.payload}
			for name, codec := range map[string]struct {
				marshal   func(any) ([]byte, error)
				unmarshal func([]byte, any) error
			}{
				"standard": {json.Marshal, json.Unmarshal}, "sonic": {ledgerjson.Marshal, ledgerjson.Unmarshal},
			} {
				t.Run(name, func(t *testing.T) {
					encoded, err := codec.marshal(original)
					require.NoError(t, err)
					var envelope struct {
						Type commonpb.LogType `json:"type"`
						Data json.RawMessage  `json:"data"`
					}
					require.NoError(t, json.Unmarshal(encoded, &envelope))
					require.Equal(t, tc.kind, envelope.Type)
					var wireData map[string]json.RawMessage
					require.NoError(t, json.Unmarshal(envelope.Data, &wireData))
					if tc.kind == commonpb.OrderSkippedLogType {
						require.Contains(t, wireData, "reason")
						require.NotContains(t, wireData, "orderSkipped")
					} else {
						field := tc.payload.ProtoReflect().WhichOneof(tc.payload.ProtoReflect().Descriptor().Oneofs().Get(0))
						require.Len(t, wireData, 1)
						require.Contains(t, wireData, field.JSONName())
					}
					t.Run("hydrate", func(t *testing.T) {
						hydrated, err := commonpb.HydrateLog(envelope.Type, envelope.Data)
						require.NoError(t, err)
						field := tc.payload.ProtoReflect().WhichOneof(tc.payload.ProtoReflect().Descriptor().Oneofs().Get(0))
						require.True(t, proto.Equal(tc.payload.ProtoReflect().Get(field).Message().Interface(), hydrated), "payload lost: %s", hydrated)
					})
					var decoded commonpb.LedgerLog
					require.NoError(t, codec.unmarshal(encoded, &decoded))
					require.True(t, proto.Equal(original, &decoded), "payload lost: %s", &decoded)
					reencoded, err := codec.marshal(&decoded)
					require.NoError(t, err)
					require.JSONEq(t, string(encoded), string(reencoded))
				})
			}
		})
	}
}

func TestHydrateLogRejectsMalformedPayload(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		kind    commonpb.LogType
		data    string
		message string
	}{
		{"unknown type", 99, `{}`, "unknown log type"},
		{"invalid JSON", commonpb.NewTransactionLogType, `{`, ""},
		{"missing envelope", commonpb.NewTransactionLogType, `{"transaction":{}}`, "unknown field"},
		{"empty envelope", commonpb.NewTransactionLogType, `{}`, "exactly one payload"},
		{"null envelope", commonpb.NewTransactionLogType, `null`, "exactly one payload"},
		{"null payload", commonpb.NewTransactionLogType, `{"createdTransaction":null}`, "is null"},
		{"two variants", commonpb.NewTransactionLogType, `{"createdTransaction":{},"revertedTransaction":{}}`, "exactly one payload"},
		{"wrong discriminator", commonpb.NewTransactionLogType, `{"revertedTransaction":{}}`, "does not match"},
		{"wrapped skip", commonpb.OrderSkippedLogType, `{"orderSkipped":{"reason":"TRANSACTION_REFERENCE_CONFLICT"}}`, "must contain a reason"},
		{"invalid skip reason", commonpb.OrderSkippedLogType, `{"reason":"INVALID"}`, "unknown ErrorReason"},
		{"unknown target", commonpb.SetMetadataLogType, `{"savedMetadata":{"targetType":"UNKNOWN"}}`, "unknown type"},
		{"bad target ID", commonpb.DeleteMetadataLogType, `{"deletedMetadata":{"targetType":"TRANSACTION","transactionId":"x"}}`, ""},
		{"bad schema enum", commonpb.SetMetadataFieldTypeLogType, `{"setMetadataFieldType":{"type":"NOT_A_TYPE"}}`, ""},
		{"bad nested index", commonpb.RemovedMetadataFieldTypeLogType, `{"removedMetadataFieldType":{"droppedIndex":{"unknown":true}}}`, "unknown field"},
		{"metadata overflow", commonpb.SetMetadataLogType, `{"savedMetadata":{"targetType":"ACCOUNT","accountId":"a","metadata":{"n":18446744073709551616}}}`, "metadata key"},
		{"metadata fractional", commonpb.NewTransactionLogType, `{"createdTransaction":{"accountMetadata":{"a":{"n":1.5}}}}`, "metadata key"},
		{"metadata object", commonpb.NewTransactionLogType, `{"createdTransaction":{"transaction":{"metadata":{"n":{}}}}}`, "metadata key"},
		{"bad volumes", commonpb.RevertedTransactionLogType, `{"revertedTransaction":{"revertTransaction":{"postCommitVolumes":{"a":{}}}}}`, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			value, err := commonpb.HydrateLog(tc.kind, []byte(tc.data))
			require.Error(t, err)
			require.Nil(t, value)
			if tc.message != "" {
				require.ErrorContains(t, err, tc.message)
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
	var decoded commonpb.LedgerLog
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	reencoded, err := json.Marshal(&decoded)
	require.NoError(t, err)
	require.JSONEq(t, string(encoded), string(reencoded))
	require.Equal(t, uint64(42), decoded.GetData().GetSavedMetadata().GetMetadata()["signed"].GetUintValue())
	require.Equal(t, "2023-11-14T22:13:20.123456Z", decoded.GetData().GetSavedMetadata().GetMetadata()["datetime"].GetStringValue())
	require.Contains(t, decoded.GetData().GetSavedMetadata().GetMetadata(), "null")
}

func TestLedgerLogJSONNullAccountMetadata(t *testing.T) {
	t.Parallel()
	original := &commonpb.LedgerLog{Data: &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_CreatedTransaction{CreatedTransaction: &commonpb.CreatedTransaction{
		AccountMetadata: map[string]*commonpb.MetadataMap{"nil": nil, "nil-values": {}, "empty": {Values: map[string]*commonpb.MetadataValue{}}},
	}}}}
	encoded, err := json.Marshal(original)
	require.NoError(t, err)
	var decoded commonpb.LedgerLog
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	reencoded, err := json.Marshal(&decoded)
	require.NoError(t, err)
	require.JSONEq(t, string(encoded), string(reencoded))
}
