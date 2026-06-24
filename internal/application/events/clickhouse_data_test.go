//go:build clickhouse

package events

import (
	"encoding/hex"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/eventspb"
)

func TestEventToClickHouseJSON_NilLog(t *testing.T) {
	t.Parallel()

	event := &eventspb.Event{
		Type:        commonpb.EventType_COMMITTED_TRANSACTION,
		Ledger:      "test",
		LogSequence: 1,
		Log:         nil,
	}

	data, err := eventToSinkJSON(event)
	require.NoError(t, err)
	require.NotEmpty(t, data)

	var result sinkEventData
	require.NoError(t, json.Unmarshal(data, &result))
}

func TestEventToClickHouseJSON_NilPayload(t *testing.T) {
	t.Parallel()

	event := &eventspb.Event{
		Type:        commonpb.EventType_COMMITTED_TRANSACTION,
		Ledger:      "test",
		LogSequence: 1,
		Log: &commonpb.Log{
			Sequence: 1,
		},
	}

	data, err := eventToSinkJSON(event)
	require.NoError(t, err)

	var result sinkEventData
	require.NoError(t, json.Unmarshal(data, &result))
}

func TestEventToClickHouseJSON_CreateLedger(t *testing.T) {
	t.Parallel()

	event := &eventspb.Event{
		Type:        commonpb.EventType_CREATED_LEDGER,
		Ledger:      "orders",
		LogSequence: 1,
		Log: &commonpb.Log{
			Sequence: 1,
			Payload: &commonpb.LogPayload{
				Type: &commonpb.LogPayload_CreateLedger{
					CreateLedger: &commonpb.CreatedLedgerLog{
						Name: "orders",
					},
				},
			},
		},
	}

	data, err := eventToSinkJSON(event)
	require.NoError(t, err)

	var result sinkEventData
	require.NoError(t, json.Unmarshal(data, &result))
	require.NotNil(t, result.LedgerName)
	require.Equal(t, "orders", *result.LedgerName)
}

func TestEventToClickHouseJSON_DeleteLedger(t *testing.T) {
	t.Parallel()

	event := &eventspb.Event{
		Type:        commonpb.EventType_DELETED_LEDGER,
		Ledger:      "old-ledger",
		LogSequence: 2,
		Log: &commonpb.Log{
			Sequence: 2,
			Payload: &commonpb.LogPayload{
				Type: &commonpb.LogPayload_DeleteLedger{
					DeleteLedger: &commonpb.DeletedLedgerLog{
						Name: "old-ledger",
					},
				},
			},
		},
	}

	data, err := eventToSinkJSON(event)
	require.NoError(t, err)

	var result sinkEventData
	require.NoError(t, json.Unmarshal(data, &result))
	require.NotNil(t, result.LedgerName)
	require.Equal(t, "old-ledger", *result.LedgerName)
}

func TestEventToClickHouseJSON_CommittedTransaction(t *testing.T) {
	t.Parallel()

	event := &eventspb.Event{
		Type:        commonpb.EventType_COMMITTED_TRANSACTION,
		Ledger:      "payments",
		LogSequence: 3,
		Log: &commonpb.Log{
			Sequence: 3,
			Payload: &commonpb.LogPayload{
				Type: &commonpb.LogPayload_Apply{
					Apply: &commonpb.ApplyLedgerLog{
						LedgerName: "payments",
						Log: &commonpb.LedgerLog{
							Id:   1,
							Date: &commonpb.Timestamp{Data: 1700000100},
							Data: &commonpb.LedgerLogPayload{
								Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
									CreatedTransaction: &commonpb.CreatedTransaction{
										Transaction: &commonpb.Transaction{
											Id:        1,
											Timestamp: &commonpb.Timestamp{Data: 1700000100},
											Postings: []*commonpb.Posting{
												{
													Source:      "world",
													Destination: "users:001",
													Amount:      commonpb.NewUint256FromUint64(500),
													Asset:       "USD/2",
												},
											},
											Metadata: map[string]*commonpb.MetadataValue{
												"type": commonpb.NewStringValue("transfer"),
											},
											Reference:  "tx-001",
											InsertedAt: &commonpb.Timestamp{Data: 1700000100},
										},
										AccountMetadata: map[string]*commonpb.MetadataMap{
											"users:001": {
												Values: map[string]*commonpb.MetadataValue{
													"name": commonpb.NewStringValue("Alice"),
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	data, err := eventToSinkJSON(event)
	require.NoError(t, err)

	// Use map[string]any because sinkTime has no UnmarshalJSON
	var result map[string]any
	require.NoError(t, json.Unmarshal(data, &result))
	require.NotNil(t, result["transaction"])

	tx := result["transaction"].(map[string]any)
	require.Equal(t, float64(1), tx["id"])
	postings := tx["postings"].([]any)
	require.Len(t, postings, 1)
	p := postings[0].(map[string]any)
	require.Equal(t, "world", p["source"])
	require.Equal(t, "users:001", p["destination"])
	require.Equal(t, "USD/2", p["asset"])

	meta := tx["metadata"].(map[string]any)
	require.Equal(t, "transfer", meta["type"])

	acctMeta := result["accountMetadata"].(map[string]any)
	userMeta := acctMeta["users:001"].(map[string]any)
	require.Equal(t, "Alice", userMeta["name"])
}

func TestEventToClickHouseJSON_RevertedTransaction(t *testing.T) {
	t.Parallel()

	event := &eventspb.Event{
		Type:        commonpb.EventType_REVERTED_TRANSACTION,
		Ledger:      "payments",
		LogSequence: 4,
		Log: &commonpb.Log{
			Sequence: 4,
			Payload: &commonpb.LogPayload{
				Type: &commonpb.LogPayload_Apply{
					Apply: &commonpb.ApplyLedgerLog{
						LedgerName: "payments",
						Log: &commonpb.LedgerLog{
							Id:   2,
							Date: &commonpb.Timestamp{Data: 1700000200},
							Data: &commonpb.LedgerLogPayload{
								Payload: &commonpb.LedgerLogPayload_RevertedTransaction{
									RevertedTransaction: &commonpb.RevertedTransaction{
										RevertedTransactionId: 1,
										RevertTransaction: &commonpb.Transaction{
											Id:        2,
											Timestamp: &commonpb.Timestamp{Data: 1700000200},
											Postings: []*commonpb.Posting{
												{
													Source:      "users:001",
													Destination: "world",
													Amount:      commonpb.NewUint256FromUint64(500),
													Asset:       "USD/2",
												},
											},
											InsertedAt: &commonpb.Timestamp{Data: 1700000200},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	data, err := eventToSinkJSON(event)
	require.NoError(t, err)

	var result map[string]any
	require.NoError(t, json.Unmarshal(data, &result))
	require.Equal(t, float64(1), result["revertedTransactionId"])

	revertTx := result["revertTransaction"].(map[string]any)
	require.Equal(t, float64(2), revertTx["id"])
}

func TestEventToClickHouseJSON_SavedMetadata_Account(t *testing.T) {
	t.Parallel()

	event := &eventspb.Event{
		Type:        commonpb.EventType_SAVED_METADATA,
		Ledger:      "orders",
		LogSequence: 5,
		Log: &commonpb.Log{
			Sequence: 5,
			Payload: &commonpb.LogPayload{
				Type: &commonpb.LogPayload_Apply{
					Apply: &commonpb.ApplyLedgerLog{
						LedgerName: "orders",
						Log: &commonpb.LedgerLog{
							Id:   3,
							Date: &commonpb.Timestamp{Data: 1700000300},
							Data: &commonpb.LedgerLogPayload{
								Payload: &commonpb.LedgerLogPayload_SavedMetadata{
									SavedMetadata: &commonpb.SavedMetadata{
										Target: &commonpb.Target{
											Target: &commonpb.Target_Account{
												Account: &commonpb.TargetAccount{Addr: "user:123"},
											},
										},
										Metadata: map[string]*commonpb.MetadataValue{
											"status": commonpb.NewStringValue("active"),
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	data, err := eventToSinkJSON(event)
	require.NoError(t, err)

	var result sinkEventData
	require.NoError(t, json.Unmarshal(data, &result))
	require.NotNil(t, result.TargetType)
	require.Equal(t, "account", *result.TargetType)
	require.Equal(t, "user:123", result.TargetID)
	require.NotNil(t, result.Metadata)
	require.Equal(t, "active", result.Metadata["status"])
}

func TestEventToClickHouseJSON_DeletedMetadata_Transaction(t *testing.T) {
	t.Parallel()

	event := &eventspb.Event{
		Type:        commonpb.EventType_DELETED_METADATA,
		Ledger:      "orders",
		LogSequence: 6,
		Log: &commonpb.Log{
			Sequence: 6,
			Payload: &commonpb.LogPayload{
				Type: &commonpb.LogPayload_Apply{
					Apply: &commonpb.ApplyLedgerLog{
						LedgerName: "orders",
						Log: &commonpb.LedgerLog{
							Id:   4,
							Date: &commonpb.Timestamp{Data: 1700000400},
							Data: &commonpb.LedgerLogPayload{
								Payload: &commonpb.LedgerLogPayload_DeletedMetadata{
									DeletedMetadata: &commonpb.DeletedMetadata{
										Target: &commonpb.Target{
											Target: &commonpb.Target_TransactionId{TransactionId: 42},
										},
										Key: "some-key",
									},
								},
							},
						},
					},
				},
			},
		},
	}

	data, err := eventToSinkJSON(event)
	require.NoError(t, err)

	var result sinkEventData
	require.NoError(t, json.Unmarshal(data, &result))
	require.NotNil(t, result.TargetType)
	require.Equal(t, "transaction", *result.TargetType)
	require.NotNil(t, result.Key)
	require.Equal(t, "some-key", *result.Key)
}

func TestEventToClickHouseJSON_RegisterSigningKey(t *testing.T) {
	t.Parallel()

	event := &eventspb.Event{
		Type:        commonpb.EventType_EVENT_TYPE_UNSPECIFIED,
		LogSequence: 7,
		Log: &commonpb.Log{
			Sequence: 7,
			Payload: &commonpb.LogPayload{
				Type: &commonpb.LogPayload_RegisterSigningKey{
					RegisterSigningKey: &commonpb.RegisteredSigningKeyLog{
						KeyId:     "key-001",
						PublicKey: []byte{0xab, 0xcd},
					},
				},
			},
		},
	}

	data, err := eventToSinkJSON(event)
	require.NoError(t, err)

	var result sinkEventData
	require.NoError(t, json.Unmarshal(data, &result))
	require.NotNil(t, result.SigningKeyID)
	require.Equal(t, "key-001", *result.SigningKeyID)
	require.NotNil(t, result.PublicKey)
	require.Equal(t, hex.EncodeToString([]byte{0xab, 0xcd}), *result.PublicKey)
}

func TestEventToClickHouseJSON_RevokeSigningKey(t *testing.T) {
	t.Parallel()

	event := &eventspb.Event{
		Type:        commonpb.EventType_EVENT_TYPE_UNSPECIFIED,
		LogSequence: 8,
		Log: &commonpb.Log{
			Sequence: 8,
			Payload: &commonpb.LogPayload{
				Type: &commonpb.LogPayload_RevokeSigningKey{
					RevokeSigningKey: &commonpb.RevokedSigningKeyLog{
						KeyId: "key-001",
					},
				},
			},
		},
	}

	data, err := eventToSinkJSON(event)
	require.NoError(t, err)

	var result sinkEventData
	require.NoError(t, json.Unmarshal(data, &result))
	require.NotNil(t, result.SigningKeyID)
	require.Equal(t, "key-001", *result.SigningKeyID)
}

func TestEventToClickHouseJSON_SetSigningConfig(t *testing.T) {
	t.Parallel()

	event := &eventspb.Event{
		Type:        commonpb.EventType_EVENT_TYPE_UNSPECIFIED,
		LogSequence: 9,
		Log: &commonpb.Log{
			Sequence: 9,
			Payload: &commonpb.LogPayload{
				Type: &commonpb.LogPayload_SetSigningConfig{
					SetSigningConfig: &commonpb.SetSigningConfigLog{
						RequireSignatures: true,
					},
				},
			},
		},
	}

	data, err := eventToSinkJSON(event)
	require.NoError(t, err)

	var result sinkEventData
	require.NoError(t, json.Unmarshal(data, &result))
	require.NotNil(t, result.RequireSignatures)
	require.True(t, *result.RequireSignatures)
}

func TestEventToClickHouseJSON_AddedEventsSink(t *testing.T) {
	t.Parallel()

	event := &eventspb.Event{
		Type:        commonpb.EventType_EVENT_TYPE_UNSPECIFIED,
		LogSequence: 10,
		Log: &commonpb.Log{
			Sequence: 10,
			Payload: &commonpb.LogPayload{
				Type: &commonpb.LogPayload_AddedEventsSink{
					AddedEventsSink: &commonpb.AddedEventsSinkLog{
						Config: &commonpb.SinkConfig{
							Name: "my-sink",
						},
					},
				},
			},
		},
	}

	data, err := eventToSinkJSON(event)
	require.NoError(t, err)

	var result sinkEventData
	require.NoError(t, json.Unmarshal(data, &result))
	require.NotNil(t, result.SinkName)
	require.Equal(t, "my-sink", *result.SinkName)
}

func TestEventToClickHouseJSON_RemovedEventsSink(t *testing.T) {
	t.Parallel()

	event := &eventspb.Event{
		Type:        commonpb.EventType_EVENT_TYPE_UNSPECIFIED,
		LogSequence: 11,
		Log: &commonpb.Log{
			Sequence: 11,
			Payload: &commonpb.LogPayload{
				Type: &commonpb.LogPayload_RemovedEventsSink{
					RemovedEventsSink: &commonpb.RemovedEventsSinkLog{
						Name: "my-sink",
					},
				},
			},
		},
	}

	data, err := eventToSinkJSON(event)
	require.NoError(t, err)

	var result sinkEventData
	require.NoError(t, json.Unmarshal(data, &result))
	require.NotNil(t, result.SinkName)
	require.Equal(t, "my-sink", *result.SinkName)
}

func TestSinkPopulateApply_NilApply(t *testing.T) {
	t.Parallel()

	data := &sinkEventData{}
	sinkPopulateApply(data, nil)
	require.Nil(t, data.Transaction)
}

func TestSinkPopulateApply_NilLog(t *testing.T) {
	t.Parallel()

	data := &sinkEventData{}
	sinkPopulateApply(data, &commonpb.ApplyLedgerLog{Log: nil})
	require.Nil(t, data.Transaction)
}

func TestSinkPopulateApply_NilData(t *testing.T) {
	t.Parallel()

	data := &sinkEventData{}
	sinkPopulateApply(data, &commonpb.ApplyLedgerLog{
		Log: &commonpb.LedgerLog{Data: nil},
	})
	require.Nil(t, data.Transaction)
}

func TestSinkPopulateApply_SchemaOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		payload *commonpb.LedgerLogPayload
	}{
		{
			name: "set_metadata_field_type",
			payload: &commonpb.LedgerLogPayload{
				Payload: &commonpb.LedgerLogPayload_SetMetadataFieldType{
					SetMetadataFieldType: &commonpb.SetMetadataFieldTypeLog{
						TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
						Key:        "age",
						Type:       commonpb.MetadataType_METADATA_TYPE_INT64,
					},
				},
			},
		},
		{
			name: "removed_metadata_field_type",
			payload: &commonpb.LedgerLogPayload{
				Payload: &commonpb.LedgerLogPayload_RemovedMetadataFieldType{
					RemovedMetadataFieldType: &commonpb.RemovedMetadataFieldTypeLog{
						TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
						Key:        "age",
					},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			data := &sinkEventData{}
			sinkPopulateApply(data, &commonpb.ApplyLedgerLog{
				Log: &commonpb.LedgerLog{Data: tc.payload},
			})
			require.Nil(t, data.Transaction)
			require.Nil(t, data.TargetType)
		})
	}
}

func TestSinkConvertTarget_Nil(t *testing.T) {
	t.Parallel()

	tt, id := sinkConvertTarget(nil)
	require.Nil(t, tt)
	require.Nil(t, id)
}

func TestSinkConvertTarget_Account(t *testing.T) {
	t.Parallel()

	target := &commonpb.Target{
		Target: &commonpb.Target_Account{
			Account: &commonpb.TargetAccount{Addr: "user:123"},
		},
	}

	tt, id := sinkConvertTarget(target)
	require.NotNil(t, tt)
	require.Equal(t, "account", *tt)
	require.Equal(t, "user:123", id)
}

func TestSinkConvertTarget_Transaction(t *testing.T) {
	t.Parallel()

	target := &commonpb.Target{
		Target: &commonpb.Target_TransactionId{TransactionId: 42},
	}

	tt, id := sinkConvertTarget(target)
	require.NotNil(t, tt)
	require.Equal(t, "transaction", *tt)
	require.Equal(t, uint64(42), id)
}

func TestSinkConvertMetadata_Nil(t *testing.T) {
	t.Parallel()

	result := sinkConvertMetadata(nil)
	require.Nil(t, result)
}

func TestSinkConvertMetadata_Empty(t *testing.T) {
	t.Parallel()

	result := sinkConvertMetadata(map[string]*commonpb.MetadataValue{})
	require.Nil(t, result)
}

func TestSinkConvertMetadata_WithValues(t *testing.T) {
	t.Parallel()

	ms := map[string]*commonpb.MetadataValue{
		"status": commonpb.NewStringValue("active"),
		"empty":  nil,
	}

	result := sinkConvertMetadata(ms)
	require.NotNil(t, result)
	require.Equal(t, "active", result["status"])
	// nil values are skipped
	_, hasEmpty := result["empty"]
	require.False(t, hasEmpty)
}

func TestSinkConvertAccountMetadataMap_Nil(t *testing.T) {
	t.Parallel()

	result := sinkConvertAccountMetadataMap(nil)
	require.Nil(t, result)
}

func TestSinkConvertAccountMetadataMap_WithValues(t *testing.T) {
	t.Parallel()

	am := map[string]*commonpb.MetadataMap{
		"user:123": {
			Values: map[string]*commonpb.MetadataValue{
				"name": commonpb.NewStringValue("Alice"),
			},
		},
	}

	result := sinkConvertAccountMetadataMap(am)
	require.NotNil(t, result)
	require.Equal(t, "Alice", result["user:123"]["name"])
}

func TestSinkConvertTransaction_Nil(t *testing.T) {
	t.Parallel()

	result := sinkConvertTransaction(nil)
	require.Nil(t, result)
}

func TestClickHouseCreateTableDDL(t *testing.T) {
	t.Parallel()

	ddl := ClickHouseCreateTableDDL("test_events")
	require.Contains(t, ddl, "CREATE TABLE IF NOT EXISTS test_events")
	require.Contains(t, ddl, "log_sequence UInt64")
	require.Contains(t, ddl, "MergeTree()")
}

func TestSinkTime_MarshalJSON(t *testing.T) {
	t.Parallel()

	// 2023-11-14 22:13:20 UTC (timestamp 1700000000)
	ts := &commonpb.Timestamp{Data: 1700000000}
	goTime := ts.AsTime().Time
	ct := sinkTime(goTime)

	data, err := ct.MarshalJSON()
	require.NoError(t, err)
	require.NotEmpty(t, data)
	// Should be quoted string in sink datetime format
	require.Contains(t, string(data), "\"")
}
