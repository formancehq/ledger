//go:build clickhouse

package events

import (
	"encoding/hex"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"

	"github.com/formancehq/ledger/v3/internal/proto/eventspb"
)

func TestEventToClickHouseJSON_NilLog(t *testing.T) {
	t.Parallel()

	event := &eventspb.Event{
		Type:        ledgerpb.EventType_COMMITTED_TRANSACTION,
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
		Type:        ledgerpb.EventType_COMMITTED_TRANSACTION,
		Ledger:      "test",
		LogSequence: 1,
		Log: &ledgerpb.Log{
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
		Type:        ledgerpb.EventType_CREATED_LEDGER,
		Ledger:      "orders",
		LogSequence: 1,
		Log: &ledgerpb.Log{
			Sequence: 1,
			Payload: &ledgerpb.LogPayload{
				Type: &ledgerpb.LogPayload_CreateLedger{
					CreateLedger: &ledgerpb.CreatedLedgerLog{
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
		Type:        ledgerpb.EventType_DELETED_LEDGER,
		Ledger:      "old-ledger",
		LogSequence: 2,
		Log: &ledgerpb.Log{
			Sequence: 2,
			Payload: &ledgerpb.LogPayload{
				Type: &ledgerpb.LogPayload_DeleteLedger{
					DeleteLedger: &ledgerpb.DeletedLedgerLog{
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
		Type:        ledgerpb.EventType_COMMITTED_TRANSACTION,
		Ledger:      "payments",
		LogSequence: 3,
		Log: &ledgerpb.Log{
			Sequence: 3,
			Payload: &ledgerpb.LogPayload{
				Type: &ledgerpb.LogPayload_Apply{
					Apply: &ledgerpb.ApplyLedgerLog{
						LedgerName: "payments",
						Log: &ledgerpb.LedgerLog{
							Id:   1,
							Date: &ledgerpb.Timestamp{Data: 1700000100},
							Data: &ledgerpb.LedgerLogPayload{
								Payload: &ledgerpb.LedgerLogPayload_CreatedTransaction{
									CreatedTransaction: &ledgerpb.CreatedTransaction{
										Transaction: &ledgerpb.Transaction{
											Id:        1,
											Timestamp: &ledgerpb.Timestamp{Data: 1700000100},
											Postings: []*ledgerpb.Posting{
												{
													Source:      "world",
													Destination: "users:001",
													Amount:      ledgerpb.NewUint256FromUint64(500),
													Asset:       "USD/2",
												},
											},
											Metadata: map[string]*ledgerpb.MetadataValue{
												"type": ledgerpb.NewStringValue("transfer"),
											},
											Reference:  "tx-001",
											InsertedAt: &ledgerpb.Timestamp{Data: 1700000100},
										},
										AccountMetadata: map[string]*ledgerpb.MetadataMap{
											"users:001": {
												Values: map[string]*ledgerpb.MetadataValue{
													"name": ledgerpb.NewStringValue("Alice"),
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
		Type:        ledgerpb.EventType_REVERTED_TRANSACTION,
		Ledger:      "payments",
		LogSequence: 4,
		Log: &ledgerpb.Log{
			Sequence: 4,
			Payload: &ledgerpb.LogPayload{
				Type: &ledgerpb.LogPayload_Apply{
					Apply: &ledgerpb.ApplyLedgerLog{
						LedgerName: "payments",
						Log: &ledgerpb.LedgerLog{
							Id:   2,
							Date: &ledgerpb.Timestamp{Data: 1700000200},
							Data: &ledgerpb.LedgerLogPayload{
								Payload: &ledgerpb.LedgerLogPayload_RevertedTransaction{
									RevertedTransaction: &ledgerpb.RevertedTransaction{
										RevertedTransactionId: 1,
										RevertTransaction: &ledgerpb.Transaction{
											Id:        2,
											Timestamp: &ledgerpb.Timestamp{Data: 1700000200},
											Postings: []*ledgerpb.Posting{
												{
													Source:      "users:001",
													Destination: "world",
													Amount:      ledgerpb.NewUint256FromUint64(500),
													Asset:       "USD/2",
												},
											},
											InsertedAt: &ledgerpb.Timestamp{Data: 1700000200},
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
		Type:        ledgerpb.EventType_SAVED_METADATA,
		Ledger:      "orders",
		LogSequence: 5,
		Log: &ledgerpb.Log{
			Sequence: 5,
			Payload: &ledgerpb.LogPayload{
				Type: &ledgerpb.LogPayload_Apply{
					Apply: &ledgerpb.ApplyLedgerLog{
						LedgerName: "orders",
						Log: &ledgerpb.LedgerLog{
							Id:   3,
							Date: &ledgerpb.Timestamp{Data: 1700000300},
							Data: &ledgerpb.LedgerLogPayload{
								Payload: &ledgerpb.LedgerLogPayload_SavedMetadata{
									SavedMetadata: &ledgerpb.SavedMetadata{
										Target: &ledgerpb.Target{
											Target: &ledgerpb.Target_Account{
												Account: &ledgerpb.TargetAccount{Addr: "user:123"},
											},
										},
										Metadata: map[string]*ledgerpb.MetadataValue{
											"status": ledgerpb.NewStringValue("active"),
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
		Type:        ledgerpb.EventType_DELETED_METADATA,
		Ledger:      "orders",
		LogSequence: 6,
		Log: &ledgerpb.Log{
			Sequence: 6,
			Payload: &ledgerpb.LogPayload{
				Type: &ledgerpb.LogPayload_Apply{
					Apply: &ledgerpb.ApplyLedgerLog{
						LedgerName: "orders",
						Log: &ledgerpb.LedgerLog{
							Id:   4,
							Date: &ledgerpb.Timestamp{Data: 1700000400},
							Data: &ledgerpb.LedgerLogPayload{
								Payload: &ledgerpb.LedgerLogPayload_DeletedMetadata{
									DeletedMetadata: &ledgerpb.DeletedMetadata{
										Target: &ledgerpb.Target{
											Target: &ledgerpb.Target_TransactionId{TransactionId: 42},
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

func TestEventToClickHouseJSON_OrderSkipped(t *testing.T) {
	t.Parallel()

	event := &eventspb.Event{
		Type:        ledgerpb.EventType_SKIPPED_ORDER,
		Ledger:      "orders",
		LogSequence: 7,
		Log: &ledgerpb.Log{
			Sequence: 7,
			Payload: &ledgerpb.LogPayload{
				Type: &ledgerpb.LogPayload_Apply{
					Apply: &ledgerpb.ApplyLedgerLog{
						LedgerName: "orders",
						Log: &ledgerpb.LedgerLog{
							Id:   5,
							Date: &ledgerpb.Timestamp{Data: 1700000500},
							Data: &ledgerpb.LedgerLogPayload{
								Payload: &ledgerpb.LedgerLogPayload_OrderSkipped{
									OrderSkipped: &ledgerpb.OrderSkippedLog{
										Reason:  ledgerpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT,
										Context: map[string]string{"reference": "ref-1"},
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
	require.NotNil(t, result.SkippedReason)
	// The sink must expose the short public identifier that callers submit in
	// skippableReasons, NOT the generated enum name (ERROR_REASON_...).
	require.Equal(t, "TRANSACTION_REFERENCE_CONFLICT", *result.SkippedReason)
	require.Equal(t, map[string]string{"reference": "ref-1"}, result.SkippedContext)
}

func TestEventToClickHouseJSON_RegisterSigningKey(t *testing.T) {
	t.Parallel()

	event := &eventspb.Event{
		Type:        ledgerpb.EventType_EVENT_TYPE_UNSPECIFIED,
		LogSequence: 7,
		Log: &ledgerpb.Log{
			Sequence: 7,
			Payload: &ledgerpb.LogPayload{
				Type: &ledgerpb.LogPayload_RegisterSigningKey{
					RegisterSigningKey: &ledgerpb.RegisteredSigningKeyLog{
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
		Type:        ledgerpb.EventType_EVENT_TYPE_UNSPECIFIED,
		LogSequence: 8,
		Log: &ledgerpb.Log{
			Sequence: 8,
			Payload: &ledgerpb.LogPayload{
				Type: &ledgerpb.LogPayload_RevokeSigningKey{
					RevokeSigningKey: &ledgerpb.RevokedSigningKeyLog{
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
		Type:        ledgerpb.EventType_EVENT_TYPE_UNSPECIFIED,
		LogSequence: 9,
		Log: &ledgerpb.Log{
			Sequence: 9,
			Payload: &ledgerpb.LogPayload{
				Type: &ledgerpb.LogPayload_SetSigningConfig{
					SetSigningConfig: &ledgerpb.SetSigningConfigLog{
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
		Type:        ledgerpb.EventType_EVENT_TYPE_UNSPECIFIED,
		LogSequence: 10,
		Log: &ledgerpb.Log{
			Sequence: 10,
			Payload: &ledgerpb.LogPayload{
				Type: &ledgerpb.LogPayload_AddedEventsSink{
					AddedEventsSink: &ledgerpb.AddedEventsSinkLog{
						Config: &ledgerpb.SinkConfig{
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
		Type:        ledgerpb.EventType_EVENT_TYPE_UNSPECIFIED,
		LogSequence: 11,
		Log: &ledgerpb.Log{
			Sequence: 11,
			Payload: &ledgerpb.LogPayload{
				Type: &ledgerpb.LogPayload_RemovedEventsSink{
					RemovedEventsSink: &ledgerpb.RemovedEventsSinkLog{
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
	sinkPopulateApply(data, &ledgerpb.ApplyLedgerLog{Log: nil})
	require.Nil(t, data.Transaction)
}

func TestSinkPopulateApply_NilData(t *testing.T) {
	t.Parallel()

	data := &sinkEventData{}
	sinkPopulateApply(data, &ledgerpb.ApplyLedgerLog{
		Log: &ledgerpb.LedgerLog{Data: nil},
	})
	require.Nil(t, data.Transaction)
}

func TestSinkPopulateApply_SchemaOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		payload *ledgerpb.LedgerLogPayload
	}{
		{
			name: "set_metadata_field_type",
			payload: &ledgerpb.LedgerLogPayload{
				Payload: &ledgerpb.LedgerLogPayload_SetMetadataFieldType{
					SetMetadataFieldType: &ledgerpb.SetMetadataFieldTypeLog{
						TargetType: ledgerpb.TargetType_TARGET_TYPE_ACCOUNT,
						Key:        "age",
						Type:       ledgerpb.MetadataType_METADATA_TYPE_INT64,
					},
				},
			},
		},
		{
			name: "removed_metadata_field_type",
			payload: &ledgerpb.LedgerLogPayload{
				Payload: &ledgerpb.LedgerLogPayload_RemovedMetadataFieldType{
					RemovedMetadataFieldType: &ledgerpb.RemovedMetadataFieldTypeLog{
						TargetType: ledgerpb.TargetType_TARGET_TYPE_ACCOUNT,
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
			sinkPopulateApply(data, &ledgerpb.ApplyLedgerLog{
				Log: &ledgerpb.LedgerLog{Data: tc.payload},
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

	target := &ledgerpb.Target{
		Target: &ledgerpb.Target_Account{
			Account: &ledgerpb.TargetAccount{Addr: "user:123"},
		},
	}

	tt, id := sinkConvertTarget(target)
	require.NotNil(t, tt)
	require.Equal(t, "account", *tt)
	require.Equal(t, "user:123", id)
}

func TestSinkConvertTarget_Transaction(t *testing.T) {
	t.Parallel()

	target := &ledgerpb.Target{
		Target: &ledgerpb.Target_TransactionId{TransactionId: 42},
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

	result := sinkConvertMetadata(map[string]*ledgerpb.MetadataValue{})
	require.Nil(t, result)
}

func TestSinkConvertMetadata_WithValues(t *testing.T) {
	t.Parallel()

	ms := map[string]*ledgerpb.MetadataValue{
		"status": ledgerpb.NewStringValue("active"),
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

	am := map[string]*ledgerpb.MetadataMap{
		"user:123": {
			Values: map[string]*ledgerpb.MetadataValue{
				"name": ledgerpb.NewStringValue("Alice"),
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

// TestSinkConvertTransaction_PreservesColor pins that the color dimension
// reaches every analytical sink (ClickHouse/Databricks/Kafka/NATS share this
// converter). Without this guarantee, downstream warehouses cannot reconstruct
// the segregated balance history from emitted events.
func TestSinkConvertTransaction_PreservesColor(t *testing.T) {
	t.Parallel()

	tx := &ledgerpb.Transaction{
		Id: 1,
		Postings: []*ledgerpb.Posting{
			{
				Source:      "world",
				Destination: "alice",
				Asset:       "USD/2",
				Color:       "GRANTS",
				Amount:      ledgerpb.NewUint256FromUint64(100),
			},
		},
		Timestamp:  &ledgerpb.Timestamp{Data: 1700000000},
		InsertedAt: &ledgerpb.Timestamp{Data: 1700000000},
	}

	result := sinkConvertTransaction(tx)
	require.NotNil(t, result)
	require.Len(t, result.Postings, 1)
	require.Equal(t, "GRANTS", result.Postings[0].Color,
		"color must flow through to the analytical-sink payload")
}

// TestSinkPosting_AlwaysEmitsColor pins the analytical-sink JSON contract:
// the uncolored bucket must serialize as `color:""` (not be omitted), so
// downstream warehouses can distinguish a NULL color from a pre-color schema
// row. Mirrors the contract enforced by ledgerpb.Posting.MarshalJSON.
func TestSinkPosting_AlwaysEmitsColor(t *testing.T) {
	t.Parallel()

	tx := &ledgerpb.Transaction{
		Id: 1,
		Postings: []*ledgerpb.Posting{
			{
				Source:      "world",
				Destination: "alice",
				Asset:       "USD/2",
				Amount:      ledgerpb.NewUint256FromUint64(100),
				// Color intentionally left empty — uncolored bucket.
			},
		},
		Timestamp:  &ledgerpb.Timestamp{Data: 1700000000},
		InsertedAt: &ledgerpb.Timestamp{Data: 1700000000},
	}

	sink := sinkConvertTransaction(tx)
	require.NotNil(t, sink)

	data, err := json.Marshal(sink.Postings[0])
	require.NoError(t, err)
	require.Contains(t, string(data), `"color":""`,
		"uncolored postings must surface color:\"\" in sink JSON, not be omitted")
}

func TestClickHouseCreateTableDDL(t *testing.T) {
	t.Parallel()

	ddl := ClickHouseCreateTableDDL("test_events")
	require.Contains(t, ddl, "CREATE TABLE IF NOT EXISTS test_events")
	require.Contains(t, ddl, "log_sequence UInt64")
	require.Contains(t, ddl, "ReplacingMergeTree()")
	// color is part of the posting sub-schema so warehouse queries can
	// reconstruct the segregated buckets — see sinkPosting.MarshalJSON.
	require.Contains(t, ddl, "color String",
		"ClickHouse posting columns must include color to match the sink JSON shape")
}

func TestSinkTime_MarshalJSON(t *testing.T) {
	t.Parallel()

	// 2023-11-14 22:13:20 UTC (timestamp 1700000000)
	ts := &ledgerpb.Timestamp{Data: 1700000000}
	goTime := ts.AsTime()
	ct := sinkTime(goTime)

	data, err := ct.MarshalJSON()
	require.NoError(t, err)
	require.NotEmpty(t, data)
	// Should be quoted string in sink datetime format
	require.Contains(t, string(data), "\"")
}
