package events

import (
	"encoding/json"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"

	"github.com/formancehq/ledger/v3/internal/proto/eventspb"
	"github.com/formancehq/ledger/v3/internal/protohelpers"
)

func TestLogToEvent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		log          *ledgerpb.Log
		expectedType ledgerpb.EventType
		expectedName string
	}{
		{
			name: "CREATED_LEDGER",
			log: &ledgerpb.Log{
				Sequence: 1,
				Payload: &ledgerpb.LogPayload{
					Type: &ledgerpb.LogPayload_CreateLedger{
						CreateLedger: &ledgerpb.CreatedLedgerLog{
							Name:      "orders",
							CreatedAt: &ledgerpb.Timestamp{Data: 1000},
						},
					},
				},
			},
			expectedType: ledgerpb.EventType_CREATED_LEDGER,
			expectedName: "orders",
		},
		{
			name: "DELETED_LEDGER",
			log: &ledgerpb.Log{
				Sequence: 2,
				Payload: &ledgerpb.LogPayload{
					Type: &ledgerpb.LogPayload_DeleteLedger{
						DeleteLedger: &ledgerpb.DeletedLedgerLog{
							Name:      "orders",
							DeletedAt: &ledgerpb.Timestamp{Data: 2000},
						},
					},
				},
			},
			expectedType: ledgerpb.EventType_DELETED_LEDGER,
			expectedName: "orders",
		},
		{
			name: "COMMITTED_TRANSACTION",
			log: &ledgerpb.Log{
				Sequence: 3,
				Payload: &ledgerpb.LogPayload{
					Type: &ledgerpb.LogPayload_Apply{
						Apply: &ledgerpb.ApplyLedgerLog{
							LedgerName: "payments",
							Log: &ledgerpb.LedgerLog{
								Date: &ledgerpb.Timestamp{Data: 3000},
								Id:   1,
								Data: &ledgerpb.LedgerLogPayload{
									Payload: &ledgerpb.LedgerLogPayload_CreatedTransaction{
										CreatedTransaction: &ledgerpb.CreatedTransaction{
											Transaction: &ledgerpb.Transaction{Id: 1},
										},
									},
								},
							},
						},
					},
				},
			},
			expectedType: ledgerpb.EventType_COMMITTED_TRANSACTION,
			expectedName: "payments",
		},
		{
			name: "REVERTED_TRANSACTION",
			log: &ledgerpb.Log{
				Sequence: 4,
				Payload: &ledgerpb.LogPayload{
					Type: &ledgerpb.LogPayload_Apply{
						Apply: &ledgerpb.ApplyLedgerLog{
							LedgerName: "payments",
							Log: &ledgerpb.LedgerLog{
								Date: &ledgerpb.Timestamp{Data: 4000},
								Id:   2,
								Data: &ledgerpb.LedgerLogPayload{
									Payload: &ledgerpb.LedgerLogPayload_RevertedTransaction{
										RevertedTransaction: &ledgerpb.RevertedTransaction{
											RevertedTransactionId: 1,
											RevertTransaction:     &ledgerpb.Transaction{Id: 2},
										},
									},
								},
							},
						},
					},
				},
			},
			expectedType: ledgerpb.EventType_REVERTED_TRANSACTION,
			expectedName: "payments",
		},
		{
			name: "SAVED_METADATA",
			log: &ledgerpb.Log{
				Sequence: 5,
				Payload: &ledgerpb.LogPayload{
					Type: &ledgerpb.LogPayload_Apply{
						Apply: &ledgerpb.ApplyLedgerLog{
							LedgerName: "orders",
							Log: &ledgerpb.LedgerLog{
								Date: &ledgerpb.Timestamp{Data: 5000},
								Id:   3,
								Data: &ledgerpb.LedgerLogPayload{
									Payload: &ledgerpb.LedgerLogPayload_SavedMetadata{
										SavedMetadata: &ledgerpb.SavedMetadata{
											Target: &ledgerpb.Target{
												Target: &ledgerpb.Target_Account{
													Account: &ledgerpb.TargetAccount{Addr: "user:123"},
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
			expectedType: ledgerpb.EventType_SAVED_METADATA,
			expectedName: "orders",
		},
		{
			name: "DELETED_METADATA",
			log: &ledgerpb.Log{
				Sequence: 6,
				Payload: &ledgerpb.LogPayload{
					Type: &ledgerpb.LogPayload_Apply{
						Apply: &ledgerpb.ApplyLedgerLog{
							LedgerName: "orders",
							Log: &ledgerpb.LedgerLog{
								Date: &ledgerpb.Timestamp{Data: 6000},
								Id:   4,
								Data: &ledgerpb.LedgerLogPayload{
									Payload: &ledgerpb.LedgerLogPayload_DeletedMetadata{
										DeletedMetadata: &ledgerpb.DeletedMetadata{
											Target: &ledgerpb.Target{
												Target: &ledgerpb.Target_Account{
													Account: &ledgerpb.TargetAccount{Addr: "user:123"},
												},
											},
											Key: "status",
										},
									},
								},
							},
						},
					},
				},
			},
			expectedType: ledgerpb.EventType_DELETED_METADATA,
			expectedName: "orders",
		},
		{
			name: "SKIPPED_ORDER",
			log: &ledgerpb.Log{
				Sequence: 7,
				Payload: &ledgerpb.LogPayload{
					Type: &ledgerpb.LogPayload_Apply{
						Apply: &ledgerpb.ApplyLedgerLog{
							LedgerName: "orders",
							Log: &ledgerpb.LedgerLog{
								Date: &ledgerpb.Timestamp{Data: 7000},
								Id:   5,
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
			expectedType: ledgerpb.EventType_SKIPPED_ORDER,
			expectedName: "orders",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			event := LogToEvent(tc.log)

			require.Equal(t, EventApp, event.GetApp())
			require.Equal(t, EventVersion, event.GetVersion())
			require.Equal(t, tc.expectedType, event.GetType())
			require.Equal(t, tc.expectedName, event.GetLedger())
			require.Equal(t, tc.log.GetSequence(), event.GetLogSequence())
			require.Equal(t, tc.log, event.GetLog())
		})
	}
}

func TestSerializeEvent_JSON(t *testing.T) {
	t.Parallel()

	event := &eventspb.Event{
		App:         EventApp,
		Version:     EventVersion,
		Type:        ledgerpb.EventType_COMMITTED_TRANSACTION,
		Ledger:      "orders",
		LogSequence: 42,
		Date:        &ledgerpb.Timestamp{Data: 1000},
	}

	data, err := SerializeEvent(event, FormatJSON)
	require.NoError(t, err)
	require.NotEmpty(t, data)

	decoded := map[string]any{}
	require.NoError(t, json.Unmarshal(data, &decoded))
	require.Equal(t, EventApp, decoded["app"])
	require.Equal(t, EventVersion, decoded["version"])
	require.Equal(t, "COMMITTED_TRANSACTION", decoded["type"])
	require.Equal(t, "orders", decoded["ledger"])
}

func TestSerializeEvent_JSONLedgerLogOutput(t *testing.T) {
	t.Parallel()

	wantLog := &ledgerpb.LedgerLog{
		Id:   7,
		Date: &ledgerpb.Timestamp{Data: 1_700_000_000_000_000},
		Data: &ledgerpb.LedgerLogPayload{Payload: &ledgerpb.LedgerLogPayload_CreatedTransaction{
			CreatedTransaction: &ledgerpb.CreatedTransaction{
				Transaction: &ledgerpb.Transaction{
					Id:        9,
					Reference: "order-456",
					Postings: []*ledgerpb.Posting{
						protohelpers.NewColoredPosting("world", "alice", "USD/2", "pending", big.NewInt(1000)),
					},
					Metadata: map[string]*ledgerpb.MetadataValue{"note": ledgerpb.NewStringValue("checkout")},
				},
				AccountMetadata: map[string]*ledgerpb.MetadataMap{
					"alice": {Values: map[string]*ledgerpb.MetadataValue{"tier": ledgerpb.NewStringValue("gold")}},
				},
			},
		}},
	}
	event := LogToEvent(&ledgerpb.Log{
		Sequence: 42,
		Payload: &ledgerpb.LogPayload{Type: &ledgerpb.LogPayload_Apply{
			Apply: &ledgerpb.ApplyLedgerLog{LedgerName: "orders", Log: wantLog},
		}},
	})

	data, err := SerializeEvent(event, FormatJSON)
	require.NoError(t, err)
	require.NotContains(t, string(data), `"createdTransaction":`)
	require.Contains(t, string(data), `"data":{"transaction":`)
	require.Contains(t, string(data), `"type":"NEW_TRANSACTION"`)
	require.Contains(t, string(data), `"logSequence":42`)
	var response struct {
		Log struct {
			Payload struct {
				Apply struct {
					Log json.RawMessage `json:"log"`
				} `json:"apply"`
			} `json:"payload"`
		} `json:"log"`
	}
	require.NoError(t, json.Unmarshal(data, &response))
	require.JSONEq(t, `{
		"id":7,"date":"2023-11-14T22:13:20Z","type":"NEW_TRANSACTION",
		"data":{
			"transaction":{"id":9,"reference":"order-456","reverted":false,
				"postings":[{"source":"world","destination":"alice","asset":"USD/2","color":"pending","amount":1000}],
				"metadata":{"note":"checkout"}},
			"accountMetadata":{"alice":{"tier":"gold"}}
		}
	}`, string(response.Log.Payload.Apply.Log))
}

func TestSerializeEvent_Proto(t *testing.T) {
	t.Parallel()

	event := &eventspb.Event{
		App:         EventApp,
		Version:     EventVersion,
		Type:        ledgerpb.EventType_COMMITTED_TRANSACTION,
		Ledger:      "orders",
		LogSequence: 42,
		Date:        &ledgerpb.Timestamp{Data: 1000},
	}

	data, err := SerializeEvent(event, FormatProto)
	require.NoError(t, err)
	require.NotEmpty(t, data)

	// Verify we can unmarshal back
	decoded := &eventspb.Event{}
	require.NoError(t, decoded.UnmarshalVT(data))
	require.Equal(t, event.GetApp(), decoded.GetApp())
	require.Equal(t, event.GetVersion(), decoded.GetVersion())
	require.Equal(t, event.GetType(), decoded.GetType())
	require.Equal(t, event.GetLedger(), decoded.GetLedger())
	require.Equal(t, event.GetLogSequence(), decoded.GetLogSequence())
}

func TestSerializeEvent_UnsupportedFormat(t *testing.T) {
	t.Parallel()

	event := &eventspb.Event{Type: ledgerpb.EventType_COMMITTED_TRANSACTION}

	_, err := SerializeEvent(event, Format("xml"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported event format")
}
