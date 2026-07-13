package events

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/eventspb"
)

func TestLogToEvent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		log          *commonpb.Log
		expectedType commonpb.EventType
		expectedName string
	}{
		{
			name: "CREATED_LEDGER",
			log: &commonpb.Log{
				Sequence: 1,
				Payload: &commonpb.LogPayload{
					Type: &commonpb.LogPayload_CreateLedger{
						CreateLedger: &commonpb.CreatedLedgerLog{
							Name:      "orders",
							CreatedAt: &commonpb.Timestamp{Data: 1000},
						},
					},
				},
			},
			expectedType: commonpb.EventType_CREATED_LEDGER,
			expectedName: "orders",
		},
		{
			name: "DELETED_LEDGER",
			log: &commonpb.Log{
				Sequence: 2,
				Payload: &commonpb.LogPayload{
					Type: &commonpb.LogPayload_DeleteLedger{
						DeleteLedger: &commonpb.DeletedLedgerLog{
							Name:      "orders",
							DeletedAt: &commonpb.Timestamp{Data: 2000},
						},
					},
				},
			},
			expectedType: commonpb.EventType_DELETED_LEDGER,
			expectedName: "orders",
		},
		{
			name: "COMMITTED_TRANSACTION",
			log: &commonpb.Log{
				Sequence: 3,
				Payload: &commonpb.LogPayload{
					Type: &commonpb.LogPayload_Apply{
						Apply: &commonpb.ApplyLedgerLog{
							LedgerName: "payments",
							Log: &commonpb.LedgerLog{
								Date: &commonpb.Timestamp{Data: 3000},
								Id:   1,
								Data: &commonpb.LedgerLogPayload{
									Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
										CreatedTransaction: &commonpb.CreatedTransaction{
											Transaction: &commonpb.Transaction{Id: 1},
										},
									},
								},
							},
						},
					},
				},
			},
			expectedType: commonpb.EventType_COMMITTED_TRANSACTION,
			expectedName: "payments",
		},
		{
			name: "REVERTED_TRANSACTION",
			log: &commonpb.Log{
				Sequence: 4,
				Payload: &commonpb.LogPayload{
					Type: &commonpb.LogPayload_Apply{
						Apply: &commonpb.ApplyLedgerLog{
							LedgerName: "payments",
							Log: &commonpb.LedgerLog{
								Date: &commonpb.Timestamp{Data: 4000},
								Id:   2,
								Data: &commonpb.LedgerLogPayload{
									Payload: &commonpb.LedgerLogPayload_RevertedTransaction{
										RevertedTransaction: &commonpb.RevertedTransaction{
											RevertedTransactionId: 1,
											RevertTransaction:     &commonpb.Transaction{Id: 2},
										},
									},
								},
							},
						},
					},
				},
			},
			expectedType: commonpb.EventType_REVERTED_TRANSACTION,
			expectedName: "payments",
		},
		{
			name: "SAVED_METADATA",
			log: &commonpb.Log{
				Sequence: 5,
				Payload: &commonpb.LogPayload{
					Type: &commonpb.LogPayload_Apply{
						Apply: &commonpb.ApplyLedgerLog{
							LedgerName: "orders",
							Log: &commonpb.LedgerLog{
								Date: &commonpb.Timestamp{Data: 5000},
								Id:   3,
								Data: &commonpb.LedgerLogPayload{
									Payload: &commonpb.LedgerLogPayload_SavedMetadata{
										SavedMetadata: &commonpb.SavedMetadata{
											Target: &commonpb.Target{
												Target: &commonpb.Target_Account{
													Account: &commonpb.TargetAccount{Addr: "user:123"},
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
			expectedType: commonpb.EventType_SAVED_METADATA,
			expectedName: "orders",
		},
		{
			name: "DELETED_METADATA",
			log: &commonpb.Log{
				Sequence: 6,
				Payload: &commonpb.LogPayload{
					Type: &commonpb.LogPayload_Apply{
						Apply: &commonpb.ApplyLedgerLog{
							LedgerName: "orders",
							Log: &commonpb.LedgerLog{
								Date: &commonpb.Timestamp{Data: 6000},
								Id:   4,
								Data: &commonpb.LedgerLogPayload{
									Payload: &commonpb.LedgerLogPayload_DeletedMetadata{
										DeletedMetadata: &commonpb.DeletedMetadata{
											Target: &commonpb.Target{
												Target: &commonpb.Target_Account{
													Account: &commonpb.TargetAccount{Addr: "user:123"},
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
			expectedType: commonpb.EventType_DELETED_METADATA,
			expectedName: "orders",
		},
		{
			name: "SKIPPED_ORDER",
			log: &commonpb.Log{
				Sequence: 7,
				Payload: &commonpb.LogPayload{
					Type: &commonpb.LogPayload_Apply{
						Apply: &commonpb.ApplyLedgerLog{
							LedgerName: "orders",
							Log: &commonpb.LedgerLog{
								Date: &commonpb.Timestamp{Data: 7000},
								Id:   5,
								Data: &commonpb.LedgerLogPayload{
									Payload: &commonpb.LedgerLogPayload_OrderSkipped{
										OrderSkipped: &commonpb.OrderSkippedLog{
											Reason:  commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT,
											Context: map[string]string{"reference": "ref-1"},
										},
									},
								},
							},
						},
					},
				},
			},
			expectedType: commonpb.EventType_SKIPPED_ORDER,
			expectedName: "orders",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			event := LogToEvent(tc.log)

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
		Type:        commonpb.EventType_COMMITTED_TRANSACTION,
		Ledger:      "orders",
		LogSequence: 42,
		Date:        &commonpb.Timestamp{Data: 1000},
	}

	data, err := SerializeEvent(event, FormatJSON)
	require.NoError(t, err)
	require.NotEmpty(t, data)
	require.Contains(t, string(data), "COMMITTED_TRANSACTION")
	require.Contains(t, string(data), "orders")
}

func TestSerializeEvent_Proto(t *testing.T) {
	t.Parallel()

	event := &eventspb.Event{
		Type:        commonpb.EventType_COMMITTED_TRANSACTION,
		Ledger:      "orders",
		LogSequence: 42,
		Date:        &commonpb.Timestamp{Data: 1000},
	}

	data, err := SerializeEvent(event, FormatProto)
	require.NoError(t, err)
	require.NotEmpty(t, data)

	// Verify we can unmarshal back
	decoded := &eventspb.Event{}
	require.NoError(t, decoded.UnmarshalVT(data))
	require.Equal(t, event.GetType(), decoded.GetType())
	require.Equal(t, event.GetLedger(), decoded.GetLedger())
	require.Equal(t, event.GetLogSequence(), decoded.GetLogSequence())
}

func TestSerializeEvent_UnsupportedFormat(t *testing.T) {
	t.Parallel()

	event := &eventspb.Event{Type: commonpb.EventType_COMMITTED_TRANSACTION}

	_, err := SerializeEvent(event, Format("xml"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported event format")
}
