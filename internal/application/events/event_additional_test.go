package events

import (
	"testing"

	"github.com/stretchr/testify/require"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
)

func TestLogToEvent_SchemaOperations(t *testing.T) {
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

			log := &ledgerpb.Log{
				Sequence: 100,
				Payload: &ledgerpb.LogPayload{
					Type: &ledgerpb.LogPayload_Apply{
						Apply: &ledgerpb.ApplyLedgerLog{
							LedgerName: "test-ledger",
							Log: &ledgerpb.LedgerLog{
								Id:   10,
								Date: &ledgerpb.Timestamp{Data: 1700000000},
								Data: tc.payload,
							},
						},
					},
				},
			}

			event := LogToEvent(log)

			// Schema operations produce EVENT_TYPE_UNSPECIFIED
			require.Equal(t, ledgerpb.EventType_EVENT_TYPE_UNSPECIFIED, event.GetType())
			require.Equal(t, "test-ledger", event.GetLedger())
			require.Equal(t, uint64(100), event.GetLogSequence())
		})
	}
}

func TestLogToEvent_RegisterSigningKey(t *testing.T) {
	t.Parallel()

	log := &ledgerpb.Log{
		Sequence: 200,
		Payload: &ledgerpb.LogPayload{
			Type: &ledgerpb.LogPayload_RegisterSigningKey{
				RegisterSigningKey: &ledgerpb.RegisteredSigningKeyLog{
					KeyId:     "key-001",
					PublicKey: []byte{0xab, 0xcd},
				},
			},
		},
	}

	event := LogToEvent(log)

	// Signing key operations don't match any Apply sub-case,
	// they fall through to the top-level switch without matching Apply.
	require.Equal(t, ledgerpb.EventType_EVENT_TYPE_UNSPECIFIED, event.GetType())
}

func TestLogToEvent_AddedEventsSink(t *testing.T) {
	t.Parallel()

	log := &ledgerpb.Log{
		Sequence: 201,
		Payload: &ledgerpb.LogPayload{
			Type: &ledgerpb.LogPayload_AddedEventsSink{
				AddedEventsSink: &ledgerpb.AddedEventsSinkLog{
					Config: &ledgerpb.SinkConfig{Name: "my-sink"},
				},
			},
		},
	}

	event := LogToEvent(log)
	require.Equal(t, ledgerpb.EventType_EVENT_TYPE_UNSPECIFIED, event.GetType())
}
