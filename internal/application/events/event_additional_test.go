package events

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
)

func TestLogToEvent_SchemaOperations(t *testing.T) {
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
		{
			name: "convert_metadata_batch",
			payload: &commonpb.LedgerLogPayload{
				Payload: &commonpb.LedgerLogPayload_ConvertMetadataBatch{
					ConvertMetadataBatch: &commonpb.ConvertMetadataBatchLog{
						TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
						Key:        "age",
						Count:      10,
					},
				},
			},
		},
		{
			name: "metadata_conversion_complete",
			payload: &commonpb.LedgerLogPayload{
				Payload: &commonpb.LedgerLogPayload_MetadataConversionComplete{
					MetadataConversionComplete: &commonpb.MetadataConversionCompleteLog{
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

			log := &commonpb.Log{
				Sequence: 100,
				Payload: &commonpb.LogPayload{
					Type: &commonpb.LogPayload_Apply{
						Apply: &commonpb.ApplyLedgerLog{
							LedgerName: "test-ledger",
							Log: &commonpb.LedgerLog{
								Id:   10,
								Date: &commonpb.Timestamp{Data: 1700000000},
								Data: tc.payload,
							},
						},
					},
				},
			}

			event := LogToEvent(log)

			// Schema operations produce EVENT_TYPE_UNSPECIFIED
			require.Equal(t, commonpb.EventType_EVENT_TYPE_UNSPECIFIED, event.GetType())
			require.Equal(t, "test-ledger", event.GetLedger())
			require.Equal(t, uint64(100), event.GetLogSequence())
		})
	}
}

func TestLogToEvent_RegisterSigningKey(t *testing.T) {
	t.Parallel()

	log := &commonpb.Log{
		Sequence: 200,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_RegisterSigningKey{
				RegisterSigningKey: &commonpb.RegisterSigningKeyLog{
					KeyId:     "key-001",
					PublicKey: []byte{0xab, 0xcd},
				},
			},
		},
	}

	event := LogToEvent(log)

	// Signing key operations don't match any Apply sub-case,
	// they fall through to the top-level switch without matching Apply.
	require.Equal(t, commonpb.EventType_EVENT_TYPE_UNSPECIFIED, event.GetType())
}

func TestLogToEvent_AddedEventsSink(t *testing.T) {
	t.Parallel()

	log := &commonpb.Log{
		Sequence: 201,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_AddedEventsSink{
				AddedEventsSink: &commonpb.AddedEventsSinkLog{
					Config: &commonpb.SinkConfig{Name: "my-sink"},
				},
			},
		},
	}

	event := LogToEvent(log)
	require.Equal(t, commonpb.EventType_EVENT_TYPE_UNSPECIFIED, event.GetType())
}
