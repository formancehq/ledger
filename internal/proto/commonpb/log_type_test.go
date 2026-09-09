package commonpb_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// TestLogType_OrderSkippedRoundTrip pins the REST log-type integration for
// OrderSkippedLog. Without OrderSkippedLogType wired through GetLogType /
// LogTypeFromString / HydrateLog / LedgerLog.MarshalJSON, the REST log APIs
// would emit `{"type":""}` for any skip and the client could not hydrate
// the payload back through HydrateLog.
func TestLogType_OrderSkippedRoundTrip(t *testing.T) {
	t.Parallel()

	require.Equal(t, "ORDER_SKIPPED", commonpb.OrderSkippedLogType.String())

	got, err := commonpb.LogTypeFromString("ORDER_SKIPPED")
	require.NoError(t, err)
	require.Equal(t, commonpb.OrderSkippedLogType, got)
}

func TestGetLogType_OrderSkipped(t *testing.T) {
	t.Parallel()

	payload := &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_OrderSkipped{
			OrderSkipped: &commonpb.OrderSkippedLog{
				Reason: commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT,
			},
		},
	}

	require.Equal(t, commonpb.OrderSkippedLogType, commonpb.GetLogType(payload))
}

func TestLedgerLog_MarshalJSON_OrderSkipped(t *testing.T) {
	t.Parallel()

	log := &commonpb.LedgerLog{
		Id: 7,
		Data: &commonpb.LedgerLogPayload{
			Payload: &commonpb.LedgerLogPayload_OrderSkipped{
				OrderSkipped: &commonpb.OrderSkippedLog{
					Reason: commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT,
					Context: map[string]string{
						"reference":             "ref-x",
						"existingTransactionId": "42",
					},
				},
			},
		},
	}

	encoded, err := json.Marshal(log)
	require.NoError(t, err)

	var round map[string]any
	require.NoError(t, json.Unmarshal(encoded, &round))
	require.Equal(t, "ORDER_SKIPPED", round["type"])
}

// TestLedgerLog_OrderSkippedRoundTrip ensures the JSON round trip:
// marshalling a LedgerLog with an OrderSkipped payload and decoding it
// back produces the same payload (LogType drives HydrateLog through the
// new "ORDER_SKIPPED" branch).
func TestLedgerLog_OrderSkippedRoundTrip(t *testing.T) {
	t.Parallel()

	original := &commonpb.LedgerLog{
		Id: 7,
		Data: &commonpb.LedgerLogPayload{
			Payload: &commonpb.LedgerLogPayload_OrderSkipped{
				OrderSkipped: &commonpb.OrderSkippedLog{
					Reason: commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT,
					Context: map[string]string{
						"reference":             "ref-x",
						"existingTransactionId": "42",
					},
				},
			},
		},
	}

	encoded, err := json.Marshal(original)
	require.NoError(t, err)

	var round commonpb.LedgerLog
	require.NoError(t, json.Unmarshal(encoded, &round))

	skipped := round.GetData().GetOrderSkipped()
	require.NotNil(t, skipped, "round-trip must produce an OrderSkipped payload (HydrateLog wired)")
	require.Equal(t, commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT, skipped.GetReason())
	require.Equal(t, "ref-x", skipped.GetContext()["reference"])
	require.Equal(t, "42", skipped.GetContext()["existingTransactionId"])
}

// Every wire discriminator is pinned independently from the encoder/decoder
// mapping, so a mutually consistent rename or missing variant still fails.
func TestLogTypeJSONNames(t *testing.T) {
	t.Parallel()
	for kind, name := range map[commonpb.LogType]string{
		commonpb.SetMetadataLogType:                   "SET_METADATA",
		commonpb.NewTransactionLogType:                "NEW_TRANSACTION",
		commonpb.RevertedTransactionLogType:           "REVERTED_TRANSACTION",
		commonpb.DeleteMetadataLogType:                "DELETE_METADATA",
		commonpb.SetMetadataFieldTypeLogType:          "SET_METADATA_FIELD_TYPE",
		commonpb.RemovedMetadataFieldTypeLogType:      "REMOVED_METADATA_FIELD_TYPE",
		commonpb.OrderSkippedLogType:                  "ORDER_SKIPPED",
		commonpb.FillGapLogType:                       "FILL_GAP",
		commonpb.CreateIndexLogType:                   "CREATE_INDEX",
		commonpb.DropIndexLogType:                     "DROP_INDEX",
		commonpb.AddedAccountTypeLogType:              "ADDED_ACCOUNT_TYPE",
		commonpb.RemovedAccountTypeLogType:            "REMOVED_ACCOUNT_TYPE",
		commonpb.UpdatedDefaultEnforcementModeLogType: "UPDATED_DEFAULT_ENFORCEMENT_MODE",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, name, kind.String())
			decoded, err := commonpb.LogTypeFromString(name)
			require.NoError(t, err)
			require.Equal(t, kind, decoded)
		})
	}
}

func TestLedgerLogJSONRejectsMissingPayload(t *testing.T) {
	t.Parallel()
	for name, payload := range map[string]*commonpb.LedgerLogPayload{
		"nil":                  nil,
		"empty":                {},
		"nil selected message": {Payload: &commonpb.LedgerLogPayload_CreatedTransaction{CreatedTransaction: nil}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := json.Marshal(&commonpb.LedgerLog{Data: payload})
			require.ErrorContains(t, err, "missing log payload")
		})
	}
}
