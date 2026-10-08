package grpc_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
)

// TestLogType_OrderSkippedJSONName pins the public discriminator spelling.
func TestLogType_OrderSkippedJSONName(t *testing.T) {
	t.Parallel()

	require.Equal(t, "ORDER_SKIPPED", ledgerpb.OrderSkippedLogType.String())

	got, err := ledgerpb.LogTypeFromString("ORDER_SKIPPED")
	require.NoError(t, err)
	require.Equal(t, ledgerpb.OrderSkippedLogType, got)
}

func TestGetLogType_OrderSkipped(t *testing.T) {
	t.Parallel()

	payload := &ledgerpb.LedgerLogPayload{
		Payload: &ledgerpb.LedgerLogPayload_OrderSkipped{
			OrderSkipped: &ledgerpb.OrderSkippedLog{
				Reason: ledgerpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT,
			},
		},
	}

	require.Equal(t, ledgerpb.OrderSkippedLogType, ledgerpb.GetLogType(payload))
}

func TestLedgerLog_MarshalJSON_OrderSkipped(t *testing.T) {
	t.Parallel()

	log := &ledgerpb.LedgerLog{
		Id: 7,
		Data: &ledgerpb.LedgerLogPayload{
			Payload: &ledgerpb.LedgerLogPayload_OrderSkipped{
				OrderSkipped: &ledgerpb.OrderSkippedLog{
					Reason: ledgerpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT,
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

// Every wire discriminator is pinned independently from the encoder/decoder
// mapping, so a mutually consistent rename or missing variant still fails.
func TestLogTypeJSONNames(t *testing.T) {
	t.Parallel()
	for kind, name := range map[ledgerpb.LogType]string{
		ledgerpb.SetMetadataLogType:                   "SET_METADATA",
		ledgerpb.NewTransactionLogType:                "NEW_TRANSACTION",
		ledgerpb.RevertedTransactionLogType:           "REVERTED_TRANSACTION",
		ledgerpb.DeleteMetadataLogType:                "DELETE_METADATA",
		ledgerpb.SetMetadataFieldTypeLogType:          "SET_METADATA_FIELD_TYPE",
		ledgerpb.RemovedMetadataFieldTypeLogType:      "REMOVED_METADATA_FIELD_TYPE",
		ledgerpb.OrderSkippedLogType:                  "ORDER_SKIPPED",
		ledgerpb.FillGapLogType:                       "FILL_GAP",
		ledgerpb.CreateIndexLogType:                   "CREATE_INDEX",
		ledgerpb.DropIndexLogType:                     "DROP_INDEX",
		ledgerpb.AddedAccountTypeLogType:              "ADDED_ACCOUNT_TYPE",
		ledgerpb.RemovedAccountTypeLogType:            "REMOVED_ACCOUNT_TYPE",
		ledgerpb.UpdatedDefaultEnforcementModeLogType: "UPDATED_DEFAULT_ENFORCEMENT_MODE",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, name, kind.String())
			decoded, err := ledgerpb.LogTypeFromString(name)
			require.NoError(t, err)
			require.Equal(t, kind, decoded)
		})
	}
}

func TestLedgerLogJSONRejectsMissingPayload(t *testing.T) {
	t.Parallel()
	for name, payload := range map[string]*ledgerpb.LedgerLogPayload{
		"nil":                  nil,
		"empty":                {},
		"nil selected message": {Payload: &ledgerpb.LedgerLogPayload_CreatedTransaction{CreatedTransaction: nil}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := json.Marshal(&ledgerpb.LedgerLog{Data: payload})
			require.ErrorContains(t, err, "missing log payload")
		})
	}
}
