package grpc_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	commonpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
)

// TestLogType_OrderSkippedJSONName pins the public discriminator spelling.
func TestLogType_OrderSkippedJSONName(t *testing.T) {
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
			OrderSkipped: &commonpb.OrderSkippedLog{},
		},
	}

	require.Equal(t, commonpb.OrderSkippedLogType, commonpb.GetLogType(payload))
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
